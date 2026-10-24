package kafka

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type ResetRequest struct {
	Topic string `json:"topic"`
	// Plan applies exactly a preview's targets, refusing changed commits or
	// targets no longer in the retained range instead of recalculating them.
	Plan []ResetPlan `json:"plan"`
	// Partitions limits the reset; empty means every partition of the topic.
	Partitions []int32 `json:"partitions"`
	// Strategy is earliest, latest, offset, timestamp, shift or offsets.
	Strategy  string `json:"strategy"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	// Shift moves each partition relative to its committed offset (negative
	// re-reads, positive skips); partitions with no commit shift from the
	// low watermark.
	Shift int64 `json:"shift"`
	// Offsets gives each partition its own target, for the offsets strategy
	// (restoring a backup, copying another group). Only these partitions
	// move; a target outside a partition's range is clamped to it.
	Offsets []PartitionOffset `json:"offsets"`
	// DryRun returns the planned offsets without committing.
	DryRun bool `json:"dry_run"`
}

type ResetPlan struct {
	Partition int32 `json:"partition"`
	// From is the committed offset before the reset, -1 when none.
	From int64 `json:"from"`
	To   int64 `json:"to"`
	Low  int64 `json:"low"`
	High int64 `json:"high"`
}

func (c *Client) ResetOffsets(ctx context.Context, group string, req ResetRequest) ([]ResetPlan, error) {
	if !req.DryRun {
		if err := c.requireWritable(); err != nil {
			return nil, err
		}
	}

	reviewed := map[int32]ResetPlan{}
	if req.Plan != nil {
		if len(req.Plan) == 0 {
			return nil, ErrInvalid{errors.New("review at least one partition before applying offsets")}
		}
		req.Strategy, req.Partitions, req.Offsets = "offsets", nil, nil
		for _, p := range req.Plan {
			if _, duplicate := reviewed[p.Partition]; duplicate || p.Partition < 0 || p.From < -1 ||
				p.Low < 0 || p.High < p.Low || p.To < p.Low || p.To > p.High {
				return nil, ErrInvalid{fmt.Errorf("invalid reviewed offset for partition %d", p.Partition)}
			}
			reviewed[p.Partition] = p
			req.Offsets = append(req.Offsets, PartitionOffset{Partition: p.Partition, Offset: p.To})
		}
	}

	if err := c.idleGroup(ctx, group, !req.DryRun); err != nil {
		return nil, err
	}

	switch req.Strategy {
	case "earliest", "latest", "offset", "offsets", "timestamp", "shift":
	default:
		return nil, ErrInvalid{fmt.Errorf("unknown strategy %q", req.Strategy)}
	}

	var at *int64
	if req.Strategy == "timestamp" {
		at = &req.Timestamp
	}

	b, err := c.bounds(ctx, req.Topic, false, at, nil)
	if err != nil {
		return nil, err
	}

	// Without the committed offsets "from" is unknown and a shift would
	// move from the start of the log instead; never guess on a reset.
	current, err := c.admin.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch committed offsets; %w", err)
	}

	for _, p := range req.Partitions {
		if p < 0 {
			return nil, ErrInvalid{fmt.Errorf("partition %d is not valid", p)}
		}
	}

	wanted := partitionSet(req.Partitions)

	// For the offsets strategy the list itself names the partitions.
	targets := map[int32]int64{}
	if req.Strategy == "offsets" {
		if len(req.Offsets) == 0 {
			return nil, ErrInvalid{errors.New("give at least one partition offset")}
		}

		wanted = map[int32]bool{}

		for _, po := range req.Offsets {
			switch {
			case po.Partition < 0:
				return nil, ErrInvalid{fmt.Errorf("partition %d is not valid", po.Partition)}
			case po.Offset < 0:
				return nil, ErrInvalid{fmt.Errorf("partition %d: offset must not be negative", po.Partition)}
			case wanted[po.Partition]:
				return nil, ErrInvalid{fmt.Errorf("partition %d is listed twice", po.Partition)}
			}

			wanted[po.Partition] = true
			targets[po.Partition] = po.Offset
		}
	}

	if req.Strategy == "offset" && req.Offset < 0 {
		return nil, ErrInvalid{fmt.Errorf("offset must not be negative")}
	}

	// A partition whose bounds could not be read (a leader moving, say)
	// fails the reset: committing -1 or skipping it silently would both
	// leave the group somewhere the user did not ask for.
	var boundErr error

	for p := range wanted {
		if !b.has(p) {
			return nil, noPartition(req.Topic, p)
		}
	}

	parts := slices.DeleteFunc(b.partitions(), func(p int32) bool { return len(wanted) > 0 && !wanted[p] })

	var plan []ResetPlan

	offsets := make(kadm.Offsets)

	for _, p := range parts {
		lo, hi, err := b.partition(p)
		if err != nil {
			boundErr = errors.Join(boundErr, err)

			continue
		}

		from := int64(-1)
		if cur, ok := current.Lookup(req.Topic, p); ok {
			if cur.Err != nil {
				boundErr = errors.Join(boundErr, offsetUnknown(p, "committed offset", cur.Err))

				continue
			}

			from = cur.At
		}

		target, err := resetTarget(req, b, p, lo, hi, from, targets[p])
		if err != nil {
			boundErr = errors.Join(boundErr, err)

			continue
		}

		if r, ok := reviewed[p]; ok {
			if from != r.From || r.To < lo || r.To > hi {
				return nil, ErrConflict{fmt.Errorf("partition %d changed since preview; preview the offsets again", p)}
			}

			target = r.To
		}

		plan = append(plan, ResetPlan{Partition: p, From: from, To: target, Low: lo, High: hi})
		offsets.AddOffset(req.Topic, p, target, -1)
	}

	if boundErr != nil {
		return nil, fmt.Errorf("nothing changed; %w", boundErr)
	}

	if len(plan) == 0 {
		return nil, fmt.Errorf("topic %q; %w", req.Topic, ErrNotFound)
	}

	if req.DryRun {
		return plan, nil
	}

	if err := c.commit(ctx, group, offsets); err != nil {
		return nil, err
	}

	return plan, nil
}

// resetTarget is where strategy moves partition p, whose retained range is
// [lo, hi] and committed offset from (-1 for none); target is its own offset
// for the offsets strategy.
func resetTarget(req ResetRequest, b offsetBounds, p int32, lo, hi, from, target int64) (int64, error) {
	switch req.Strategy {
	case "earliest":
		return lo, nil
	case "latest":
		return hi, nil
	case "offset":
		return min(max(req.Offset, lo), hi), nil
	case "offsets":
		return min(max(target, lo), hi), nil
	case "timestamp":
		t, err := b.at(p)
		if err != nil {
			return 0, err
		}

		if t < 0 {
			return hi, nil
		}

		return min(t, hi), nil
	case "shift":
		base := from
		if base < 0 {
			base = lo
		}

		return min(max(base+req.Shift, lo), hi), nil
	}

	return 0, ErrInvalid{fmt.Errorf("unknown strategy %q", req.Strategy)}
}

// commit commits offsets for group, failing on any partition's error.
func (c *Client) commit(ctx context.Context, group string, offsets kadm.Offsets) error {
	resp, err := c.admin.CommitOffsets(ctx, group, offsets)
	if err != nil {
		return fmt.Errorf("commit offsets; %w", err)
	}

	if err := resp.Error(); err != nil {
		return fmt.Errorf("commit offsets; %w", err)
	}

	return nil
}

type PartitionOffset struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type CreateGroupRequest struct {
	Group   string            `json:"group"`
	Topic   string            `json:"topic"`
	Offsets []PartitionOffset `json:"offsets"`
}

// CreateGroup starts a consumer group by committing its first offsets. Kafka
// has no other way to make a group exist before a consumer joins it. A name
// that already has state is refused, so this can never overwrite a live
// group's position; changing an existing group goes through ResetOffsets.
func (c *Client) CreateGroup(ctx context.Context, req CreateGroupRequest) ([]ResetPlan, error) {
	if err := c.requireWritable(); err != nil {
		return nil, err
	}

	if err := req.validate(); err != nil {
		return nil, err
	}

	listed, err := c.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups; %w", err)
	}

	if _, exists := listed[req.Group]; exists {
		return nil, ErrInvalid{fmt.Errorf("group %q already exists; add the topic from the group page instead", req.Group)}
	}

	return c.commitFirstOffsets(ctx, req)
}

// AddGroupTopic gives an existing group a starting position on a topic it
// does not read yet. Topics it already has offsets for are refused: moving
// those is a reset, and has its own preview.
func (c *Client) AddGroupTopic(ctx context.Context, req CreateGroupRequest) ([]ResetPlan, error) {
	if err := c.requireWritable(); err != nil {
		return nil, err
	}

	if err := req.validate(); err != nil {
		return nil, err
	}

	if err := c.idleGroup(ctx, req.Group, true); err != nil {
		return nil, err
	}

	current, err := c.admin.FetchOffsets(ctx, req.Group)
	if err != nil {
		return nil, fmt.Errorf("fetch offsets; %w", err)
	}

	if _, has := current[req.Topic]; has {
		return nil, ErrInvalid{fmt.Errorf("group %q already has offsets for %q; change them instead", req.Group, req.Topic)}
	}

	return c.commitFirstOffsets(ctx, req)
}

// idleGroup checks that group exists and, with requireIdle, has no members.
// Committing to a group that does not exist would create it; a typo in its
// name must not. New groups go through CreateGroup.
func (c *Client) idleGroup(ctx context.Context, group string, requireIdle bool) error {
	described, err := c.admin.DescribeGroups(ctx, group)
	if err != nil {
		return fmt.Errorf("describe group; %w", err)
	}

	g, ok := described[group]
	if !ok || errors.Is(g.Err, kerr.GroupIDNotFound) || g.Err == nil && g.State == "Dead" {
		return fmt.Errorf("group %q; %w", group, ErrNotFound)
	}

	if !requireIdle {
		return nil
	}

	// A failed describe has no members listed; that is not "idle".
	if g.Err != nil {
		return fmt.Errorf("describe group; %w", g.Err)
	}

	if len(g.Members) > 0 {
		return ErrConflict{fmt.Errorf("group %q has %d active members; stop the consumers first", group, len(g.Members))}
	}

	return nil
}

func (r *CreateGroupRequest) validate() error {
	r.Group = strings.TrimSpace(r.Group)

	switch {
	case r.Group == "":
		return ErrInvalid{errors.New("group name is required")}
	case r.Topic == "":
		return ErrInvalid{errors.New("topic is required")}
	case len(r.Offsets) == 0:
		return ErrInvalid{errors.New("pick at least one partition")}
	}

	return nil
}

// commitFirstOffsets checks every offset against the partition's retained
// range and commits them in one request.
func (c *Client) commitFirstOffsets(ctx context.Context, req CreateGroupRequest) ([]ResetPlan, error) {
	b, err := c.bounds(ctx, req.Topic, false, nil, nil)
	if err != nil {
		return nil, err
	}

	offsets := make(kadm.Offsets)
	plan := make([]ResetPlan, 0, len(req.Offsets))
	seen := map[int32]bool{}

	for _, po := range req.Offsets {
		if seen[po.Partition] {
			return nil, ErrInvalid{fmt.Errorf("partition %d is listed twice", po.Partition)}
		}

		seen[po.Partition] = true

		if !b.has(po.Partition) {
			return nil, noPartition(req.Topic, po.Partition)
		}

		lo, hi, err := b.partition(po.Partition)
		if err != nil {
			return nil, err
		}

		if po.Offset < lo || po.Offset > hi {
			return nil, ErrInvalid{fmt.Errorf("partition %d: offset %d is outside %d–%d", po.Partition, po.Offset, lo, hi)}
		}

		offsets.AddOffset(req.Topic, po.Partition, po.Offset, -1)
		plan = append(plan, ResetPlan{Partition: po.Partition, From: -1, To: po.Offset, Low: lo, High: hi})
	}

	slices.SortFunc(plan, func(a, b ResetPlan) int { return cmp.Compare(a.Partition, b.Partition) })

	if err := c.commit(ctx, req.Group, offsets); err != nil {
		return nil, err
	}

	return plan, nil
}
