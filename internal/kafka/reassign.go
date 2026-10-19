package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// ─── Partition reassignment ───────────────────────────────

// Reassignment is one partition's replica move.
type Reassignment struct {
	Partition int32 `json:"partition"`
	// Replicas is the target broker list; its first broker becomes the
	// preferred leader.
	Replicas []int32 `json:"replicas"`
}

// ReassignRequest moves partition replicas of one topic between brokers.
type ReassignRequest struct {
	Partitions []Reassignment `json:"partitions"`
	// Cancel stops the listed partitions' moves in progress instead.
	Cancel bool `json:"cancel"`
	DryRun bool `json:"dry_run"`
}

type ReassignResult struct {
	Partition int32   `json:"partition"`
	From      []int32 `json:"from"`
	To        []int32 `json:"to"`
	Error     string  `json:"error,omitempty"`
}

// PartitionMove is a reassignment the cluster is still carrying out.
type PartitionMove struct {
	Partition int32   `json:"partition"`
	Replicas  []int32 `json:"replicas"`
	Adding    []int32 `json:"adding"`
	Removing  []int32 `json:"removing"`
}

// Reassign checks and starts (or, with DryRun, only checks) replica moves.
// Each partition keeps its replication factor, every broker must exist and
// a broker may hold a partition once. A move in progress is listed by
// Reassignments until the new replicas have caught up.
func (c *Client) Reassign(ctx context.Context, topic string, req ReassignRequest) ([]ReassignResult, error) {
	if !req.DryRun {
		if err := c.requireWritable(); err != nil {
			return nil, err
		}
	}

	if len(req.Partitions) == 0 {
		return nil, ErrInvalid{errors.New("no partitions given")}
	}

	meta, err := c.admin.Metadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("metadata; %w", err)
	}

	t, ok := meta.Topics[topic]
	if !ok || t.Err != nil {
		return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
	}

	brokers := map[int32]bool{}
	for _, b := range meta.Brokers {
		brokers[b.NodeID] = true
	}

	out := make([]ReassignResult, 0, len(req.Partitions))
	assign := kadm.AlterPartitionAssignmentsReq{}
	seen := map[int32]bool{}

	for _, r := range req.Partitions {
		p, ok := t.Partitions[r.Partition]
		if !ok {
			return nil, ErrInvalid{fmt.Errorf("partition %d does not exist", r.Partition)}
		}

		if seen[r.Partition] {
			return nil, ErrInvalid{fmt.Errorf("partition %d listed twice", r.Partition)}
		}

		seen[r.Partition] = true

		res := ReassignResult{Partition: r.Partition, From: nonNil(p.Replicas), To: nonNil(r.Replicas)}

		if req.Cancel {
			assign.CancelAssign(topic, r.Partition)
			out = append(out, res)

			continue
		}

		if len(r.Replicas) != len(p.Replicas) {
			return nil, ErrInvalid{fmt.Errorf("partition %d: %d replicas given, the topic has replication factor %d; changing it is not supported here",
				r.Partition, len(r.Replicas), len(p.Replicas))}
		}

		dup := map[int32]bool{}
		for _, b := range r.Replicas {
			if !brokers[b] {
				return nil, ErrInvalid{fmt.Errorf("partition %d: broker %d does not exist", r.Partition, b)}
			}

			if dup[b] {
				return nil, ErrInvalid{fmt.Errorf("partition %d: broker %d listed twice", r.Partition, b)}
			}

			dup[b] = true
		}

		if !equalReplicas(p.Replicas, r.Replicas) {
			assign.Assign(topic, r.Partition, r.Replicas)
		}

		out = append(out, res)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })

	if req.DryRun || len(assign) == 0 {
		return out, nil
	}

	// Metadata lags a move in progress, so a new plan made from it could
	// undo or repeat the running one; let it finish (or cancel it) first.
	if !req.Cancel {
		moving, err := c.Reassignments(ctx, topic)
		if err != nil {
			return nil, err
		}

		if len(moving) > 0 {
			return nil, ErrConflict{fmt.Errorf("%d partitions of %q are still moving; wait for them to finish or cancel them first", len(moving), topic)}
		}
	}

	resp, err := c.admin.AlterPartitionAssignments(ctx, assign)
	if err != nil {
		return nil, fmt.Errorf("alter partition assignments; %w", unsupported(err))
	}

	for i := range out {
		if r, ok := resp[topic][out[i].Partition]; ok && r.Err != nil {
			out[i].Error = r.Err.Error()
			if r.ErrMessage != "" {
				out[i].Error += ": " + r.ErrMessage
			}
		}
	}

	return out, nil
}

func equalReplicas(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}

// Reassignments lists the topic's moves in progress.
func (c *Client) Reassignments(ctx context.Context, topic string) ([]PartitionMove, error) {
	meta, err := c.admin.Metadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("metadata; %w", err)
	}

	t, ok := meta.Topics[topic]
	if !ok || t.Err != nil {
		return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
	}

	set := kadm.TopicsSet{}
	for p := range t.Partitions {
		set.Add(topic, p)
	}

	resp, err := c.admin.ListPartitionReassignments(ctx, set)
	if err != nil {
		return nil, fmt.Errorf("list reassignments; %w", unsupported(err))
	}

	out := []PartitionMove{}
	for _, r := range resp.Sorted() {
		if len(r.AddingReplicas) == 0 && len(r.RemovingReplicas) == 0 {
			continue
		}

		out = append(out, PartitionMove{
			Partition: r.Partition, Replicas: nonNil(r.Replicas),
			Adding: nonNil(r.AddingReplicas), Removing: nonNil(r.RemovingReplicas),
		})
	}

	return out, nil
}

// ElectPreferredLeaders moves leadership of the topic's partitions (all when
// none are given) back to each one's first replica, where it is in sync.
func (c *Client) ElectPreferredLeaders(ctx context.Context, topic string, partitions []int32) ([]ReassignResult, error) {
	if err := c.requireWritable(); err != nil {
		return nil, err
	}

	set := kadm.TopicsSet{}
	if len(partitions) == 0 {
		details, err := c.admin.ListTopics(ctx, topic)
		if err != nil {
			return nil, fmt.Errorf("describe topic; %w", err)
		}

		d, ok := details[topic]
		if !ok || d.Err != nil {
			return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
		}

		for p := range d.Partitions {
			set.Add(topic, p)
		}
	} else {
		set.Add(topic, partitions...)
	}

	resp, err := c.admin.ElectLeaders(ctx, kadm.ElectPreferredReplica, set)
	if err != nil {
		return nil, fmt.Errorf("elect leaders; %w", unsupported(err))
	}

	out := []ReassignResult{}
	for _, ps := range resp {
		for p, r := range ps {
			res := ReassignResult{Partition: p, From: []int32{}, To: []int32{}}
			// Already led by the preferred replica: nothing to do, not a failure.
			if r.Err != nil && !errors.Is(r.Err, kerr.ElectionNotNeeded) {
				res.Error = r.Err.Error()
			}

			out = append(out, res)
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Partition < out[j].Partition })

	return out, nil
}

// BalancePlan proposes replica lists that spread the topic's leaders and
// replicas evenly over the brokers, moving as few replicas as it can: each
// partition keeps the brokers it has unless they are over their share.
// Racks are not considered. It only proposes; Reassign carries it out.
func (c *Client) BalancePlan(ctx context.Context, topic string) ([]Reassignment, error) {
	meta, err := c.admin.Metadata(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("metadata; %w", err)
	}

	t, ok := meta.Topics[topic]
	if !ok || t.Err != nil {
		return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
	}

	if moving, err := c.Reassignments(ctx, topic); err == nil && len(moving) > 0 {
		return nil, ErrConflict{fmt.Errorf("%d partitions of %q are still moving; plan again once they finish", len(moving), topic)}
	}

	ids := make([]int32, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		ids = append(ids, b.NodeID)
	}

	current := map[int32][]int32{}
	for p, d := range t.Partitions {
		current[p] = d.Replicas
	}

	return balance(ids, current), nil
}

// balance spreads replicas so no broker holds more than its share (rounded
// up), keeping replicas in place where possible, then picks leaders (first
// replica) so each broker leads about the same number of partitions.
func balance(brokers []int32, current map[int32][]int32) []Reassignment {
	sort.Slice(brokers, func(i, j int) bool { return brokers[i] < brokers[j] })

	parts := make([]int32, 0, len(current))
	total := 0

	for p, rs := range current {
		parts = append(parts, p)
		total += len(rs)
	}

	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })

	if len(brokers) == 0 {
		return nil
	}

	limit := (total + len(brokers) - 1) / len(brokers)
	load := map[int32]int{}
	alive := map[int32]bool{}

	for _, b := range brokers {
		alive[b] = true
	}

	// Keep existing replicas on live brokers, then trim brokers over the
	// limit: from each overloaded broker drop replicas of the partitions
	// that keep the most others, so the drops spread over partitions
	// instead of emptying the first ones.
	next := map[int32][]int32{}

	for _, p := range parts {
		for _, b := range current[p] {
			if alive[b] && !contains(next[p], b) {
				next[p] = append(next[p], b)
				load[b]++
			}
		}
	}

	for _, b := range brokers {
		for load[b] > limit {
			// The partition holding b whose list is longest, and among
			// those the one whose other brokers are most loaded: dropping
			// there leaves room where it is most needed.
			var pick int32 = -1

			score := func(p int32) int {
				s := 0
				for _, o := range next[p] {
					if o != b {
						s += load[o]
					}
				}

				return s
			}

			for _, p := range parts {
				if !contains(next[p], b) {
					continue
				}

				if pick < 0 || len(next[p]) > len(next[pick]) || len(next[p]) == len(next[pick]) && score(p) > score(pick) {
					pick = p
				}
			}

			next[pick] = remove(next[pick], b)
			load[b]--
		}
	}

	// Fill what is missing from the least loaded brokers, swapping when
	// every broker the partition lacks is full (see below).
	for _, p := range parts {
		for len(next[p]) < len(current[p]) && len(next[p]) < len(brokers) {
			var pick int32 = -1

			for _, b := range brokers {
				if contains(next[p], b) {
					continue
				}

				if pick < 0 || load[b] < load[pick] {
					pick = b
				}
			}

			if load[pick] >= limit {
				// pick is full, and so is every broker p lacks. Some broker
				// u has room (the limits add up to at least the total), and
				// p already holds u. Find a partition q on pick that lacks
				// u, move q's replica pick -> u; then p takes pick.
				for _, u := range brokers {
					if load[u] >= limit || !contains(next[p], u) {
						continue
					}

					moved := false

					for _, q := range parts {
						if q != p && contains(next[q], pick) && !contains(next[q], u) {
							next[q] = append(remove(next[q], pick), u)
							load[pick]--
							load[u]++
							moved = true

							break
						}
					}

					if moved {
						break
					}
				}
			}

			next[p] = append(next[p], pick)
			load[pick]++
		}
	}

	// Leaders: each partition's first broker, assigned so every broker
	// leads at most its share. This is a bipartite matching (partitions to
	// leader slots of their replicas), solved exactly with augmenting
	// paths; the current leader is tried first so balanced topics keep
	// their leaders.
	share := (len(parts) + len(brokers) - 1) / len(brokers)
	leader := map[int32]int32{}
	led := map[int32][]int32{} // broker -> partitions it leads

	var try func(p int32, seen map[int32]bool) bool
	try = func(p int32, seen map[int32]bool) bool {
		cands := append([]int32(nil), next[p]...)
		if len(current[p]) > 0 && contains(cands, current[p][0]) {
			cands = append([]int32{current[p][0]}, remove(cands, current[p][0])...)
		}

		for _, b := range cands {
			if seen[b] {
				continue
			}

			seen[b] = true

			if len(led[b]) < share {
				led[b] = append(led[b], p)
				leader[p] = b

				return true
			}

			// b is full: move one of its partitions to another of its
			// replicas, freeing a slot.
			for i, q := range led[b] {
				if try(q, seen) {
					led[b] = append(led[b][:i:i], led[b][i+1:]...)
					led[b] = append(led[b], p)
					leader[p] = b

					return true
				}
			}
		}

		return false
	}

	for _, p := range parts {
		if !try(p, map[int32]bool{}) {
			// No balanced assignment exists; keep its first replica.
			leader[p] = next[p][0]
			led[next[p][0]] = append(led[next[p][0]], p)
		}
	}

	for _, p := range parts {
		l := leader[p]
		next[p] = append([]int32{l}, remove(next[p], l)...)
	}

	out := make([]Reassignment, 0, len(parts))
	for _, p := range parts {
		out = append(out, Reassignment{Partition: p, Replicas: next[p]})
	}

	return out
}

func contains(s []int32, v int32) bool {
	for _, x := range s {
		if x == v {
			return true
		}
	}

	return false
}

func remove(s []int32, v int32) []int32 {
	out := make([]int32, 0, len(s))
	for _, x := range s {
		if x != v {
			out = append(out, x)
		}
	}

	return out
}
