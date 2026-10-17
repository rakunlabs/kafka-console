package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type Broker struct {
	ID         int32   `json:"id"`
	Host       string  `json:"host"`
	Port       int32   `json:"port"`
	Rack       *string `json:"rack,omitempty"`
	Controller bool    `json:"controller"`
}

type Overview struct {
	Cluster          string   `json:"cluster"`
	ClusterID        string   `json:"cluster_id"`
	Brokers          []Broker `json:"brokers"`
	Controller       int32    `json:"controller"`
	Topics           int      `json:"topics"`
	InternalTopics   int      `json:"internal_topics"`
	Partitions       int      `json:"partitions"`
	UnderReplicated  int      `json:"under_replicated"`
	OfflinePartitons int      `json:"offline_partitions"`
	Groups           int      `json:"groups"`
}

func (c *Client) Overview(ctx context.Context) (*Overview, error) {
	meta, err := c.admin.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("metadata; %w", err)
	}

	o := &Overview{
		Cluster:    c.Name,
		ClusterID:  meta.Cluster,
		Controller: meta.Controller,
		Brokers:    brokers(meta),
	}

	for _, t := range meta.Topics {
		if t.IsInternal {
			o.InternalTopics++
		} else {
			o.Topics++
		}

		for _, p := range t.Partitions {
			o.Partitions++

			if p.Leader < 0 {
				o.OfflinePartitons++
			}

			if len(p.ISR) < len(p.Replicas) {
				o.UnderReplicated++
			}
		}
	}

	if groups, err := c.admin.ListGroups(ctx); err == nil {
		o.Groups = len(groups)
	}

	return o, nil
}

func brokers(meta kadm.Metadata) []Broker {
	out := make([]Broker, 0, len(meta.Brokers))
	for _, b := range meta.Brokers {
		out = append(out, Broker{
			ID:         b.NodeID,
			Host:       b.Host,
			Port:       b.Port,
			Rack:       b.Rack,
			Controller: b.NodeID == meta.Controller,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })

	return out
}

// ─── Topics ───────────────────────────────────────────────

type TopicSummary struct {
	Name              string `json:"name"`
	Internal          bool   `json:"internal"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	UnderReplicated   int    `json:"under_replicated"`
	Messages          int64  `json:"messages"`
}

func (c *Client) Topics(ctx context.Context) ([]TopicSummary, error) {
	details, err := c.admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics; %w", err)
	}

	names := details.Names()

	starts, _ := c.admin.ListStartOffsets(ctx, names...)
	ends, _ := c.admin.ListEndOffsets(ctx, names...)

	out := make([]TopicSummary, 0, len(names))
	for _, name := range names {
		d := details[name]
		s := TopicSummary{Name: name, Internal: d.IsInternal, Partitions: len(d.Partitions)}

		for _, p := range d.Partitions {
			if len(p.Replicas) > s.ReplicationFactor {
				s.ReplicationFactor = len(p.Replicas)
			}

			if len(p.ISR) < len(p.Replicas) {
				s.UnderReplicated++
			}

			start, okS := starts.Lookup(name, p.Partition)
			end, okE := ends.Lookup(name, p.Partition)
			if okS && okE && start.Err == nil && end.Err == nil {
				s.Messages += end.Offset - start.Offset
			}
		}

		out = append(out, s)
	}

	return out, nil
}

type Partition struct {
	ID              int32   `json:"id"`
	Leader          int32   `json:"leader"`
	Replicas        []int32 `json:"replicas"`
	ISR             []int32 `json:"isr"`
	OfflineReplicas []int32 `json:"offline_replicas"`
	Low             int64   `json:"low"`
	High            int64   `json:"high"`
	Error           string  `json:"error,omitempty"`
}

type ConfigEntry struct {
	Key       string  `json:"key"`
	Value     *string `json:"value"`
	Sensitive bool    `json:"sensitive"`
	Source    string  `json:"source"`
}

type TopicDetail struct {
	Name       string        `json:"name"`
	ID         string        `json:"id"`
	Internal   bool          `json:"internal"`
	Partitions []Partition   `json:"partitions"`
	Configs    []ConfigEntry `json:"configs"`
	Groups     []string      `json:"groups"`
}

var ErrNotFound = errors.New("not found")

func (c *Client) Topic(ctx context.Context, name string) (*TopicDetail, error) {
	details, err := c.admin.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("describe topic; %w", err)
	}

	d, ok := details[name]
	if !ok || d.Err != nil {
		return nil, fmt.Errorf("topic %q; %w", name, ErrNotFound)
	}

	starts, _ := c.admin.ListStartOffsets(ctx, name)
	ends, _ := c.admin.ListEndOffsets(ctx, name)

	out := &TopicDetail{Name: name, ID: d.ID.String(), Internal: d.IsInternal}

	for _, p := range d.Partitions.Sorted() {
		part := Partition{
			ID:              p.Partition,
			Leader:          p.Leader,
			Replicas:        nonNil(p.Replicas),
			ISR:             nonNil(p.ISR),
			OfflineReplicas: nonNil(p.OfflineReplicas),
		}

		if p.Err != nil {
			part.Error = p.Err.Error()
		}

		if s, ok := starts.Lookup(name, p.Partition); ok && s.Err == nil {
			part.Low = s.Offset
		}

		if e, ok := ends.Lookup(name, p.Partition); ok && e.Err == nil {
			part.High = e.Offset
		}

		out.Partitions = append(out.Partitions, part)
	}

	if configs, err := c.admin.DescribeTopicConfigs(ctx, name); err == nil {
		if rc, err := configs.On(name, nil); err == nil {
			for _, cfg := range rc.Configs {
				out.Configs = append(out.Configs, ConfigEntry{
					Key:       cfg.Key,
					Value:     cfg.Value,
					Sensitive: cfg.Sensitive,
					Source:    cfg.Source.String(),
				})
			}

			sort.Slice(out.Configs, func(i, j int) bool { return out.Configs[i].Key < out.Configs[j].Key })
		}
	}

	if lags, err := c.groupLags(ctx); err == nil {
		for _, l := range lags.Sorted() {
			if _, ok := l.Lag[name]; ok {
				out.Groups = append(out.Groups, l.Group)
			}
		}
	}

	return out, nil
}

type CreateTopicRequest struct {
	Name              string             `json:"name"`
	Partitions        int32              `json:"partitions"`
	ReplicationFactor int16              `json:"replication_factor"`
	Configs           map[string]*string `json:"configs"`
}

func (c *Client) CreateTopic(ctx context.Context, req CreateTopicRequest) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	if req.Partitions == 0 {
		req.Partitions = -1
	}

	if req.ReplicationFactor == 0 {
		req.ReplicationFactor = -1
	}

	resp, err := c.admin.CreateTopic(ctx, req.Partitions, req.ReplicationFactor, req.Configs, req.Name)
	if err != nil {
		return fmt.Errorf("create topic; %w", err)
	}

	if resp.Err != nil {
		return fmt.Errorf("create topic; %w %s", resp.Err, resp.ErrMessage)
	}

	return nil
}

func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	resp, err := c.admin.DeleteTopic(ctx, name)
	if err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return fmt.Errorf("topic %q; %w", name, ErrNotFound)
		}

		return fmt.Errorf("delete topic; %w %s", err, resp.ErrMessage)
	}

	return nil
}

// ─── Consumer groups ──────────────────────────────────────

type GroupSummary struct {
	Name         string   `json:"name"`
	State        string   `json:"state"`
	ProtocolType string   `json:"protocol_type"`
	Members      int      `json:"members"`
	Topics       []string `json:"topics"`
	Lag          int64    `json:"lag"`
	Error        string   `json:"error,omitempty"`
}

func (c *Client) groupLags(ctx context.Context) (kadm.DescribedGroupLags, error) {
	listed, err := c.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups; %w", err)
	}

	if len(listed) == 0 {
		return kadm.DescribedGroupLags{}, nil
	}

	return c.admin.Lag(ctx, listed.Groups()...)
}

func (c *Client) Groups(ctx context.Context) ([]GroupSummary, error) {
	lags, err := c.groupLags(ctx)
	if err != nil {
		return nil, err
	}

	out := make([]GroupSummary, 0, len(lags))
	for _, l := range lags.Sorted() {
		s := GroupSummary{
			Name:         l.Group,
			State:        l.State,
			ProtocolType: l.ProtocolType,
			Members:      len(l.Members),
			Topics:       []string{},
		}

		if err := l.Error(); err != nil {
			s.Error = err.Error()
		}

		for topic := range l.Lag {
			s.Topics = append(s.Topics, topic)
		}

		sort.Strings(s.Topics)
		s.Lag = positiveLag(l.Lag)

		out = append(out, s)
	}

	return out, nil
}

type Member struct {
	ID         string  `json:"id"`
	InstanceID *string `json:"instance_id,omitempty"`
	ClientID   string  `json:"client_id"`
	Host       string  `json:"host"`
}

type PartitionLag struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Low       int64  `json:"low"`
	High      int64  `json:"high"`
	Committed int64  `json:"committed"`
	Lag       int64  `json:"lag"`
	MemberID  string `json:"member_id,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	Host      string `json:"host,omitempty"`
	Error     string `json:"error,omitempty"`
}

type GroupDetail struct {
	Name         string         `json:"name"`
	State        string         `json:"state"`
	ProtocolType string         `json:"protocol_type"`
	Protocol     string         `json:"protocol"`
	Coordinator  int32          `json:"coordinator"`
	Members      []Member       `json:"members"`
	Partitions   []PartitionLag `json:"partitions"`
	Lag          int64          `json:"lag"`
}

func (c *Client) Group(ctx context.Context, name string) (*GroupDetail, error) {
	lags, err := c.admin.Lag(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("describe group; %w", err)
	}

	l, ok := lags[name]
	if !ok {
		return nil, fmt.Errorf("group %q; %w", name, ErrNotFound)
	}

	if l.DescribeErr != nil {
		return nil, fmt.Errorf("describe group; %w", l.DescribeErr)
	}

	out := &GroupDetail{
		Name:         l.Group,
		State:        l.State,
		ProtocolType: l.ProtocolType,
		Protocol:     l.Protocol,
		Coordinator:  l.Coordinator.NodeID,
		Members:      []Member{},
		Partitions:   []PartitionLag{},
		Lag:          positiveLag(l.Lag),
	}

	for _, m := range l.Members {
		out.Members = append(out.Members, Member{
			ID: m.MemberID, InstanceID: m.InstanceID, ClientID: m.ClientID, Host: m.ClientHost,
		})
	}

	for _, p := range l.Lag.Sorted() {
		pl := PartitionLag{
			Topic:     p.Topic,
			Partition: p.Partition,
			Low:       p.Start.Offset,
			High:      p.End.Offset,
			Committed: p.Commit.At,
			Lag:       p.Lag,
		}

		if p.Member != nil {
			pl.MemberID = p.Member.MemberID
			pl.ClientID = p.Member.ClientID
			pl.Host = p.Member.ClientHost
		}

		if p.Err != nil {
			pl.Error = p.Err.Error()
		}

		out.Partitions = append(out.Partitions, pl)
	}

	return out, nil
}

func (c *Client) DeleteGroup(ctx context.Context, name string) error {
	if err := c.requireWritable(); err != nil {
		return err
	}

	resp, err := c.admin.DeleteGroup(ctx, name)
	if err != nil {
		return fmt.Errorf("delete group; %w", err)
	}

	if resp.Err != nil {
		return fmt.Errorf("delete group; %w", resp.Err)
	}

	return nil
}

func positiveLag(l kadm.GroupLag) int64 {
	var total int64
	for _, ps := range l {
		for _, p := range ps {
			if p.Lag > 0 {
				total += p.Lag
			}
		}
	}

	return total
}

// ─── Offset reset ─────────────────────────────────────────

type ResetRequest struct {
	Topic string `json:"topic"`
	// Partitions limits the reset; empty means every partition of the topic.
	Partitions []int32 `json:"partitions"`
	// Strategy is earliest, latest, offset or timestamp.
	Strategy  string `json:"strategy"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	// DryRun returns the planned offsets without committing.
	DryRun bool `json:"dry_run"`
}

type ResetPlan struct {
	Partition int32 `json:"partition"`
	From      int64 `json:"from"`
	To        int64 `json:"to"`
}

func (c *Client) ResetOffsets(ctx context.Context, group string, req ResetRequest) ([]ResetPlan, error) {
	if !req.DryRun {
		if err := c.requireWritable(); err != nil {
			return nil, err
		}
	}

	described, err := c.admin.DescribeGroups(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("describe group; %w", err)
	}

	if g, ok := described[group]; ok && len(g.Members) > 0 && !req.DryRun {
		return nil, fmt.Errorf("group %q has %d active members; stop the consumers first", group, len(g.Members))
	}

	starts, err := c.admin.ListStartOffsets(ctx, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets; %w", err)
	}

	ends, err := c.admin.ListEndOffsets(ctx, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets; %w", err)
	}

	var atTime kadm.ListedOffsets
	if req.Strategy == "timestamp" {
		atTime, err = c.admin.ListOffsetsAfterMilli(ctx, req.Timestamp, req.Topic)
		if err != nil {
			return nil, fmt.Errorf("list offsets by time; %w", err)
		}
	}

	current, _ := c.admin.FetchOffsets(ctx, group)

	wanted := map[int32]bool{}
	for _, p := range req.Partitions {
		wanted[p] = true
	}

	var startList []kadm.ListedOffset
	starts.Each(func(o kadm.ListedOffset) {
		if o.Topic == req.Topic && o.Err == nil {
			startList = append(startList, o)
		}
	})
	sort.Slice(startList, func(i, j int) bool { return startList[i].Partition < startList[j].Partition })

	var plan []ResetPlan

	offsets := make(kadm.Offsets)

	for _, s := range startList {
		if len(wanted) > 0 && !wanted[s.Partition] {
			continue
		}

		e, _ := ends.Lookup(req.Topic, s.Partition)

		var target int64

		switch req.Strategy {
		case "earliest":
			target = s.Offset
		case "latest":
			target = e.Offset
		case "offset":
			target = min(max(req.Offset, s.Offset), e.Offset)
		case "timestamp":
			t, ok := atTime.Lookup(req.Topic, s.Partition)
			if !ok || t.Err != nil || t.Offset < 0 {
				target = e.Offset
			} else {
				target = t.Offset
			}
		default:
			return nil, fmt.Errorf("unknown strategy %q", req.Strategy)
		}

		from := int64(-1)
		if cur, ok := current.Lookup(req.Topic, s.Partition); ok && cur.Err == nil {
			from = cur.At
		}

		plan = append(plan, ResetPlan{Partition: s.Partition, From: from, To: target})
		offsets.AddOffset(req.Topic, s.Partition, target, -1)
	}

	if req.DryRun {
		return plan, nil
	}

	resp, err := c.admin.CommitOffsets(ctx, group, offsets)
	if err != nil {
		return nil, fmt.Errorf("commit offsets; %w", err)
	}

	if err := resp.Error(); err != nil {
		return nil, fmt.Errorf("commit offsets; %w", err)
	}

	return plan, nil
}

func nonNil(v []int32) []int32 {
	if v == nil {
		return []int32{}
	}

	return v
}
