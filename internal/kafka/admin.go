package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

type Broker struct {
	ID         int32   `json:"id"`
	Host       string  `json:"host"`
	Port       int32   `json:"port"`
	Rack       *string `json:"rack,omitempty"`
	Controller bool    `json:"controller"`
	Leaders    int     `json:"leaders"`
	Replicas   int     `json:"replicas"`
	Version    string  `json:"version,omitempty"`
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
	Size             int64    `json:"size"`
	HasRegistry      bool     `json:"has_registry"`
	// Problems lists only the topics with an unhealthy partition, sorted by
	// name, so the payload stays small however many topics the cluster has.
	// Partitions has one character per partition in id order: "o" ok,
	// "u" under-replicated, "x" offline.
	Problems []TopicHealth `json:"problems"`
}

type TopicHealth struct {
	Topic      string `json:"topic"`
	Partitions string `json:"partitions"`
}

func (c *Client) Overview(ctx context.Context) (*Overview, error) {
	meta, err := c.admin.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("metadata; %w", err)
	}

	o := &Overview{
		Cluster:     c.Name,
		ClusterID:   meta.Cluster,
		Controller:  meta.Controller,
		Brokers:     brokers(meta),
		Size:        -1,
		HasRegistry: c.registry != nil,
		Problems:    []TopicHealth{},
	}

	index := map[int32]int{}
	for i, b := range o.Brokers {
		index[b.ID] = i
	}

	if versions, err := c.admin.ApiVersions(ctx); err == nil {
		versions.Each(func(v kadm.BrokerApiVersions) {
			if i, ok := index[v.NodeID]; ok && v.Err == nil {
				o.Brokers[i].Version = v.VersionGuess()
			}
		})
	}

	if dirs, err := c.admin.DescribeAllLogDirs(ctx, nil); err == nil {
		o.Size = 0

		dirs.Each(func(d kadm.DescribedLogDir) { o.Size += d.Size() })
	}

	for _, t := range meta.Topics.Sorted() {
		if t.IsInternal {
			o.InternalTopics++
		} else {
			o.Topics++
		}

		marks := make([]byte, 0, len(t.Partitions))
		unhealthy := false

		for _, p := range t.Partitions.Sorted() {
			switch {
			case p.Leader < 0:
				marks = append(marks, 'x')
				unhealthy = true
			case len(p.ISR) < len(p.Replicas):
				marks = append(marks, 'u')
				unhealthy = true
			default:
				marks = append(marks, 'o')
			}

			o.Partitions++

			if i, ok := index[p.Leader]; ok {
				o.Brokers[i].Leaders++
			}

			for _, r := range p.Replicas {
				if i, ok := index[r]; ok {
					o.Brokers[i].Replicas++
				}
			}

			if p.Leader < 0 {
				o.OfflinePartitons++
			}

			if len(p.ISR) < len(p.Replicas) {
				o.UnderReplicated++
			}
		}

		if unhealthy {
			o.Problems = append(o.Problems, TopicHealth{Topic: t.Topic, Partitions: string(marks)})
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
	// Size is the leader replicas' on-disk bytes; -1 when unknown.
	Size int64 `json:"size"`
	// ReplicatedSize sums every replica.
	ReplicatedSize int64  `json:"replicated_size"`
	CleanupPolicy  string `json:"cleanup_policy,omitempty"`
}

func (c *Client) Topics(ctx context.Context) ([]TopicSummary, error) {
	details, err := c.admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics; %w", err)
	}

	names := details.Names()

	starts, _ := c.admin.ListStartOffsets(ctx, names...)
	ends, _ := c.admin.ListEndOffsets(ctx, names...)
	sizes := c.topicSizes(ctx)

	policies := map[string]string{}
	if configs, err := c.admin.DescribeTopicConfigs(ctx, names...); err == nil {
		for _, rc := range configs {
			for _, cfg := range rc.Configs {
				if cfg.Key == "cleanup.policy" && cfg.Value != nil {
					policies[rc.Name] = *cfg.Value
				}
			}
		}
	}

	out := make([]TopicSummary, 0, len(names))
	for _, name := range names {
		d := details[name]
		s := TopicSummary{
			Name: name, Internal: d.IsInternal, Partitions: len(d.Partitions),
			Size: -1, ReplicatedSize: -1, CleanupPolicy: policies[name],
		}

		if sz, ok := sizes[name]; ok {
			s.Size, s.ReplicatedSize = sz[0], sz[1]
		} else if sizes != nil {
			s.Size, s.ReplicatedSize = 0, 0
		}

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
	Size            int64   `json:"size"`
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
	Size       int64         `json:"size"`
	// ReplicatedSize sums every replica.
	ReplicatedSize int64 `json:"replicated_size"`
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

	out := &TopicDetail{Name: name, ID: d.ID.String(), Internal: d.IsInternal, Size: -1, ReplicatedSize: -1}

	partSizes := map[int32]int64{}

	set := kadm.TopicsSet{}
	for p := range d.Partitions {
		set.Add(name, p)
	}

	if dirs, err := c.admin.DescribeAllLogDirs(ctx, set); err == nil {
		out.Size, out.ReplicatedSize = 0, 0

		dirs.Each(func(ld kadm.DescribedLogDir) {
			for _, p := range ld.Topics[name] {
				if p.IsFuture {
					continue
				}

				out.ReplicatedSize += p.Size

				if lp, ok := d.Partitions[p.Partition]; ok && lp.Leader == ld.Broker {
					partSizes[p.Partition] = p.Size
					out.Size += p.Size
				}
			}
		})
	}

	for _, p := range d.Partitions.Sorted() {
		part := Partition{
			ID:              p.Partition,
			Leader:          p.Leader,
			Replicas:        nonNil(p.Replicas),
			ISR:             nonNil(p.ISR),
			OfflineReplicas: nonNil(p.OfflineReplicas),
			Size:            -1,
		}

		if sz, ok := partSizes[p.Partition]; ok {
			part.Size = sz
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
			out.Configs = configEntries(rc.Configs)
		}
	}

	out.Groups, _ = c.topicGroups(ctx, name)

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

// topicGroups names the groups that committed offsets for, or are assigned
// partitions of, the topic. It reads committed offsets and group
// descriptions only, without listing any topic offsets, so it stays cheap on
// clusters with many groups.
func (c *Client) topicGroups(ctx context.Context, topic string) ([]string, error) {
	listed, err := c.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups; %w", err)
	}

	names := listed.Groups()
	if len(names) == 0 {
		return []string{}, nil
	}

	found := map[string]bool{}

	for _, r := range c.admin.FetchManyOffsets(ctx, names...) {
		if r.Err != nil {
			continue
		}

		if _, ok := r.Fetched[topic]; ok {
			found[r.Group] = true
		}
	}

	// Partial results are still useful when some coordinators fail.
	described, _ := c.admin.DescribeGroups(ctx, names...)
	for _, g := range described {
		if found[g.Group] {
			continue
		}

		if _, ok := g.AssignedPartitions()[topic]; ok {
			found[g.Group] = true
		}
	}

	out := make([]string, 0, len(found))
	for g := range found {
		out = append(out, g)
	}

	sort.Strings(out)

	return out, nil
}

// Names lists topic, group and subject names without any offsets, sizes or
// lag, for pickers and search that must stay fast on large clusters.
type Names struct {
	Topics   []TopicName `json:"topics"`
	Groups   []string    `json:"groups"`
	Subjects []string    `json:"subjects"`
}

type TopicName struct {
	Name       string `json:"name"`
	Internal   bool   `json:"internal"`
	Partitions int    `json:"partitions"`
}

func (c *Client) Names(ctx context.Context) (*Names, error) {
	details, err := c.admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics; %w", err)
	}

	out := &Names{Topics: make([]TopicName, 0, len(details)), Groups: []string{}, Subjects: []string{}}

	for _, d := range details.Sorted() {
		out.Topics = append(out.Topics, TopicName{Name: d.Topic, Internal: d.IsInternal, Partitions: len(d.Partitions)})
	}

	if listed, err := c.admin.ListGroups(ctx); err == nil {
		out.Groups = listed.Groups()
		sort.Strings(out.Groups)
	}

	if c.registry != nil {
		if subjects, err := c.registry.Subjects(ctx); err == nil {
			sort.Strings(subjects)
			out.Subjects = subjects
		}
	}

	return out, nil
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

// TopicConsumer is one group's view of a single topic: who owns which
// partition and how far behind each one is.
type TopicConsumer struct {
	Group      string         `json:"group"`
	State      string         `json:"state"`
	Members    int            `json:"members"`
	Lag        int64          `json:"lag"`
	Partitions []PartitionLag `json:"partitions"`
	Error      string         `json:"error,omitempty"`
}

// TopicConsumers lists every group that has committed offsets for, or is
// assigned partitions of, the topic.
func (c *Client) TopicConsumers(ctx context.Context, topic string) ([]TopicConsumer, error) {
	out := []TopicConsumer{}

	groups, err := c.topicGroups(ctx, topic)
	if err != nil || len(groups) == 0 {
		return out, err
	}

	// Lag only for the groups on this topic: computing it for every group
	// would list offsets of every topic any group reads.
	lags, err := c.admin.Lag(ctx, groups...)
	if err != nil {
		return nil, fmt.Errorf("group lag; %w", err)
	}

	for _, l := range lags.Sorted() {
		parts, ok := l.Lag[topic]
		if !ok {
			continue
		}

		tc := TopicConsumer{Group: l.Group, State: l.State, Members: len(l.Members), Partitions: []PartitionLag{}}
		if l.DescribeErr != nil {
			tc.Error = l.DescribeErr.Error()
		} else if l.FetchErr != nil {
			tc.Error = l.FetchErr.Error()
		}

		for _, p := range (kadm.GroupLag{topic: parts}).Sorted() {
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

			if p.Lag > 0 {
				tc.Lag += p.Lag
			}

			tc.Partitions = append(tc.Partitions, pl)
		}

		out = append(out, tc)
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
	// Strategy is earliest, latest, offset, timestamp or shift.
	Strategy  string `json:"strategy"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
	// Shift moves each partition relative to its committed offset (negative
	// re-reads, positive skips); partitions with no commit shift from the
	// low watermark.
	Shift int64 `json:"shift"`
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
		case "shift":
			// Needs the committed offset, resolved below.
		default:
			return nil, fmt.Errorf("unknown strategy %q", req.Strategy)
		}

		from := int64(-1)
		if cur, ok := current.Lookup(req.Topic, s.Partition); ok && cur.Err == nil {
			from = cur.At
		}

		if req.Strategy == "shift" {
			base := from
			if base < 0 {
				base = s.Offset
			}

			target = min(max(base+req.Shift, s.Offset), e.Offset)
		}

		plan = append(plan, ResetPlan{Partition: s.Partition, From: from, To: target, Low: s.Offset, High: e.Offset})
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

// ─── Create group ─────────────────────────────────────────

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

	described, err := c.admin.DescribeGroups(ctx, req.Group)
	if err != nil {
		return nil, fmt.Errorf("describe group; %w", err)
	}

	g, ok := described[req.Group]
	if !ok || g.State == "Dead" {
		return nil, fmt.Errorf("group %q; %w", req.Group, ErrNotFound)
	}

	if len(g.Members) > 0 {
		return nil, fmt.Errorf("group %q has %d active members; stop the consumers first", req.Group, len(g.Members))
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
	starts, err := c.admin.ListStartOffsets(ctx, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets; %w", err)
	}

	ends, err := c.admin.ListEndOffsets(ctx, req.Topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets; %w", err)
	}

	offsets := make(kadm.Offsets)
	plan := make([]ResetPlan, 0, len(req.Offsets))
	seen := map[int32]bool{}

	for _, po := range req.Offsets {
		if seen[po.Partition] {
			return nil, ErrInvalid{fmt.Errorf("partition %d is listed twice", po.Partition)}
		}

		seen[po.Partition] = true

		s, ok := starts.Lookup(req.Topic, po.Partition)
		if !ok || s.Err != nil {
			return nil, ErrInvalid{fmt.Errorf("topic %q has no partition %d", req.Topic, po.Partition)}
		}

		e, _ := ends.Lookup(req.Topic, po.Partition)

		if po.Offset < s.Offset || po.Offset > e.Offset {
			return nil, ErrInvalid{fmt.Errorf("partition %d: offset %d is outside %d–%d", po.Partition, po.Offset, s.Offset, e.Offset)}
		}

		offsets.AddOffset(req.Topic, po.Partition, po.Offset, -1)
		plan = append(plan, ResetPlan{Partition: po.Partition, From: -1, To: po.Offset, Low: s.Offset, High: e.Offset})
	}

	sort.Slice(plan, func(i, j int) bool { return plan[i].Partition < plan[j].Partition })

	resp, err := c.admin.CommitOffsets(ctx, req.Group, offsets)
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
