package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

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

// TopicQuery selects one page of the topic list. Name, partition and
// under-replicated sorts only need metadata, so offsets, sizes and configs
// are fetched for the returned page alone. Sorting by messages or size needs
// those numbers for every matching topic first.
type TopicQuery struct {
	Search   string
	Internal bool
	Sort     string
	Desc     bool
	Offset   int
	// Limit is the page size; 0 returns every matching topic.
	Limit int
}

type TopicPage struct {
	Topics []TopicSummary `json:"topics"`
	// Total counts every topic matching the search and internal filter.
	Total int `json:"total"`
}

func (c *Client) Topics(ctx context.Context, q TopicQuery) (*TopicPage, error) {
	details, _, err := c.listTopics(ctx)
	if err != nil {
		return nil, fmt.Errorf("list topics; %w", err)
	}

	search := strings.ToLower(strings.TrimSpace(q.Search))
	all := make([]TopicSummary, 0, len(details))
	for name, d := range details {
		if d.IsInternal && !q.Internal || search != "" && !strings.Contains(strings.ToLower(name), search) {
			continue
		}

		s := TopicSummary{Name: name, Internal: d.IsInternal, Partitions: len(d.Partitions), Size: -1, ReplicatedSize: -1}
		for _, p := range d.Partitions {
			s.ReplicationFactor = max(s.ReplicationFactor, len(p.Replicas))
			if len(p.ISR) < len(p.Replicas) {
				s.UnderReplicated++
			}
		}
		all = append(all, s)
	}

	page := &TopicPage{Total: len(all)}

	heavy := q.Sort == "messages" || q.Sort == "size"
	if heavy {
		c.topicStats(ctx, details, all)
	}

	sortTopics(all, q.Sort, q.Desc)

	page.Topics = pageOf(all, q.Offset, q.Limit)

	if !heavy {
		c.topicStats(ctx, details, page.Topics)
	}

	return page, nil
}

// sortTopics orders by key, falling back to name so pages are stable.
func sortTopics(ts []TopicSummary, key string, desc bool) {
	sort.SliceStable(ts, func(i, j int) bool {
		a, b := ts[i], ts[j]
		var d int64
		switch key {
		case "partitions":
			d = int64(a.Partitions - b.Partitions)
		case "under_replicated":
			d = int64(a.UnderReplicated - b.UnderReplicated)
		case "messages":
			d = a.Messages - b.Messages
		case "size":
			d = a.Size - b.Size
		}
		if desc {
			d = -d
		}
		if d != 0 {
			return d < 0
		}
		if key == "name" && desc {
			return a.Name > b.Name
		}
		return a.Name < b.Name
	})
}

// topicStats fills messages, sizes and cleanup policy for ts in place. The
// four broker requests only cover these topics and run in parallel. Each is
// best effort: a failed one leaves its fields at their zero or -1 values.
func (c *Client) topicStats(ctx context.Context, details kadm.TopicDetails, ts []TopicSummary) {
	if len(ts) == 0 {
		return
	}

	names := make([]string, len(ts))
	subset := make(kadm.TopicDetails, len(ts))
	for i, t := range ts {
		names[i] = t.Name
		subset[t.Name] = details[t.Name]
	}

	var (
		wg           sync.WaitGroup
		starts, ends kadm.ListedOffsets
		sizes        map[string]topicSize
		configs      kadm.ResourceConfigs
		configsErr   error
	)
	wg.Go(func() { starts, _ = c.admin.ListStartOffsets(ctx, names...) })
	wg.Go(func() { ends, _ = c.admin.ListEndOffsets(ctx, names...) })
	wg.Go(func() { sizes = c.topicSizes(ctx, subset) })
	wg.Go(func() { configs, configsErr = c.admin.DescribeTopicConfigs(ctx, names...) })
	wg.Wait()

	policies := map[string]string{}
	if configsErr == nil {
		for _, rc := range configs {
			for _, cfg := range rc.Configs {
				if cfg.Key == "cleanup.policy" && cfg.Value != nil {
					policies[rc.Name] = *cfg.Value
				}
			}
		}
	}

	for i := range ts {
		s := &ts[i]
		s.CleanupPolicy = policies[s.Name]

		if sz, ok := sizes[s.Name]; ok {
			s.Size, s.ReplicatedSize = sz.leader, sz.replicated
		} else if sizes != nil {
			s.Size, s.ReplicatedSize = 0, 0
		}

		for _, p := range details[s.Name].Partitions {
			start, okS := starts.Lookup(s.Name, p.Partition)
			end, okE := ends.Lookup(s.Name, p.Partition)
			if okS && okE && start.Err == nil && end.Err == nil {
				s.Messages += end.Offset - start.Offset
			}
		}
	}
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

func (c *Client) Topic(ctx context.Context, name string) (*TopicDetail, error) {
	_, d, err := c.topicDetail(ctx, name)
	if err != nil {
		return nil, err
	}

	out := &TopicDetail{Name: name, ID: d.ID.String(), Internal: d.IsInternal, Size: -1, ReplicatedSize: -1}

	// Independent reads, each a round trip or more; partial failures leave
	// their part empty instead of failing the page.
	var (
		starts, ends kadm.ListedOffsets
		sizes        map[string]topicSize
		configs      kadm.ResourceConfigs
		configsErr   error
		wg           sync.WaitGroup
	)

	wg.Go(func() { starts, _ = c.admin.ListStartOffsets(ctx, name) })
	wg.Go(func() { ends, _ = c.admin.ListEndOffsets(ctx, name) })
	wg.Go(func() { sizes = c.topicSizes(ctx, kadm.TopicDetails{name: d}) })
	wg.Go(func() { configs, configsErr = c.admin.DescribeTopicConfigs(ctx, name) })
	wg.Go(func() { out.Groups, _ = c.topicGroups(ctx, name) })
	wg.Wait()

	size := sizes[name]
	if sizes != nil {
		out.Size, out.ReplicatedSize = size.leader, size.replicated
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

		if sz, ok := size.partitions[p.Partition]; ok {
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

	if configsErr == nil {
		if rc, err := configs.On(name, nil); err == nil {
			out.Configs = configEntries(rc.Configs)
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

	// kadm returns the response's own error as err; its ErrMessage says
	// why, e.g. "replication factor larger than available brokers".
	resp, err := c.admin.CreateTopic(ctx, req.Partitions, req.ReplicationFactor, req.Configs, req.Name)
	if err != nil {
		return respErr("create topic", err, resp.ErrMessage)
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

		return respErr("delete topic", err, resp.ErrMessage)
	}

	return nil
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
	out := &Names{Groups: []string{}, Subjects: []string{}}

	var (
		details kadm.TopicDetails
		err     error
		wg      sync.WaitGroup
	)

	wg.Go(func() { details, _, err = c.listTopics(ctx) })
	wg.Go(func() {
		if listed, err := c.admin.ListGroups(ctx); err == nil {
			out.Groups = listed.Groups()
			sort.Strings(out.Groups)
		}
	})

	if c.registry != nil {
		wg.Go(func() {
			if subjects, err := c.registry.Subjects(ctx); err == nil {
				sort.Strings(subjects)
				out.Subjects = subjects
			}
		})
	}

	wg.Wait()

	if err != nil {
		return nil, fmt.Errorf("list topics; %w", err)
	}

	out.Topics = make([]TopicName, 0, len(details))
	for _, d := range details.Sorted() {
		out.Topics = append(out.Topics, TopicName{Name: d.Topic, Internal: d.IsInternal, Partitions: len(d.Partitions)})
	}

	return out, nil
}

// pageOf returns the window of s starting at offset; limit 0 means no end.
func pageOf[T any](s []T, offset, limit int) []T {
	start := min(max(offset, 0), len(s))
	end := len(s)
	if limit > 0 {
		end = min(start+limit, len(s))
	}

	return s[start:end]
}

func nonNil(v []int32) []int32 {
	if v == nil {
		return []int32{}
	}

	return v
}

func configEntries(cfgs []kadm.Config) []ConfigEntry {
	out := make([]ConfigEntry, 0, len(cfgs))
	for _, cfg := range cfgs {
		out = append(out, ConfigEntry{
			Key:       cfg.Key,
			Value:     cfg.Value,
			Sensitive: cfg.Sensitive,
			Source:    cfg.Source.String(),
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })

	return out
}

// topicSize is a topic's on-disk size: its leader replicas, which
// approximate the logical size, and every replica.
type topicSize struct {
	leader, replicated int64
	// partitions holds each partition's leader replica size.
	partitions map[int32]int64
}

// topicSizes returns the on-disk size of the given topics. Errors are
// swallowed: sizes are best effort. Leaders come from topics, the metadata
// the caller already holds.
func (c *Client) topicSizes(ctx context.Context, topics kadm.TopicDetails) map[string]topicSize {
	dirs, err := c.admin.DescribeAllLogDirs(ctx, topics.TopicsSet())
	if err != nil {
		return nil
	}

	return logSizes(dirs, topics)
}

// logSizes sums the log dirs' replicas per topic, leaving out future ones
// (a move in progress) that would count a partition twice.
func logSizes(dirs kadm.DescribedAllLogDirs, topics kadm.TopicDetails) map[string]topicSize {
	out := map[string]topicSize{}

	dirs.Each(func(d kadm.DescribedLogDir) {
		for t, ps := range d.Topics {
			for p, part := range ps {
				if part.IsFuture {
					continue
				}

				s := out[t]
				s.replicated += part.Size

				if mp, ok := topics[t].Partitions[p]; ok && mp.Leader == d.Broker {
					if s.partitions == nil {
						s.partitions = map[int32]int64{}
					}

					s.leader += part.Size
					s.partitions[p] = part.Size
				}

				out[t] = s
			}
		}
	})

	return out
}
