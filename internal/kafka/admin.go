package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

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
	Cluster           string   `json:"cluster"`
	ClusterID         string   `json:"cluster_id"`
	Brokers           []Broker `json:"brokers"`
	Controller        int32    `json:"controller"`
	Topics            int      `json:"topics"`
	InternalTopics    int      `json:"internal_topics"`
	Partitions        int      `json:"partitions"`
	UnderReplicated   int      `json:"under_replicated"`
	OfflinePartitions int      `json:"offline_partitions"`
	Groups            int      `json:"groups"`
	HasRegistry       bool     `json:"has_registry"`
	// Problems lists only the topics with an unhealthy partition, sorted by
	// name, so the payload stays small however many topics the cluster has.
	// Partitions has one character per partition in id order: "o" ok,
	// "u" under-replicated, "x" offline.
	Problems []TopicHealth `json:"problems"`
	// At is when the numbers were read; they may be served from memory for
	// a few seconds.
	At time.Time `json:"at"`
}

type TopicHealth struct {
	Topic      string `json:"topic"`
	Partitions string `json:"partitions"`
}

// DiskUsage is the cluster's on-disk size across every replica. It is read
// apart from the overview: it asks every broker for the size of every
// partition it holds, which on a large cluster takes far longer than the rest.
type DiskUsage struct {
	Size int64     `json:"size"`
	At   time.Time `json:"at"`
}

// Overview and disk usage are the first page everyone opens, and both are
// cluster-wide reads. They are kept in memory for config.Cache and shared
// between callers. A read gives up after readTimeout.
const readTimeout = 30 * time.Second

// Overview returns the cluster summary, from memory when a recent one is
// kept; fresh asks for a new read unless one finished moments ago.
func (c *Client) Overview(ctx context.Context, fresh bool) (*Overview, error) {
	return c.overviews.get(ctx, fresh, c.readOverview)
}

// DiskUsage returns the cluster's total size on disk, kept like Overview.
func (c *Client) DiskUsage(ctx context.Context, fresh bool) (*DiskUsage, error) {
	return c.diskUsage.get(ctx, fresh, c.readDiskUsage)
}

func (c *Client) readDiskUsage(ctx context.Context) (*DiskUsage, error) {
	dirs, err := c.admin.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("describe log dirs; %w", err)
	}

	var size int64
	dirs.Each(func(d kadm.DescribedLogDir) { size += d.Size() })

	return &DiskUsage{Size: size, At: time.Now()}, nil
}

func (c *Client) readOverview(ctx context.Context) (*Overview, error) {
	meta, err := c.metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("metadata; %w", err)
	}

	o := &Overview{
		Cluster:     c.Name,
		ClusterID:   meta.Cluster,
		Controller:  meta.Controller,
		Brokers:     brokers(meta),
		HasRegistry: c.registry != nil,
		Problems:    []TopicHealth{},
		At:          time.Now(),
	}

	index := map[int32]int{}
	for i, b := range o.Brokers {
		index[b.ID] = i
	}

	// These only need the metadata above, so they run together; one after
	// another each waits its own round trip.
	var (
		wg       sync.WaitGroup
		versions kadm.BrokersApiVersions
		verErr   error
		groups   = -1
	)
	wg.Go(func() { versions, verErr = c.admin.ApiVersions(ctx) })
	wg.Go(func() {
		if listed, err := c.admin.ListGroups(ctx); err == nil {
			groups = len(listed)
		}
	})

	o.countTopics(meta, index)

	wg.Wait()

	if verErr == nil {
		versions.Each(func(v kadm.BrokerApiVersions) {
			if i, ok := index[v.NodeID]; ok && v.Err == nil {
				o.Brokers[i].Version = v.VersionGuess()
			}
		})
	}

	o.Groups = max(groups, 0)

	return o, nil
}

// countTopics fills topic, partition, leader and health counts from metadata.
func (o *Overview) countTopics(meta kadm.Metadata, index map[int32]int) {
	for _, t := range meta.Topics.Sorted() {
		// A topic the console may not describe is left out. Brokers already
		// omit such topics from a full listing, so no count of them would be
		// reliable; one answered from the client cache still carries its
		// refusal here.
		if t.Err != nil {
			continue
		}

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
				o.OfflinePartitions++
			}

			if len(p.ISR) < len(p.Replicas) {
				o.UnderReplicated++
			}
		}

		if unhealthy {
			o.Problems = append(o.Problems, TopicHealth{Topic: t.Topic, Partitions: string(marks)})
		}
	}
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
		sizes        map[string][2]int64
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
			s.Size, s.ReplicatedSize = sz[0], sz[1]
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

var ErrNotFound = errors.New("not found")

// ErrConflict is a request the cluster's current state refuses, like
// resetting a group whose consumers are still running.
type ErrConflict struct{ Err error }

func (e ErrConflict) Error() string { return e.Err.Error() }
func (e ErrConflict) Unwrap() error { return e.Err }

func (c *Client) Topic(ctx context.Context, name string) (*TopicDetail, error) {
	details, _, err := c.listTopics(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("describe topic; %w", err)
	}

	d, ok := details[name]
	switch {
	case !ok || errors.Is(d.Err, kerr.UnknownTopicOrPartition):
		return nil, fmt.Errorf("topic %q; %w", name, ErrNotFound)
	case d.Err != nil:
		return nil, fmt.Errorf("topic %q; %w", name, d.Err)
	}

	out := &TopicDetail{Name: name, ID: d.ID.String(), Internal: d.IsInternal, Size: -1, ReplicatedSize: -1}

	set := kadm.TopicsSet{}
	for p := range d.Partitions {
		set.Add(name, p)
	}

	// Independent reads, each a round trip or more; partial failures leave
	// their part empty instead of failing the page.
	var (
		starts, ends kadm.ListedOffsets
		dirs         kadm.DescribedAllLogDirs
		dirsErr      error
		configs      kadm.ResourceConfigs
		configsErr   error
		wg           sync.WaitGroup
	)

	wg.Go(func() { starts, _ = c.admin.ListStartOffsets(ctx, name) })
	wg.Go(func() { ends, _ = c.admin.ListEndOffsets(ctx, name) })
	wg.Go(func() { dirs, dirsErr = c.admin.DescribeAllLogDirs(ctx, set) })
	wg.Go(func() { configs, configsErr = c.admin.DescribeTopicConfigs(ctx, name) })
	wg.Go(func() { out.Groups, _ = c.topicGroups(ctx, name) })
	wg.Wait()

	partSizes := map[int32]int64{}

	if dirsErr == nil {
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
		if resp.ErrMessage != "" {
			return fmt.Errorf("create topic; %w: %s", err, resp.ErrMessage)
		}

		return fmt.Errorf("create topic; %w", err)
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
	// Unreadable lists topics whose lag is unknown because the console may
	// not read their offsets. Lag counts the other topics only.
	Unreadable []string `json:"unreadable,omitempty"`
}

// groupTopics maps each group to the topics it committed offsets for or is
// assigned partitions of. It reads committed offsets and group descriptions
// only, without listing any topic offsets, so it stays cheap on clusters
// with many groups. Partial results are kept when some coordinators fail.
func (c *Client) groupTopics(ctx context.Context, groups []string) map[string]map[string]bool {
	out := make(map[string]map[string]bool, len(groups))
	add := func(group, topic string) {
		if out[group] == nil {
			out[group] = map[string]bool{}
		}
		out[group][topic] = true
	}

	if len(groups) == 0 {
		return out
	}

	var (
		fetched   kadm.FetchOffsetsResponses
		described kadm.DescribedGroups
		wg        sync.WaitGroup
	)

	wg.Go(func() { fetched = c.admin.FetchManyOffsets(ctx, groups...) })
	wg.Go(func() { described, _ = c.admin.DescribeGroups(ctx, groups...) })
	wg.Wait()

	for _, r := range fetched {
		if r.Err != nil {
			continue
		}

		for topic := range r.Fetched {
			add(r.Group, topic)
		}
	}

	for _, g := range described {
		for topic := range g.AssignedPartitions() {
			add(g.Group, topic)
		}
	}

	return out
}

// topicGroups names the groups that committed offsets for, or are assigned
// partitions of, the topic.
func (c *Client) topicGroups(ctx context.Context, topic string) ([]string, error) {
	listed, err := c.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups; %w", err)
	}

	out := []string{}
	for group, topics := range c.groupTopics(ctx, listed.Groups()) {
		if topics[topic] {
			out = append(out, group)
		}
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

// GroupQuery selects one page of the group list. Lag is computed for the
// returned page alone unless the list is sorted by lag, which needs every
// matching group's lag first. A search also matches topic names, which
// reads committed offsets (but no lag) of every group.
type GroupQuery struct {
	Search    string
	SortByLag bool
	Offset    int
	// Limit is the page size; 0 returns every matching group.
	Limit int
}

type GroupPage struct {
	Groups []GroupSummary `json:"groups"`
	// Total counts every group matching the search.
	Total int `json:"total"`
}

func (c *Client) Groups(ctx context.Context, q GroupQuery) (*GroupPage, error) {
	listed, err := c.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups; %w", err)
	}

	names := listed.Groups()
	sort.Strings(names)

	if search := strings.ToLower(strings.TrimSpace(q.Search)); search != "" {
		topics := c.groupTopics(ctx, names)
		matched := names[:0]
		for _, n := range names {
			if strings.Contains(strings.ToLower(n), search) || anyContains(topics[n], search) {
				matched = append(matched, n)
			}
		}
		names = matched
	}

	page := &GroupPage{Total: len(names), Groups: []GroupSummary{}}

	if !q.SortByLag {
		names = pageOf(names, q.Offset, q.Limit)
	}

	summaries, err := c.groupSummaries(ctx, names)
	if err != nil {
		return nil, err
	}

	if q.SortByLag {
		sort.SliceStable(summaries, func(i, j int) bool { return summaries[i].Lag > summaries[j].Lag })
		summaries = pageOf(summaries, q.Offset, q.Limit)
	}

	page.Groups = summaries

	return page, nil
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

func anyContains(set map[string]bool, sub string) bool {
	for s := range set {
		if strings.Contains(strings.ToLower(s), sub) {
			return true
		}
	}
	return false
}

// groupSummaries describes the groups and computes their lag, in name order.
func (c *Client) groupSummaries(ctx context.Context, names []string) ([]GroupSummary, error) {
	if len(names) == 0 {
		return []GroupSummary{}, nil
	}

	lags, err := c.groupLag(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("group lag; %w", err)
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
		s.Unreadable = unreadable(l.Lag)

		out = append(out, s)
	}

	return out, nil
}

// unreadable names the topics whose lag failed for lack of access.
func unreadable(l kadm.GroupLag) []string {
	var out []string
	for topic, ps := range l {
		for _, p := range ps {
			if errors.Is(p.Err, ErrNoTopicAccess) {
				out = append(out, topic)
				break
			}
		}
	}

	sort.Strings(out)

	return out
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
	// NoAccess is set when the console may not read this topic's offsets.
	NoAccess bool `json:"no_access,omitempty"`
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
	lags, err := c.groupLag(ctx, name)
	if err != nil {
		if errors.Is(err, kerr.GroupAuthorizationFailed) {
			return nil, fmt.Errorf("group %q; %w", name, ErrNoGroupAccess)
		}

		return nil, fmt.Errorf("describe group; %w", err)
	}

	l, ok := lags[name]
	if !ok {
		return nil, fmt.Errorf("group %q; %w", name, ErrNotFound)
	}

	if l.DescribeErr != nil {
		if errors.Is(l.DescribeErr, kerr.GroupAuthorizationFailed) {
			return nil, fmt.Errorf("group %q; %w", name, ErrNoGroupAccess)
		}

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
			pl.NoAccess = errors.Is(p.Err, ErrNoTopicAccess)
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
	lags, err := c.groupLag(ctx, groups...)
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
				pl.NoAccess = errors.Is(p.Err, ErrNoTopicAccess)
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

	if _, err := c.admin.DeleteGroup(ctx, name); err != nil {
		return fmt.Errorf("delete group; %w", err)
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

	described, err := c.admin.DescribeGroups(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("describe group; %w", err)
	}

	// Committing to a group that does not exist would create it; a typo in
	// its name must not. New groups go through CreateGroup.
	g, ok := described[group]
	if !ok || errors.Is(g.Err, kerr.GroupIDNotFound) || g.Err == nil && g.State == "Dead" {
		return nil, fmt.Errorf("group %q; %w", group, ErrNotFound)
	}

	if !req.DryRun {
		// A failed describe has no members listed; that is not "idle".
		if g.Err != nil {
			return nil, fmt.Errorf("describe group; %w", g.Err)
		}

		if len(g.Members) > 0 {
			return nil, ErrConflict{fmt.Errorf("group %q has %d active members; stop the consumers first", group, len(g.Members))}
		}
	}

	var at *int64
	if req.Strategy == "timestamp" {
		at = &req.Timestamp
	}

	b, err := c.bounds(ctx, req.Topic, false, at)
	if err != nil {
		return nil, err
	}

	starts, ends, atTime := b.starts, b.ends, b.atTime

	// Without the committed offsets "from" is unknown and a shift would
	// move from the start of the log instead; never guess on a reset.
	current, err := c.admin.FetchOffsets(ctx, group)
	if err != nil {
		return nil, fmt.Errorf("fetch committed offsets; %w", err)
	}

	wanted := map[int32]bool{}
	for _, p := range req.Partitions {
		if p < 0 {
			return nil, ErrInvalid{fmt.Errorf("partition %d is not valid", p)}
		}

		wanted[p] = true
	}

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
	var (
		startList []kadm.ListedOffset
		boundErr  error
	)

	starts.Each(func(o kadm.ListedOffset) {
		if o.Topic != req.Topic || o.Partition < 0 || (len(wanted) > 0 && !wanted[o.Partition]) {
			return
		}

		if o.Err != nil || o.Offset < 0 {
			boundErr = errors.Join(boundErr, fmt.Errorf("partition %d: start offset unknown; %v", o.Partition, o.Err))

			return
		}

		startList = append(startList, o)
	})
	sort.Slice(startList, func(i, j int) bool { return startList[i].Partition < startList[j].Partition })

	found := map[int32]bool{}
	for _, s := range startList {
		found[s.Partition] = true
	}

	for p := range wanted {
		if !found[p] && boundErr == nil {
			return nil, ErrInvalid{fmt.Errorf("topic %q has no partition %d", req.Topic, p)}
		}
	}

	var plan []ResetPlan

	offsets := make(kadm.Offsets)

	for _, s := range startList {
		e, ok := ends.Lookup(req.Topic, s.Partition)
		if !ok || e.Err != nil || e.Offset < 0 {
			boundErr = errors.Join(boundErr, fmt.Errorf("partition %d: end offset unknown; %v", s.Partition, e.Err))

			continue
		}

		var target int64

		switch req.Strategy {
		case "earliest":
			target = s.Offset
		case "latest":
			target = e.Offset
		case "offset":
			target = min(max(req.Offset, s.Offset), e.Offset)
		case "offsets":
			target = min(max(targets[s.Partition], s.Offset), e.Offset)
		case "timestamp":
			// kadm gives the end offset when nothing is that new; an
			// error means the offset at that time is unknown.
			t, ok := atTime.Lookup(req.Topic, s.Partition)
			if !ok || t.Err != nil {
				var cause error
				if ok {
					cause = t.Err
				}

				boundErr = errors.Join(boundErr, fmt.Errorf("partition %d: offset at that time unknown; %v", s.Partition, cause))

				continue
			}

			target = e.Offset
			if t.Offset >= 0 {
				target = min(t.Offset, e.Offset)
			}
		case "shift":
			// Needs the committed offset, resolved below.
		default:
			return nil, ErrInvalid{fmt.Errorf("unknown strategy %q", req.Strategy)}
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

	if boundErr != nil {
		return nil, fmt.Errorf("nothing changed; %w", boundErr)
	}

	if len(plan) == 0 {
		return nil, fmt.Errorf("topic %q; %w", req.Topic, ErrNotFound)
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

	// A failed describe lists no members; that is not "idle".
	if g.Err != nil {
		return nil, fmt.Errorf("describe group; %w", g.Err)
	}

	if len(g.Members) > 0 {
		return nil, ErrConflict{fmt.Errorf("group %q has %d active members; stop the consumers first", req.Group, len(g.Members))}
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
	b, err := c.bounds(ctx, req.Topic, false, nil)
	if err != nil {
		return nil, err
	}

	starts, ends := b.starts, b.ends

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

		e, ok := ends.Lookup(req.Topic, po.Partition)
		if !ok || e.Err != nil {
			return nil, fmt.Errorf("end offset of partition %d unavailable", po.Partition)
		}

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
