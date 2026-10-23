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
