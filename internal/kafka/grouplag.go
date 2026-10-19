package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// ErrNoTopicAccess marks lag that could not be computed because the console's
// credentials may not read the topic's offsets. The group itself is shown;
// only these partitions carry the error.
var ErrNoTopicAccess = errors.New("no access to this topic")

// groupLag is kadm.Client.Lag without its all-or-nothing failure: kadm drops
// the whole result when any one topic of any group is not authorized, so a
// user who may see a group but not every topic it reads saw nothing at all.
//
// Here a group that cannot be described or whose offsets cannot be fetched
// keeps that error on its own entry, and offsets are listed per topic so an
// unauthorized topic only marks its own partitions with ErrNoTopicAccess.
// Only failures that leave no group to show are returned as errors.
func (c *Client) groupLag(ctx context.Context, groups ...string) (kadm.DescribedGroupLags, error) {
	out := make(kadm.DescribedGroupLags, len(groups))
	if len(groups) == 0 {
		return out, nil
	}

	described, err := c.admin.DescribeGroups(ctx, groups...)
	if err != nil {
		var se *kadm.ShardErrors
		if !errors.As(err, &se) || se.AllFailed {
			return nil, fmt.Errorf("describe groups; %w", err)
		}
		// Partial: groups on failing coordinators are missing below.
	}

	// Groups the console may not describe still appear, with the reason.
	for _, g := range groups {
		if _, ok := described[g]; !ok {
			out[g] = kadm.DescribedGroupLag{Group: g, DescribeErr: describeErr(err)}
		}
	}

	live := make(kadm.DescribedGroups, len(described))
	for name, g := range described {
		out[name] = kadm.DescribedGroupLag{
			Group:        g.Group,
			Coordinator:  g.Coordinator,
			State:        g.State,
			ProtocolType: g.ProtocolType,
			Protocol:     g.Protocol,
			Members:      g.Members,
			DescribeErr:  g.Err,
		}
		if g.Err == nil {
			live[name] = g
		}
	}

	if len(live) == 0 {
		return out, nil
	}

	names := make([]string, 0, len(live))
	for name := range live {
		names = append(names, name)
	}

	fetched := c.admin.FetchManyOffsets(ctx, names...)
	for _, r := range fetched {
		if r.Err == nil {
			continue
		}
		l := out[r.Group]
		l.FetchErr = r.Err
		out[r.Group] = l
		delete(live, r.Group)
	}

	if len(live) == 0 {
		return out, nil
	}

	topics := live.AssignedPartitions()
	topics.MergeTopics(live.JoinTopics())
	for name := range live {
		topics.Merge(fetched[name].CommittedPartitions())
	}

	starts, ends := c.listOffsetsPerTopic(ctx, topics)

	for name, g := range live {
		l := out[name]
		l.Lag = kadm.CalculateGroupLagWithStartOffsets(g, fetched[name].Fetched, starts, ends)
		out[name] = l
	}

	return out, nil
}

// describeErr is the reason a requested group is missing from a describe.
func describeErr(err error) error {
	if err != nil {
		return err
	}

	return kerr.GroupIDNotFound
}

// listOffsetsPerTopic lists start and end offsets of the given partitions.
// When the combined listing fails it retries one topic at a time, so an
// unauthorized or missing topic records its error on its own partitions
// instead of failing the listing of every other topic.
func (c *Client) listOffsetsPerTopic(ctx context.Context, set kadm.TopicsSet) (starts, ends kadm.ListedOffsets) {
	starts, ends = make(kadm.ListedOffsets), make(kadm.ListedOffsets)
	topics := set.Topics()
	if len(topics) == 0 {
		return starts, ends
	}

	// One combined request first: the common case where every topic is
	// readable costs two round trips, as before.
	s, errS := c.admin.ListStartOffsets(ctx, topics...)
	e, errE := c.admin.ListEndOffsets(ctx, topics...)
	if errS == nil && errE == nil {
		return s, e
	}

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, registryParallel)
	)

	for _, t := range topics {
		wg.Go(func() {
			sem <- struct{}{}
			defer func() { <-sem }()

			ts, errS := c.admin.ListStartOffsets(ctx, t)
			te, errE := c.admin.ListEndOffsets(ctx, t)

			mu.Lock()
			defer mu.Unlock()

			if errS == nil {
				starts[t] = ts[t]
			}
			if errE == nil {
				ends[t] = te[t]
				return
			}

			// The lag calculation takes a partition's error from its end
			// offset, so the cause shows on each partition of this topic.
			failed := make(map[int32]kadm.ListedOffset, len(set[t]))
			for p := range set[t] {
				failed[p] = kadm.ListedOffset{Topic: t, Partition: p, Err: accessErr(errE)}
			}
			ends[t] = failed
		})
	}

	wg.Wait()

	return starts, ends
}

func accessErr(err error) error {
	if errors.Is(err, kerr.TopicAuthorizationFailed) {
		return ErrNoTopicAccess
	}

	return err
}
