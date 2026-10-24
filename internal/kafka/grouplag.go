package kafka

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

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

	// Describe and committed offsets do not depend on each other; fetching
	// for a group that turns out not to be describable is cheap.
	var (
		described kadm.DescribedGroups
		err       error
		fetched   kadm.FetchOffsetsResponses
		wg        sync.WaitGroup
	)

	wg.Go(func() { described, err = c.admin.DescribeGroups(ctx, groups...) })
	wg.Go(func() { fetched = c.admin.FetchManyOffsets(ctx, groups...) })
	wg.Wait()

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

	for _, r := range fetched {
		if _, ok := live[r.Group]; !ok || r.Err == nil {
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
