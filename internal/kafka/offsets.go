package kafka

import (
	"context"
	"fmt"
	"slices"
	"sync"

	"github.com/twmb/franz-go/pkg/kadm"
)

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
	var (
		s, e       kadm.ListedOffsets
		errS, errE error
		both       sync.WaitGroup
	)

	both.Go(func() { s, errS = c.admin.ListStartOffsets(ctx, topics...) })
	both.Go(func() { e, errE = c.admin.ListEndOffsets(ctx, topics...) })
	both.Wait()
	if errS == nil && errE == nil {
		return s, e
	}

	var (
		mu  sync.Mutex
		wg  sync.WaitGroup
		sem = make(chan struct{}, offsetsParallel)
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

// offsetBounds is a topic's start and end offsets, plus the offsets at a
// time and just past an end time when asked for. With committed, ends are
// last stable offsets instead of high watermarks.
type offsetBounds struct {
	topic                       string
	starts, ends, atTime, until kadm.ListedOffsets
}

// bounds lists a topic's offsets; each listing is a separate round trip, so
// they run in parallel. at lists the first offsets at or after a time; until
// the first ones after a time, the exclusive end of a range up to it.
func (c *Client) bounds(ctx context.Context, topic string, committed bool, at, until *int64) (offsetBounds, error) {
	var (
		b                      = offsetBounds{topic: topic}
		errS, errE, errT, errU error
		wg                     sync.WaitGroup
	)

	wg.Go(func() { b.starts, errS = c.admin.ListStartOffsets(ctx, topic) })
	wg.Go(func() {
		if committed {
			b.ends, errE = c.admin.ListCommittedOffsets(ctx, topic)
		} else {
			b.ends, errE = c.admin.ListEndOffsets(ctx, topic)
		}
	})

	if at != nil {
		wg.Go(func() { b.atTime, errT = c.admin.ListOffsetsAfterMilli(ctx, *at, topic) })
	}

	if until != nil {
		wg.Go(func() { b.until, errU = c.admin.ListOffsetsAfterMilli(ctx, *until+1, topic) })
	}

	wg.Wait()

	switch {
	case errS != nil:
		return b, fmt.Errorf("list start offsets; %w", accessErr(errS))
	case errE != nil:
		return b, fmt.Errorf("list end offsets; %w", accessErr(errE))
	case errT != nil:
		return b, fmt.Errorf("list offsets by time; %w", accessErr(errT))
	case errU != nil:
		return b, fmt.Errorf("list offsets by time; %w", accessErr(errU))
	}

	return b, nil
}

// partitions lists, in order, every partition the listing names, those
// whose offsets failed included; partition says why.
func (b offsetBounds) partitions() []int32 {
	var out []int32

	b.starts.Each(func(o kadm.ListedOffset) {
		if o.Topic == b.topic && o.Partition >= 0 {
			out = append(out, o.Partition)
		}
	})

	slices.Sort(out)

	return out
}

// has reports whether the listing names partition p.
func (b offsetBounds) has(p int32) bool {
	_, ok := b.starts.Lookup(b.topic, p)

	return ok && p >= 0
}

// partition is p's retained range [lo, hi). A partition the topic lacks is
// noPartition; one whose offsets are unknown says which and why.
func (b offsetBounds) partition(p int32) (lo, hi int64, err error) {
	s, ok := b.starts.Lookup(b.topic, p)
	if !ok || p < 0 {
		return 0, 0, noPartition(b.topic, p)
	}

	if s.Err != nil || s.Offset < 0 {
		return 0, 0, offsetUnknown(p, "start offset", s.Err)
	}

	e, ok := b.ends.Lookup(b.topic, p)
	if !ok || e.Err != nil || e.Offset < 0 {
		return 0, 0, offsetUnknown(p, "end offset", e.Err)
	}

	return s.Offset, e.Offset, nil
}

// at is p's first offset at or after the listed time; kadm answers -1 or the
// end offset when nothing is that new. An error means it is unknown.
func (b offsetBounds) at(p int32) (int64, error) {
	t, ok := b.atTime.Lookup(b.topic, p)
	if !ok || t.Err != nil {
		return -1, offsetUnknown(p, "offset at that time", t.Err)
	}

	return t.Offset, nil
}

func noPartition(topic string, p int32) error {
	return ErrInvalid{fmt.Errorf("topic %q has no partition %d", topic, p)}
}

func offsetUnknown(p int32, what string, cause error) error {
	if cause == nil {
		return fmt.Errorf("partition %d: %s unknown", p, what)
	}

	return fmt.Errorf("partition %d: %s unknown; %w", p, what, accessErr(cause))
}

// timeOffset is the first offset at or after a listed time; a partition with
// nothing that late resolves to its end.
func timeOffset(listed kadm.ListedOffsets, topic string, p int32, end int64) int64 {
	if t, ok := listed.Lookup(topic, p); ok && t.Err == nil && t.Offset >= 0 {
		return t.Offset
	}

	return end
}

// partitionSet is ps as a set; empty when ps is, which callers read as all.
func partitionSet(ps []int32) map[int32]bool {
	out := make(map[int32]bool, len(ps))
	for _, p := range ps {
		out[p] = true
	}

	return out
}

// offsetsParallel bounds the per-topic offset requests of the group lag
// fallback, a Kafka load unrelated to the registry's.
const offsetsParallel = 8
