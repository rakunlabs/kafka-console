package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type window struct {
	partition  int32
	start, end int64 // [start, end)
}

func (c *Client) windows(ctx context.Context, topic string, req ConsumeRequest) ([]window, error) {
	// The fast path uses the time index. Exact keeps the captured end so
	// later offsets with older producer timestamps are checked too.
	until := req.Until
	if req.Exact {
		until = nil
	}

	b, err := c.bounds(ctx, topic, false, req.Since, until)
	if err != nil {
		return nil, err
	}

	wanted := partitionSet(req.Partitions)

	var out []window

	for _, p := range b.partitions() {
		if len(wanted) > 0 && !wanted[p] {
			continue
		}

		lo, hi, err := b.partition(p)
		if err != nil {
			continue
		}

		w := window{partition: p, start: lo, end: hi}

		if until != nil {
			w.end = min(w.end, timeOffset(b.until, topic, p, w.end))
		}

		switch req.Start {
		case StartLast:
			w.start = max(w.end-max(req.Offset, 0), w.start)
		case StartOffset:
			w.start = min(max(req.Offset, w.start), w.end)
		}

		// Nothing produced since then: the partition has nothing in range.
		if req.Since != nil {
			w.start = max(timeOffset(b.atTime, topic, p, w.end), w.start)
		}

		if req.After != nil {
			at, ok := req.After[p]
			if !ok {
				continue
			}

			if req.Start == StartNewest {
				w.end = min(w.end, at)
			} else {
				w.start = max(w.start, at)
			}
		}

		if w.start < w.end {
			out = append(out, w)
		}
	}

	if len(out) == 0 {
		starts := b.starts
		if err := starts.Error(); err != nil {
			// Every partition failed: say why instead of an empty result,
			// a missing topic as not found and a refused one as such.
			if errors.Is(err, kerr.UnknownTopicOrPartition) || len(starts) == 0 {
				return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
			}

			return nil, fmt.Errorf("topic %q; %w", topic, err)
		}

		if _, ok := starts[topic]; !ok {
			return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
		}

		for p := range wanted {
			if !b.has(p) {
				return nil, fmt.Errorf("topic %q partition %d; %w", topic, p, ErrNotFound)
			}
		}
	}

	return out, nil
}

// resumeFrom is where each partition continues: just past the last record
// it visited, or where its window began if none was. Partitions with nothing
// left are omitted.
func resumeFrom(windows []window, seen map[int32]int64, newestFirst bool) map[int32]int64 {
	out := map[int32]int64{}

	for _, w := range windows {
		at := w.end
		if !newestFirst {
			at = w.start
		}

		if o, ok := seen[w.partition]; ok {
			at = o
		}

		if newestFirst && at > w.start || !newestFirst && at < w.end {
			out[w.partition] = at
		}
	}

	return out
}

// minChunk keeps a round from shrinking to a few records per partition,
// where each round's fetch round trip would cost more than the records.
const minChunk = 32

// cursor walks windows breadth-first: each round takes the next chunk of
// every partition that has records left, so a newest-first scan sees the
// newest records of all partitions before older ones. Rounds are made one
// at a time and sized to what the caller still needs, so a small limit on
// a wide topic does not fetch chunkSize records from every partition.
type cursor struct {
	left        []window // what each partition has not read yet
	newestFirst bool
}

func newCursor(windows []window, newestFirst bool) *cursor {
	return &cursor{left: append([]window(nil), windows...), newestFirst: newestFirst}
}

// next returns the following round; perPartition gets the number of
// partitions with records left and returns how many to read from each.
// nil once every window is read or nothing more is needed.
func (c *cursor) next(perPartition func(live int64) int64) []window {
	live := int64(0)
	for _, w := range c.left {
		if w.start < w.end {
			live++
		}
	}

	if live == 0 {
		return nil
	}

	want := perPartition(live)
	if want <= 0 {
		return nil
	}

	size := min(max(want, minChunk), chunkSize)
	round := make([]window, 0, live)

	for i := range c.left {
		w := &c.left[i]
		if w.start >= w.end {
			continue
		}

		if c.newestFirst {
			from := max(w.start, w.end-size)
			round = append(round, window{partition: w.partition, start: from, end: w.end})
			w.end = from
		} else {
			to := min(w.end, w.start+size)
			round = append(round, window{partition: w.partition, start: w.start, end: to})
			w.start = to
		}
	}

	return round
}

// errIncomplete says a scan gave up on partitions that stopped sending
// records before the end of their window, as a slow or moving leader can.
// What was visited is contiguous per partition, so the read can continue
// from where it stopped.
var errIncomplete = errors.New("the broker sent no more records before the end of the range")

func scan(ctx context.Context, client *kgo.Client, topic string, round []window, newestFirst bool, visit func(*kgo.Record) bool) error {
	active := make(windowEnds, len(round))
	offsets := make(map[int32]kgo.Offset, len(round))
	parts := make([]int32, 0, len(round))

	for _, w := range round {
		active[w.partition] = w.end
		offsets[w.partition] = kgo.NewOffset().At(w.start)
		parts = append(parts, w.partition)
	}

	client.AddConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets})
	defer client.RemoveConsumePartitions(map[string][]int32{topic: parts})

	// Records arrive oldest first per partition; buffer the round so the
	// visit order follows the requested direction across partitions.
	var buffered []*kgo.Record

	empty := 0

	for len(active) > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}

		pctx, cancel := context.WithTimeout(ctx, pollTimeout)
		fetches := client.PollFetches(pctx)
		cancel()

		if err := fetches.Err0(); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			if !errors.Is(err, context.DeadlineExceeded) {
				return fmt.Errorf("fetch from %s; %w", topic, err)
			}
		}

		if fetches.Empty() {
			empty++
			if empty >= maxEmptyPolls {
				break
			}

			continue
		}

		empty = 0

		fetches.EachRecord(func(rec *kgo.Record) {
			if keep, _ := active.accept(rec); keep {
				buffered = append(buffered, rec)
			}
		})
	}

	// Merge the partitions by timestamp, but keep each partition in offset
	// order even when its timestamps are not (producers set them): a read
	// that stops mid-round then has visited a contiguous run per partition,
	// which is what lets it continue without skipping records.
	//
	// A partition left unfinished arrived from its window's start, so read
	// newest first its records would not join what was visited before;
	// they are left for the continued read instead.
	byPart := map[int32][]*kgo.Record{}
	for _, rec := range buffered {
		if _, unfinished := active[rec.Partition]; unfinished && newestFirst {
			continue
		}

		byPart[rec.Partition] = append(byPart[rec.Partition], rec)
	}

	heads := make([][]*kgo.Record, 0, len(byPart))
	for _, rs := range byPart {
		sort.Slice(rs, func(i, j int) bool { return (rs[i].Offset > rs[j].Offset) == newestFirst })
		heads = append(heads, rs)
	}

	for len(heads) > 0 {
		best := 0

		for i := 1; i < len(heads); i++ {
			a, b := heads[i][0], heads[best][0]
			if !a.Timestamp.Equal(b.Timestamp) {
				if a.Timestamp.After(b.Timestamp) == newestFirst {
					best = i
				}
			} else if a.Partition < b.Partition {
				best = i
			}
		}

		rec := heads[best][0]
		if heads[best] = heads[best][1:]; len(heads[best]) == 0 {
			heads = append(heads[:best], heads[best+1:]...)
		}

		if !visit(rec) {
			return nil
		}
	}

	if len(active) > 0 {
		return errIncomplete
	}

	return nil
}
