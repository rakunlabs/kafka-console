package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// maxResend bounds one resend request; a replay larger than this belongs in a
// purpose-built tool, not a console button.
const maxResend = 1000

type RecordRef struct {
	Partition int32 `json:"partition"`
	Offset    int64 `json:"offset"`
}

type ResendRequest struct {
	Records []RecordRef `json:"records"`
	// Target topic; empty means the source topic.
	Target string `json:"target"`
	// KeepPartition writes each record to the same partition number it came
	// from; otherwise the target's partitioner decides (by key).
	KeepPartition bool `json:"keep_partition"`
	// Headers added to every resent record.
	ExtraHeaders []Header `json:"extra_headers"`
	// MarkSource adds x-resent-from: <topic>/<partition>/<offset> to each
	// record, naming the exact original it was copied from.
	MarkSource bool `json:"mark_source"`
}

type ResendResult struct {
	Source     RecordRef `json:"source"`
	Partition  int32     `json:"partition"`
	Offset     int64     `json:"offset"`
	Error      string    `json:"error,omitempty"`
	ProducedAt time.Time `json:"produced_at,omitzero"`
}

// Message reads one exact record without the browse view's payload cutoff.
// A compacted or expired offset must not silently resolve to the next record.
func (c *Client) Message(ctx context.Context, topic string, ref RecordRef) (*Message, error) {
	if ref.Partition < 0 || ref.Offset < 0 {
		return nil, ErrInvalid{errors.New("partition and offset must be non-negative")}
	}

	records, err := c.fetchExact(ctx, topic, []RecordRef{ref})
	if err != nil {
		return nil, err
	}

	rec, ok := records[ref]
	if !ok {
		return nil, fmt.Errorf("message no longer exists (deleted by retention or compaction): %w", ErrNotFound)
	}

	d := c.decodeRecord(ctx, rec, false, DecodeAs{})
	cutoff := len(rec.Value)
	if d.value != nil {
		cutoff = max(cutoff, len(d.value.Text))
	}
	m := render(rec, d, cutoff)

	return &m, nil
}

// RawRecord reads one exact record's bytes, for downloading a key or value
// too large (or too binary) to show.
func (c *Client) RawRecord(ctx context.Context, topic string, ref RecordRef) (*kgo.Record, error) {
	if ref.Partition < 0 || ref.Offset < 0 {
		return nil, ErrInvalid{errors.New("partition and offset must be non-negative")}
	}

	records, err := c.fetchExact(ctx, topic, []RecordRef{ref})
	if err != nil {
		return nil, err
	}

	rec, ok := records[ref]
	if !ok {
		return nil, fmt.Errorf("message no longer exists (deleted by retention or compaction): %w", ErrNotFound)
	}

	return rec, nil
}

// Resend copies records byte for byte, key, value and headers included, to a
// topic. Bytes never pass through the browser, so registry-encoded and binary
// payloads arrive exactly as they were written.
func (c *Client) Resend(ctx context.Context, topic string, req ResendRequest) ([]ResendResult, error) {
	if err := c.requireWritable(); err != nil {
		return nil, err
	}

	if len(req.Records) == 0 {
		return nil, ErrInvalid{errors.New("select at least one message")}
	}

	if len(req.Records) > maxResend {
		return nil, ErrInvalid{fmt.Errorf("at most %d messages per resend", maxResend)}
	}

	target := req.Target
	if target == "" {
		target = topic
	}

	partitions := make([]int32, 0, len(req.Records))
	for _, r := range req.Records {
		partitions = append(partitions, r.Partition)
	}

	if err := c.checkTarget(ctx, target, req.KeepPartition, partitions); err != nil {
		return nil, err
	}

	records, err := c.fetchExact(ctx, topic, req.Records)
	if err != nil {
		return nil, err
	}

	producer, err := c.producer(req.KeepPartition, "")
	if err != nil {
		return nil, fmt.Errorf("producer; %w", err)
	}

	pctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	out := make([]ResendResult, len(req.Records))
	batch := make([]*kgo.Record, 0, len(req.Records))
	index := make([]int, 0, len(req.Records))

	for i, ref := range req.Records {
		out[i] = ResendResult{Source: ref, Partition: -1, Offset: -1}

		src, ok := records[ref]
		if !ok {
			out[i].Error = "message no longer exists (deleted by retention or compaction)"

			continue
		}

		rec := copyRecord(src, replayOptions{
			source: topic, target: target, keepPartition: req.KeepPartition,
			markSource: req.MarkSource, extraHeaders: req.ExtraHeaders,
		})

		batch = append(batch, rec)
		index = append(index, i)
	}

	// Produced in the order given, which is the order the user saw them in,
	// so per-key order is preserved within the batch.
	// Results arrive in completion order, not the order produced.
	at := make(map[*kgo.Record]int, len(batch))
	for j, rec := range batch {
		at[rec] = index[j]
	}

	for _, r := range producer.ProduceSync(pctx, batch...) {
		i := at[r.Record]
		if r.Err != nil {
			out[i].Error = r.Err.Error()

			continue
		}

		out[i].Partition = r.Record.Partition
		out[i].Offset = r.Record.Offset
		out[i].ProducedAt = r.Record.Timestamp
	}

	return out, nil
}

// exactGap is how far apart two wanted offsets of a partition may be and
// still be read in one pass; farther ones get their own window, so picking
// offsets 0 and 10,000,000 reads two records, not everything between.
const exactGap = chunkSize

// fetchExact reads specific offsets. Nearby offsets of a partition are read
// in one window; each round reads the next window of every partition.
func (c *Client) fetchExact(ctx context.Context, topic string, refs []RecordRef) (map[RecordRef]*kgo.Record, error) {
	want := map[RecordRef]bool{}
	byPartition := map[int32][]int64{}

	for _, r := range refs {
		if !want[r] {
			want[r] = true
			byPartition[r.Partition] = append(byPartition[r.Partition], r.Offset)
		}
	}

	// Offsets outside what the partition still holds are reported missing
	// up front instead of waiting for records that will never arrive.
	b, err := c.bounds(ctx, topic, false, nil)
	if err != nil {
		return nil, err
	}

	starts, ends := b.starts, b.ends

	// Windows per partition, in offset order.
	windows := map[int32][]window{}
	depth := 0

	for p, offs := range byPartition {
		lo, hi := int64(0), int64(math.MaxInt64)
		if s, ok := starts.Lookup(topic, p); ok && s.Err == nil {
			lo = s.Offset
		}

		if e, ok := ends.Lookup(topic, p); ok && e.Err == nil {
			hi = e.Offset
		}

		slices.Sort(offs)

		var ws []window

		for _, o := range offs {
			if o < lo || o >= hi {
				continue
			}

			if n := len(ws); n > 0 && o-ws[n-1].end < exactGap {
				ws[n-1].end = o + 1

				continue
			}

			ws = append(ws, window{partition: p, start: o, end: o + 1})
		}

		if len(ws) > 0 {
			windows[p] = ws
			depth = max(depth, len(ws))
		}
	}

	found := make(map[RecordRef]*kgo.Record, len(refs))
	if depth == 0 {
		return found, nil
	}

	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	reader, release, err := c.scanReader(rctx, topic)
	if err != nil {
		return nil, fmt.Errorf("reader; %w", err)
	}

	sound := false
	defer func() { release(sound && rctx.Err() == nil) }()

	for i := range depth {
		round := make([]window, 0, len(windows))
		for _, ws := range windows {
			if i < len(ws) {
				round = append(round, ws[i])
			}
		}

		sort.Slice(round, func(a, b int) bool { return round[a].partition < round[b].partition })

		err = scan(rctx, reader, topic, round, false, func(rec *kgo.Record) bool {
			ref := RecordRef{Partition: rec.Partition, Offset: rec.Offset}
			if want[ref] {
				found[ref] = rec
			}

			return true
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}

			return nil, err
		}
	}

	sound = true

	return found, nil
}
