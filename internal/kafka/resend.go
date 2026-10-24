package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// maxResend bounds one resend request; a replay larger than this belongs in a
// purpose-built tool, not a console button.
const maxResend = 1000

const (
	// resendTimeout bounds writing one resend's copies.
	resendTimeout = 30 * time.Second
	// fetchExactTimeout bounds reading the records a resend or a single
	// message view asks for.
	fetchExactTimeout = 20 * time.Second
)

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

	rec, err := c.fetchOne(ctx, topic, ref)
	if err != nil {
		return nil, err
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

	return c.fetchOne(ctx, topic, ref)
}

// errGone says a wanted record was read past, or lies outside what its
// partition still holds.
const errGone = "message no longer exists (deleted by retention or compaction)"

// fetchOne reads one exact record.
func (c *Client) fetchOne(ctx context.Context, topic string, ref RecordRef) (*kgo.Record, error) {
	records, err := c.fetchExact(ctx, topic, []RecordRef{ref})
	if rec, ok := records[ref]; ok {
		return rec, nil
	}

	if err != nil {
		return nil, err
	}

	return nil, fmt.Errorf("%s: %w", errGone, ErrNotFound)
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

	// A timeout still sends what was read; the rest is reported as not read.
	records, err := c.fetchExact(ctx, topic, req.Records)
	timedOut := errors.Is(err, context.DeadlineExceeded) || errors.Is(err, errIncomplete)
	if err != nil && !timedOut {
		return nil, err
	}

	producer, err := c.producer(req.KeepPartition, "")
	if err != nil {
		return nil, fmt.Errorf("producer; %w", err)
	}

	pctx, cancel := context.WithTimeout(ctx, resendTimeout)
	defer cancel()

	out := make([]ResendResult, len(req.Records))
	batch := make([]*kgo.Record, 0, len(req.Records))
	index := make([]int, 0, len(req.Records))

	for i, ref := range req.Records {
		out[i] = ResendResult{Source: ref, Partition: -1, Offset: -1}

		src, ok := records[ref]
		if !ok {
			out[i].Error = errGone
			if timedOut {
				out[i].Error = "not read before the read stopped; retry"
			}

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
	for j, r := range produceInOrder(pctx, producer, batch) {
		i := index[j]
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
// in one window; each round reads the next window of every partition. A
// record missing from the result was not found; with a timeout error, it
// may not have been reached.
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
	b, err := c.bounds(ctx, topic, false, nil, nil)
	if err != nil {
		return nil, err
	}

	// Windows per partition, in offset order.
	windows := map[int32][]window{}
	depth := 0

	for p, offs := range byPartition {
		// A partition the topic lacks, or whose bounds are unknown, has
		// none of its offsets: reading it would only wait out empty polls.
		lo, hi, err := b.partition(p)
		if err != nil {
			continue
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

	rctx, cancel := context.WithTimeout(ctx, fetchExactTimeout)
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
			if rctx.Err() != nil && ctx.Err() == nil {
				return found, fmt.Errorf("read %s; %w", topic, context.DeadlineExceeded)
			}

			return found, err
		}
	}

	sound = true

	return found, nil
}
