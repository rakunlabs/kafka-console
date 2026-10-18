package kafka

import (
	"context"
	"errors"
	"fmt"
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

	d := c.decodeRecord(ctx, rec, false)
	cutoff := len(rec.Value)
	if d.value != nil {
		cutoff = max(cutoff, len(d.value.Text))
	}
	m := render(rec, d, cutoff)

	return &m, nil
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

	details, err := c.admin.ListTopics(ctx, target)
	if err != nil {
		return nil, fmt.Errorf("describe topic; %w", err)
	}

	td, ok := details[target]
	if !ok || td.Err != nil {
		return nil, ErrInvalid{fmt.Errorf("target topic %q does not exist", target)}
	}

	if req.KeepPartition {
		for _, r := range req.Records {
			if _, ok := td.Partitions[r.Partition]; !ok {
				return nil, ErrInvalid{fmt.Errorf("target %q has no partition %d; turn off \"keep partition\"", target, r.Partition)}
			}
		}
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

		rec := &kgo.Record{Topic: target, Key: src.Key, Value: src.Value}
		rec.Headers = append(rec.Headers, src.Headers...)

		for _, h := range req.ExtraHeaders {
			if h.Key != "" {
				rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)})
			}
		}

		if req.MarkSource {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{
				Key:   "x-resent-from",
				Value: fmt.Appendf(nil, "%s/%d/%d", topic, src.Partition, src.Offset),
			})
		}

		if req.KeepPartition {
			rec.Partition = src.Partition
		}

		batch = append(batch, rec)
		index = append(index, i)
	}

	// Produced in the order given, which is the order the user saw them in,
	// so per-key order is preserved within the batch.
	results := producer.ProduceSync(pctx, batch...)

	for j, r := range results {
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

// fetchExact reads specific offsets. Each partition is read once from its
// lowest requested offset up to its highest.
func (c *Client) fetchExact(ctx context.Context, topic string, refs []RecordRef) (map[RecordRef]*kgo.Record, error) {
	want := map[RecordRef]bool{}
	bounds := map[int32][2]int64{}

	for _, r := range refs {
		want[r] = true

		b, ok := bounds[r.Partition]
		if !ok {
			b = [2]int64{r.Offset, r.Offset + 1}
		}

		b[0] = min(b[0], r.Offset)
		b[1] = max(b[1], r.Offset+1)
		bounds[r.Partition] = b
	}

	// Offsets outside what the partition still holds are reported missing
	// up front instead of waiting for records that will never arrive.
	starts, err := c.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets; %w", err)
	}

	ends, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets; %w", err)
	}

	round := make([]window, 0, len(bounds))

	for p, b := range bounds {
		lo, hi := b[0], b[1]

		if s, ok := starts.Lookup(topic, p); ok && s.Err == nil {
			lo = max(lo, s.Offset)
		}

		if e, ok := ends.Lookup(topic, p); ok && e.Err == nil {
			hi = min(hi, e.Offset)
		}

		if lo < hi {
			round = append(round, window{partition: p, start: lo, end: hi})
		}
	}

	found := make(map[RecordRef]*kgo.Record, len(refs))
	if len(round) == 0 {
		return found, nil
	}

	sort.Slice(round, func(i, j int) bool { return round[i].partition < round[j].partition })

	reader, err := c.newReader(topic)
	if err != nil {
		return nil, fmt.Errorf("reader; %w", err)
	}
	defer reader.Close()

	rctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	err = scan(rctx, reader, topic, round, false, func(rec *kgo.Record) bool {
		ref := RecordRef{Partition: rec.Partition, Offset: rec.Offset}
		if want[ref] {
			found[ref] = rec
		}

		return true
	})
	if err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return nil, err
	}

	return found, nil
}
