package kafka

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dop251/goja"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rakunlabs/kafka-console/internal/script"
)

const (
	defaultLimit       = 50
	maxLimit           = 1000
	defaultMaxScan     = 10_000
	maxScanCap         = 1_000_000
	defaultTimeout     = 15 * time.Second
	maxTimeout         = 2 * time.Minute
	chunkSize          = 500
	pollTimeout        = 2 * time.Second
	maxEmptyPolls      = 2
	defaultValueCutoff = 64 * 1024
)

type Header struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding"`
}

type Message struct {
	Partition     int32     `json:"partition"`
	Offset        int64     `json:"offset"`
	Timestamp     time.Time `json:"timestamp"`
	Key           *string   `json:"key"`
	KeyEncoding   string    `json:"key_encoding,omitempty"`
	KeyFormat     string    `json:"key_format,omitempty"`
	KeySchemaID   int       `json:"key_schema_id,omitempty"`
	Value         *string   `json:"value"`
	ValueEncoding string    `json:"value_encoding,omitempty"`
	ValueFormat   string    `json:"value_format,omitempty"`
	ValueSchemaID int       `json:"value_schema_id,omitempty"`
	ValueBytes    int       `json:"value_bytes"`
	KeyBytes      int       `json:"key_bytes"`
	Truncated     bool      `json:"truncated"`
	Headers       []Header  `json:"headers"`
	DecodeError   string    `json:"decode_error,omitempty"`
}

type ConsumeRequest struct {
	// Partitions empty means every partition.
	Partitions []int32
	// Start is newest, oldest, offset or timestamp.
	Start     string
	Offset    int64
	Timestamp int64
	Limit     int
	MaxScan   int
	Filter    string
	// Search keeps messages whose key, value or headers contain this text,
	// case-insensitively. Applied before Filter, which is far more costly.
	Search  string
	Timeout time.Duration
	// Raw skips schema registry decoding.
	Raw bool
}

type ConsumeResult struct {
	Messages     []Message `json:"messages"`
	Scanned      int       `json:"scanned"`
	ScriptErrors int       `json:"script_errors"`
	FirstError   string    `json:"first_error,omitempty"`
	StoppedBy    string    `json:"stopped_by"`
	ElapsedMs    int64     `json:"elapsed_ms"`
}

type window struct {
	partition  int32
	start, end int64 // [start, end)
}

func (c *Client) Consume(ctx context.Context, topic string, req ConsumeRequest) (*ConsumeResult, error) {
	begin := time.Now()

	req.Limit = clamp(req.Limit, defaultLimit, maxLimit)
	req.MaxScan = clamp(req.MaxScan, defaultMaxScan, maxScanCap)

	if req.Timeout <= 0 {
		req.Timeout = defaultTimeout
	}

	req.Timeout = min(req.Timeout, maxTimeout)

	if req.Start == "" {
		req.Start = "newest"
	}

	var filter *script.Script
	if strings.TrimSpace(req.Filter) != "" {
		var err error

		filter, err = script.Compile(req.Filter, filterParameters...)
		if err != nil {
			return nil, err
		}
	}

	ctx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	windows, err := c.windows(ctx, topic, req)
	if err != nil {
		return nil, err
	}

	reader, err := c.newReader(topic)
	if err != nil {
		return nil, fmt.Errorf("create reader; %w", err)
	}
	defer reader.Close()

	stopGuard := script.Guard(ctx, filter, "filter timed out")
	defer stopGuard()

	result := &ConsumeResult{Messages: []Message{}}

	visit := func(rec *kgo.Record) bool {
		result.Scanned++

		d := c.decodeRecord(ctx, rec, req.Raw)

		if !textMatch(req.Search, rec, d) {
			return result.Scanned < req.MaxScan
		}

		if filter != nil {
			ok, err := filter.Call(filterArguments(filter, rec, d)...)
			if err != nil {
				if ctx.Err() != nil {
					result.StoppedBy = "timeout"

					return false
				}

				result.ScriptErrors++
				if result.FirstError == "" {
					result.FirstError = err.Error()
				}

				return result.Scanned < req.MaxScan
			}

			if !ok {
				return result.Scanned < req.MaxScan
			}
		}

		result.Messages = append(result.Messages, render(rec, d, defaultValueCutoff))

		if len(result.Messages) >= req.Limit {
			result.StoppedBy = "limit"

			return false
		}

		return result.Scanned < req.MaxScan
	}

	newestFirst := req.Start == "newest"

	for _, round := range rounds(windows, newestFirst) {
		if err := scan(ctx, reader, topic, round, newestFirst, visit); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				result.StoppedBy = "timeout"

				break
			}

			if ctx.Err() == nil {
				return nil, err
			}

			result.StoppedBy = "timeout"

			break
		}

		if ctx.Err() != nil {
			result.StoppedBy = "timeout"

			break
		}

		if result.StoppedBy != "" {
			break
		}

		if result.Scanned >= req.MaxScan {
			result.StoppedBy = "max_scan"

			break
		}
	}

	if result.StoppedBy == "" {
		result.StoppedBy = "end"
	}

	sort.SliceStable(result.Messages, func(i, j int) bool {
		a, b := result.Messages[i], result.Messages[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			if newestFirst {
				return a.Timestamp.After(b.Timestamp)
			}

			return a.Timestamp.Before(b.Timestamp)
		}

		if a.Partition != b.Partition {
			return a.Partition < b.Partition
		}

		if newestFirst {
			return a.Offset > b.Offset
		}

		return a.Offset < b.Offset
	})

	result.ElapsedMs = time.Since(begin).Milliseconds()

	return result, nil
}

func (c *Client) windows(ctx context.Context, topic string, req ConsumeRequest) ([]window, error) {
	starts, err := c.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets; %w", err)
	}

	ends, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets; %w", err)
	}

	var atTime kadm.ListedOffsets
	if req.Start == "timestamp" {
		atTime, err = c.admin.ListOffsetsAfterMilli(ctx, req.Timestamp, topic)
		if err != nil {
			return nil, fmt.Errorf("list offsets by time; %w", err)
		}
	}

	wanted := map[int32]bool{}
	for _, p := range req.Partitions {
		wanted[p] = true
	}

	var out []window

	starts.Each(func(s kadm.ListedOffset) {
		if s.Topic != topic || s.Err != nil || (len(wanted) > 0 && !wanted[s.Partition]) {
			return
		}

		e, ok := ends.Lookup(topic, s.Partition)
		if !ok || e.Err != nil {
			return
		}

		w := window{partition: s.Partition, start: s.Offset, end: e.Offset}

		switch req.Start {
		case "offset":
			w.start = min(max(req.Offset, w.start), w.end)
		case "timestamp":
			if t, ok := atTime.Lookup(topic, s.Partition); ok && t.Err == nil && t.Offset >= 0 {
				w.start = max(t.Offset, w.start)
			} else {
				w.start = w.end
			}
		}

		if w.start < w.end {
			out = append(out, w)
		}
	})

	if len(wanted) > 0 && len(out) == 0 && starts.Error() != nil {
		return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].partition < out[j].partition })

	return out, nil
}

// rounds splits windows into chunks; round N holds depth N of every
// partition, so a newest-first scan reads the whole topic breadth-first.
func rounds(windows []window, newestFirst bool) [][]window {
	var out [][]window

	for depth := 0; ; depth++ {
		var round []window

		for _, w := range windows {
			var chunk window

			if newestFirst {
				end := w.end - int64(depth)*chunkSize
				if end <= w.start {
					continue
				}

				chunk = window{partition: w.partition, start: max(w.start, end-chunkSize), end: end}
			} else {
				start := w.start + int64(depth)*chunkSize
				if start >= w.end {
					continue
				}

				chunk = window{partition: w.partition, start: start, end: min(w.end, start+chunkSize)}
			}

			round = append(round, chunk)
		}

		if len(round) == 0 {
			return out
		}

		out = append(out, round)
	}
}

func scan(ctx context.Context, client *kgo.Client, topic string, round []window, newestFirst bool, visit func(*kgo.Record) bool) error {
	active := make(map[int32]int64, len(round))
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
			end, ok := active[rec.Partition]
			if !ok {
				return
			}

			if rec.Offset >= end {
				delete(active, rec.Partition)

				return
			}

			if rec.Offset == end-1 {
				delete(active, rec.Partition)
			}

			buffered = append(buffered, rec)
		})
	}

	sort.SliceStable(buffered, func(i, j int) bool {
		a, b := buffered[i], buffered[j]
		if !a.Timestamp.Equal(b.Timestamp) {
			return a.Timestamp.After(b.Timestamp) == newestFirst
		}

		if a.Partition != b.Partition {
			return a.Partition < b.Partition
		}

		return (a.Offset > b.Offset) == newestFirst
	})

	for _, rec := range buffered {
		if !visit(rec) {
			return nil
		}
	}

	return nil
}

// ─── Decoding ─────────────────────────────────────────────

// recordView is a record with key and value decoded once, shared by the JS
// filter and the renderer.
type recordView struct {
	key, value *decoded
	err        string
}

func (c *Client) decodeRecord(ctx context.Context, rec *kgo.Record, raw bool) recordView {
	var v recordView
	if raw {
		return v
	}

	var errs []string

	if d, ok, err := c.decodeRegistry(ctx, rec.Key); ok {
		if err != nil {
			errs = append(errs, "key: "+err.Error())
		} else {
			v.key = d
		}
	}

	if d, ok, err := c.decodeRegistry(ctx, rec.Value); ok {
		if err != nil {
			errs = append(errs, "value: "+err.Error())
		} else {
			v.value = d
		}
	}

	v.err = strings.Join(errs, "; ")

	return v
}

// ─── JS filter ────────────────────────────────────────────

var filterParameters = []string{"value", "key", "headers", "partition", "offset", "timestamp", "raw"}

// CheckFilter reports the problems the message filter would be rejected for,
// without reading any data.
func CheckFilter(source string) []script.Problem {
	return script.Check(source, filterParameters...)
}

func filterArguments(s *script.Script, rec *kgo.Record, d recordView) []goja.Value {
	rt := s.Runtime()

	headers := make(map[string]any, len(rec.Headers))
	for _, h := range rec.Headers {
		headers[h.Key] = decodeText(h.Value)
	}

	ts, err := rt.New(rt.Get("Date"), rt.ToValue(rec.Timestamp.UnixMilli()))

	var tsValue goja.Value = goja.Null()
	if err == nil {
		tsValue = ts
	}

	var value, key any
	if d.value != nil {
		value = d.value.Doc
	} else {
		value = decodeValue(rec.Value)
	}

	if d.key != nil {
		key = d.key.Doc
	} else {
		key = decodeText(rec.Key)
	}

	return []goja.Value{
		s.Value(value),
		s.Value(key),
		s.Value(headers),
		s.Value(rec.Partition),
		s.Value(rec.Offset),
		tsValue,
		s.Value(string(rec.Value)),
	}
}

// decodeValue gives the filter a parsed document for JSON and a string for text.
func decodeValue(b []byte) any {
	if b == nil {
		return nil
	}

	var doc any
	if json.Valid(b) && json.Unmarshal(b, &doc) == nil {
		return doc
	}

	return decodeText(b)
}

func decodeText(b []byte) any {
	if b == nil {
		return nil
	}

	if utf8.Valid(b) {
		return string(b)
	}

	return b
}

// ─── Rendering ────────────────────────────────────────────

func render(rec *kgo.Record, d recordView, cutoff int) Message {
	m := Message{
		Partition:   rec.Partition,
		Offset:      rec.Offset,
		Timestamp:   rec.Timestamp,
		ValueBytes:  len(rec.Value),
		KeyBytes:    len(rec.Key),
		Headers:     make([]Header, 0, len(rec.Headers)),
		DecodeError: d.err,
	}

	switch {
	case d.key != nil:
		k := d.key.Text
		m.Key, m.KeyEncoding, m.KeyFormat, m.KeySchemaID = &k, "utf8", d.key.Format, d.key.SchemaID
	case rec.Key != nil:
		k, enc := encode(rec.Key)
		m.Key, m.KeyEncoding = &k, enc
	}

	switch {
	case d.value != nil:
		text := d.value.Text
		if len(text) > cutoff {
			text = text[:cutoff]
			m.Truncated = true
		}

		m.Value, m.ValueEncoding, m.ValueFormat, m.ValueSchemaID = &text, "utf8", d.value.Format, d.value.SchemaID
	case rec.Value != nil:
		value := rec.Value
		if len(value) > cutoff {
			value = value[:cutoff]
			m.Truncated = true
		}

		v, enc := encode(value)
		m.Value, m.ValueEncoding = &v, enc

		switch {
		case enc == "base64":
			m.ValueFormat = "binary"
		case !m.Truncated && json.Valid(rec.Value):
			m.ValueFormat = "json"
		default:
			m.ValueFormat = "text"
		}
	}

	for _, h := range rec.Headers {
		v, enc := encode(h.Value)
		m.Headers = append(m.Headers, Header{Key: h.Key, Value: v, Encoding: enc})
	}

	return m
}

func encode(b []byte) (string, string) {
	if utf8.Valid(b) {
		return string(b), "utf8"
	}

	return base64.StdEncoding.EncodeToString(b), "base64"
}

// ─── Produce ──────────────────────────────────────────────

type ProduceRequest struct {
	Key       *string  `json:"key"`
	Value     *string  `json:"value"`
	Headers   []Header `json:"headers"`
	Partition *int32   `json:"partition"`
	// KeySchema / ValueSchema serialize the JSON text through the registry.
	KeySchema   *EncodeRequest `json:"key_schema"`
	ValueSchema *EncodeRequest `json:"value_schema"`
	// Encoding of value: utf8 (default) or base64 for binary payloads.
	ValueEncoding string `json:"value_encoding"`
	KeyEncoding   string `json:"key_encoding"`
	// Compression for the produced batch: none, gzip, snappy, lz4, zstd.
	Compression string `json:"compression"`
}

type ProduceResult struct {
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
}

func payload(ctx context.Context, c *Client, text *string, encoding string, schema *EncodeRequest, what string) ([]byte, error) {
	if text == nil {
		return nil, nil
	}

	if schema != nil && (schema.Subject != "" || schema.ID > 0) {
		b, err := c.encodeRegistry(ctx, schema, []byte(*text))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", what, err)
		}

		return b, nil
	}

	if encoding == "base64" {
		b, err := base64.StdEncoding.DecodeString(strings.TrimSpace(*text))
		if err != nil {
			return nil, fmt.Errorf("%s: invalid base64; %w", what, err)
		}

		return b, nil
	}

	return []byte(*text), nil
}

func (c *Client) Produce(ctx context.Context, topic string, req ProduceRequest) (*ProduceResult, error) {
	if err := c.requireWritable(); err != nil {
		return nil, err
	}

	rec := &kgo.Record{Topic: topic}

	var err error

	if rec.Key, err = payload(ctx, c, req.Key, req.KeyEncoding, req.KeySchema, "key"); err != nil {
		return nil, ErrInvalid{err}
	}

	if rec.Value, err = payload(ctx, c, req.Value, req.ValueEncoding, req.ValueSchema, "value"); err != nil {
		return nil, ErrInvalid{err}
	}

	for _, h := range req.Headers {
		if h.Key == "" {
			continue
		}

		value := []byte(h.Value)
		if h.Encoding == "base64" {
			if value, err = base64.StdEncoding.DecodeString(h.Value); err != nil {
				return nil, ErrInvalid{fmt.Errorf("header %s: invalid base64; %w", h.Key, err)}
			}
		}

		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: value})
	}

	codec, err := compression(req.Compression)
	if err != nil {
		return nil, ErrInvalid{err}
	}

	producer, err := c.producer(req.Partition != nil, codec)
	if err != nil {
		return nil, fmt.Errorf("producer; %w", err)
	}

	if req.Partition != nil {
		rec.Partition = *req.Partition
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	out, err := producer.ProduceSync(ctx, rec).First()
	if err != nil {
		return nil, fmt.Errorf("produce; %w", err)
	}

	return &ProduceResult{Partition: out.Partition, Offset: out.Offset, Timestamp: out.Timestamp}, nil
}

func compression(name string) (string, error) {
	switch strings.ToLower(name) {
	case "", "none":
		return "", nil
	case "gzip", "snappy", "lz4", "zstd":
		return strings.ToLower(name), nil
	}

	return "", fmt.Errorf("unknown compression %q", name)
}

// ErrInvalid marks a request the caller can fix.
type ErrInvalid struct{ Err error }

func (e ErrInvalid) Error() string { return e.Err.Error() }
func (e ErrInvalid) Unwrap() error { return e.Err }

func clamp(v, def, maxV int) int {
	if v <= 0 {
		return def
	}

	return min(v, maxV)
}
