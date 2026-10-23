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
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rakunlabs/kafka-console/internal/script"
)

// MaxLimit is the most messages one read returns.
const MaxLimit = 1000

const (
	defaultLimit   = 50
	maxLimit       = MaxLimit
	defaultTimeout = 15 * time.Second
	maxTimeout     = 2 * time.Minute
	// A streamed read shows its progress and can be stopped by its client,
	// so it may run much longer than a plain one.
	maxStreamTimeout = 30 * time.Minute
	chunkSize        = 500
	// roundBudget bounds the records one round buffers across partitions,
	// so a search over a topic with many partitions does not hold
	// chunkSize records of each of them at once.
	roundBudget        = 20_000
	pollTimeout        = 2 * time.Second
	maxEmptyPolls      = 2
	defaultValueCutoff = 64 * 1024
)

type Header struct {
	Key      string `json:"key"`
	Value    string `json:"value"`
	Encoding string `json:"encoding"`
	Null     bool   `json:"null,omitempty"`
}

type Message struct {
	Partition          int32     `json:"partition"`
	Offset             int64     `json:"offset"`
	Timestamp          time.Time `json:"timestamp"`
	Key                *string   `json:"key"`
	KeyEncoding        string    `json:"key_encoding,omitempty"`
	KeyFormat          string    `json:"key_format,omitempty"`
	KeySchemaID        int       `json:"key_schema_id,omitempty"`
	KeySchemaMessage   string    `json:"key_schema_message,omitempty"`
	Value              *string   `json:"value"`
	ValueEncoding      string    `json:"value_encoding,omitempty"`
	ValueFormat        string    `json:"value_format,omitempty"`
	ValueSchemaID      int       `json:"value_schema_id,omitempty"`
	ValueSchemaMessage string    `json:"value_schema_message,omitempty"`
	ValueBytes         int       `json:"value_bytes"`
	KeyBytes           int       `json:"key_bytes"`
	Truncated          bool      `json:"truncated"`
	Headers            []Header  `json:"headers"`
	DecodeError        string    `json:"decode_error,omitempty"`
}

type ConsumeRequest struct {
	// Partitions empty means every partition.
	Partitions []int32
	// Start is newest, oldest, offset, timestamp or last (the last Offset
	// records of each partition, read oldest first).
	Start     string
	Offset    int64
	Timestamp int64
	// Since and Until bound the read to a time range in unix milliseconds,
	// both inclusive, in any direction; a timestamp start sets Since.
	// Records whose own timestamp falls outside it are skipped, as
	// producers may set timestamps out of order.
	Since, Until *int64
	// Exact checks every later offset for out-of-order producer timestamps.
	// Otherwise Until uses Kafka's time index to keep large ranges fast.
	Exact bool
	// After continues a previous read: per partition, where it stopped (its
	// ConsumeResult.Next). Partitions missing from it are done.
	After  map[int32]int64
	Limit  int
	Filter string
	// Search keeps messages whose key, value or headers contain this text,
	// case-insensitively. Applied before Filter, which is far more costly.
	Search  string
	Timeout time.Duration
	// Raw skips schema registry decoding.
	Raw bool
	// As reads keys and values in a chosen format instead of guessing.
	As DecodeAs
}

type ConsumeResult struct {
	Messages     []Message `json:"messages"`
	Scanned      int       `json:"scanned"`
	ScriptErrors int       `json:"script_errors"`
	FirstError   string    `json:"first_error,omitempty"`
	StoppedBy    string    `json:"stopped_by"`
	ElapsedMs    int64     `json:"elapsed_ms"`
	// Next is where to continue, per partition with records left to read in
	// the same direction; empty once everything was read. Newest-first it is
	// the end (exclusive) of what is still unread, otherwise its start.
	Next map[int32]int64 `json:"next"`
}

type window struct {
	partition  int32
	start, end int64 // [start, end)
}

// ConsumeEvent is one step of a streamed read: a matched message, or a
// progress report (Message nil) with where a read stopped now would continue.
// Next is only on progress reports; a client stopping later advances it past
// the messages it got since.
type ConsumeEvent struct {
	Message *Message
	Scanned int
	Next    map[int32]int64
}

// consumeProgressEvery spaces the progress events of a streamed read.
const consumeProgressEvery = 500 * time.Millisecond

func (c *Client) Consume(ctx context.Context, topic string, req ConsumeRequest) (*ConsumeResult, error) {
	return c.consume(ctx, topic, req, nil)
}

// ConsumeStream is Consume that hands each match to emit as soon as it is
// found, in scan order, instead of collecting them; the result holds no
// messages. emit returning false stops the read.
func (c *Client) ConsumeStream(ctx context.Context, topic string, req ConsumeRequest, emit func(ConsumeEvent) bool) (*ConsumeResult, error) {
	return c.consume(ctx, topic, req, emit)
}

func (c *Client) consume(ctx context.Context, topic string, req ConsumeRequest, emit func(ConsumeEvent) bool) (*ConsumeResult, error) {
	begin := time.Now()

	if err := req.As.validate(); err != nil {
		return nil, err
	}

	req.Limit = clamp(req.Limit, defaultLimit, maxLimit)

	limitTime := maxTimeout
	if emit != nil {
		limitTime = maxStreamTimeout
	}

	if req.Timeout <= 0 {
		req.Timeout = defaultTimeout
		if emit != nil {
			req.Timeout = maxStreamTimeout
		}
	}

	req.Timeout = min(req.Timeout, limitTime)

	if req.Start == "timestamp" {
		since := req.Timestamp
		if req.Since != nil {
			since = max(since, *req.Since)
		}

		req.Since = &since
	}

	if req.Since != nil && req.Until != nil && *req.Until < *req.Since {
		return nil, ErrInvalid{errors.New("the end of the time range is before its start")}
	}

	if req.Start == "" {
		req.Start = "newest"
	}

	filter, err := compileFilter(req.Filter)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	windows, err := c.windows(ctx, topic, req)
	if err != nil {
		return nil, err
	}

	reader, release, err := c.scanReader(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("create reader; %w", err)
	}

	sound := false
	defer func() { release(sound) }()

	stopGuard := script.Guard(ctx, filter, "filter timed out")
	defer stopGuard()

	result := &ConsumeResult{Messages: []Message{}}

	newestFirst := req.Start == "newest"

	// Per partition, where a continued read picks up: newest-first the
	// lowest offset visited, otherwise one past the highest. scan visits each
	// partition's records in offset order (see scan), so everything between
	// the window's edge and this point was visited.
	seen := map[int32]int64{}

	matched := 0
	lastProgress := time.Now()

	// progress reports how far a streamed read got, so a client that stops
	// it can still continue from there.
	progress := func() bool {
		lastProgress = time.Now()
		if emit(ConsumeEvent{Scanned: result.Scanned, Next: resumeFrom(windows, seen, newestFirst)}) {
			return true
		}

		result.StoppedBy = "cancelled"

		return false
	}

	// The read's own deadline is a timeout; the caller going away is not.
	stopped := func() string {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "timeout"
		}

		return "cancelled"
	}

	visit := func(rec *kgo.Record) bool {
		// Before this record counts as seen: a read stopped right after the
		// report continues with it.
		if emit != nil && time.Since(lastProgress) >= consumeProgressEvery && !progress() {
			return false
		}

		result.Scanned++

		if newestFirst {
			seen[rec.Partition] = rec.Offset
		} else {
			seen[rec.Partition] = rec.Offset + 1
		}

		if !inTime(req, rec) {
			return true
		}

		d := c.decodeRecord(ctx, rec, req.Raw, req.As)

		if !textMatch(req.Search, rec, d) {
			return true
		}

		if filter != nil {
			ok, err := filter.Call(filterArguments(filter, rec, d)...)
			if err != nil {
				if ctx.Err() != nil {
					result.StoppedBy = stopped()

					return false
				}

				result.ScriptErrors++
				if result.FirstError == "" {
					result.FirstError = err.Error()
				}

				return true
			}

			if !ok {
				return true
			}
		}

		m := render(rec, d, defaultValueCutoff)
		matched++

		if emit != nil {
			if !emit(ConsumeEvent{Message: &m, Scanned: result.Scanned}) {
				result.StoppedBy = "cancelled"

				return false
			}
		} else {
			result.Messages = append(result.Messages, m)
		}

		if matched >= req.Limit {
			result.StoppedBy = "limit"

			return false
		}

		return true
	}

	// A round reads what is still needed. Without a search or filter nearly
	// every record is kept, so each partition needs at most the rest of the
	// limit (all of it could sit in one partition); the few skipped for an
	// out-of-range timestamp only cost another round. Otherwise how many will
	// match is unknown, so rounds read full chunks until the limit, the end,
	// a stop or the timeout.
	matchAll := req.Search == "" && filter == nil
	need := func(live int64) int64 {
		if matchAll {
			return int64(req.Limit - matched)
		}

		return roundBudget / live
	}

	cur := newCursor(windows, newestFirst)

	if emit != nil && !progress() {
		return result, nil
	}

	for round := cur.next(need); len(round) > 0; round = cur.next(need) {
		if err := scan(ctx, reader, topic, round, newestFirst, visit); err != nil {
			switch {
			case ctx.Err() != nil:
				result.StoppedBy = stopped()
			case errors.Is(err, errIncomplete):
				result.StoppedBy = "incomplete"
			default:
				return nil, err
			}

			break
		}

		if ctx.Err() != nil {
			result.StoppedBy = stopped()

			break
		}

		if result.StoppedBy != "" {
			break
		}
	}

	if result.StoppedBy == "" {
		result.StoppedBy = "end"
	}

	result.Next = map[int32]int64{}
	if result.StoppedBy != "end" {
		result.Next = resumeFrom(windows, seen, newestFirst)
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

	// Only a reader whose scans all finished is reused; one cut off by a
	// timeout or error may still have a fetch in flight.
	sound = ctx.Err() == nil && result.StoppedBy != "incomplete"

	return result, nil
}

func (c *Client) windows(ctx context.Context, topic string, req ConsumeRequest) ([]window, error) {
	b, err := c.bounds(ctx, topic, false, req.Since)
	if err != nil {
		return nil, err
	}

	starts, ends, atTime := b.starts, b.ends, b.atTime

	var untilTime kadm.ListedOffsets
	if req.Until != nil && !req.Exact {
		if untilTime, err = c.admin.ListOffsetsAfterMilli(ctx, *req.Until+1, topic); err != nil {
			return nil, fmt.Errorf("list offsets by time; %w", accessErr(err))
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

		// The fast path uses the time index. Exact keeps the captured end so
		// later offsets with older producer timestamps are checked too.
		if req.Until != nil && !req.Exact {
			if t, ok := untilTime.Lookup(topic, s.Partition); ok && t.Err == nil && t.Offset >= 0 {
				w.end = min(w.end, t.Offset)
			}
		}

		switch req.Start {
		case "last":
			w.start = max(w.end-max(req.Offset, 0), w.start)
		case "offset":
			w.start = min(max(req.Offset, w.start), w.end)
		}

		// Nothing produced since then: the partition has nothing in range.
		if req.Since != nil {
			if t, ok := atTime.Lookup(topic, s.Partition); ok && t.Err == nil && t.Offset >= 0 {
				w.start = max(t.Offset, w.start)
			} else {
				w.start = w.end
			}
		}

		if req.After != nil {
			at, ok := req.After[s.Partition]
			if !ok {
				return
			}

			if req.Start == "newest" {
				w.end = min(w.end, at)
			} else {
				w.start = max(w.start, at)
			}
		}

		if w.start < w.end {
			out = append(out, w)
		}
	})

	if len(out) == 0 {
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
			if _, ok := starts.Lookup(topic, p); !ok {
				return nil, fmt.Errorf("topic %q partition %d; %w", topic, p, ErrNotFound)
			}
		}
	}

	sort.Slice(out, func(i, j int) bool { return out[i].partition < out[j].partition })

	return out, nil
}

// inTime reports whether a record's timestamp is inside the requested time
// range, when there is one.
func inTime(req ConsumeRequest, rec *kgo.Record) bool {
	ms := rec.Timestamp.UnixMilli()

	return (req.Since == nil || ms >= *req.Since) && (req.Until == nil || ms <= *req.Until)
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

// ─── Decoding ─────────────────────────────────────────────

// recordView is a record with key and value decoded once, shared by the JS
// filter and the renderer.
type recordView struct {
	key, value *decoded
	err        string
}

func (c *Client) decodeRecord(ctx context.Context, rec *kgo.Record, raw bool, as DecodeAs) recordView {
	var (
		v    recordView
		errs []string
	)

	// Registry framing first, then a local .proto type configured for the
	// topic, then the requested format. A chosen format skips both: the
	// user says what the bytes are, and binary data can start like framing
	// by chance.
	read := func(b []byte, format, side string) *decoded {
		if !raw && format == AsAuto && b != nil {
			if d, ok, err := c.decodeRegistry(ctx, b); ok {
				if err != nil {
					errs = append(errs, side+": "+err.Error())

					return nil
				}

				return d
			}

			if md := c.protos.message(rec.Topic, side == "key"); md != nil {
				d, err := decodeProto(md, b)
				if err != nil {
					errs = append(errs, side+": "+err.Error())
				}

				return d
			}
		}

		d, ok, err := decodeAs(format, b)
		if ok && err != nil {
			errs = append(errs, side+": "+err.Error())
		}

		return d
	}

	v.key = read(rec.Key, as.Key, "key")
	v.value = read(rec.Value, as.Value, "value")
	v.err = strings.Join(errs, "; ")

	return v
}

// ─── JS filter ────────────────────────────────────────────

var filterParameters = []string{"value", "key", "headers", "partition", "offset", "timestamp", "raw"}

// compileFilter compiles a message filter, or returns nil for none. A filter
// that does not compile is the caller's to fix.
func compileFilter(source string) (*script.Script, error) {
	if strings.TrimSpace(source) == "" {
		return nil, nil
	}

	filter, err := script.Compile(source, filterParameters...)
	if err != nil {
		return nil, ErrInvalid{err}
	}

	return filter, nil
}

// streamScriptBudget bounds one filter call of a tail or replay, so a
// runaway script cannot stall a stream that otherwise runs for long.
const streamScriptBudget = 2 * time.Second

// evalFilter runs filter on one record within streamScriptBudget.
func evalFilter(ctx context.Context, filter *script.Script, rec *kgo.Record, d recordView) (bool, error) {
	fctx, cancel := context.WithTimeout(ctx, streamScriptBudget)
	defer cancel()

	guard := script.Guard(fctx, filter, "filter timed out")
	ok, err := filter.Call(filterArguments(filter, rec, d)...)

	guard()
	filter.Close()

	return ok, err
}

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

	tsValue := goja.Null()
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
		m.KeySchemaMessage = d.key.Message
	case rec.Key != nil:
		k, enc := encode(rec.Key)
		m.Key, m.KeyEncoding = &k, enc
	}

	switch {
	case d.value != nil:
		text := d.value.Text
		if len(text) > cutoff {
			text = string(trimRune([]byte(text[:cutoff])))
			m.Truncated = true
		}

		m.Value, m.ValueEncoding, m.ValueFormat, m.ValueSchemaID = &text, "utf8", d.value.Format, d.value.SchemaID
		m.ValueSchemaMessage = d.value.Message
	case rec.Value != nil:
		value := rec.Value
		if len(value) > cutoff {
			value = trimRune(value[:cutoff])
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
		m.Headers = append(m.Headers, Header{Key: h.Key, Value: v, Encoding: enc, Null: h.Value == nil})
	}

	return m
}

// trimRune drops a UTF-8 sequence cut off at the end of b, so a truncated
// text value stays valid text instead of being shown as binary.
func trimRune(b []byte) []byte {
	for i := 0; i < utf8.UTFMax && i < len(b); i++ {
		start := len(b) - 1 - i
		if !utf8.RuneStart(b[start]) {
			continue
		}

		if r, _ := utf8.DecodeRune(b[start:]); r == utf8.RuneError && !utf8.FullRune(b[start:]) {
			return b[:start]
		}

		break
	}

	return b
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

	rec, err := c.produceRecord(ctx, topic, req)
	if err != nil {
		return nil, err
	}

	codec, err := compression(req.Compression)
	if err != nil {
		return nil, ErrInvalid{err}
	}

	producer, err := c.producer(req.Partition != nil, codec)
	if err != nil {
		return nil, fmt.Errorf("producer; %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	out, err := producer.ProduceSync(ctx, rec).First()
	if err != nil {
		// The manual partitioner refuses a partition the topic lacks; that
		// is a bad request, not a broker failure.
		if req.Partition != nil && strings.Contains(err.Error(), "invalid record partitioning choice") {
			return nil, ErrInvalid{fmt.Errorf("topic %q has no partition %d", topic, *req.Partition)}
		}

		return nil, fmt.Errorf("produce; %w", err)
	}

	return &ProduceResult{Partition: out.Partition, Offset: out.Offset, Timestamp: out.Timestamp}, nil
}

// produceRecord is shared by ordinary produce and drafts seeded from a message.
func (c *Client) produceRecord(ctx context.Context, topic string, req ProduceRequest) (*kgo.Record, error) {
	rec := &kgo.Record{Topic: topic}

	var err error

	if rec.Key, err = payload(ctx, c, req.Key, req.KeyEncoding, req.KeySchema, "key"); err != nil {
		return nil, ErrInvalid{err}
	}

	if rec.Value, err = payload(ctx, c, req.Value, req.ValueEncoding, req.ValueSchema, "value"); err != nil {
		return nil, ErrInvalid{err}
	}

	for _, h := range req.Headers {
		value := []byte(h.Value)
		if h.Null {
			value = nil
		} else if h.Encoding == "base64" {
			if value, err = base64.StdEncoding.DecodeString(h.Value); err != nil {
				return nil, ErrInvalid{fmt.Errorf("header %s: invalid base64; %w", h.Key, err)}
			}
		}

		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: value})
	}

	if req.Partition != nil {
		if *req.Partition < 0 {
			return nil, ErrInvalid{fmt.Errorf("partition %d is not valid", *req.Partition)}
		}

		rec.Partition = *req.Partition
	}

	return rec, nil
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
