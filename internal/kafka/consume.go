package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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

// Where a read starts. They are plain strings, as requests carry them.
const (
	StartNewest    = "newest"
	StartOldest    = "oldest"
	StartOffset    = "offset"
	StartTimestamp = "timestamp"
	// StartLast reads the last Offset records of each partition.
	StartLast = "last"
)

// Why a read or replay stopped, as reported in stopped_by.
const (
	StopEnd        = "end"
	StopLimit      = "limit"
	StopTimeout    = "timeout"
	StopCancelled  = "cancelled"
	StopIncomplete = "incomplete"
	StopError      = "error"
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

// validStart checks a requested start; empty is the caller's default.
func validStart(start string) error {
	switch start {
	case "", StartNewest, StartOldest, StartOffset, StartTimestamp, StartLast:
		return nil
	}

	return ErrInvalid{fmt.Errorf("unknown start %q", start)}
}

// normalize checks the request and fills in its defaults; a streamed read
// may run longer.
func (req *ConsumeRequest) normalize(stream bool) error {
	if err := req.As.validate(); err != nil {
		return err
	}

	if err := validStart(req.Start); err != nil {
		return err
	}

	req.Limit = clamp(req.Limit, defaultLimit, maxLimit)

	limitTime := maxTimeout
	if stream {
		limitTime = maxStreamTimeout
	}

	if req.Timeout <= 0 {
		req.Timeout = defaultTimeout
		if stream {
			req.Timeout = maxStreamTimeout
		}
	}

	req.Timeout = min(req.Timeout, limitTime)

	if req.Start == StartTimestamp {
		since := req.Timestamp
		if req.Since != nil {
			since = max(since, *req.Since)
		}

		req.Since = &since
	}

	if req.Since != nil && req.Until != nil && *req.Until < *req.Since {
		return ErrInvalid{errors.New("the end of the time range is before its start")}
	}

	if req.Start == "" {
		req.Start = StartNewest
	}

	return nil
}

// sortMessages orders by timestamp in the read's direction, then partition,
// then offset.
func sortMessages(ms []Message, newestFirst bool) {
	sort.SliceStable(ms, func(i, j int) bool {
		a, b := ms[i], ms[j]
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
}

func (c *Client) consume(ctx context.Context, topic string, req ConsumeRequest, emit func(ConsumeEvent) bool) (*ConsumeResult, error) {
	begin := time.Now()

	if err := req.normalize(emit != nil); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, req.Timeout)
	defer cancel()

	match, err := c.newMatcher(ctx, req.Search, req.Filter, req.Raw, req.As)
	if err != nil {
		return nil, err
	}
	defer match.Stop()

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

	result := &ConsumeResult{Messages: []Message{}}

	newestFirst := req.Start == StartNewest

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

		result.StoppedBy = StopCancelled

		return false
	}

	// The read's own deadline is a timeout; the caller going away is not.
	stopped := func() string {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return StopTimeout
		}

		return StopCancelled
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

		d, ok, err := match.match(ctx, rec)
		if err != nil {
			result.StoppedBy = stopped()

			return false
		}

		if !ok {
			return true
		}

		m := render(rec, d, defaultValueCutoff)
		matched++

		if emit != nil {
			if !emit(ConsumeEvent{Message: &m, Scanned: result.Scanned}) {
				result.StoppedBy = StopCancelled

				return false
			}
		} else {
			result.Messages = append(result.Messages, m)
		}

		if matched >= req.Limit {
			result.StoppedBy = StopLimit

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
	matchAll := !match.selective()
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
				result.StoppedBy = StopIncomplete
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
		result.StoppedBy = StopEnd
	}

	result.ScriptErrors = int(match.scriptErrors)
	result.FirstError = match.firstError

	result.Next = map[int32]int64{}
	if result.StoppedBy != StopEnd {
		result.Next = resumeFrom(windows, seen, newestFirst)
	}

	sortMessages(result.Messages, newestFirst)

	result.ElapsedMs = time.Since(begin).Milliseconds()

	// Only a reader whose scans all finished is reused; one cut off by a
	// timeout or error may still have a fetch in flight.
	sound = ctx.Err() == nil && result.StoppedBy != StopIncomplete

	return result, nil
}

// inTime reports whether a record's timestamp is inside the requested time
// range, when there is one.
func inTime(req ConsumeRequest, rec *kgo.Record) bool {
	ms := rec.Timestamp.UnixMilli()

	return (req.Since == nil || ms >= *req.Since) && (req.Until == nil || ms <= *req.Until)
}

func clamp(v, def, maxV int) int {
	if v <= 0 {
		return def
	}

	return min(v, maxV)
}
