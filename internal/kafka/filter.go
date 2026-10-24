package kafka

import (
	"context"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/dop251/goja"
	"github.com/rakunlabs/kafka-console/internal/script"
	"github.com/twmb/franz-go/pkg/kgo"
)

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

// scriptBudget bounds one filter call, so a runaway script cannot stall a
// read, least of all a stream that otherwise runs for long.
const scriptBudget = 2 * time.Second

// matcher decides which records a read keeps: those in the search text, then
// those the filter accepts. It counts the records the filter failed on.
type matcher struct {
	c      *Client
	search string
	filter *script.Watchdog
	raw    bool
	as     DecodeAs

	scriptErrors int64
	firstError   string
}

// newMatcher compiles the filter; Stop must be called when the read ends.
func (c *Client) newMatcher(ctx context.Context, search, filter string, raw bool, as DecodeAs) (*matcher, error) {
	f, err := compileFilter(filter)
	if err != nil {
		return nil, err
	}

	m := &matcher{c: c, search: search, raw: raw, as: as}
	if f != nil {
		m.filter = script.Watch(ctx, f, scriptBudget, "filter timed out")
	}

	return m, nil
}

// selective reports whether the matcher can reject a record.
func (m *matcher) selective() bool { return m.search != "" || m.filter != nil }

// match decodes rec and reports whether it is kept. err is only the context's:
// the read is over and no later record will be matched.
func (m *matcher) match(ctx context.Context, rec *kgo.Record) (recordView, bool, error) {
	d := m.c.decodeRecord(ctx, rec, m.raw, m.as)

	if !textMatch(m.search, rec, d) {
		return d, false, nil
	}

	if m.filter == nil {
		return d, true, nil
	}

	ok, err := m.filter.Call(filterArguments(m.filter.Script(), rec, d)...)
	if err != nil {
		if ctx.Err() != nil {
			return d, false, ctx.Err()
		}

		m.scriptErrors++
		if m.firstError == "" {
			m.firstError = err.Error()
		}

		return d, false, nil
	}

	return d, ok, nil
}

func (m *matcher) Stop() {
	if m.filter != nil {
		m.filter.Stop()
	}
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
