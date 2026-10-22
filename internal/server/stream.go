package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

// readParams are the query parameters a message read and a live tail share.
type readParams struct {
	start      string
	ints       map[string]int64
	partitions []int32
	filter     string
	search     string
	raw        bool
	as         kafka.DecodeAs
}

// signedParams may be negative: times before 1970 are valid, if unusual.
var signedParams = []string{"timestamp", "since", "until"}

// parseRead validates the shared parameters and the integer ones named in
// ints, which must be non-negative unless signed.
func parseRead(q url.Values, starts []string, ints ...string) (readParams, error) {
	p := readParams{
		start:  q.Get("start"),
		ints:   map[string]int64{},
		filter: q.Get("filter"),
		search: q.Get("q"),
		raw:    q.Get("raw") == "true",
		as:     kafka.DecodeAs{Key: q.Get("key_as"), Value: q.Get("value_as")},
	}

	if p.start != "" && !slices.Contains(starts, p.start) {
		return p, ada.WrapHTTPError(http.StatusBadRequest, fmt.Errorf("unknown start %q", p.start))
	}

	for _, key := range ints {
		v := q.Get(key)
		if v == "" {
			continue
		}

		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || (n < 0 && !slices.Contains(signedParams, key)) {
			return p, ada.WrapHTTPError(http.StatusBadRequest, fmt.Errorf("%s must be a non-negative integer", key))
		}

		p.ints[key] = n
	}

	var err error
	p.partitions, err = partitions(q.Get("partitions"))

	return p, err
}

// optional returns a pointer to the named integer when it was given.
func (p readParams) optional(key string) *int64 {
	v, ok := p.ints[key]
	if !ok {
		return nil
	}

	return &v
}

func partitions(v string) ([]int32, error) {
	if v == "" {
		return nil, nil
	}

	var out []int32

	for p := range strings.SplitSeq(v, ",") {
		n, err := strconv.ParseInt(strings.TrimSpace(p), 10, 32)
		if err != nil || n < 0 {
			return nil, ada.NewHTTPError(http.StatusBadRequest, "partitions must be comma separated non-negative numbers")
		}

		out = append(out, int32(n))
	}

	return out, nil
}

// resumeAt parses "after": partition:offset pairs, comma separated, from a
// previous read's next. An empty value but a present parameter means the
// read is over (nothing left anywhere).
func resumeAt(q url.Values) (map[int32]int64, error) {
	if !q.Has("after") {
		return nil, nil
	}

	out := map[int32]int64{}

	for pair := range strings.SplitSeq(q.Get("after"), ",") {
		if pair = strings.TrimSpace(pair); pair == "" {
			continue
		}

		p, o, ok := strings.Cut(pair, ":")
		pn, errP := strconv.ParseInt(p, 10, 32)
		on, errO := strconv.ParseInt(o, 10, 64)

		if !ok || errP != nil || errO != nil {
			return nil, ada.NewHTTPError(http.StatusBadRequest, "after must be partition:offset pairs, comma separated")
		}

		out[int32(pn)] = on
	}

	return out, nil
}

// stream writes events to a streaming response, sending the headers with
// the first one, so an error before it can still be a plain HTTP error.
type stream struct {
	w           http.ResponseWriter
	rc          *http.ResponseController
	contentType string
	started     bool
}

func newStream(w http.ResponseWriter, contentType string) *stream {
	return &stream{w: w, rc: http.NewResponseController(w), contentType: contentType}
}

func (s *stream) start() {
	if s.started {
		return
	}

	s.started = true

	s.w.Header().Set("Content-Type", s.contentType)
	s.w.Header().Set("Cache-Control", "no-cache")
	s.w.Header().Set("X-Accel-Buffering", "no")
	s.w.WriteHeader(http.StatusOK)
}

// line writes v as one line of newline-delimited JSON; false once the
// client is gone.
func (s *stream) line(v any) bool {
	s.start()

	if json.NewEncoder(s.w).Encode(v) != nil {
		return false
	}

	return s.rc.Flush() == nil
}

// event writes v as a server-sent event; false once the client is gone.
func (s *stream) event(name string, v any) bool {
	s.start()

	b, err := json.Marshal(v)
	if err != nil {
		return false
	}

	if _, err := fmt.Fprintf(s.w, "event: %s\ndata: %s\n\n", name, b); err != nil {
		return false
	}

	return s.rc.Flush() == nil
}

// fail answers err as an HTTP error before the stream started, and as a
// last event after.
func (s *stream) fail(err error, last func()) error {
	if !s.started {
		return fail(err)
	}

	last()

	return nil
}

const (
	ndjson      = "application/x-ndjson"
	eventStream = "text/event-stream"
)
