package server

import (
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

func TestParseRead(t *testing.T) {
	starts := []string{"newest", "oldest"}

	for _, tc := range []struct {
		query string
		bad   string
	}{
		{"start=newest&offset=5&since=-10&partitions=0,2", ""},
		{"start=sideways", "unknown start"},
		{"offset=abc", "offset must be"},
		{"limit=-1", "limit must be"},
		{"partitions=0,-1", "partitions must be"},
	} {
		q, _ := url.ParseQuery(tc.query)

		p, err := parseRead(q, starts, "offset", "since", "limit")
		switch {
		case tc.bad == "" && err != nil:
			t.Errorf("%s: %v", tc.query, err)
		case tc.bad != "" && (err == nil || !strings.Contains(err.Error(), tc.bad)):
			t.Errorf("%s: %v, want %q", tc.query, err, tc.bad)
		case tc.bad == "":
			if p.ints["offset"] != 5 || *p.optional("since") != -10 || p.optional("limit") != nil || len(p.partitions) != 2 {
				t.Errorf("%s: parsed %+v", tc.query, p)
			}
		}
	}
}

func TestStreamSendsHeadersOnce(t *testing.T) {
	w := httptest.NewRecorder()
	s := newStream(w, ndjson)

	if !s.line(map[string]int{"a": 1}) || !s.line(map[string]int{"a": 2}) {
		t.Fatal("writes to a live response must succeed")
	}

	if got := w.Header().Get("Content-Type"); got != ndjson {
		t.Fatalf("content type %q", got)
	}

	if got := w.Body.String(); got != "{\"a\":1}\n{\"a\":2}\n" {
		t.Fatalf("body %q", got)
	}
}

func TestStreamFinish(t *testing.T) {
	boom := kafka.ErrNotFound

	// Before the first event the error is still an HTTP error.
	w := httptest.NewRecorder()
	s := newStream(w, ndjson)
	called := false
	if err := s.finish(boom, func() { called = true }); err == nil || called {
		t.Fatalf("before start: err %v, last called %v", err, called)
	}

	// After it, the error is the last event and the handler returns nil.
	s.line(map[string]int{"a": 1})
	if err := s.finish(boom, func() { called = true }); err != nil || !called {
		t.Fatalf("after start: err %v, last called %v", err, called)
	}
}
