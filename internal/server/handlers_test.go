package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/rakunlabs/kafka-console/internal/kafka"
)

func testServer(t *testing.T) *Server {
	t.Helper()

	registry, err := kafka.NewRegistry(&config.Config{Clusters: map[string]*config.Cluster{"local": {Brokers: []string{"localhost:1"}}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)

	return &Server{registry: registry, features: newFeatures(config.Features{})}
}

func TestHandleStatuses(t *testing.T) {
	s := testServer(t)
	mux := ada.New()
	mux.GET("/clusters/{cluster}/value", func(c *ada.Context) error {
		return s.handle(c, func(*kafka.Client) (any, error) { return map[string]int{"n": 1}, nil })
	})
	mux.GET("/clusters/{cluster}/nothing", func(c *ada.Context) error {
		return s.handle(c, func(*kafka.Client) (any, error) { return nil, nil })
	})
	mux.GET("/clusters/{cluster}/made", func(c *ada.Context) error {
		return s.created(c, func(*kafka.Client) (any, error) { return map[string]string{"name": "t"}, nil })
	})
	mux.GET("/clusters/{cluster}/failed", func(c *ada.Context) error {
		return s.created(c, func(*kafka.Client) (any, error) { return map[string]string{"name": "t"}, kafka.ErrNotFound })
	})

	for path, want := range map[string]int{
		"/clusters/local/value":   http.StatusOK,
		"/clusters/local/nothing": http.StatusNoContent,
		"/clusters/local/made":    http.StatusCreated,
		"/clusters/local/failed":  http.StatusNotFound,
		"/clusters/nope/value":    http.StatusNotFound,
	} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
		if w.Code != want {
			t.Errorf("%s: got %d, want %d: %s", path, w.Code, want, w.Body.String())
		}
	}
}

// A malformed body is the caller's mistake whichever cluster it names, so
// every handler binds before it looks the cluster up.
func TestBindBeforeClusterLookup(t *testing.T) {
	s := testServer(t)
	mux := ada.New()
	s.routes(mux.Group(""))

	for _, path := range []string{
		"/clusters/nope/topics",
		"/clusters/nope/topics/t/messages",
		"/clusters/nope/topics/t/replay",
		"/clusters/nope/groups/g/reset",
	} {
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader("{"))
		r.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(w, r)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400: %s", path, w.Code, w.Body.String())
		}
	}
}

func TestCreateTopicNeedsAName(t *testing.T) {
	s := testServer(t)
	mux := ada.New()
	s.routes(mux.Group(""))

	w := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/clusters/nope/topics", strings.NewReader(`{"name":"  "}`))
	r.Header.Set("Content-Type", "application/json")
	mux.ServeHTTP(w, r)
	if w.Code != http.StatusBadRequest || !strings.Contains(w.Body.String(), "topic name is required") {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}
}
