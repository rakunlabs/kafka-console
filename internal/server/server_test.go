package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/rakunlabs/kafka-console/internal/kafka"
)

func newTestServer(t *testing.T, cfg *config.Config) http.Handler {
	t.Helper()

	registry, err := kafka.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)

	ui := fstest.MapFS{
		"index.html":             {Data: []byte("<!doctype html><title>kafka-console</title>")},
		"assets/app-AbCd1234.js": {Data: []byte("console.log(1)")},
	}

	server, err := newServer(cfg, registry, ui)
	if err != nil {
		t.Fatal(err)
	}

	return server
}

func serve(t *testing.T, h http.Handler, method, path string) *httptest.ResponseRecorder {
	t.Helper()

	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), method, path, nil))

	return w
}

func TestSecurityHeaders(t *testing.T) {
	for _, base := range []string{"", "/console"} {
		h := newTestServer(t, &config.Config{
			Server:   config.Server{BasePath: base},
			Clusters: map[string]*config.Cluster{"local": {Brokers: []string{"localhost:1"}}},
		})

		if base != "" {
			if w := serve(t, h, http.MethodGet, base); w.Code != http.StatusMovedPermanently || w.Header().Get("Location") != base+"/" {
				t.Errorf("%s: %d to %q, want a redirect to %s/", base, w.Code, w.Header().Get("Location"), base)
			}
		}

		for path, status := range map[string]int{
			base + "/":                            http.StatusOK,
			base + "/topics/orders":               http.StatusOK, // SPA route
			base + "/assets/app-AbCd1234.js":      http.StatusOK,
			base + "/api/v1/info":                 http.StatusOK,
			base + "/api/v1/nope":                 http.StatusNotFound,
			base + "/api/v1/clusters/nope/topics": http.StatusNotFound,
		} {
			w := serve(t, h, http.MethodGet, path)
			if w.Code != status {
				t.Errorf("%s: status %d, want %d", path, w.Code, status)
			}

			for name, want := range map[string]string{
				"X-Frame-Options":         "SAMEORIGIN",
				"Content-Security-Policy": "frame-ancestors 'self'",
				"X-Content-Type-Options":  "nosniff",
			} {
				if got := w.Header().Get(name); got != want {
					t.Errorf("%s: %s = %q, want %q", path, name, got, want)
				}
			}
		}
	}
}

func TestJSONNotFound(t *testing.T) {
	off := false
	h := newTestServer(t, &config.Config{
		Clusters: map[string]*config.Cluster{"local": {Brokers: []string{"localhost:1"}, Features: config.Features{Security: &off}}},
	})

	// An unknown API path and a switched-off feature answer the same JSON.
	for _, path := range []string{"/api/v1/nope", "/api/v1/clusters/local/acls"} {
		w := serve(t, h, http.MethodGet, path)

		var body map[string]string
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || w.Code != http.StatusNotFound || body["message"] != "not found" {
			t.Errorf("%s: %d %q", path, w.Code, w.Body.String())
		}

		if ct := w.Header().Get("Content-Type"); ct != "application/json" {
			t.Errorf("%s: content type %q", path, ct)
		}
	}
}

func TestWriteJSONErrorEscapes(t *testing.T) {
	w := httptest.NewRecorder()
	writeJSONError(w, http.StatusBadRequest, `say "hi"`)

	var body map[string]string
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["message"] != `say "hi"` {
		t.Fatalf("%q: %v", w.Body.String(), err)
	}
}

// The UI reads features by their config keys.
func TestFeaturesJSON(t *testing.T) {
	off := false
	h := newTestServer(t, &config.Config{
		Features: config.Features{DeleteTopic: &off},
		Clusters: map[string]*config.Cluster{"local": {Brokers: []string{"localhost:1"}, Features: config.Features{Security: &off}}},
	})

	var info struct {
		Features map[string]bool `json:"features"`
	}
	if err := json.Unmarshal(serve(t, h, http.MethodGet, "/api/v1/info").Body.Bytes(), &info); err != nil {
		t.Fatal(err)
	}

	want := map[string]bool{
		"security": true, "create_topic": true, "delete_topic": false, "alter_topic": true,
		"delete_records": true, "delete_group": true, "alter_broker": true,
	}
	if len(info.Features) != len(want) {
		t.Fatalf("features %v, want %v", info.Features, want)
	}
	for k, v := range want {
		if info.Features[k] != v {
			t.Errorf("global %s = %v, want %v", k, info.Features[k], v)
		}
	}

	var clusters []clusterInfo
	if err := json.Unmarshal(serve(t, h, http.MethodGet, "/api/v1/clusters").Body.Bytes(), &clusters); err != nil {
		t.Fatal(err)
	}

	if f := clusters[0].Features; f["security"] || f["delete_topic"] || !f["create_topic"] {
		t.Errorf("cluster features %v", f)
	}
}
