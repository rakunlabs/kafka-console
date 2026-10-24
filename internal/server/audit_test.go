package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rakunlabs/ada"
)

func TestAuditWrites(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	var seen string

	mux := ada.New()
	api := mux.Group("/api/v1", auditWrites("X-User"))
	c := api.Group("/clusters/{cluster}")
	handler := func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		seen = string(b)
		w.WriteHeader(http.StatusNoContent)
	}
	c.POST("/groups/{group}/reset", handler)
	c.POST("/users", handler)
	c.POST("/topics/{topic}/replay", handler)
	c.POST("/topics/{topic}/import", handler)
	c.DELETE("/topics/{topic}", handler)
	c.GET("/topics", handler)

	do := func(method, path, body string, header map[string]string) map[string]any {
		buf.Reset()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		for k, v := range header {
			req.Header.Set(k, v)
		}
		mux.ServeHTTP(httptest.NewRecorder(), req)
		if buf.Len() == 0 {
			return nil
		}
		var line map[string]any
		if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
			t.Fatalf("bad log line %q: %v", buf.String(), err)
		}
		return line
	}

	t.Run("write is logged with user and target", func(t *testing.T) {
		body := `{"topic":"orders","partitions":[2],"strategy":"offset","offset":40,"dry_run":false}`
		line := do("POST", "/api/v1/clusters/prod/groups/billing/reset", body, map[string]string{"X-User": "alice@corp"})
		if line == nil {
			t.Fatal("no audit line")
		}
		if line["user"] != "alice@corp" || line["cluster"] != "prod" || line["action"] != "groups.reset" || line["target_group"] != "billing" {
			t.Fatalf("unexpected line: %v", line)
		}
		if seen != body {
			t.Fatalf("handler saw %q, want the original body", seen)
		}
	})

	t.Run("dry run is not logged", func(t *testing.T) {
		if line := do("POST", "/api/v1/clusters/prod/groups/billing/reset", `{"topic":"orders","dry_run":true}`, nil); line != nil {
			t.Fatalf("dry run logged: %v", line)
		}
	})

	t.Run("replay and import dry runs are not logged, real ones are", func(t *testing.T) {
		for _, op := range []string{"replay", "import"} {
			path := "/api/v1/clusters/prod/topics/orders/" + op
			if line := do("POST", path, `{"dry_run":true}`, nil); line != nil {
				t.Fatalf("%s dry run logged: %v", op, line)
			}
			line := do("POST", path, `{"records":[{"key":"k","value":"secret"}],"dry_run":false}`, nil)
			if line == nil || line["action"] != "topics."+op || line["target_topic"] != "orders" {
				t.Fatalf("%s not logged: %v", op, line)
			}
			if strings.Contains(buf.String(), "secret") {
				t.Fatalf("%s payload logged: %s", op, buf.String())
			}
		}
	})

	t.Run("a delete of a resource named like a dry-run route is logged", func(t *testing.T) {
		for _, name := range []string{"reset", "replay", "import"} {
			line := do("DELETE", "/api/v1/clusters/prod/topics/"+name, `{"dry_run":true}`, nil)
			if line == nil || line["action"] != "topics.delete" {
				t.Fatalf("delete of topic %q not logged: %v", name, line)
			}
		}
	})

	t.Run("reads are not logged", func(t *testing.T) {
		if line := do("GET", "/api/v1/clusters/prod/topics", "", nil); line != nil {
			t.Fatalf("read logged: %v", line)
		}
	})

	t.Run("header names kept, values dropped", func(t *testing.T) {
		do("POST", "/api/v1/clusters/prod/users", `{"name":"svc","extra_headers":[{"key":"x-trace","value":"secret-ish"}]}`, nil)
		if !strings.Contains(buf.String(), `"x-trace"`) || strings.Contains(buf.String(), "secret-ish") {
			t.Fatalf("headers not scrubbed as expected: %s", buf.String())
		}
	})

	t.Run("secrets are redacted and user falls back", func(t *testing.T) {
		line := do("POST", "/api/v1/clusters/prod/users", `{"name":"svc","password":"hunter2","mechanism":"SCRAM-SHA-512"}`, nil)
		if line["user"] != "anonymous" {
			t.Fatalf("user = %v", line["user"])
		}
		if strings.Contains(buf.String(), "hunter2") {
			t.Fatalf("password leaked: %s", buf.String())
		}
	})
}

func TestRouteKey(t *testing.T) {
	var got []string

	mux := ada.New()
	api := mux.Group("/kafka"+apiPrefix, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			got = append(got, routeKey(r))
			next.ServeHTTP(w, r)
		})
	})
	noop := func(http.ResponseWriter, *http.Request) {}
	api.POST("/filter/check", noop)
	api.POST("/clusters/{cluster}/topics/{topic}/import", noop)

	for _, path := range []string{"/kafka/api/v1/filter/check", "/kafka/api/v1/clusters/c/topics/t/import"} {
		mux.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("POST", path, nil))
	}

	if len(got) != 2 || !checkRoutes[got[0]] || got[1] != importRoute {
		t.Fatalf("route keys = %q", got)
	}
}

func TestAuditTrustedProxies(t *testing.T) {
	var buf bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })

	proxies, err := parseProxies([]string{"10.0.0.0/8", "192.168.1.5"})
	if err != nil {
		t.Fatal(err)
	}

	mux := ada.New()
	api := mux.Group("/api/v1", auditWrites("X-User", proxies...))
	api.POST("/clusters/{cluster}/users", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })

	log := func(remote string, forwarded ...string) map[string]any {
		buf.Reset()
		req := httptest.NewRequest("POST", "/api/v1/clusters/prod/users", strings.NewReader(`{}`))
		req.RemoteAddr = remote
		req.Header.Set("X-User", "admin")
		xff := "203.0.113.9"
		if len(forwarded) > 0 {
			xff = forwarded[0]
		}
		req.Header.Set("X-Forwarded-For", xff)
		mux.ServeHTTP(httptest.NewRecorder(), req)

		var line map[string]any
		if err := json.Unmarshal(buf.Bytes(), &line); err != nil {
			t.Fatalf("bad log line %q: %v", buf.String(), err)
		}

		return line
	}

	if l := log("10.1.2.3:4000"); l["user"] != "admin" || l["remote"] != "203.0.113.9" {
		t.Fatalf("via proxy: %v %v", l["user"], l["remote"])
	}

	if l := log("192.168.1.5:4000"); l["user"] != "admin" {
		t.Fatalf("single-host proxy: %v", l["user"])
	}

	// The client wrote the leftmost entry; the proxies appended the rest.
	if l := log("10.1.2.3:4000", "1.2.3.4, 203.0.113.9, 10.0.0.7"); l["remote"] != "203.0.113.9" {
		t.Fatalf("forged X-Forwarded-For: remote = %v", l["remote"])
	}

	if l := log("198.51.100.7:4000"); l["user"] != "unverified:admin" || l["remote"] != "198.51.100.7:4000" {
		t.Fatalf("direct: %v %v", l["user"], l["remote"])
	}

	if _, err := parseProxies([]string{"not-an-ip"}); err == nil {
		t.Fatal("bad proxy accepted")
	}
}
