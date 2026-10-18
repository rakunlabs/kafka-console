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
