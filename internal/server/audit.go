package server

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// maxAuditBody bounds how much of a request body is read for the audit line.
// Writes are small JSON documents; a larger body (a big produced message) is
// logged by size only.
const maxAuditBody = 64 << 10

// Request fields that never reach the log: credentials outright, and message
// payloads, which are data rather than intent and may hold personal data.
var (
	auditSecret  = map[string]bool{"password": true, "token": true, "secret": true, "client_secret": true}
	auditPayload = map[string]bool{"value": true, "key": true, "schema": true}
)

// auditWrites logs one line for every request that changes a cluster, after
// it completes, with who asked (from userHeader), what, and how it ended.
// Nothing is stored; the line goes to the process log like any other.
func auditWrites(userHeader string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
				next.ServeHTTP(w, r)

				return
			}

			body, fields := readAuditBody(r)
			if isPreview(r, body) {
				next.ServeHTTP(w, r)

				return
			}

			rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
			start := time.Now()

			next.ServeHTTP(rec, r)

			user := strings.TrimSpace(r.Header.Get(userHeader))
			if user == "" {
				user = "anonymous"
			}

			attrs := []any{
				"user", user,
				"cluster", r.PathValue("cluster"),
				"action", action(r),
				"method", r.Method,
				"path", r.URL.Path,
				"status", rec.status,
				"duration_ms", time.Since(start).Milliseconds(),
				"remote", clientAddr(r),
			}

			if id := r.Header.Get("X-Request-Id"); id != "" {
				attrs = append(attrs, "request_id", id)
			}

			for _, name := range []string{"topic", "group", "subject", "version", "broker", "user"} {
				if v := r.PathValue(name); v != "" {
					attrs = append(attrs, "target_"+name, v)
				}
			}

			if fields != nil {
				attrs = append(attrs, "request", fields)
			}

			level := slog.LevelInfo
			if rec.status >= http.StatusBadRequest {
				level = slog.LevelWarn
			}

			slog.Log(r.Context(), level, "audit", attrs...)
		})
	}
}

// readAuditBody reads the body for logging and puts it back for the handler.
func readAuditBody(r *http.Request) ([]byte, any) {
	if r.Body == nil || r.Body == http.NoBody {
		return nil, nil
	}

	raw, err := io.ReadAll(io.LimitReader(r.Body, maxAuditBody+1))
	rest := r.Body
	r.Body = struct {
		io.Reader
		io.Closer
	}{io.MultiReader(bytes.NewReader(raw), rest), rest}

	if err != nil {
		return nil, nil
	}

	if len(raw) > maxAuditBody {
		return raw, map[string]any{"body_bytes": "> 64KiB"}
	}

	var v any
	if json.Unmarshal(raw, &v) != nil {
		return raw, map[string]any{"body_bytes": len(raw)}
	}

	return raw, scrub(v)
}

// scrub removes secrets and replaces payloads with their size. Header lists
// keep their names (they say what was sent) and drop their values.
func scrub(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))

		for k, val := range t {
			lk := strings.ToLower(k)

			if strings.HasSuffix(lk, "headers") {
				out[k] = headerNames(val)

				continue
			}

			switch {
			case auditSecret[lk]:
				out[k] = "[redacted]"
			case auditPayload[lk]:
				if s, ok := val.(string); ok {
					out[k+"_bytes"] = len(s)
				} else if val != nil {
					out[k] = "[omitted]"
				}
			default:
				out[k] = scrub(val)
			}
		}

		return out
	case []any:
		// Long lists (headers, partitions) are summarised past a few items.
		if len(t) > 20 {
			return map[string]any{"items": len(t)}
		}

		out := make([]any, len(t))
		for i, val := range t {
			out[i] = scrub(val)
		}

		return out
	default:
		return v
	}
}

func headerNames(v any) any {
	list, ok := v.([]any)
	if !ok {
		return "[omitted]"
	}

	names := make([]string, 0, len(list))

	for _, h := range list {
		if m, ok := h.(map[string]any); ok {
			if k, ok := m["key"].(string); ok {
				names = append(names, k)
			}
		}
	}

	return names
}

// isPreview reports requests that use a write method but change nothing.
func isPreview(r *http.Request, body []byte) bool {
	if strings.HasSuffix(r.URL.Path, "/schemas/check") || strings.HasSuffix(r.URL.Path, "/filter/check") {
		return true
	}

	dryRunnable := strings.HasSuffix(r.URL.Path, "/reset") || strings.HasSuffix(r.URL.Path, "/replay") || strings.HasSuffix(r.URL.Path, "/import")
	if dryRunnable && len(body) > 0 {
		var req struct {
			DryRun bool `json:"dry_run"`
		}

		if json.Unmarshal(body, &req) == nil && req.DryRun {
			return true
		}
	}

	return false
}

// action names the operation from the matched route, e.g.
// "POST /clusters/{cluster}/groups/{group}/reset" → "groups.reset".
func action(r *http.Request) string {
	pattern := r.Pattern
	if i := strings.Index(pattern, " "); i >= 0 {
		pattern = pattern[i+1:]
	}

	if i := strings.Index(pattern, "/clusters/{cluster}/"); i >= 0 {
		pattern = pattern[i+len("/clusters/{cluster}/"):]
	}

	var parts []string

	for _, seg := range strings.Split(pattern, "/") {
		if seg == "" || strings.HasPrefix(seg, "{") {
			continue
		}

		parts = append(parts, seg)
	}

	name := strings.Join(parts, ".")

	switch r.Method {
	case http.MethodDelete:
		return name + ".delete"
	case http.MethodPatch, http.MethodPut:
		return name + ".update"
	}

	return name
}

func clientAddr(r *http.Request) string {
	if f := r.Header.Get("X-Forwarded-For"); f != "" {
		return strings.TrimSpace(strings.Split(f, ",")[0])
	}

	return r.RemoteAddr
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) {
	s.status = code
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter { return s.ResponseWriter }
