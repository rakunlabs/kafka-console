package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/rakunlabs/ada"
)

func TestLimitBody(t *testing.T) {
	mux := ada.New()
	api := mux.Group("/api/v1", limitBody, auditWrites(""))
	handler := func(c *ada.Context) error {
		v, err := bind[struct {
			V string `json:"v"`
		}](c)
		if err != nil {
			return err
		}

		return c.SendJSON(v)
	}
	api.POST("/x", handler)
	api.POST("/topics/t/import", handler)

	do := func(path string, size int) int {
		body := `{"v":"` + strings.Repeat("a", size) + `"}`
		w := httptest.NewRecorder()
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		mux.ServeHTTP(w, r)

		return w.Code
	}

	if got := do("/api/v1/x", 1024); got != http.StatusOK {
		t.Fatalf("small body: %d", got)
	}

	if got := do("/api/v1/x", maxBody+1); got != http.StatusRequestEntityTooLarge {
		t.Fatalf("body over the limit: %d, want 413", got)
	}

	if got := do("/api/v1/topics/t/import", maxBody+1); got != http.StatusOK {
		t.Fatalf("import gets the larger limit: %d", got)
	}

}
