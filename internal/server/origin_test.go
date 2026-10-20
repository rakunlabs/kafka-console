package server

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestSameOrigin(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	h := sameOrigin(ok)

	for _, tc := range []struct {
		name    string
		method  string
		body    string
		headers map[string]string
		want    int
	}{
		{"read from anywhere", http.MethodGet, "", map[string]string{"Sec-Fetch-Site": "cross-site"}, http.StatusNoContent},
		{"same-origin json", http.MethodPost, "{}", map[string]string{"Sec-Fetch-Site": "same-origin", "Content-Type": "application/json"}, http.StatusNoContent},
		{"cross-site form", http.MethodPost, "x", map[string]string{"Sec-Fetch-Site": "cross-site", "Content-Type": "text/plain"}, http.StatusForbidden},
		{"same-origin text/plain", http.MethodPost, "x", map[string]string{"Sec-Fetch-Site": "same-origin", "Content-Type": "text/plain"}, http.StatusUnsupportedMediaType},
		{"no body, no type", http.MethodPost, "", map[string]string{"Sec-Fetch-Site": "same-origin"}, http.StatusNoContent},
		{"other origin, old browser", http.MethodDelete, "", map[string]string{"Origin": "https://evil.example"}, http.StatusForbidden},
		{"matching origin, old browser", http.MethodDelete, "", map[string]string{"Origin": "http://console.local"}, http.StatusNoContent},
		{"curl", http.MethodPost, "{}", map[string]string{"Content-Type": "application/json; charset=utf-8"}, http.StatusNoContent},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, "http://console.local/api/v1/clusters/x/topics/t/delete-records", strings.NewReader(tc.body))
			if tc.body == "" {
				r.ContentLength = 0
			}

			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}

			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)

			if w.Code != tc.want {
				t.Fatalf("status %d, want %d", w.Code, tc.want)
			}
		})
	}
}
