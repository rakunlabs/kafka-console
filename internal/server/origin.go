package server

import (
	"mime"
	"net/http"
	"net/url"
	"strings"
)

// sameOrigin refuses writes another site could make the browser send. A form
// or an image on any page can POST or GET to the console with the user's
// cookies and network access; it cannot set a JSON content type without a
// CORS preflight, which the console never answers, and the browser marks it
// as cross-site.
//
// Writes must therefore be same-origin (Sec-Fetch-Site, or Origin matching
// Host for browsers without it) and, when they carry a body, JSON. Requests
// without either header (curl, scripts) are not from a browser and pass.
func sameOrigin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet || r.Method == http.MethodHead || r.Method == http.MethodOptions {
			next.ServeHTTP(w, r)

			return
		}

		if crossSite(r) {
			writeJSONError(w, http.StatusForbidden, "cross-site request refused")

			return
		}

		if r.ContentLength != 0 || r.Header.Get("Content-Type") != "" {
			mt, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
			if mt != "application/json" {
				writeJSONError(w, http.StatusUnsupportedMediaType, "request body must be application/json")

				return
			}
		}

		next.ServeHTTP(w, r)
	})
}

func crossSite(r *http.Request) bool {
	switch r.Header.Get("Sec-Fetch-Site") {
	case "same-origin", "none":
		return false
	case "":
	default:
		return true
	}

	origin := r.Header.Get("Origin")
	if origin == "" {
		return false
	}

	u, err := url.Parse(origin)
	if err != nil {
		return true
	}

	return !strings.EqualFold(u.Host, r.Host)
}
