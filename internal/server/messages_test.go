package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/rakunlabs/ada"
)

func TestMessageReferenceValidation(t *testing.T) {
	s := &Server{}
	mux := ada.New()
	mux.GET("/topics/{topic}/messages/{partition}/{offset}", s.message)
	for _, path := range []string{
		"/topics/orders/messages/no/1",
		"/topics/orders/messages/-1/1",
		"/topics/orders/messages/2147483648/1",
		"/topics/orders/messages/0/no",
		"/topics/orders/messages/0/-1",
		"/topics/orders/messages/0/9223372036854775808",
	} {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400: %s", path, w.Code, w.Body.String())
		}
	}
}
