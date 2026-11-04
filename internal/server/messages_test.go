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

func TestParseRefs(t *testing.T) {
	refs, err := parseRefs(" 0:12, 3:40")
	if err != nil || len(refs) != 2 || refs[1].Partition != 3 || refs[1].Offset != 40 {
		t.Fatalf("got %+v, %v", refs, err)
	}
	if refs, err := parseRefs(""); err != nil || len(refs) != 0 {
		t.Fatalf("empty: %+v, %v", refs, err)
	}
	for _, bad := range []string{"0", "a:1", "0:-1", "-1:0", "0:1,"} {
		if _, err := parseRefs(bad); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
