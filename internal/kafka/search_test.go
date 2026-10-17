package kafka

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTextMatch(t *testing.T) {
	rec := &kgo.Record{
		Key:     []byte("order-42"),
		Value:   []byte(`{"status":"FAILED","customer":"Ada"}`),
		Headers: []kgo.RecordHeader{{Key: "trace-id", Value: []byte("7f3a91")}},
	}

	decoded := recordView{value: &decoded{Text: `{"city":"Istanbul"}`}}

	cases := []struct {
		name string
		q    string
		rec  *kgo.Record
		d    recordView
		want bool
	}{
		{"empty matches", "", rec, recordView{}, true},
		{"value, case-insensitive", "failed", rec, recordView{}, true},
		{"key", "ORDER-42", rec, recordView{}, true},
		{"header value", "7f3a", rec, recordView{}, true},
		{"header name", "trace-id", rec, recordView{}, true},
		{"no match", "shipped", rec, recordView{}, false},
		{"decoded value is searched", "istanbul", rec, decoded, true},
		{"raw bytes ignored when decoded", "failed", rec, decoded, false},
		{"nil value", "x", &kgo.Record{}, recordView{}, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := textMatch(tc.q, tc.rec, tc.d); got != tc.want {
				t.Fatalf("textMatch(%q) = %v, want %v", tc.q, got, tc.want)
			}
		})
	}
}
