package kafka

import (
	"bytes"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

// textMatch reports whether q appears in a record's key, value or headers,
// case-insensitively. Schema-decoded values are searched in their decoded
// JSON form, so a search for a field value finds Avro and Protobuf records
// too. An empty query matches everything.
func textMatch(q string, rec *kgo.Record, d recordView) bool {
	if q == "" {
		return true
	}

	needle := []byte(strings.ToLower(q))

	has := func(b []byte) bool {
		return len(b) >= len(needle) && bytes.Contains(bytes.ToLower(b), needle)
	}

	if d.key != nil {
		if has([]byte(d.key.Text)) {
			return true
		}
	} else if has(rec.Key) {
		return true
	}

	if d.value != nil {
		if has([]byte(d.value.Text)) {
			return true
		}
	} else if has(rec.Value) {
		return true
	}

	for _, h := range rec.Headers {
		if has([]byte(h.Key)) || has(h.Value) {
			return true
		}
	}

	return false
}
