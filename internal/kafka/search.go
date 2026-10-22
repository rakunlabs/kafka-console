package kafka

import (
	"bytes"
	"strings"
	"unicode/utf8"
	"unsafe"

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

	has := func(b []byte) bool { return containsFold(b, needle) }
	hasText := func(s string) bool { return containsFold(unsafe.Slice(unsafe.StringData(s), len(s)), needle) }

	if d.key != nil {
		if hasText(d.key.Text) {
			return true
		}
	} else if has(rec.Key) {
		return true
	}

	if d.value != nil {
		if hasText(d.value.Text) {
			return true
		}
	} else if has(rec.Value) {
		return true
	}

	for _, h := range rec.Headers {
		if hasText(h.Key) || has(h.Value) {
			return true
		}
	}

	return false
}

// containsFold reports whether lower, already lower case, appears in b
// case-insensitively. ASCII text, the common case, is compared in place
// without copying b; anything else is lowered first, because lowering can
// change a character's length in bytes.
func containsFold(b, lower []byte) bool {
	n := len(lower)
	if n == 0 {
		return true
	}

	if !isASCII(b) || !isASCII(lower) {
		return bytes.Contains(bytes.ToLower(b), lower)
	}

	first := lower[0]

	for i := 0; i+n <= len(b); i++ {
		if toLowerASCII(b[i]) != first {
			continue
		}

		j := 1
		for j < n && toLowerASCII(b[i+j]) == lower[j] {
			j++
		}

		if j == n {
			return true
		}
	}

	return false
}

func isASCII(b []byte) bool {
	for _, c := range b {
		if c >= utf8.RuneSelf {
			return false
		}
	}

	return true
}

func toLowerASCII(c byte) byte {
	if 'A' <= c && c <= 'Z' {
		return c + 'a' - 'A'
	}

	return c
}
