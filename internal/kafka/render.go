package kafka

import (
	"encoding/base64"
	"encoding/json"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

func render(rec *kgo.Record, d recordView, cutoff int) Message {
	m := Message{
		Partition:   rec.Partition,
		Offset:      rec.Offset,
		Timestamp:   rec.Timestamp,
		ValueBytes:  len(rec.Value),
		KeyBytes:    len(rec.Key),
		Headers:     make([]Header, 0, len(rec.Headers)),
		DecodeError: d.err,
	}

	switch {
	case d.key != nil:
		k := d.key.Text
		m.Key, m.KeyEncoding, m.KeyFormat, m.KeySchemaID = &k, "utf8", d.key.Format, d.key.SchemaID
		m.KeySchemaMessage = d.key.Message
	case rec.Key != nil:
		k, enc := encode(rec.Key)
		m.Key, m.KeyEncoding = &k, enc
	}

	switch {
	case d.value != nil:
		text := d.value.Text
		if len(text) > cutoff {
			text = string(trimRune([]byte(text[:cutoff])))
			m.Truncated = true
		}

		m.Value, m.ValueEncoding, m.ValueFormat, m.ValueSchemaID = &text, "utf8", d.value.Format, d.value.SchemaID
		m.ValueSchemaMessage = d.value.Message
	case rec.Value != nil:
		value := rec.Value
		if len(value) > cutoff {
			value = trimRune(value[:cutoff])
			m.Truncated = true
		}

		v, enc := encode(value)
		m.Value, m.ValueEncoding = &v, enc

		switch {
		case enc == "base64":
			m.ValueFormat = "binary"
		case !m.Truncated && json.Valid(rec.Value):
			m.ValueFormat = "json"
		default:
			m.ValueFormat = "text"
		}
	}

	for _, h := range rec.Headers {
		v, enc := encode(h.Value)
		m.Headers = append(m.Headers, Header{Key: h.Key, Value: v, Encoding: enc, Null: h.Value == nil})
	}

	return m
}

// trimRune drops a UTF-8 sequence cut off at the end of b, so a truncated
// text value stays valid text instead of being shown as binary.
func trimRune(b []byte) []byte {
	for i := 0; i < utf8.UTFMax && i < len(b); i++ {
		start := len(b) - 1 - i
		if !utf8.RuneStart(b[start]) {
			continue
		}

		if r, _ := utf8.DecodeRune(b[start:]); r == utf8.RuneError && !utf8.FullRune(b[start:]) {
			return b[:start]
		}

		break
	}

	return b
}

func encode(b []byte) (string, string) {
	if utf8.Valid(b) {
		return string(b), "utf8"
	}

	return base64.StdEncoding.EncodeToString(b), "base64"
}
