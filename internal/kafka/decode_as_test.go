package kafka

import (
	"encoding/binary"
	"strings"
	"testing"

	"github.com/fxamacker/cbor/v2"
	"github.com/vmihailenco/msgpack/v5"
)

func TestDecodeAs(t *testing.T) {
	doc := map[string]any{
		"id": int64(9007199254740993), "name": "ğ", "tags": []any{"a", int64(2)}, "blob": []byte{1, 2},
		"inner": map[int]any{1: map[string]any{"deep": true}},
	}

	mp, err := msgpack.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}

	cb, err := cbor.Marshal(map[any]any{"id": uint64(9007199254740993), 7: "seven"})
	if err != nil {
		t.Fatal(err)
	}

	u64 := binary.BigEndian.AppendUint64(nil, 1<<63)

	for _, tc := range []struct {
		as   string
		in   []byte
		want []string // substrings of the JSON text
	}{
		// Integers past 2^53 keep every digit.
		{AsMsgpack, mp, []string{`"id":9007199254740993`, `"name":"ğ"`, `"tags":["a",2]`, `"blob":"AQI="`, `"inner":{"1":{"deep":true}}`}},
		{AsCBOR, cb, []string{`"id":9007199254740993`, `"7":"seven"`}},
		{AsUint, u64, []string{"9223372036854775808"}},
		{AsUint, []byte{0, 42}, []string{"42"}},
		{AsHex, []byte{0xde, 0xad}, []string{"dead"}},
		{AsBase64, []byte{0xff}, []string{"/w=="}},
		{AsText, []byte("plain"), []string{"plain"}},
		{AsJSON, []byte(`{"big":9007199254740993}`), []string{`9007199254740993`}},
	} {
		d, ok, err := decodeAs(tc.as, tc.in)
		if !ok || err != nil {
			t.Fatalf("%s: ok=%v err=%v", tc.as, ok, err)
		}

		for _, w := range tc.want {
			if !strings.Contains(d.Text, w) {
				t.Errorf("%s: %s lacks %s", tc.as, d.Text, w)
			}
		}

		if d.Format != tc.as {
			t.Errorf("%s: format %q", tc.as, d.Format)
		}
	}

	for _, tc := range []struct {
		as string
		in []byte
	}{
		{AsMsgpack, []byte{0xc1}},
		{AsCBOR, []byte{0xff, 0xff}},
		{AsUint, []byte{1, 2, 3}},
		{AsText, []byte{0xff}},
		{AsJSON, []byte("{")},
	} {
		if _, ok, err := decodeAs(tc.as, tc.in); !ok || err == nil {
			t.Errorf("%s of %v: want an error", tc.as, tc.in)
		}
	}

	if _, ok, _ := decodeAs(AsAuto, []byte("x")); ok {
		t.Error("auto must leave the record to the usual rendering")
	}

	if err := (DecodeAs{Value: "smile"}).validate(); err == nil {
		t.Error("unknown format accepted")
	}
}
