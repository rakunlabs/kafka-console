package kafka

import (
	"bytes"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"
	"unicode/utf8"

	"github.com/fxamacker/cbor/v2"
	"github.com/vmihailenco/msgpack/v5"
)

// Formats a key or value can be read as instead of the automatic choice
// (registry, then JSON, then text, then base64). A chosen format reads the
// bytes as given and skips the registry.
const (
	AsAuto    = ""
	AsText    = "text"
	AsJSON    = "json"
	AsMsgpack = "msgpack"
	AsCBOR    = "cbor"
	AsHex     = "hex"
	AsBase64  = "base64"
	AsUint    = "uint" // big-endian unsigned integer of 1, 2, 4 or 8 bytes
)

// DecodeAs picks how keys and values are read.
type DecodeAs struct {
	Key   string
	Value string
}

func validAs(as string) error {
	switch as {
	case AsAuto, AsText, AsJSON, AsMsgpack, AsCBOR, AsHex, AsBase64, AsUint:
		return nil
	}

	return ErrInvalid{fmt.Errorf("unknown format %q; use text, json, msgpack, cbor, hex, base64 or uint", as)}
}

func (d DecodeAs) validate() error {
	return errors.Join(validAs(d.Key), validAs(d.Value))
}

// decodeAs reads b in the requested format. ok is false for AsAuto, which
// leaves the record to the usual rendering. Formats that yield a document
// (json, msgpack, cbor, uint) give JSON text and the parsed document, so
// search and the JS filter see the decoded data; the byte views (text, hex,
// base64) give the text as is.
func decodeAs(as string, b []byte) (*decoded, bool, error) {
	if as == AsAuto || b == nil {
		return nil, false, nil
	}

	var doc any

	switch as {
	case AsText:
		if !utf8.Valid(b) {
			return nil, true, errors.New("not valid UTF-8 text")
		}

		return &decoded{Text: string(b), Doc: string(b), Format: AsText}, true, nil
	case AsHex:
		s := hex.EncodeToString(b)

		return &decoded{Text: s, Doc: s, Format: AsHex}, true, nil
	case AsBase64:
		s := base64.StdEncoding.EncodeToString(b)

		return &decoded{Text: s, Doc: s, Format: AsBase64}, true, nil
	case AsJSON:
		if err := decodeJSON(b, &doc); err != nil {
			return nil, true, fmt.Errorf("json; %w", err)
		}
	case AsMsgpack:
		// Not "loose": that reads msgpack bin as a string. Integers come
		// back as their exact Go types and jsonable keeps them whole.
		dec := msgpack.NewDecoder(bytes.NewReader(b))
		dec.SetMapDecoder(func(d *msgpack.Decoder) (any, error) { return d.DecodeUntypedMap() })

		v, err := dec.DecodeInterface()
		if err != nil {
			return nil, true, fmt.Errorf("msgpack; %w", err)
		}

		doc = jsonable(v)
	case AsCBOR:
		var v any
		if err := cbor.Unmarshal(b, &v); err != nil {
			return nil, true, fmt.Errorf("cbor; %w", err)
		}

		doc = jsonable(v)
	case AsUint:
		switch len(b) {
		case 1:
			doc = json.Number(strconv.FormatUint(uint64(b[0]), 10))
		case 2:
			doc = json.Number(strconv.FormatUint(uint64(binary.BigEndian.Uint16(b)), 10))
		case 4:
			doc = json.Number(strconv.FormatUint(uint64(binary.BigEndian.Uint32(b)), 10))
		case 8:
			doc = json.Number(strconv.FormatUint(binary.BigEndian.Uint64(b), 10))
		default:
			return nil, true, fmt.Errorf("uint; %d bytes, want 1, 2, 4 or 8", len(b))
		}
	}

	text, err := json.Marshal(doc)
	if err != nil {
		return nil, true, fmt.Errorf("%s; %w", as, err)
	}

	return &decoded{Text: string(text), Doc: doc, Format: as}, true, nil
}

// jsonable turns what msgpack and CBOR decode into values encoding/json can
// write: maps with non-string keys get string keys, byte strings become
// base64, and non-finite floats become strings. Integers stay exact.
func jsonable(v any) any {
	switch t := v.(type) {
	case map[string]any:
		for k, x := range t {
			t[k] = jsonable(x)
		}

		return t
	case map[any]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[fmt.Sprint(jsonable(k))] = jsonable(x)
		}

		return out
	case []any:
		for i, x := range t {
			t[i] = jsonable(x)
		}

		return t
	case []byte:
		return base64.StdEncoding.EncodeToString(t)
	case float32:
		return jsonFloat(float64(t))
	case float64:
		return jsonFloat(t)
	case int64:
		return json.Number(strconv.FormatInt(t, 10))
	case uint64:
		return json.Number(strconv.FormatUint(t, 10))
	case int8, int16, int32, int, uint8, uint16, uint32, uint:
		return json.Number(fmt.Sprint(t))
	case cbor.Tag:
		return map[string]any{"tag": json.Number(strconv.FormatUint(t.Number, 10)), "value": jsonable(t.Content)}
	case fmt.Stringer:
		return t.String()
	}

	return v
}

func jsonFloat(f float64) any {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}

	return f
}
