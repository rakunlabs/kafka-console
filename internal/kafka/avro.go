package kafka

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"reflect"
	"time"

	"github.com/twmb/avro"
)

// avroCodec is a parsed Avro schema plus its node tree, used to adapt JSON
// input before encoding.
type avroCodec struct {
	schema *avro.Schema
	root   *avro.SchemaNode
	named  map[string]*avro.SchemaNode
}

// avroDisplay turns logical types into the JSON forms the produce path
// accepts back: decimals keep the schema's scale, uuids are canonical
// strings, dates are YYYY-MM-DD and durations are objects.
var avroDisplay = []avro.SchemaOpt{
	avro.CustomType{LogicalType: "decimal", Decode: func(v any, n *avro.SchemaNode) (any, error) {
		b, ok := v.([]byte)
		if !ok {
			return nil, avro.ErrSkipCustomType
		}

		return avro.RatFromBytes(b, n.Scale).FloatString(max(n.Scale, 0)), nil
	}},
	avro.CustomType{LogicalType: "uuid", AvroType: "fixed", Decode: func(v any, _ *avro.SchemaNode) (any, error) {
		b, ok := v.([]byte)
		if !ok || len(b) != 16 {
			return nil, avro.ErrSkipCustomType
		}

		return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
	}},
	avro.CustomType{LogicalType: "date", Decode: func(v any, _ *avro.SchemaNode) (any, error) {
		days, ok := v.(int32)
		if !ok {
			return nil, avro.ErrSkipCustomType
		}

		return time.Unix(int64(days)*86400, 0).UTC().Format(time.DateOnly), nil
	}},
	// Time of day stays the raw wire number; time.Duration would print as
	// nanoseconds.
	avro.CustomType{LogicalType: "time-millis", Decode: func(v any, _ *avro.SchemaNode) (any, error) { return v, nil }},
	avro.CustomType{LogicalType: "time-micros", Decode: func(v any, _ *avro.SchemaNode) (any, error) { return v, nil }},
	avro.CustomType{LogicalType: "duration", Decode: func(v any, _ *avro.SchemaNode) (any, error) {
		b, ok := v.([]byte)
		if !ok || len(b) != 12 {
			return nil, avro.ErrSkipCustomType
		}

		d := avro.DurationFromBytes(b)

		return map[string]any{"months": d.Months, "days": d.Days, "milliseconds": d.Milliseconds}, nil
	}},
}

// parseAvro parses root after its references, which must be ordered so
// every dependency comes before its dependents.
func parseAvro(root string, refs []namedSchema) (*avroCodec, error) {
	var cache avro.SchemaCache

	for _, r := range refs {
		if _, err := cache.Parse(r.schema.Schema, avroDisplay...); err != nil {
			return nil, fmt.Errorf("parse avro reference %s; %w", r.name, err)
		}
	}

	s, err := cache.Parse(root, avroDisplay...)
	if err != nil {
		return nil, fmt.Errorf("parse avro schema; %w", err)
	}

	c := &avroCodec{schema: s, root: s.Root(), named: map[string]*avro.SchemaNode{}}
	collectNamed(c.root, "", c.named)

	return c, nil
}

func (c *avroCodec) decode(payload []byte) (any, error) {
	var v any
	if _, err := c.schema.Decode(payload, &v); err != nil {
		return nil, err
	}

	return avroToJSON(v), nil
}

// encode serializes a JSON document decoded with UseNumber.
func (c *avroCodec) encode(doc any) ([]byte, error) {
	doc, err := c.adapt(c.root, doc, "", 0)
	if err != nil {
		return nil, err
	}

	return c.schema.Encode(doc)
}

// ─── JSON input adaptation ────────────────────────────────

const maxAdaptDepth = 256

// adapt rewrites only what the library cannot read from JSON on its own:
// base64 text for bytes and fixed (the form decoding displays), and duration
// objects. Everything else passes through untouched.
func (c *avroCodec) adapt(n *avro.SchemaNode, v any, ns string, depth int) (any, error) {
	if depth > maxAdaptDepth || v == nil {
		return v, nil
	}

	if def, ok := c.resolve(n, ns); ok {
		n = def
	}

	if n.Namespace != "" {
		ns = n.Namespace
	}

	switch n.Type {
	case "bytes", "fixed":
		if n.LogicalType == "duration" {
			if m, ok := v.(map[string]any); ok {
				return durationFromJSON(m)
			}
		}

		if n.LogicalType != "" {
			return v, nil
		}

		if s, ok := v.(string); ok {
			if b, err := base64.StdEncoding.DecodeString(s); err == nil {
				return b, nil
			}
		}
	case "record", "error":
		m, ok := v.(map[string]any)
		if !ok {
			return v, nil
		}

		out := make(map[string]any, len(m))
		for k, e := range m {
			out[k] = e
		}

		for i := range n.Fields {
			f := &n.Fields[i]
			if e, ok := m[f.Name]; ok {
				a, err := c.adapt(&f.Type, e, ns, depth+1)
				if err != nil {
					return nil, err
				}

				out[f.Name] = a
			}
		}

		return out, nil
	case "array":
		items, ok := v.([]any)
		if !ok || n.Items == nil {
			return v, nil
		}

		out := make([]any, len(items))
		for i, e := range items {
			a, err := c.adapt(n.Items, e, ns, depth+1)
			if err != nil {
				return nil, err
			}

			out[i] = a
		}

		return out, nil
	case "map":
		m, ok := v.(map[string]any)
		if !ok || n.Values == nil {
			return v, nil
		}

		out := make(map[string]any, len(m))
		for k, e := range m {
			a, err := c.adapt(n.Values, e, ns, depth+1)
			if err != nil {
				return nil, err
			}

			out[k] = a
		}

		return out, nil
	case "union":
		return c.adaptUnion(n, v, ns, depth)
	}

	return v, nil
}

func (c *avroCodec) adaptUnion(n *avro.SchemaNode, v any, ns string, depth int) (any, error) {
	// Tagged form {"branch": value}: adapt inside the named branch.
	if m, ok := v.(map[string]any); ok && len(m) == 1 {
		for k, inner := range m {
			for i := range n.Branches {
				b := &n.Branches[i]
				if c.branchName(b, ns) == k {
					a, err := c.adapt(b, inner, ns, depth+1)
					if err != nil {
						return nil, err
					}

					return map[string]any{k: a}, nil
				}
			}
		}
	}

	// A bare value goes to the first branch whose shape fits; the library
	// picks the same branch for the adapted value.
	hasText := false

	for i := range n.Branches {
		b := &n.Branches[i]
		if def, ok := c.resolve(b, ns); ok {
			b = def
		}

		if b.Type == "string" || b.Type == "enum" {
			hasText = true
		}
	}

	for i := range n.Branches {
		b := &n.Branches[i]
		if def, ok := c.resolve(b, ns); ok {
			b = def
		}

		if fitsBare(b, v, hasText) {
			return c.adapt(b, v, ns, depth+1)
		}
	}

	return v, nil
}

// fitsBare reports whether a bare JSON value belongs to branch b in a way
// adapt would change. A string is read as base64 bytes only when the union
// has no string or enum branch that could claim it as text.
func fitsBare(b *avro.SchemaNode, v any, hasText bool) bool {
	switch v.(type) {
	case map[string]any:
		return b.Type == "record" || b.Type == "error" || b.Type == "map" || b.LogicalType == "duration"
	case []any:
		return b.Type == "array"
	case string:
		return !hasText && (b.Type == "bytes" || b.Type == "fixed") && b.LogicalType == ""
	}

	return false
}

func (c *avroCodec) resolve(n *avro.SchemaNode, ns string) (*avro.SchemaNode, bool) {
	switch n.Type {
	case "null", "boolean", "int", "long", "float", "double", "string", "bytes",
		"record", "error", "enum", "array", "map", "fixed", "union":
		return nil, false
	}

	if def, ok := c.named[n.Type]; ok {
		return def, true
	}

	if def, ok := c.named[ns+"."+n.Type]; ok && ns != "" {
		return def, true
	}

	return nil, false
}

// branchName mirrors how Avro names union branches in tagged JSON.
func (c *avroCodec) branchName(b *avro.SchemaNode, ns string) string {
	if def, ok := c.resolve(b, ns); ok {
		b = def
	}

	switch b.Type {
	case "record", "error", "enum", "fixed":
		return fullName(b, ns)
	}

	return b.Type
}

func fullName(n *avro.SchemaNode, ns string) string {
	if n.Namespace != "" {
		return n.Namespace + "." + n.Name
	}

	if ns != "" && !containsDot(n.Name) {
		return ns + "." + n.Name
	}

	return n.Name
}

func containsDot(s string) bool {
	for i := range len(s) {
		if s[i] == '.' {
			return true
		}
	}

	return false
}

func collectNamed(n *avro.SchemaNode, ns string, out map[string]*avro.SchemaNode) {
	if n == nil {
		return
	}

	switch n.Type {
	case "record", "error", "enum", "fixed":
		full := fullName(n, ns)
		if _, seen := out[full]; seen {
			return
		}

		out[full] = n

		if n.Namespace != "" {
			ns = n.Namespace
		} else if i := lastDot(n.Name); i >= 0 {
			ns = n.Name[:i]
		}
	}

	for i := range n.Fields {
		collectNamed(&n.Fields[i].Type, ns, out)
	}

	for i := range n.Branches {
		collectNamed(&n.Branches[i], ns, out)
	}

	collectNamed(n.Items, ns, out)
	collectNamed(n.Values, ns, out)
}

func lastDot(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '.' {
			return i
		}
	}

	return -1
}

func durationFromJSON(m map[string]any) (avro.Duration, error) {
	var d avro.Duration

	for key, dst := range map[string]*uint32{"months": &d.Months, "days": &d.Days, "milliseconds": &d.Milliseconds} {
		raw, ok := m[key]
		if !ok {
			continue
		}

		n, ok := raw.(json.Number)
		if !ok {
			return d, fmt.Errorf("duration %s must be a number", key)
		}

		v, err := n.Int64()
		if err != nil || v < 0 || v > 1<<32-1 {
			return d, fmt.Errorf("duration %s must be an unsigned 32-bit integer", key)
		}

		*dst = uint32(v)
	}

	return d, nil
}

// ─── Output ───────────────────────────────────────────────

// avroToJSON makes generic decoder output JSON friendly: big.Rat (from
// big-decimal) becomes a string, bytes become base64, times RFC 3339.
func avroToJSON(v any) any {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			x[k] = avroToJSON(e)
		}

		return x
	case []any:
		for i, e := range x {
			x[i] = avroToJSON(e)
		}

		return x
	case *big.Rat:
		return x.FloatString(ratScale(x))
	case []byte:
		return base64.StdEncoding.EncodeToString(x)
	case time.Time:
		return x.Format(time.RFC3339Nano)
	case time.Duration:
		return x.String()
	case avro.Duration:
		return map[string]any{"months": x.Months, "days": x.Days, "milliseconds": x.Milliseconds}
	}

	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Array && rv.Type().Elem().Kind() == reflect.Uint8 {
		b := make([]byte, rv.Len())
		reflect.Copy(reflect.ValueOf(b), rv)

		return base64.StdEncoding.EncodeToString(b)
	}

	return v
}

// ratScale is the fewest decimal places that represent r exactly; a
// denominator with factors other than 2 and 5 falls back to 10.
func ratScale(r *big.Rat) int {
	d := new(big.Int).Set(r.Denom())

	twos, fives := 0, 0

	for _, f := range []struct {
		n     int64
		count *int
	}{{2, &twos}, {5, &fives}} {
		div := big.NewInt(f.n)
		for {
			q, m := new(big.Int).QuoRem(d, div, new(big.Int))
			if m.Sign() != 0 {
				break
			}

			d = q
			*f.count++
		}
	}

	if d.Cmp(big.NewInt(1)) != 0 {
		return 10
	}

	return max(twos, fives)
}
