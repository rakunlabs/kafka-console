package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bufbuild/protocompile"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/twmb/franz-go/pkg/sr"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// codec decodes and encodes payloads for one registered schema.
type codec struct {
	id    int
	kind  sr.SchemaType
	avro  *avroCodec
	proto protoreflect.FileDescriptor
	json  *jsonschema.Schema
}

// codecCache resolves schema IDs once; registered schemas are immutable.
type codecCache struct {
	registry *sr.Client

	mu     sync.Mutex
	byID   map[int]*codec
	failed map[int]time.Time
}

const codecRetryAfter = 30 * time.Second

func newCodecCache(registry *sr.Client) *codecCache {
	return &codecCache{registry: registry, byID: map[int]*codec{}, failed: map[int]time.Time{}}
}

func (cc *codecCache) get(ctx context.Context, id int) (*codec, error) {
	cc.mu.Lock()
	if c, ok := cc.byID[id]; ok {
		cc.mu.Unlock()

		return c, nil
	}

	// A missing or broken schema would otherwise cost an HTTP round trip
	// for every record that carries its ID.
	if at, ok := cc.failed[id]; ok && time.Since(at) < codecRetryAfter {
		cc.mu.Unlock()

		return nil, fmt.Errorf("schema %d unavailable", id)
	}
	cc.mu.Unlock()

	c, err := cc.load(ctx, id)

	cc.mu.Lock()
	defer cc.mu.Unlock()

	if err != nil {
		cc.failed[id] = time.Now()

		return nil, err
	}

	cc.byID[id] = c

	return c, nil
}

func (cc *codecCache) load(ctx context.Context, id int) (*codec, error) {
	s, err := cc.registry.SchemaByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("fetch schema %d; %w", id, err)
	}

	var refs []namedSchema
	if err := cc.references(ctx, s.References, map[string]bool{}, &refs, 0); err != nil {
		return nil, err
	}

	return compile(id, s, refs)
}

type namedSchema struct {
	name   string
	schema sr.Schema
}

const maxReferenceDepth = 32

// references fetches referenced schemas depth first, so each one lands after
// everything it depends on; Avro needs that order to resolve named types.
func (cc *codecCache) references(ctx context.Context, refs []sr.SchemaReference, seen map[string]bool, out *[]namedSchema, depth int) error {
	if depth > maxReferenceDepth {
		return errors.New("schema references nest too deeply")
	}

	for _, r := range refs {
		key := fmt.Sprintf("%s/%d", r.Subject, r.Version)
		if seen[key] {
			continue
		}

		seen[key] = true

		s, err := cc.registry.SchemaByVersion(ctx, r.Subject, r.Version)
		if err != nil {
			return fmt.Errorf("fetch reference %s (%s v%d); %w", r.Name, r.Subject, r.Version, err)
		}

		if err := cc.references(ctx, s.References, seen, out, depth+1); err != nil {
			return err
		}

		*out = append(*out, namedSchema{name: r.Name, schema: s.Schema})
	}

	return nil
}

func compile(id int, s sr.Schema, refs []namedSchema) (*codec, error) {
	c := &codec{id: id, kind: s.Type}

	switch s.Type {
	case sr.TypeAvro:
		parsed, err := parseAvro(s.Schema, refs)
		if err != nil {
			return nil, err
		}

		c.avro = parsed
	case sr.TypeProtobuf:
		fd, err := compileProto(s.Schema, refs)
		if err != nil {
			return nil, err
		}

		c.proto = fd
	case sr.TypeJSON:
		compiler := jsonschema.NewCompiler()

		for _, ref := range refs {
			doc, err := jsonschema.UnmarshalJSON(strings.NewReader(ref.schema.Schema))
			if err != nil {
				return nil, fmt.Errorf("parse json schema reference %s; %w", ref.name, err)
			}

			if err := compiler.AddResource(ref.name, doc); err != nil {
				return nil, fmt.Errorf("add json schema reference %s; %w", ref.name, err)
			}
		}

		doc, err := jsonschema.UnmarshalJSON(strings.NewReader(s.Schema))
		if err != nil {
			return nil, fmt.Errorf("parse json schema; %w", err)
		}

		const root = "mem://schema.json"
		if err := compiler.AddResource(root, doc); err != nil {
			return nil, fmt.Errorf("add json schema; %w", err)
		}

		compiled, err := compiler.Compile(root)
		if err != nil {
			return nil, fmt.Errorf("compile json schema; %w", err)
		}

		c.json = compiled
	default:
		return nil, fmt.Errorf("unsupported schema type %v", s.Type)
	}

	return c, nil
}

func compileProto(source string, refs []namedSchema) (protoreflect.FileDescriptor, error) {
	const root = "__root__.proto"

	files := map[string]string{root: source}
	for _, ref := range refs {
		files[ref.name] = ref.schema.Schema
	}

	compiler := protocompile.Compiler{
		Resolver: protocompile.WithStandardImports(&protocompile.SourceResolver{
			Accessor: protocompile.SourceAccessorFromMap(files),
		}),
	}

	out, err := compiler.Compile(context.Background(), root)
	if err != nil {
		return nil, fmt.Errorf("compile protobuf schema; %w", err)
	}

	return out[0], nil
}

// ─── Wire format ──────────────────────────────────────────

const magicByte = 0

// schemaID reports the Confluent wire-format schema ID, if b carries one.
func schemaID(b []byte) (int, bool) {
	if len(b) < 5 || b[0] != magicByte {
		return 0, false
	}

	return int(binary.BigEndian.Uint32(b[1:5])), true
}

// protoMessage resolves the message descriptor named by the index path that
// follows the schema ID, returning the remaining payload.
func protoMessage(fd protoreflect.FileDescriptor, payload []byte) (protoreflect.MessageDescriptor, []byte, error) {
	count, n := binary.Varint(payload)
	if n <= 0 {
		return nil, nil, errors.New("invalid protobuf message index")
	}

	payload = payload[n:]

	path := []int{0}
	if count > 0 {
		if count > 64 {
			return nil, nil, errors.New("protobuf message index too deep")
		}

		path = make([]int, count)
		for i := range path {
			v, n := binary.Varint(payload)
			if n <= 0 {
				return nil, nil, errors.New("invalid protobuf message index")
			}

			path[i] = int(v)
			payload = payload[n:]
		}
	}

	msgs := fd.Messages()

	var md protoreflect.MessageDescriptor

	for _, i := range path {
		if i < 0 || i >= msgs.Len() {
			return nil, nil, fmt.Errorf("protobuf message index %v out of range", path)
		}

		md = msgs.Get(i)
		msgs = md.Messages()
	}

	return md, payload, nil
}

func protoIndex(md protoreflect.MessageDescriptor) []int {
	var path []int

	for d := protoreflect.Descriptor(md); d != nil; d = d.Parent() {
		m, ok := d.(protoreflect.MessageDescriptor)
		if !ok {
			break
		}

		path = append([]int{m.Index()}, path...)
	}

	return path
}

// ─── Decode ───────────────────────────────────────────────

// decoded is a payload rendered for display and for JS filters.
type decoded struct {
	Text     string // JSON text
	Doc      any    // parsed JSON document
	Format   string // avro, protobuf, json-schema
	SchemaID int
	Message  string // full protobuf message name, needed to re-encode the same type
}

// decodeRegistry decodes a Confluent wire-format payload. It returns
// ok=false when b is not schema framed or the cluster has no registry, so
// the caller falls back to plain rendering.
func (c *Client) decodeRegistry(ctx context.Context, b []byte) (*decoded, bool, error) {
	if c.codecs == nil {
		return nil, false, nil
	}

	id, ok := schemaID(b)
	if !ok {
		return nil, false, nil
	}

	cd, err := c.codecs.get(ctx, id)
	if err != nil {
		return nil, true, err
	}

	payload := b[5:]

	var doc any
	var message string

	switch cd.kind {
	case sr.TypeAvro:
		v, err := cd.avro.decode(payload)
		if err != nil {
			return nil, true, fmt.Errorf("avro decode (schema %d); %w", id, err)
		}

		doc = v
	case sr.TypeProtobuf:
		md, rest, err := protoMessage(cd.proto, payload)
		if err != nil {
			return nil, true, err
		}
		message = string(md.FullName())

		msg := dynamicpb.NewMessage(md)
		if err := proto.Unmarshal(rest, msg); err != nil {
			return nil, true, fmt.Errorf("protobuf decode (schema %d); %w", id, err)
		}

		raw, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}.Marshal(msg)
		if err != nil {
			return nil, true, err
		}

		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, true, err
		}
	case sr.TypeJSON:
		if err := json.Unmarshal(payload, &doc); err != nil {
			return nil, true, fmt.Errorf("json decode (schema %d); %w", id, err)
		}
	}

	text, err := json.Marshal(doc)
	if err != nil {
		return nil, true, err
	}
	if cd.kind == sr.TypeJSON {
		// Doc uses JavaScript-compatible numbers for filters, but reserializing
		// it would round large integers in the editable payload. Keep the
		// original validated JSON text, including its numeric spelling.
		text = payload
	}

	return &decoded{Text: string(text), Doc: doc, Format: formatName(cd.kind), SchemaID: id, Message: message}, true, nil
}

func formatName(t sr.SchemaType) string {
	switch t {
	case sr.TypeAvro:
		return "avro"
	case sr.TypeProtobuf:
		return "protobuf"
	case sr.TypeJSON:
		return "json-schema"
	}

	return "unknown"
}

// ─── Encode ───────────────────────────────────────────────

func decodeJSON(b []byte, v any) error {
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()

	if err := d.Decode(v); err != nil {
		return err
	}

	if d.More() {
		return errors.New("unexpected data after JSON value")
	}

	return nil
}

// EncodeRequest asks the produce path to serialize a JSON payload through a
// registered schema.
type EncodeRequest struct {
	Subject string `json:"subject"`
	// Version 0 or -1 means latest.
	Version int `json:"version"`
	// ID overrides subject/version.
	ID int `json:"id"`
	// Message selects a protobuf message by full name; empty means first.
	Message string `json:"message"`
}

func (c *Client) encodeRegistry(ctx context.Context, req *EncodeRequest, value []byte) ([]byte, error) {
	if err := c.requireRegistry(); err != nil {
		return nil, err
	}

	id := req.ID
	if id <= 0 {
		var (
			s   sr.SubjectSchema
			err error
		)

		if req.Version > 0 {
			s, err = c.registry.SchemaByVersion(ctx, req.Subject, req.Version)
		} else {
			s, err = c.registry.SchemaByVersion(ctx, req.Subject, -1)
		}

		if err != nil {
			return nil, fmt.Errorf("resolve schema %s; %w", req.Subject, err)
		}

		id = s.ID
	}

	cd, err := c.codecs.get(ctx, id)
	if err != nil {
		return nil, err
	}

	var doc any
	if err := decodeJSON(value, &doc); err != nil {
		return nil, fmt.Errorf("value must be JSON to encode with a schema; %w", err)
	}

	out := make([]byte, 5, 5+len(value))
	out[0] = magicByte
	binary.BigEndian.PutUint32(out[1:5], uint32(id))

	switch cd.kind {
	case sr.TypeAvro:
		b, err := cd.avro.encode(doc)
		if err != nil {
			return nil, err
		}

		return append(out, b...), nil
	case sr.TypeProtobuf:
		md, err := pickMessage(cd.proto, req.Message)
		if err != nil {
			return nil, err
		}

		msg := dynamicpb.NewMessage(md)
		if err := protojson.Unmarshal(value, msg); err != nil {
			return nil, fmt.Errorf("protobuf: %w", err)
		}

		b, err := proto.Marshal(msg)
		if err != nil {
			return nil, fmt.Errorf("protobuf encode; %w", err)
		}

		index := protoIndex(md)
		if len(index) == 1 && index[0] == 0 {
			out = binary.AppendVarint(out, 0)
		} else {
			out = binary.AppendVarint(out, int64(len(index)))
			for _, i := range index {
				out = binary.AppendVarint(out, int64(i))
			}
		}

		return append(out, b...), nil
	case sr.TypeJSON:
		if err := cd.json.Validate(doc); err != nil {
			return nil, fmt.Errorf("json schema validation; %w", err)
		}

		return append(out, value...), nil
	}

	return nil, fmt.Errorf("unsupported schema type %v", cd.kind)
}

func pickMessage(fd protoreflect.FileDescriptor, name string) (protoreflect.MessageDescriptor, error) {
	if name == "" {
		if fd.Messages().Len() == 0 {
			return nil, errors.New("protobuf schema has no messages")
		}

		return fd.Messages().Get(0), nil
	}

	full := protoreflect.FullName(name)
	if !strings.Contains(name, ".") && fd.Package() != "" {
		full = fd.Package().Append(protoreflect.Name(name))
	}

	d := fd.Messages().ByName(full.Name())
	if d != nil && d.FullName() == full {
		return d, nil
	}

	var found protoreflect.MessageDescriptor

	var walk func(protoreflect.MessageDescriptors)
	walk = func(ms protoreflect.MessageDescriptors) {
		for i := 0; i < ms.Len() && found == nil; i++ {
			if ms.Get(i).FullName() == full {
				found = ms.Get(i)

				return
			}

			walk(ms.Get(i).Messages())
		}
	}
	walk(fd.Messages())

	if found == nil {
		return nil, fmt.Errorf("protobuf message %q not found", name)
	}

	return found, nil
}

// SchemaMessages lists protobuf message names, for the produce form.
func protoMessageNames(fd protoreflect.FileDescriptor) []string {
	var out []string

	var walk func(protoreflect.MessageDescriptors)
	walk = func(ms protoreflect.MessageDescriptors) {
		for i := range ms.Len() {
			m := ms.Get(i)
			if m.IsMapEntry() {
				continue
			}

			out = append(out, string(m.FullName()))
			walk(m.Messages())
		}
	}
	walk(fd.Messages())

	return out
}
