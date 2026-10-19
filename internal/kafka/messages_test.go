package kafka

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

func TestEditedBinaryRecord(t *testing.T) {
	original := &kgo.Record{
		Key: []byte{0xff, 0}, Value: []byte{0xfe, 1}, Partition: 2,
		Headers: []kgo.RecordHeader{
			{Key: "binary", Value: []byte{0xff}},
			{Key: "duplicate", Value: nil},
			{Key: "duplicate", Value: []byte{}},
			{Key: "", Value: []byte("empty header name")},
		},
	}
	m := render(original, recordView{}, 1024)
	partition := int32(2)
	c := &Client{}
	got, err := c.produceRecord(t.Context(), "target", ProduceRequest{
		Key: m.Key, Value: m.Value, KeyEncoding: m.KeyEncoding, ValueEncoding: m.ValueEncoding,
		Headers: m.Headers, Partition: &partition,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Topic != "target" || got.Partition != 2 || !bytes.Equal(got.Key, original.Key) || !bytes.Equal(got.Value, original.Value) {
		t.Fatalf("binary record changed: %+v", got)
	}
	if len(got.Headers) != 4 || !bytes.Equal(got.Headers[0].Value, original.Headers[0].Value) || got.Headers[1].Value != nil || got.Headers[2].Value == nil || got.Headers[3].Key != "" {
		t.Fatalf("headers changed: %+v", got.Headers)
	}
}

func TestEditedNullAndEmptyPayloads(t *testing.T) {
	c := &Client{}
	empty := ""
	for _, value := range []*string{nil, &empty} {
		got, err := c.produceRecord(t.Context(), "target", ProduceRequest{Key: value, Value: value})
		if err != nil {
			t.Fatal(err)
		}
		if (got.Key == nil) != (value == nil) || (got.Value == nil) != (value == nil) {
			t.Fatalf("null/empty distinction lost: %+v", got)
		}
	}
}

func TestEditedPayloadValidation(t *testing.T) {
	c := &Client{}
	bad := "not base64!"
	for name, req := range map[string]ProduceRequest{
		"key":    {Key: &bad, KeyEncoding: "base64"},
		"value":  {Value: &bad, ValueEncoding: "base64"},
		"header": {Headers: []Header{{Key: "binary", Value: bad, Encoding: "base64"}}},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := c.produceRecord(t.Context(), "target", req)
			var invalid ErrInvalid
			if !errors.As(err, &invalid) || !strings.Contains(err.Error(), "invalid base64") {
				t.Fatalf("expected invalid base64, got %v", err)
			}
		})
	}
}

func TestEditedProtobufKeepsMessageType(t *testing.T) {
	cd, err := compile(17, sr.Schema{Type: sr.TypeProtobuf, Schema: `syntax = "proto3";
		package shop;
		message First { string unrelated = 1; }
		message Envelope { message Order { string status = 1; } }
	`}, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sr.NewClient(sr.URLs("http://unused.invalid"))
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{registry: registry, codecs: &codecCache{byID: map[int]*codec{17: cd}}}
	ctx := t.Context()
	encoded, err := c.encodeRegistry(ctx, &EncodeRequest{ID: 17, Message: "shop.Envelope.Order"}, []byte(`{"status":"FAILED"}`))
	if err != nil {
		t.Fatal(err)
	}
	d := c.decodeRecord(ctx, &kgo.Record{Key: encoded, Value: encoded}, false, DecodeAs{})
	m := render(&kgo.Record{Key: encoded, Value: encoded}, d, 1024)
	if m.KeySchemaMessage != "shop.Envelope.Order" || m.ValueSchemaMessage != "shop.Envelope.Order" || m.ValueSchemaID != 17 {
		t.Fatalf("missing original schema metadata: %+v", m)
	}
	edited := `{"status":"FIXED"}`
	got, err := c.produceRecord(ctx, "target", ProduceRequest{
		Key: m.Key, KeySchema: &EncodeRequest{ID: m.KeySchemaID, Message: m.KeySchemaMessage},
		Value: &edited, ValueSchema: &EncodeRequest{ID: m.ValueSchemaID, Message: m.ValueSchemaMessage},
	})
	if err != nil {
		t.Fatal(err)
	}
	view := c.decodeRecord(ctx, got, false, DecodeAs{})
	if view.err != "" || view.key.Message != "shop.Envelope.Order" || view.value.Message != "shop.Envelope.Order" || !strings.Contains(view.value.Text, "FIXED") {
		t.Fatalf("wrong message type after edit: %+v", view)
	}
}

func TestFullRecordRendering(t *testing.T) {
	value := strings.Repeat("x", defaultValueCutoff+1)
	rec := &kgo.Record{Value: []byte(value)}
	if !render(rec, recordView{}, defaultValueCutoff).Truncated {
		t.Fatal("browse view should be truncated")
	}
	m := render(rec, recordView{}, len(rec.Value))
	if m.Truncated || *m.Value != value {
		t.Fatal("full record should not be truncated")
	}
	// Base64 rendering must also preserve all bytes, not the browse prefix.
	rec.Value = append(rec.Value, 0xff)
	m = render(rec, recordView{}, len(rec.Value))
	if m.Truncated || *m.Value != base64.StdEncoding.EncodeToString(rec.Value) {
		t.Fatal("full binary record changed")
	}
}

func TestEditedJSONSchemaKeepsNumericPrecision(t *testing.T) {
	cd, err := compile(18, sr.Schema{Type: sr.TypeJSON, Schema: `{"type":"object","properties":{"id":{"type":"integer"},"amount":{"type":"number"}}}`}, nil)
	if err != nil {
		t.Fatal(err)
	}
	registry, err := sr.NewClient(sr.URLs("http://unused.invalid"))
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{registry: registry, codecs: &codecCache{byID: map[int]*codec{18: cd}}}
	input := `{"id":9007199254740993,"amount":0.12345678901234567890}`
	encoded, err := c.encodeRegistry(t.Context(), &EncodeRequest{ID: 18}, []byte(input))
	if err != nil {
		t.Fatal(err)
	}
	d := c.decodeRecord(t.Context(), &kgo.Record{Value: encoded}, false, DecodeAs{})
	m := render(&kgo.Record{Value: encoded}, d, 1024)
	if m.DecodeError != "" || m.Value == nil || *m.Value != input {
		t.Fatalf("editable JSON changed: %+v", m)
	}
	got, err := c.produceRecord(t.Context(), "target", ProduceRequest{Value: m.Value, ValueSchema: &EncodeRequest{ID: m.ValueSchemaID}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Value, encoded) {
		t.Fatalf("JSON schema round trip changed numeric data: %s", got.Value[5:])
	}
}

func TestReadOnlyProduceAndInvalidMessageRef(t *testing.T) {
	c := &Client{cfg: &config.Cluster{ReadOnly: true}}
	if _, err := c.Produce(context.Background(), "target", ProduceRequest{}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only write allowed: %v", err)
	}
	for _, ref := range []RecordRef{{Partition: -1}, {Offset: -1}} {
		_, err := c.Message(t.Context(), "source", ref)
		var invalid ErrInvalid
		if !errors.As(err, &invalid) {
			t.Fatalf("invalid message reference accepted: %v", err)
		}
	}
}
