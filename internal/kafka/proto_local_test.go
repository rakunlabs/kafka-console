package kafka

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"

	"github.com/rakunlabs/kafka-console/internal/config"
)

func TestLocalProto(t *testing.T) {
	dir := t.TempDir()
	write := func(name, body string) {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	// An import across directories and a well-known type.
	write("common/money.proto", `syntax = "proto3"; package common; message Money { string currency = 1; int64 units = 2; }`)
	write("shop/order.proto", `syntax = "proto3"; package shop.v1;
import "common/money.proto";
import "google/protobuf/timestamp.proto";
message Order { string id = 1; common.Money total = 2; google.protobuf.Timestamp at = 3; message Line { string sku = 1; } repeated Line lines = 4; }`)

	pb, err := newLocalProto(&config.Protobuf{
		Paths:  []string{dir},
		Topics: []config.ProtobufTopic{{Topic: `orders(\..+)?`, Value: "shop.v1.Order"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	md := pb.message("orders.eu", false)
	if md == nil || md.FullName() != "shop.v1.Order" {
		t.Fatalf("orders.eu value: %v", md)
	}

	if pb.message("orders.eu", true) != nil || pb.message("ordersx", false) != nil {
		t.Fatal("key, or a topic the pattern does not match as a whole, got a type")
	}

	msg := dynamicpb.NewMessage(md)
	msg.Set(md.Fields().ByName("id"), protoreflect.ValueOfString("o-1"))
	b, err := proto.Marshal(msg)
	if err != nil {
		t.Fatal(err)
	}

	c := &Client{protos: pb}
	view := c.decodeRecord(t.Context(), &kgo.Record{Topic: "orders.eu", Value: b}, false, DecodeAs{})
	if view.err != "" || view.value == nil {
		t.Fatalf("decode: %+v", view)
	}

	if !strings.Contains(view.value.Text, `"id":"o-1"`) || view.value.Format != "protobuf" || view.value.Message != "shop.v1.Order" {
		t.Fatalf("decoded %+v", view.value)
	}

	// Raw and an explicit format bypass the local type.
	if v := c.decodeRecord(t.Context(), &kgo.Record{Topic: "orders.eu", Value: b}, true, DecodeAs{}); v.value != nil {
		t.Fatal("raw read decoded with the local type")
	}

	if _, err := newLocalProto(&config.Protobuf{Paths: []string{dir}, Topics: []config.ProtobufTopic{{Topic: "x", Value: "shop.v1.Missing"}}}); err == nil {
		t.Fatal("unknown message accepted")
	}
}
