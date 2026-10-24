package kafka

import (
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

func TestRespErr(t *testing.T) {
	err := respErr("alter quota", kerr.InvalidRequest, "")
	if !errors.Is(err, kerr.InvalidRequest) || err.Error() != "alter quota; "+kerr.InvalidRequest.Error() {
		t.Fatalf("no message = %q", err)
	}

	err = respErr("alter quota", kerr.InvalidRequest, "bad key")
	if !errors.Is(err, kerr.InvalidRequest) || err.Error() != "alter quota; "+kerr.InvalidRequest.Error()+": bad key" {
		t.Fatalf("message = %q", err)
	}

	if message(nil) != "" {
		t.Fatal("nil message")
	}
}

func TestProduceToMissingPartition(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"orders": 2})
	v := "x"
	p := int32(5)

	_, err := c.Produce(t.Context(), "orders", ProduceRequest{Value: &v, Partition: &p})
	if !errors.As(err, new(ErrInvalid)) {
		t.Fatalf("err = %v, want invalid", err)
	}

	p = 1
	if _, err := c.Produce(t.Context(), "orders", ProduceRequest{Value: &v, Partition: &p}); err != nil {
		t.Fatal(err)
	}
}
