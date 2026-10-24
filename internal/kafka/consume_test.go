package kafka

import (
	"errors"
	"testing"
	"time"
)

func TestConsumeRequestNormalize(t *testing.T) {
	req := ConsumeRequest{}
	if err := req.normalize(false); err != nil {
		t.Fatal(err)
	}

	if req.Start != StartNewest || req.Limit != defaultLimit || req.Timeout != defaultTimeout {
		t.Fatalf("defaults = %+v", req)
	}

	stream := ConsumeRequest{Timeout: time.Hour}
	if err := stream.normalize(true); err != nil || stream.Timeout != maxStreamTimeout {
		t.Fatalf("stream timeout = %v %v", stream.Timeout, err)
	}

	since := int64(50)
	ts := ConsumeRequest{Start: StartTimestamp, Timestamp: 10, Since: &since}
	if err := ts.normalize(false); err != nil || *ts.Since != 50 {
		t.Fatalf("timestamp start = %v %v", ts.Since, err)
	}

	until := int64(5)
	bad := ConsumeRequest{Since: &since, Until: &until}
	if err := bad.normalize(false); !errors.As(err, new(ErrInvalid)) {
		t.Fatalf("reversed range = %v", err)
	}

	unknown := ConsumeRequest{Start: "sideways"}
	if err := unknown.normalize(false); !errors.As(err, new(ErrInvalid)) {
		t.Fatalf("unknown start = %v", err)
	}
}

func TestTailRefusesUnknownStart(t *testing.T) {
	c := &Client{}
	err := c.Tail(t.Context(), "t", TailRequest{Start: "sideways"}, func(TailEvent) bool { return false })
	if !errors.As(err, new(ErrInvalid)) {
		t.Fatalf("err = %v", err)
	}
}

func TestSortMessages(t *testing.T) {
	at := time.UnixMilli(1000)
	ms := []Message{
		{Partition: 1, Offset: 1, Timestamp: at},
		{Partition: 0, Offset: 2, Timestamp: at},
		{Partition: 0, Offset: 3, Timestamp: at},
		{Partition: 0, Offset: 0, Timestamp: at.Add(time.Second)},
	}

	sortMessages(ms, true)
	if ms[0].Offset != 0 || ms[1].Offset != 3 || ms[2].Offset != 2 || ms[3].Partition != 1 {
		t.Fatalf("newest first = %+v", ms)
	}

	sortMessages(ms, false)
	if ms[0].Offset != 2 || ms[1].Offset != 3 || ms[2].Partition != 1 || ms[3].Offset != 0 {
		t.Fatalf("oldest first = %+v", ms)
	}
}
