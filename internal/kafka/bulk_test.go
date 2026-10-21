package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rakunlabs/kafka-console/internal/config"
)

// fakeCluster starts an in-memory broker with the given topics and returns a
// console client for it.
func fakeCluster(t *testing.T, topics map[string]int32) *Client {
	t.Helper()

	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)

	c, err := New("test", &config.Cluster{Brokers: fake.ListenAddrs()}, config.Cache{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	for name, parts := range topics {
		if _, err := c.admin.CreateTopic(t.Context(), parts, 1, nil, name); err != nil {
			t.Fatal(err)
		}
	}

	return c
}

func seed(t *testing.T, c *Client, topic string, partition int32, n int, at time.Time) {
	t.Helper()

	p, err := c.producer(true, "")
	if err != nil {
		t.Fatal(err)
	}

	recs := make([]*kgo.Record, n)
	for i := range recs {
		recs[i] = &kgo.Record{
			Topic: topic, Partition: partition, Key: fmt.Appendf(nil, "k%d", i),
			Value:     fmt.Appendf(nil, `{"n":%d,"status":"%s"}`, i, map[bool]string{true: "FAILED", false: "OK"}[i%2 == 0]),
			Headers:   []kgo.RecordHeader{{Key: "h", Value: []byte{0xff}}},
			Timestamp: at.Add(time.Duration(i) * time.Second),
		}
	}

	if err := p.ProduceSync(t.Context(), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func collect(t *testing.T, c *Client, topic string, req ReplayRequest) []ReplayEvent {
	t.Helper()

	var events []ReplayEvent
	if err := c.Replay(t.Context(), topic, req, func(e ReplayEvent) bool {
		events = append(events, e)

		return true
	}); err != nil {
		t.Fatal(err)
	}

	if len(events) < 2 || events[0].Type != "plan" || events[len(events)-1].Type != "done" {
		t.Fatalf("unexpected event stream: %+v", events)
	}

	return events
}

func readAll(t *testing.T, c *Client, topic string) []Message {
	t.Helper()

	res, err := c.Consume(t.Context(), topic, ConsumeRequest{Start: "oldest", Limit: 1000, Raw: true})
	if err != nil {
		t.Fatal(err)
	}

	return res.Messages
}

func TestReplayRange(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"orders": 2, "orders.replay": 2})
	base := time.UnixMilli(1_760_000_000_000)
	seed(t, c, "orders", 0, 10, base)
	seed(t, c, "orders", 1, 10, base)

	t.Run("offset range is inclusive and keeps order, bytes and partition", func(t *testing.T) {
		events := collect(t, c, "orders", ReplayRequest{
			Partitions: []int32{0}, From: RangeBound{Type: "offset", Offset: 2}, To: RangeBound{Type: "offset", Offset: 5},
			Target: "orders.replay", KeepPartition: true, KeepTimestamp: true, MarkSource: true,
		})
		plan, done := events[0], events[len(events)-1]
		if len(plan.Plan) != 1 || plan.Plan[0] != (ReplayRange{Partition: 0, Start: 2, End: 6}) || plan.Total != 4 {
			t.Fatalf("plan: %+v", plan)
		}
		if done.StoppedBy != "end" || done.Written != 4 || done.Matched != 4 || done.Failed != 0 {
			t.Fatalf("done: %+v", done)
		}

		got := readAll(t, c, "orders.replay")
		if len(got) != 4 {
			t.Fatalf("got %d records", len(got))
		}
		for i, m := range got {
			want := fmt.Sprintf("k%d", i+2)
			if m.Partition != 0 || *m.Key != want || !m.Timestamp.Equal(base.Add(time.Duration(i+2)*time.Second)) {
				t.Fatalf("record %d: %+v", i, m)
			}
			if len(m.Headers) != 2 || m.Headers[0].Encoding != "base64" || m.Headers[1].Value != fmt.Sprintf("orders/0/%d", i+2) {
				t.Fatalf("headers %d: %+v", i, m.Headers)
			}
		}
	})

	t.Run("timestamp range with search and filter", func(t *testing.T) {
		events := collect(t, c, "orders", ReplayRequest{
			From:   RangeBound{Type: "timestamp", Timestamp: base.Add(4 * time.Second).UnixMilli()},
			To:     RangeBound{Type: "timestamp", Timestamp: base.Add(7 * time.Second).UnixMilli()},
			Search: "failed", Filter: "return value.n !== 6", DryRun: true,
		})
		done := events[len(events)-1]
		// Offsets 4..7 on both partitions; FAILED are 4 and 6; 6 is filtered out.
		if events[0].Total != 8 || done.Scanned != 8 || done.Matched != 2 || done.Written != 0 {
			t.Fatalf("done: %+v plan: %+v", done, events[0])
		}
	})

	t.Run("dry run without a predicate does not read", func(t *testing.T) {
		done := collect(t, c, "orders", ReplayRequest{DryRun: true, Limit: 15})
		last := done[len(done)-1]
		if last.Matched != 15 || last.Scanned != 0 {
			t.Fatalf("done: %+v", last)
		}
	})

	t.Run("limit stops early", func(t *testing.T) {
		before := len(readAll(t, c, "orders.replay"))
		events := collect(t, c, "orders", ReplayRequest{Target: "orders.replay", Limit: 3})
		done := events[len(events)-1]
		if done.StoppedBy != "limit" || done.Written != 3 {
			t.Fatalf("done: %+v", done)
		}
		if after := len(readAll(t, c, "orders.replay")); after != before+3 {
			t.Fatalf("wrote %d", after-before)
		}
	})

	t.Run("replaying into the source does not read its own writes", func(t *testing.T) {
		events := collect(t, c, "orders", ReplayRequest{Partitions: []int32{1}})
		done := events[len(events)-1]
		if done.Written != 10 || done.StoppedBy != "end" {
			t.Fatalf("done: %+v", done)
		}
	})

	t.Run("invalid requests", func(t *testing.T) {
		for name, req := range map[string]ReplayRequest{
			"missing target":    {Target: "nope"},
			"missing partition": {Partitions: []int32{7}},
			"bad bound":         {From: RangeBound{Type: "newest"}},
			"negative offset":   {To: RangeBound{Type: "offset", Offset: -1}},
			"bad filter":        {Filter: "return ("},
		} {
			err := c.Replay(t.Context(), "orders", req, func(ReplayEvent) bool { return true })
			var invalid ErrInvalid
			if !errors.As(err, &invalid) {
				t.Errorf("%s: %v", name, err)
			}
		}
	})
}

func TestReplayReadOnly(t *testing.T) {
	c := &Client{cfg: &config.Cluster{ReadOnly: true}}
	if err := c.Replay(context.Background(), "t", ReplayRequest{}, nil); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only replay allowed: %v", err)
	}
	if _, err := c.Import(context.Background(), "t", ImportRequest{}); !errors.Is(err, ErrReadOnly) {
		t.Fatalf("read-only import allowed: %v", err)
	}
}

func TestImport(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"in": 3})
	str := func(s string) *string { return &s }
	part := func(p int32) *int32 { return &p }
	ts := int64(1_700_000_000_000)

	records := []ImportRecord{
		{Key: str("a"), Value: str(`{"x":1}`), Partition: part(2), Timestamp: &ts, Headers: []Header{{Key: "src", Value: "file"}}},
		{Key: nil, Value: str("/wA="), ValueEncoding: "base64", Partition: part(2)},
		{Key: str("c"), Value: nil, Partition: part(1)},
	}

	t.Run("dry run writes nothing", func(t *testing.T) {
		res, err := c.Import(t.Context(), "in", ImportRequest{Records: records, KeepPartition: true, DryRun: true})
		if err != nil || len(res) != 3 {
			t.Fatal(res, err)
		}
		if n := len(readAll(t, c, "in")); n != 0 {
			t.Fatalf("dry run wrote %d", n)
		}
	})

	t.Run("bad record fails the whole batch", func(t *testing.T) {
		bad := append([]ImportRecord{}, records...)
		bad = append(bad, ImportRecord{Value: str("not base64!"), ValueEncoding: "base64", Partition: part(0)})
		_, err := c.Import(t.Context(), "in", ImportRequest{Records: bad, KeepPartition: true})
		var invalid ErrInvalid
		if !errors.As(err, &invalid) {
			t.Fatalf("got %v", err)
		}
		if n := len(readAll(t, c, "in")); n != 0 {
			t.Fatalf("partial write of %d", n)
		}
	})

	t.Run("keep partition needs partitions that exist", func(t *testing.T) {
		for _, recs := range [][]ImportRecord{{{Value: str("x")}}, {{Value: str("x"), Partition: part(9)}}} {
			_, err := c.Import(t.Context(), "in", ImportRequest{Records: recs, KeepPartition: true})
			var invalid ErrInvalid
			if !errors.As(err, &invalid) {
				t.Fatalf("got %v", err)
			}
		}
	})

	t.Run("writes in order with partition, timestamp, binary and tombstone", func(t *testing.T) {
		res, err := c.Import(t.Context(), "in", ImportRequest{Records: records, KeepPartition: true, KeepTimestamp: true, Compression: "zstd"})
		if err != nil {
			t.Fatal(err)
		}
		if res[0].Partition != 2 || res[0].Offset != 0 || res[1].Partition != 2 || res[1].Offset != 1 || res[2].Partition != 1 {
			t.Fatalf("results: %+v", res)
		}

		got := readAll(t, c, "in")
		byKey := map[string]Message{}
		for _, m := range got {
			k := "<nil>"
			if m.Key != nil {
				k = *m.Key
			}
			byKey[k] = m
		}
		if a := byKey["a"]; a.Timestamp.UnixMilli() != ts || *a.Value != `{"x":1}` || a.Headers[0].Value != "file" {
			t.Fatalf("a: %+v", a)
		}
		if b := byKey["<nil>"]; b.ValueEncoding != "base64" || *b.Value != "/wA=" {
			t.Fatalf("binary: %+v", b)
		}
		if tomb := byKey["c"]; tomb.Value != nil {
			t.Fatalf("tombstone: %+v", tomb)
		}
	})

	t.Run("batch size is bounded", func(t *testing.T) {
		_, err := c.Import(t.Context(), "in", ImportRequest{Records: make([]ImportRecord, maxImportBatch+1)})
		var invalid ErrInvalid
		if !errors.As(err, &invalid) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestResendResultsMatchTheirSource(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"src": 3, "dst": 3})
	base := time.UnixMilli(1_760_000_000_000)
	for p := range int32(3) {
		seed(t, c, "src", p, 3, base)
	}

	refs := []RecordRef{{Partition: 2, Offset: 0}, {Partition: 0, Offset: 1}, {Partition: 1, Offset: 2}, {Partition: 2, Offset: 1}}
	res, err := c.Resend(t.Context(), "src", ResendRequest{Records: refs, Target: "dst", KeepPartition: true})
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range res {
		if r.Error != "" || r.Source != refs[i] || r.Partition != refs[i].Partition {
			t.Fatalf("result %d for %+v: %+v", i, refs[i], r)
		}
	}
	if res[0].Offset != 0 || res[3].Offset != 1 {
		t.Fatalf("order within partition 2 lost: %+v", res)
	}
}
