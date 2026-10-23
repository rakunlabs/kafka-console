package kafka

import (
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBrowseTimeRangeWithUnorderedTimestamps(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"orders": 1})
	base := time.UnixMilli(1_760_000_000_000)
	p, err := c.producer(true, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, seconds := range []int{1, 100, 2, 50} {
		if err := p.ProduceSync(t.Context(), &kgo.Record{Topic: "orders", Partition: 0, Value: []byte("x"), Timestamp: base.Add(time.Duration(seconds) * time.Second)}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	to := base.Add(50 * time.Second).UnixMilli()
	from := base.Add(2 * time.Second).UnixMilli()
	for _, start := range []string{"oldest", "newest"} {
		for _, since := range []*int64{nil, &from} {
			t.Run(start+map[bool]string{true: "/both", false: "/until"}[since != nil], func(t *testing.T) {
				req := ConsumeRequest{Start: start, Since: since, Until: &to, Limit: 1, Exact: true}
				var offsets []int64
				for pages := 0; ; pages++ {
					if pages > 5 {
						t.Fatal("continuation did not finish")
					}
					result, err := c.Consume(t.Context(), "orders", req)
					if err != nil {
						t.Fatal(err)
					}
					for _, m := range result.Messages {
						offsets = append(offsets, m.Offset)
					}
					if len(result.Next) == 0 {
						break
					}
					req.After = result.Next
				}
				want := []int64{0, 2, 3}
				if since != nil {
					want = []int64{2, 3}
				}
				if start == "newest" {
					slices.Reverse(want)
				}
				if !slices.Equal(offsets, want) {
					t.Fatalf("got offsets %v, want %v", offsets, want)
				}
			})
		}
	}
	// The streamed path uses the same exact windows.
	var offsets []int64
	_, err = c.ConsumeStream(t.Context(), "orders", ConsumeRequest{Start: "oldest", Until: &to, Limit: 100, Exact: true}, func(e ConsumeEvent) bool {
		if e.Message != nil {
			offsets = append(offsets, e.Message.Offset)
		}
		return true
	})
	if err != nil || !slices.Equal(offsets, []int64{0, 2, 3}) {
		t.Fatalf("stream: %v %v", offsets, err)
	}

	// The default uses Kafka's time index and stops at the first later
	// timestamp; the UI exposes Exact for out-of-order producers.
	fast, err := c.Consume(t.Context(), "orders", ConsumeRequest{Start: "oldest", Until: &to, Limit: 100})
	if err != nil || len(fast.Messages) != 1 || fast.Messages[0].Offset != 0 {
		t.Fatalf("fast time index: %+v %v", fast, err)
	}
}
