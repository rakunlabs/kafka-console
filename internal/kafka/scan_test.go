package kafka

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCursorSizesRoundsToNeed(t *testing.T) {
	// 1000 partitions with 10M offsets each: the old split built every
	// round of the whole topic up front.
	windows := make([]window, 1000)
	for i := range windows {
		windows[i] = window{partition: int32(i), start: 0, end: 10_000_000}
	}

	c := newCursor(windows, true)

	round := c.next(func(live int64) int64 { return 50 })
	if len(round) != 1000 {
		t.Fatalf("round has %d partitions, want 1000", len(round))
	}

	// Each partition may hold all 50; nothing beyond that is read.
	if w := round[0]; w.end != 10_000_000 || w.end-w.start != 50 {
		t.Fatalf("first chunk %+v, want the newest 50", w)
	}

	// Small needs still read a useful chunk per round trip.
	if w := c.next(func(int64) int64 { return 1 })[0]; w.end-w.start != minChunk {
		t.Fatalf("chunk %+v, want minChunk", w)
	}

	if round := c.next(func(int64) int64 { return 0 }); round != nil {
		t.Fatalf("nothing needed, got a round of %d", len(round))
	}
}

func TestCursorWalksWholeWindow(t *testing.T) {
	for _, newest := range []bool{true, false} {
		c := newCursor([]window{{partition: 0, start: 10, end: 1300}, {partition: 1, start: 0, end: 40}}, newest)

		seen := map[int32]int64{}
		var prev int64 = -1

		for round := c.next(func(int64) int64 { return chunkSize }); round != nil; round = c.next(func(int64) int64 { return chunkSize }) {
			for _, w := range round {
				if w.end-w.start > chunkSize {
					t.Fatalf("chunk %+v larger than chunkSize", w)
				}

				seen[w.partition] += w.end - w.start

				if w.partition == 0 {
					if prev >= 0 && newest && w.end != prev || prev >= 0 && !newest && w.start != prev {
						t.Fatalf("newest=%v: chunk %+v does not continue from %d", newest, w, prev)
					}

					prev = w.start
					if !newest {
						prev = w.end
					}
				}
			}
		}

		if seen[0] != 1290 || seen[1] != 40 {
			t.Fatalf("newest=%v: read %v, want every offset once", newest, seen)
		}
	}
}

func TestTrimRune(t *testing.T) {
	s := []byte("ab€") // € is 3 bytes
	for cut := 2; cut <= len(s); cut++ {
		got := trimRune(s[:cut])
		if !utf8.Valid(got) {
			t.Fatalf("cut %d: %q is not valid UTF-8", cut, got)
		}

		if cut < len(s) && string(got) != "ab" || cut == len(s) && string(got) != "ab€" {
			t.Fatalf("cut %d: got %q", cut, got)
		}
	}

	// Binary stays binary: only a cut-off sequence at the very end is dropped.
	bin := []byte{0xff, 0xfe, 0x00}
	if got := trimRune(bin); len(got) != len(bin) {
		t.Fatalf("binary value changed: %v", got)
	}
}

func TestRenderTruncatedTextStaysText(t *testing.T) {
	value := []byte(`{"name":"ğğğğğğğğğğ"}`)
	m := render(&kgo.Record{Value: value}, recordView{}, 12) // cuts inside a ğ
	if m.ValueEncoding != "utf8" || !m.Truncated {
		t.Fatalf("encoding %q truncated %v, want truncated utf8", m.ValueEncoding, m.Truncated)
	}
}

func TestStreamsAreBounded(t *testing.T) {
	c := &Client{readers: newReaders()}

	var releases []func()

	for range maxStreams {
		release, err := c.stream()
		if err != nil {
			t.Fatalf("stream refused below the limit: %v", err)
		}

		releases = append(releases, release)
	}

	if _, err := c.stream(); !errors.Is(err, ErrBusy) {
		t.Fatalf("stream over the limit: %v, want ErrBusy", err)
	}

	releases[0]()

	if _, err := c.stream(); err != nil {
		t.Fatalf("stream after a release: %v", err)
	}
}

func TestScanReaderWaitsForASlot(t *testing.T) {
	c := &Client{readers: newReaders()}
	for range maxReads {
		c.readers.slots <- struct{}{}
	}

	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()

	if _, _, err := c.scanReader(ctx, "t"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want to wait until the context ends", err)
	}
}

func TestConsumeContinues(t *testing.T) {
	c := fakeCluster(t, map[string]int32{"pages": 3})
	// Producers set timestamps, so they can run backwards within a
	// partition; a continued read must still neither skip nor repeat.
	p, err := c.producer(true, "")
	if err != nil {
		t.Fatal(err)
	}

	at := time.Now().Add(-time.Hour)
	for part := range int32(3) {
		for i := range 40 {
			jitter := time.Duration((i*7919)%40) * time.Second
			rec := &kgo.Record{
				Topic: "pages", Partition: part, Timestamp: at.Add(jitter),
				Value: fmt.Appendf(nil, `{"n":%d,"status":"%s"}`, i, map[bool]string{true: "FAILED", false: "OK"}[i%3 == 0]),
			}
			if err := p.ProduceSync(t.Context(), rec).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}

	for _, start := range []string{"newest", "oldest"} {
		for _, q := range []string{"", "FAILED"} {
			full, err := c.Consume(t.Context(), "pages", ConsumeRequest{Start: start, Limit: 1000, Search: q})
			if err != nil {
				t.Fatal(err)
			}

			got := map[RecordRef]bool{}
			req := ConsumeRequest{Start: start, Limit: 7, Search: q}

			for range 100 {
				res, err := c.Consume(t.Context(), "pages", req)
				if err != nil {
					t.Fatal(err)
				}

				for _, m := range res.Messages {
					ref := RecordRef{Partition: m.Partition, Offset: m.Offset}
					if got[ref] {
						t.Fatalf("%s %q: %v shown twice", start, q, ref)
					}

					got[ref] = true
				}

				if len(res.Next) == 0 {
					break
				}

				req.After = res.Next
			}

			if len(got) != len(full.Messages) {
				t.Fatalf("%s %q: paged %d, one read %d", start, q, len(got), len(full.Messages))
			}
		}
	}

	last, err := c.Consume(t.Context(), "pages", ConsumeRequest{Start: "last", Offset: 2, Limit: 100})
	if err != nil {
		t.Fatal(err)
	}

	if len(last.Messages) != 6 {
		t.Fatalf("last 2 of 3 partitions: %d messages", len(last.Messages))
	}

	for _, m := range last.Messages {
		if m.Offset < 38 {
			t.Fatalf("last 2 returned offset %d", m.Offset)
		}
	}
}
