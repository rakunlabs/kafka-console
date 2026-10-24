package kafka

import (
	"context"
	"sync"

	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newScanReader opens a consumer for scans that stop at a known end offset.
// It consumes nothing until a scan adds partitions. It also returns
// transaction markers: they take offsets but carry no data, so
// a partition whose last offset is a commit marker would otherwise never
// deliver its end and the scan would wait out its empty polls. Callers skip
// them with isControl.
func (c *Client) newScanReader() (*kgo.Client, error) {
	return c.newKafkaClient(
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{}),
		kgo.FetchMaxWait(scanFetchWait),
		kgo.KeepControlRecords(),
	)
}

// Defaults for config.Limits fields left at 0, as in tests that build a
// Client without a loaded configuration.
const (
	defaultReads       = 16
	defaultIdleReaders = 4
	defaultStreams     = 32
	defaultMCPReads    = 8
)

// readers hands out scan readers, at most limits.Reads at a time, reusing
// idle ones. A reader holds no partitions while idle: scan removes what it
// adds. MCP tools take theirs from a pool of their own (mcp), so an agent
// cannot hold every slot the UI reads with.
type readers struct {
	slots   chan struct{}
	mcp     chan struct{}
	streams chan struct{}
	keep    int

	mu     sync.Mutex
	idle   []*kgo.Client
	closed bool
}

func newReaders(l config.Limits) *readers {
	or := func(v, def int) int {
		if v > 0 {
			return v
		}

		return def
	}

	return &readers{
		slots:   make(chan struct{}, or(l.Reads, defaultReads)),
		mcp:     make(chan struct{}, or(l.MCPReads, defaultMCPReads)),
		streams: make(chan struct{}, or(l.Streams, defaultStreams)),
		keep:    or(l.IdleReaders, defaultIdleReaders),
	}
}

// stream takes a place for a long-running consumer, or fails with ErrBusy.
func (c *Client) stream() (release func(), err error) {
	select {
	case c.readers.streams <- struct{}{}:
		return func() { <-c.readers.streams }, nil
	default:
		return nil, ErrBusy
	}
}

// scanReader returns a reader for topic and the function that gives it
// back; pass that function whether the reader is still sound (false after an
// error, so a reader in an unknown state is closed, not reused).
func (c *Client) scanReader(ctx context.Context, topic string) (*kgo.Client, func(ok bool), error) {
	return c.lendReader(ctx, c.readers.slots, topic)
}

// mcpReader is scanReader for MCP tools: from their own pool of slots, and
// given back through a release that cannot report whether the reader is
// sound, so it is only kept when the tool left it consuming nothing.
func (c *Client) mcpReader(ctx context.Context, topic string) (*kgo.Client, func(), error) {
	cl, done, err := c.lendReader(ctx, c.readers.mcp, topic)
	if err != nil {
		return nil, nil, err
	}

	var once sync.Once

	return cl, func() {
		once.Do(func() { done(len(cl.GetConsumeTopics()) == 0) })
	}, nil
}

// lendReader takes a slot from slots, then an idle reader or a new one.
func (c *Client) lendReader(ctx context.Context, slots chan struct{}, topic string) (*kgo.Client, func(ok bool), error) {
	r := c.readers

	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	r.mu.Lock()
	var cl *kgo.Client
	if n := len(r.idle); n > 0 {
		cl, r.idle = r.idle[n-1], r.idle[:n-1]
	}
	r.mu.Unlock()

	if cl == nil {
		var err error
		if cl, err = c.newScanReader(); err != nil {
			<-slots

			return nil, nil, err
		}
	}

	done := func(ok bool) {
		defer func() { <-slots }()

		// Forget the topic, so a kept reader does not keep refreshing
		// metadata for every topic it was ever lent for.
		if ok {
			cl.PurgeTopicsFromConsuming(topic)
		}

		r.mu.Lock()
		keep := ok && !r.closed && len(r.idle) < r.keep
		if keep {
			r.idle = append(r.idle, cl)
		}
		r.mu.Unlock()

		if !keep {
			cl.Close()
		}
	}

	return cl, done, nil
}

func (r *readers) close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	for _, cl := range r.idle {
		cl.Close()
	}
	r.idle = nil
}

func isControl(rec *kgo.Record) bool { return rec.Attrs.IsControl() }

// windowEnds tracks the exclusive end offset of each partition a read has
// yet to finish.
type windowEnds map[int32]int64

// accept reports whether rec is a record of an unfinished window to visit,
// and whether it finished its partition's window. Records past the end,
// buffered from before, and transaction markers are not visited, though a
// marker can finish a window.
func (w windowEnds) accept(rec *kgo.Record) (visit, finished bool) {
	end, ok := w[rec.Partition]
	if !ok {
		return false, false
	}

	if rec.Offset >= end {
		delete(w, rec.Partition)

		return false, true
	}

	if rec.Offset == end-1 {
		delete(w, rec.Partition)

		finished = true
	}

	return !isControl(rec), finished
}
