package kafka

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// maxReplay bounds the records one replay may write; larger migrations
	// belong in MirrorMaker or a purpose-built job.
	maxReplay     = 1_000_000
	maxReplayTime = 30 * time.Minute
	replayPoll    = 500
	replayEvery   = 500 * time.Millisecond
	// maxImportBatch bounds one import request. The UI sends a file as a
	// sequence of batches so progress is visible and order is kept.
	maxImportBatch = 1000
	// importTimeout bounds writing one import batch.
	importTimeout = 60 * time.Second
)

// resentFromHeader names the exact record a copy was made from:
// <topic>/<partition>/<offset>.
const resentFromHeader = "x-resent-from"

// RangeBound is one end of a replay range. From is oldest, offset or
// timestamp; To is newest, offset or timestamp. A To offset is inclusive and
// a To timestamp includes records stamped at that millisecond.
type RangeBound struct {
	Type      string `json:"type"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
}

type ReplayRequest struct {
	// Plan reuses the exact offset windows returned by a reviewed dry run.
	// Nil resolves From/To now; a present empty plan replays nothing.
	Plan []ReplayRange `json:"plan"`
	// Partitions empty means every partition.
	Partitions []int32    `json:"partitions"`
	From       RangeBound `json:"from"`
	To         RangeBound `json:"to"`
	Search     string     `json:"search"`
	Filter     string     `json:"filter"`
	// Target topic; empty means the source topic.
	Target        string   `json:"target"`
	KeepPartition bool     `json:"keep_partition"`
	KeepTimestamp bool     `json:"keep_timestamp"`
	MarkSource    bool     `json:"mark_source"`
	ExtraHeaders  []Header `json:"extra_headers"`
	// Exact checks every later offset for out-of-order producer timestamps
	// and counts committed records in previews. Otherwise a timestamp To
	// uses Kafka's time index and a predicate-free preview is an estimate.
	Exact bool `json:"exact"`
	// Limit stops after this many matching records; 0 means maxReplay.
	Limit int `json:"limit"`
	// DryRun counts matching records without writing.
	DryRun bool `json:"dry_run"`
}

// ReplayRange is one partition's [Start, End) offsets, fixed when the replay
// starts so records written meanwhile, including the replay's own when the
// target is the source, are never read.
type ReplayRange struct {
	Partition int32 `json:"partition"`
	Start     int64 `json:"start"`
	End       int64 `json:"end"`
}

// ReplayEvent is streamed while a replay runs: one plan, progress reports,
// and a final done.
type ReplayEvent struct {
	Type string        `json:"type"`
	Plan []ReplayRange `json:"plan,omitempty"`
	// Total is the number of offsets in the plan, an upper bound on records:
	// compaction and transaction markers leave gaps.
	Total   int64 `json:"total"`
	Scanned int64 `json:"scanned"`
	Matched int64 `json:"matched"`
	// Estimated means Matched is an offset-count upper bound, not a scan.
	Estimated    bool   `json:"estimated,omitempty"`
	Written      int64  `json:"written"`
	Failed       int64  `json:"failed"`
	ScriptErrors int64  `json:"script_errors"`
	FirstError   string `json:"first_error,omitempty"`
	// StoppedBy is end, limit, error, timeout or cancelled.
	StoppedBy string `json:"stopped_by,omitempty"`
	ElapsedMs int64  `json:"elapsed_ms"`
}

// replayOptions are the parts of a copy shared by resend and replay.
type replayOptions struct {
	source        string
	target        string
	keepPartition bool
	keepTimestamp bool
	markSource    bool
	extraHeaders  []Header
}

// copyRecord makes a new record with the source's key, value and headers,
// byte for byte, plus the requested extra headers.
func copyRecord(src *kgo.Record, o replayOptions) *kgo.Record {
	rec := &kgo.Record{Topic: o.target, Key: src.Key, Value: src.Value}
	rec.Headers = append(rec.Headers, src.Headers...)

	for _, h := range o.extraHeaders {
		if h.Key != "" {
			rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: []byte(h.Value)})
		}
	}

	if o.markSource {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{
			Key:   resentFromHeader,
			Value: fmt.Appendf(nil, "%s/%d/%d", o.source, src.Partition, src.Offset),
		})
	}

	if o.keepPartition {
		rec.Partition = src.Partition
	}

	if o.keepTimestamp {
		rec.Timestamp = src.Timestamp
	}

	return rec
}

// checkTarget confirms the target exists and, when partitions are kept, has
// every partition that will be written to.
func (c *Client) checkTarget(ctx context.Context, target string, keepPartition bool, partitions []int32) error {
	_, td, err := c.topicDetail(ctx, target)
	switch {
	case errors.Is(err, ErrNotFound):
		return ErrInvalid{fmt.Errorf("target topic %q does not exist", target)}
	case err != nil:
		return err
	}

	if keepPartition {
		for _, p := range partitions {
			if _, ok := td.Partitions[p]; !ok {
				return ErrInvalid{fmt.Errorf("target %q has no partition %d; turn off \"keep partition\"", target, p)}
			}
		}
	}

	return nil
}

// ReplayPlan resolves the request's bounds to offsets per partition.
func (c *Client) ReplayPlan(ctx context.Context, topic string, req ReplayRequest) ([]ReplayRange, error) {
	if err := validBound(req.From, StartOldest, "from"); err != nil {
		return nil, err
	}

	if err := validBound(req.To, StartNewest, "to"); err != nil {
		return nil, err
	}
	if req.From.Type == StartTimestamp && req.To.Type == StartTimestamp && req.To.Timestamp < req.From.Timestamp {
		return nil, ErrInvalid{errors.New("the end of the time range is before its start")}
	}
	if req.Plan != nil {
		return c.reviewedReplayPlan(ctx, topic, req.Plan)
	}

	// The last stable offset, not the high watermark: records past it belong
	// to open transactions and a read-committed reader would wait for them.
	var from, to *int64
	if req.From.Type == StartTimestamp {
		from = &req.From.Timestamp
	}

	// Exact keeps the captured stable end; records are still checked by
	// timestamp individually in both modes.
	if req.To.Type == StartTimestamp && !req.Exact {
		to = &req.To.Timestamp
	}

	b, err := c.bounds(ctx, topic, true, from, to)
	if err != nil {
		return nil, err
	}

	// Partitions whose start is unknown are left out of "all", as before;
	// asked for by number they fail below.
	all := slices.DeleteFunc(b.partitions(), func(p int32) bool {
		s, _ := b.starts.Lookup(topic, p)

		return s.Err != nil
	})
	if len(all) == 0 {
		return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
	}

	parts := req.Partitions
	if len(parts) == 0 {
		parts = all
	}

	seen := map[int32]bool{}
	out := make([]ReplayRange, 0, len(parts))

	for _, p := range parts {
		if !slices.Contains(all, p) {
			return nil, noPartition(topic, p)
		}

		if seen[p] {
			continue
		}

		seen[p] = true

		low, high, err := b.partition(p)
		if err != nil {
			return nil, err
		}

		r := ReplayRange{Partition: p, Start: low, End: high}

		switch req.From.Type {
		case StartOffset:
			r.Start = req.From.Offset
		case StartTimestamp:
			r.Start = timeOffset(b.atTime, topic, p, high)
		}

		switch {
		case req.To.Type == StartOffset && req.To.Offset < high:
			r.End = req.To.Offset + 1
		case to != nil:
			r.End = timeOffset(b.until, topic, p, high)
		}

		r.Start = min(max(r.Start, low), high)
		r.End = min(max(r.End, r.Start), high)
		out = append(out, r)
	}

	sortRanges(out)

	return out, nil
}

// reviewedReplayPlan validates a snapshot without expanding or silently
// clamping it. Retention advancing past a window requires another review.
func (c *Client) reviewedReplayPlan(ctx context.Context, topic string, plan []ReplayRange) ([]ReplayRange, error) {
	if len(plan) == 0 {
		return []ReplayRange{}, nil
	}
	b, err := c.bounds(ctx, topic, true, nil, nil)
	if err != nil {
		return nil, err
	}
	seen := map[int32]bool{}
	out := append([]ReplayRange(nil), plan...)
	for _, r := range out {
		if r.Partition < 0 || r.Start < 0 || r.End < r.Start || seen[r.Partition] {
			return nil, ErrInvalid{fmt.Errorf("invalid reviewed range for partition %d", r.Partition)}
		}
		seen[r.Partition] = true
		lo, hi, err := b.partition(r.Partition)
		if err != nil || r.Start < lo || r.End > hi {
			return nil, ErrConflict{fmt.Errorf("reviewed range for partition %d is no longer available; review the replay again", r.Partition)}
		}
	}
	sortRanges(out)
	return out, nil
}

func validBound(b RangeBound, open, name string) error {
	switch b.Type {
	case "", open, StartTimestamp:
		return nil
	case StartOffset:
		if b.Offset < 0 {
			return ErrInvalid{fmt.Errorf("%s offset must be non-negative", name)}
		}

		return nil
	}

	return ErrInvalid{fmt.Errorf("%s must be %s, offset or timestamp", name, open)}
}

func sortRanges(rs []ReplayRange) {
	slices.SortFunc(rs, func(a, b ReplayRange) int { return cmp.Compare(a.Partition, b.Partition) })
}

// replayCounters are written by produce promises on another goroutine.
type replayCounters struct {
	written, failed atomic.Int64

	mu       sync.Mutex
	firstErr string
}

func (r *replayCounters) fail(err error) {
	r.failed.Add(1)

	r.mu.Lock()
	if r.firstErr == "" {
		r.firstErr = err.Error()
	}
	r.mu.Unlock()
}

func (r *replayCounters) first() string {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.firstErr
}

// replayDrainTimeout bounds the wait for writes still in flight once a replay
// stops. The bulk producers are shared and keep kgo's unbounded delivery
// timeout, so a replay that cannot confirm its last writes reports them as
// unconfirmed instead of holding its stream slot forever.
const replayDrainTimeout = 30 * time.Second

// Replay copies a range of a topic, byte for byte, to a target topic. Records
// are read in offset order per partition and written in the order read, so
// per-key order survives. It stops at the first failed write. Errors before
// the plan is emitted are returned; later ones are reported in the done event.
func (c *Client) Replay(ctx context.Context, topic string, req ReplayRequest, emit func(ReplayEvent) bool) error {
	begin := time.Now()

	if !req.DryRun {
		if err := c.requireWritable(); err != nil {
			return err
		}
	}

	release, err := c.stream()
	if err != nil {
		return err
	}
	defer release()

	rctx, cancel := context.WithTimeout(ctx, maxReplayTime)
	defer cancel()

	// Decoding is only for the search and filter; copies use raw bytes.
	match, err := c.newMatcher(rctx, req.Search, req.Filter, false, DecodeAs{})
	if err != nil {
		return err
	}
	defer match.Stop()

	plan, err := c.ReplayPlan(ctx, topic, req)
	if err != nil {
		return err
	}

	target := req.Target
	if target == "" {
		target = topic
	}

	var total int64

	used := make([]int32, 0, len(plan))

	for _, r := range plan {
		total += r.End - r.Start
		if r.End > r.Start {
			used = append(used, r.Partition)
		}
	}

	if !req.DryRun {
		if err := c.checkTarget(ctx, target, req.KeepPartition, used); err != nil {
			return err
		}
	}

	run := &replayRun{
		ctx: ctx, rctx: rctx, cancel: cancel, begin: begin, emit: emit,
		topic: topic, req: req, match: match,
		limit: int64(clamp(req.Limit, maxReplay, maxReplay)),
		ev:    ReplayEvent{Type: "plan", Plan: plan, Total: total},
		copyOpts: replayOptions{
			source: topic, target: target, keepPartition: req.KeepPartition, keepTimestamp: req.KeepTimestamp,
			markSource: req.MarkSource, extraHeaders: req.ExtraHeaders,
		},
	}

	if !emit(run.ev) {
		return nil
	}

	run.ev.Plan = nil

	if total == 0 {
		run.done(StopEnd)

		return nil
	}

	// Without a predicate, a quick preview reports the offset count as an
	// upper bound immediately. Exact scans to count committed records.
	if req.DryRun && !match.selective() && !req.Exact {
		run.ev.Matched = min(total, run.limit)
		run.ev.Estimated = true
		run.done(StopEnd)

		return nil
	}

	if err := run.open(c, plan); err != nil {
		run.ev.FirstError = err.Error()
		run.done(StopError)

		return nil
	}
	defer run.reader.Close()

	run.poll()
	run.drain()
	run.finish()

	return nil
}

// replayRun is the state of one replay past its plan: the reader, the
// producer, what was counted and why it stopped.
type replayRun struct {
	ctx, rctx context.Context
	cancel    context.CancelFunc
	begin     time.Time
	emit      func(ReplayEvent) bool

	topic    string
	req      ReplayRequest
	match    *matcher
	limit    int64
	copyOpts replayOptions

	reader   *kgo.Client
	producer *kgo.Client
	active   windowEnds

	ev        ReplayEvent
	counters  replayCounters
	inflight  sync.WaitGroup
	pending   atomic.Int64
	stoppedBy string
	last      time.Time
}

// open starts the reader on the plan's windows and, unless a dry run, takes
// the producer.
func (r *replayRun) open(c *Client, plan []ReplayRange) error {
	offsets := make(map[int32]kgo.Offset, len(plan))
	r.active = make(windowEnds, len(plan))

	for _, p := range plan {
		if p.End > p.Start {
			offsets[p.Partition] = kgo.NewOffset().At(p.Start)
			r.active[p.Partition] = p.End
		}
	}

	reader, err := c.newKafkaClient(
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{r.topic: offsets}),
		kgo.FetchMaxWait(streamFetchWait),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		// Markers reach the planned ends; see newScanReader.
		kgo.KeepControlRecords(),
	)
	if err != nil {
		return fmt.Errorf("create reader; %w", err)
	}

	if !r.req.DryRun {
		if r.producer, err = c.bulkProducer(r.req.KeepPartition, ""); err != nil {
			reader.Close()

			return fmt.Errorf("producer; %w", err)
		}
	}

	r.reader = reader
	r.last = time.Now()

	return nil
}

func (r *replayRun) snapshot() {
	r.ev.Written = r.counters.written.Load()
	r.ev.Failed = r.counters.failed.Load()

	if e := r.counters.first(); e != "" && r.ev.FirstError == "" {
		r.ev.FirstError = e
	}
}

// progress reports at most every replayEvery; false when the client left.
func (r *replayRun) progress() bool {
	if time.Since(r.last) < replayEvery {
		return true
	}

	r.last = time.Now()
	r.snapshot()
	r.ev.Type = "progress"
	r.ev.ElapsedMs = time.Since(r.begin).Milliseconds()

	return r.emit(r.ev)
}

func (r *replayRun) done(stoppedBy string) {
	r.ev.Type = "done"
	r.ev.StoppedBy = stoppedBy
	r.ev.ElapsedMs = time.Since(r.begin).Milliseconds()
	r.emit(r.ev)
}

func (r *replayRun) visit(rec *kgo.Record) {
	if r.stoppedBy != "" {
		return
	}

	// Reported per record too: one poll can take long with a slow filter.
	if !r.progress() {
		r.cancel()
		r.stoppedBy = StopCancelled

		return
	}

	keep, finished := r.active.accept(rec)
	if finished {
		r.reader.PauseFetchPartitions(map[string][]int32{r.topic: {rec.Partition}})
	}

	if !keep {
		return
	}

	r.ev.Scanned++
	ms := rec.Timestamp.UnixMilli()
	if r.req.From.Type == StartTimestamp && ms < r.req.From.Timestamp || r.req.To.Type == StartTimestamp && ms > r.req.To.Timestamp {
		return
	}

	if r.match.selective() {
		_, ok, err := r.match.match(r.rctx, rec)
		r.ev.ScriptErrors = r.match.scriptErrors
		if r.ev.FirstError == "" {
			r.ev.FirstError = r.match.firstError
		}

		if err != nil || !ok {
			return
		}
	}

	r.ev.Matched++

	if r.producer != nil {
		if r.counters.failed.Load() > 0 {
			r.stoppedBy = StopError

			return
		}

		r.inflight.Add(1)
		r.pending.Add(1)
		r.producer.Produce(r.rctx, copyRecord(rec, r.copyOpts), func(_ *kgo.Record, err error) {
			defer r.inflight.Done()
			defer r.pending.Add(-1)

			if err != nil {
				r.counters.fail(err)

				return
			}

			r.counters.written.Add(1)
		})
	}

	if r.ev.Matched >= r.limit {
		r.stoppedBy = StopLimit
	}
}

// poll reads until every window is done, the replay stops or its context
// ends, then settles why it stopped.
func (r *replayRun) poll() {
	empty := 0

	for len(r.active) > 0 && r.stoppedBy == "" {
		if r.rctx.Err() != nil {
			break
		}

		pctx, pcancel := context.WithTimeout(r.rctx, pollTimeout)
		fetches := r.reader.PollRecords(pctx, replayPoll)
		pcancel()

		if err := fetches.Err0(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			r.ev.FirstError = fmt.Sprintf("fetch from %s; %v", r.topic, err)
			r.stoppedBy = StopError

			break
		}

		if fetches.Empty() {
			// Nothing left below the planned ends: compaction or transaction
			// markers left the last offsets without records.
			if empty++; empty >= maxEmptyPolls {
				break
			}

			continue
		}

		empty = 0

		fetches.EachRecord(r.visit)

		if r.counters.failed.Load() > 0 {
			r.stoppedBy = StopError
		}

		if !r.progress() {
			r.cancel()
		}
	}

	switch {
	case r.stoppedBy != "":
	case r.ctx.Err() != nil:
		r.stoppedBy = StopCancelled
	case r.rctx.Err() != nil:
		r.stoppedBy = StopTimeout
	default:
		r.stoppedBy = StopEnd
	}
}

// drain waits for writes still buffered, reporting progress meanwhile, for
// at most replayDrainTimeout.
func (r *replayRun) drain() {
	flushed := make(chan struct{})

	go func() {
		r.inflight.Wait()
		close(flushed)
	}()

	tick := time.NewTicker(replayEvery)
	defer tick.Stop()

	deadline := time.NewTimer(replayDrainTimeout)
	defer deadline.Stop()

	for {
		select {
		case <-flushed:
			return
		case <-deadline.C:
			// Records not sent yet fail on the cancelled context; the rest
			// may still land, which the done event cannot know.
			r.cancel()
			if n := r.pending.Load(); n > 0 && r.ev.FirstError == "" {
				r.ev.FirstError = fmt.Sprintf("%d writes not confirmed within %s", n, replayDrainTimeout)
			}

			// A replay stopped for another reason keeps it.
			if r.stoppedBy == StopEnd || r.stoppedBy == StopLimit {
				r.stoppedBy = StopError
			}

			return
		case <-tick.C:
			r.last = time.Time{}
			if !r.progress() {
				r.cancel()
			}
		}
	}
}

func (r *replayRun) finish() {
	r.snapshot()

	if r.ev.Failed > 0 {
		r.stoppedBy = StopError
	}

	r.done(r.stoppedBy)
}

// ImportRecord is one record of an imported file, in the same shape the
// produce form sends.
type ImportRecord struct {
	Key           *string        `json:"key"`
	Value         *string        `json:"value"`
	Headers       []Header       `json:"headers"`
	Partition     *int32         `json:"partition"`
	KeySchema     *EncodeRequest `json:"key_schema"`
	ValueSchema   *EncodeRequest `json:"value_schema"`
	KeyEncoding   string         `json:"key_encoding"`
	ValueEncoding string         `json:"value_encoding"`
	// Timestamp is used with KeepTimestamp; unix milliseconds.
	Timestamp *int64 `json:"timestamp"`
}

type ImportRequest struct {
	Records []ImportRecord `json:"records"`
	// KeepPartition writes each record to its partition, which every record
	// must then have; otherwise the key decides.
	KeepPartition bool   `json:"keep_partition"`
	KeepTimestamp bool   `json:"keep_timestamp"`
	Compression   string `json:"compression"`
	// DryRun encodes every record, schema payloads included, and writes none.
	DryRun bool `json:"dry_run"`
}

type ImportResult struct {
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`
	Error     string `json:"error,omitempty"`
}

// Import writes a batch of records in order. Every record is encoded before
// any is written, so a bad record fails the batch without a partial write.
func (c *Client) Import(ctx context.Context, topic string, req ImportRequest) ([]ImportResult, error) {
	if err := c.requireWritable(); err != nil {
		return nil, err
	}

	if len(req.Records) == 0 {
		return nil, ErrInvalid{errors.New("no records to import")}
	}

	if len(req.Records) > maxImportBatch {
		return nil, ErrInvalid{fmt.Errorf("at most %d records per import request", maxImportBatch)}
	}

	codec, err := compression(req.Compression)
	if err != nil {
		return nil, ErrInvalid{err}
	}

	records := make([]*kgo.Record, len(req.Records))
	partitions := map[int32]bool{}

	for i, r := range req.Records {
		pr := ProduceRequest{
			Key: r.Key, Value: r.Value, Headers: r.Headers, KeySchema: r.KeySchema, ValueSchema: r.ValueSchema,
			KeyEncoding: r.KeyEncoding, ValueEncoding: r.ValueEncoding,
		}

		if req.KeepPartition {
			if r.Partition == nil || *r.Partition < 0 {
				return nil, ErrInvalid{fmt.Errorf("record %d: no partition; turn off \"keep partition\"", i+1)}
			}

			pr.Partition = r.Partition
			partitions[*r.Partition] = true
		}

		rec, err := c.produceRecord(ctx, topic, pr)
		if err != nil {
			return nil, ErrInvalid{fmt.Errorf("record %d: %w", i+1, err)}
		}

		if req.KeepTimestamp && r.Timestamp != nil {
			rec.Timestamp = time.UnixMilli(*r.Timestamp)
		}

		records[i] = rec
	}

	used := make([]int32, 0, len(partitions))
	for p := range partitions {
		used = append(used, p)
	}

	if err := c.checkTarget(ctx, topic, req.KeepPartition, used); err != nil {
		return nil, err
	}

	out := make([]ImportResult, len(records))

	if req.DryRun {
		for i := range out {
			out[i] = ImportResult{Partition: -1, Offset: -1}
		}

		return out, nil
	}

	producer, err := c.bulkProducer(req.KeepPartition, codec)
	if err != nil {
		return nil, fmt.Errorf("producer; %w", err)
	}

	pctx, cancel := context.WithTimeout(ctx, importTimeout)
	defer cancel()

	for i, r := range produceInOrder(pctx, producer, records) {
		if r.Err != nil {
			out[i] = ImportResult{Partition: -1, Offset: -1, Error: r.Err.Error()}

			continue
		}

		out[i] = ImportResult{Partition: r.Record.Partition, Offset: r.Record.Offset}
	}

	return out, nil
}
