package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rakunlabs/kafka-console/internal/script"
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
)

// RangeBound is one end of a replay range. From is oldest, offset or
// timestamp; To is newest, offset or timestamp. A To offset is inclusive and
// a To timestamp includes records stamped at that millisecond.
type RangeBound struct {
	Type      string `json:"type"`
	Offset    int64  `json:"offset"`
	Timestamp int64  `json:"timestamp"`
}

type ReplayRequest struct {
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
	Total        int64  `json:"total"`
	Scanned      int64  `json:"scanned"`
	Matched      int64  `json:"matched"`
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
			Key:   "x-resent-from",
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
	details, err := c.admin.ListTopics(ctx, target)
	if err != nil {
		return fmt.Errorf("describe topic; %w", err)
	}

	td, ok := details[target]
	if !ok || td.Err != nil {
		return ErrInvalid{fmt.Errorf("target topic %q does not exist", target)}
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
	if err := validBound(req.From, "oldest", "from"); err != nil {
		return nil, err
	}

	if err := validBound(req.To, "newest", "to"); err != nil {
		return nil, err
	}

	starts, err := c.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list start offsets; %w", err)
	}

	// The last stable offset, not the high watermark: records past it belong
	// to open transactions and a read-committed reader would wait for them.
	ends, err := c.admin.ListCommittedOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets; %w", err)
	}

	var fromTime, toTime kadm.ListedOffsets

	if req.From.Type == "timestamp" {
		if fromTime, err = c.admin.ListOffsetsAfterMilli(ctx, req.From.Timestamp, topic); err != nil {
			return nil, fmt.Errorf("list offsets by time; %w", err)
		}
	}

	if req.To.Type == "timestamp" {
		if toTime, err = c.admin.ListOffsetsAfterMilli(ctx, req.To.Timestamp+1, topic); err != nil {
			return nil, fmt.Errorf("list offsets by time; %w", err)
		}
	}

	all := map[int32]bool{}

	starts.Each(func(s kadm.ListedOffset) {
		if s.Topic == topic && s.Partition >= 0 && s.Err == nil {
			all[s.Partition] = true
		}
	})

	if len(all) == 0 {
		return nil, fmt.Errorf("topic %q; %w", topic, ErrNotFound)
	}

	parts := req.Partitions
	if len(parts) == 0 {
		for p := range all {
			parts = append(parts, p)
		}
	}

	seen := map[int32]bool{}
	out := make([]ReplayRange, 0, len(parts))

	for _, p := range parts {
		if !all[p] {
			return nil, ErrInvalid{fmt.Errorf("topic %q has no partition %d", topic, p)}
		}

		if seen[p] {
			continue
		}

		seen[p] = true

		s, _ := starts.Lookup(topic, p)

		e, ok := ends.Lookup(topic, p)
		if !ok || e.Err != nil {
			return nil, fmt.Errorf("end offset of partition %d unavailable", p)
		}

		r := ReplayRange{Partition: p, Start: s.Offset, End: e.Offset}
		low, high := r.Start, r.End

		switch req.From.Type {
		case "offset":
			r.Start = req.From.Offset
		case "timestamp":
			r.Start = timeOffset(fromTime, topic, p, high)
		}

		switch req.To.Type {
		case "offset":
			r.End = req.To.Offset + 1
		case "timestamp":
			r.End = timeOffset(toTime, topic, p, high)
		}

		r.Start = min(max(r.Start, low), high)
		r.End = min(max(r.End, r.Start), high)
		out = append(out, r)
	}

	sortRanges(out)

	return out, nil
}

func validBound(b RangeBound, open, name string) error {
	switch b.Type {
	case "", open, "timestamp":
		return nil
	case "offset":
		if b.Offset < 0 {
			return ErrInvalid{fmt.Errorf("%s offset must be non-negative", name)}
		}

		return nil
	}

	return ErrInvalid{fmt.Errorf("%s must be %s, offset or timestamp", name, open)}
}

// timeOffset is the first offset at or after a listed time; a partition with
// nothing that late resolves to its end.
func timeOffset(listed kadm.ListedOffsets, topic string, p int32, end int64) int64 {
	if t, ok := listed.Lookup(topic, p); ok && t.Err == nil && t.Offset >= 0 {
		return t.Offset
	}

	return end
}

func sortRanges(rs []ReplayRange) {
	for i := 1; i < len(rs); i++ {
		for j := i; j > 0 && rs[j].Partition < rs[j-1].Partition; j-- {
			rs[j], rs[j-1] = rs[j-1], rs[j]
		}
	}
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

	limit := int64(req.Limit)
	if limit <= 0 || limit > maxReplay {
		limit = maxReplay
	}

	var filter *script.Script
	if strings.TrimSpace(req.Filter) != "" {
		var err error

		if filter, err = script.Compile(req.Filter, filterParameters...); err != nil {
			return ErrInvalid{err}
		}
	}

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

	ev := ReplayEvent{Type: "plan", Plan: plan, Total: total}
	if !emit(ev) {
		return nil
	}

	ev.Plan = nil

	done := func(stoppedBy string) {
		ev.Type = "done"
		ev.StoppedBy = stoppedBy
		ev.ElapsedMs = time.Since(begin).Milliseconds()
		emit(ev)
	}

	if total == 0 {
		done("end")

		return nil
	}

	if req.DryRun && filter == nil && req.Search == "" {
		ev.Matched = min(total, limit)
		done("end")

		return nil
	}

	rctx, cancel := context.WithTimeout(ctx, maxReplayTime)
	defer cancel()

	offsets := make(map[int32]kgo.Offset, len(used))
	active := make(map[int32]int64, len(used))

	for _, r := range plan {
		if r.End > r.Start {
			offsets[r.Partition] = kgo.NewOffset().At(r.Start)
			active[r.Partition] = r.End
		}
	}

	opts := append(append([]kgo.Opt(nil), c.options...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}),
		kgo.FetchMaxWait(500*time.Millisecond),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)

	reader, err := kgo.NewClient(opts...)
	if err != nil {
		ev.FirstError = fmt.Sprintf("create reader; %v", err)
		done("error")

		return nil
	}
	defer reader.Close()

	var producer *kgo.Client
	if !req.DryRun {
		if producer, err = c.producer(req.KeepPartition, ""); err != nil {
			ev.FirstError = fmt.Sprintf("producer; %v", err)
			done("error")

			return nil
		}
	}

	copyOpts := replayOptions{
		source: topic, target: target, keepPartition: req.KeepPartition, keepTimestamp: req.KeepTimestamp,
		markSource: req.MarkSource, extraHeaders: req.ExtraHeaders,
	}

	var (
		counters  replayCounters
		inflight  sync.WaitGroup
		stoppedBy string
		empty     int
		last      = time.Now()
	)

	snapshot := func() {
		ev.Written = counters.written.Load()
		ev.Failed = counters.failed.Load()

		if e := counters.first(); e != "" && ev.FirstError == "" {
			ev.FirstError = e
		}
	}

	progress := func() bool {
		if time.Since(last) < replayEvery {
			return true
		}

		last = time.Now()
		snapshot()
		ev.Type = "progress"
		ev.ElapsedMs = time.Since(begin).Milliseconds()

		return emit(ev)
	}

	finish := func(rec *kgo.Record) {
		delete(active, rec.Partition)
		reader.PauseFetchPartitions(map[string][]int32{topic: {rec.Partition}})
	}

	visit := func(rec *kgo.Record) {
		if stoppedBy != "" {
			return
		}

		// Reported per record too: one poll can take long with a slow filter.
		if !progress() {
			cancel()
			stoppedBy = "cancelled"

			return
		}

		end, ok := active[rec.Partition]
		if !ok {
			return
		}

		if rec.Offset >= end {
			finish(rec)

			return
		}

		if rec.Offset == end-1 {
			finish(rec)
		}

		ev.Scanned++

		d := c.decodeRecord(rctx, rec, false)
		if !textMatch(req.Search, rec, d) {
			return
		}

		if filter != nil {
			fctx, fcancel := context.WithTimeout(rctx, tailScriptBudget)
			guard := script.Guard(fctx, filter, "filter timed out")
			ok, err := filter.Call(filterArguments(filter, rec, d)...)

			guard()
			fcancel()
			filter.Close()

			if err != nil {
				ev.ScriptErrors++
				if ev.FirstError == "" {
					ev.FirstError = err.Error()
				}

				return
			}

			if !ok {
				return
			}
		}

		ev.Matched++

		if producer != nil {
			if counters.failed.Load() > 0 {
				stoppedBy = "error"

				return
			}

			inflight.Add(1)
			producer.Produce(rctx, copyRecord(rec, copyOpts), func(_ *kgo.Record, err error) {
				defer inflight.Done()

				if err != nil {
					counters.fail(err)

					return
				}

				counters.written.Add(1)
			})
		}

		if ev.Matched >= limit {
			stoppedBy = "limit"
		}
	}

	for len(active) > 0 && stoppedBy == "" {
		if rctx.Err() != nil {
			break
		}

		pctx, pcancel := context.WithTimeout(rctx, pollTimeout)
		fetches := reader.PollRecords(pctx, replayPoll)
		pcancel()

		if err := fetches.Err0(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			ev.FirstError = fmt.Sprintf("fetch from %s; %v", topic, err)
			stoppedBy = "error"

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

		fetches.EachRecord(visit)

		if counters.failed.Load() > 0 {
			stoppedBy = "error"
		}

		if !progress() {
			cancel()
		}
	}

	switch {
	case stoppedBy != "":
	case ctx.Err() != nil:
		stoppedBy = "cancelled"
	case rctx.Err() != nil:
		stoppedBy = "timeout"
	default:
		stoppedBy = "end"
	}

	// Wait for writes still buffered, reporting progress meanwhile.
	flushed := make(chan struct{})

	go func() {
		inflight.Wait()
		close(flushed)
	}()

	tick := time.NewTicker(replayEvery)
	defer tick.Stop()

	for waiting := true; waiting; {
		select {
		case <-flushed:
			waiting = false
		case <-tick.C:
			last = time.Time{}
			if !progress() {
				cancel()
			}
		}
	}

	snapshot()

	if stoppedBy != "error" && ev.Failed > 0 {
		stoppedBy = "error"
	}

	done(stoppedBy)

	return nil
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

	producer, err := c.producer(req.KeepPartition, codec)
	if err != nil {
		return nil, fmt.Errorf("producer; %w", err)
	}

	pctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	// Results arrive in completion order, not the order produced.
	at := make(map[*kgo.Record]int, len(records))
	for i, rec := range records {
		at[rec] = i
	}

	for _, r := range producer.ProduceSync(pctx, records...) {
		i := at[r.Record]
		if r.Err != nil {
			out[i] = ImportResult{Partition: -1, Offset: -1, Error: r.Err.Error()}

			continue
		}

		out[i] = ImportResult{Partition: r.Record.Partition, Offset: r.Record.Offset}
	}

	return out, nil
}
