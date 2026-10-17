package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/rakunlabs/kafka-console/internal/script"
)

// TailRequest follows a topic from now on, or from a chosen start.
type TailRequest struct {
	Partitions []int32
	// Start is newest (only new messages), oldest, offset or timestamp.
	Start     string
	Offset    int64
	Timestamp int64
	Filter    string
	Search    string
	Raw       bool
}

// TailEvent is one message or a periodic progress report.
type TailEvent struct {
	Message *Message `json:"message,omitempty"`
	Scanned int64    `json:"scanned"`
	Matched int64    `json:"matched"`
	Error   string   `json:"error,omitempty"`
}

const (
	tailBatchCap      = 200
	tailProgressEvery = time.Second
	// tailScriptBudget bounds a single filter call so a runaway script cannot
	// stall a stream that otherwise never ends.
	tailScriptBudget = 2 * time.Second
)

// Tail streams records to emit until ctx ends or emit returns false.
func (c *Client) Tail(ctx context.Context, topic string, req TailRequest, emit func(TailEvent) bool) error {
	var filter *script.Script
	if strings.TrimSpace(req.Filter) != "" {
		var err error

		filter, err = script.Compile(req.Filter, filterParameters...)
		if err != nil {
			return ErrInvalid{err}
		}
	}

	offsets, err := c.tailOffsets(ctx, topic, req)
	if err != nil {
		return err
	}

	if len(offsets) == 0 {
		return fmt.Errorf("topic %q; %w", topic, ErrNotFound)
	}

	opts := append(append([]kgo.Opt(nil), c.options...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}),
		kgo.FetchMaxWait(500*time.Millisecond),
	)

	reader, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("create reader; %w", err)
	}
	defer reader.Close()

	var scanned, matched int64

	lastProgress := time.Now()

	for {
		if ctx.Err() != nil {
			return nil
		}

		pctx, cancel := context.WithTimeout(ctx, tailProgressEvery)
		fetches := reader.PollRecords(pctx, tailBatchCap)
		cancel()

		if err := fetches.Err0(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			if !emit(TailEvent{Scanned: scanned, Matched: matched, Error: err.Error()}) {
				return nil
			}
		}

		stop := false

		fetches.EachRecord(func(rec *kgo.Record) {
			if stop {
				return
			}

			scanned++

			d := c.decodeRecord(ctx, rec, req.Raw)

			if !textMatch(req.Search, rec, d) {
				return
			}

			if filter != nil {
				fctx, fcancel := context.WithTimeout(ctx, tailScriptBudget)
				guard := script.Guard(fctx, filter, "filter timed out")
				ok, err := filter.Call(filterArguments(filter, rec, d)...)

				guard()
				fcancel()
				filter.Close()

				if err != nil || !ok {
					return
				}
			}

			matched++

			m := render(rec, d, defaultValueCutoff)
			if !emit(TailEvent{Message: &m, Scanned: scanned, Matched: matched}) {
				stop = true
			}
		})

		if stop {
			return nil
		}

		if time.Since(lastProgress) >= tailProgressEvery {
			lastProgress = time.Now()

			if !emit(TailEvent{Scanned: scanned, Matched: matched}) {
				return nil
			}
		}
	}
}

func (c *Client) tailOffsets(ctx context.Context, topic string, req TailRequest) (map[int32]kgo.Offset, error) {
	ends, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets; %w", err)
	}

	wanted := map[int32]bool{}
	for _, p := range req.Partitions {
		wanted[p] = true
	}

	out := map[int32]kgo.Offset{}

	for _, ps := range ends {
		for p, o := range ps {
			if o.Err != nil || (len(wanted) > 0 && !wanted[p]) {
				continue
			}

			switch req.Start {
			case "oldest":
				out[p] = kgo.NewOffset().AtStart()
			case "offset":
				out[p] = kgo.NewOffset().At(req.Offset)
			case "timestamp":
				out[p] = kgo.NewOffset().AfterMilli(req.Timestamp)
			default:
				out[p] = kgo.NewOffset().At(o.Offset)
			}
		}
	}

	return out, nil
}
