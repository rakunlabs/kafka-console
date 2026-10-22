package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TailRequest follows a topic from now on, or from a chosen start.
type TailRequest struct {
	Partitions []int32
	// Start is newest (only new messages), oldest, offset, timestamp or last
	// (the last Offset records of each partition, then new ones).
	Start     string
	Offset    int64
	Timestamp int64
	Filter    string
	Search    string
	Raw       bool
	As        DecodeAs
}

// TailEvent is one message or a periodic progress report.
type TailEvent struct {
	Message *Message `json:"message,omitempty"`
	Scanned int64    `json:"scanned"`
	Matched int64    `json:"matched"`
	Error   string   `json:"error,omitempty"`
	// ScriptErrors counts records the filter failed on; FirstError says why.
	ScriptErrors int64  `json:"script_errors,omitempty"`
	FirstError   string `json:"first_error,omitempty"`
}

const (
	tailBatchCap      = 200
	tailProgressEvery = time.Second
	// tailErrorPause spaces fetch errors that come back at once, such as a
	// deleted topic's, so a tail does not spin reporting them.
	tailErrorPause = time.Second
)

// Tail streams records to emit until ctx ends or emit returns false.
func (c *Client) Tail(ctx context.Context, topic string, req TailRequest, emit func(TailEvent) bool) error {
	if err := req.As.validate(); err != nil {
		return err
	}

	filter, err := compileFilter(req.Filter)
	if err != nil {
		return err
	}

	release, err := c.stream()
	if err != nil {
		return err
	}
	defer release()

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

	var scanned, matched, scriptErrors int64

	firstError := ""

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

			if fetches.NumRecords() == 0 {
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(tailErrorPause):
				}
			}
		}

		stop := false

		fetches.EachRecord(func(rec *kgo.Record) {
			if stop {
				return
			}

			scanned++

			d := c.decodeRecord(ctx, rec, req.Raw, req.As)

			if !textMatch(req.Search, rec, d) {
				return
			}

			if filter != nil {
				ok, err := evalFilter(ctx, filter, rec, d)
				if err != nil {
					scriptErrors++
					if firstError == "" {
						firstError = err.Error()
					}

					return
				}

				if !ok {
					return
				}
			}

			matched++

			m := render(rec, d, defaultValueCutoff)
			if !emit(TailEvent{Message: &m, Scanned: scanned, Matched: matched, ScriptErrors: scriptErrors, FirstError: firstError}) {
				stop = true
			}
		})

		if stop {
			return nil
		}

		if time.Since(lastProgress) >= tailProgressEvery {
			lastProgress = time.Now()

			if !emit(TailEvent{Scanned: scanned, Matched: matched, ScriptErrors: scriptErrors, FirstError: firstError}) {
				return nil
			}
		}
	}
}

func (c *Client) tailOffsets(ctx context.Context, topic string, req TailRequest) (map[int32]kgo.Offset, error) {
	ends, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("list end offsets; %w", accessErr(err))
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
			case "last":
				// The last N of each partition, then everything new.
				out[p] = kgo.NewOffset().AtEnd().Relative(-max(req.Offset, 0))
			case "offset":
				// Past a partition's end the broker answers out of range and
				// the reader would rewind it to the start, replaying the
				// whole partition; from its end it just waits for new ones.
				out[p] = kgo.NewOffset().At(min(max(req.Offset, 0), o.Offset))
			case "timestamp":
				out[p] = kgo.NewOffset().AfterMilli(req.Timestamp)
			default:
				out[p] = kgo.NewOffset().At(o.Offset)
			}
		}
	}

	// Every partition failed: say why, a refused topic as refused rather
	// than missing.
	if len(out) == 0 {
		if err := ends.Error(); err != nil && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			return nil, fmt.Errorf("topic %q; %w", topic, accessErr(err))
		}
	}

	return out, nil
}
