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

	if err := validStart(req.Start); err != nil {
		return err
	}

	match, err := c.newMatcher(ctx, req.Search, req.Filter, req.Raw, req.As)
	if err != nil {
		return err
	}
	defer match.Stop()

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

	reader, err := c.newKafkaClient(
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: offsets}),
		kgo.FetchMaxWait(streamFetchWait),
	)
	if err != nil {
		return fmt.Errorf("create reader; %w", err)
	}
	defer reader.Close()

	var scanned, matched int64

	lastProgress := time.Now()

	progress := func(m *Message, errText string) bool {
		return emit(TailEvent{
			Message: m, Scanned: scanned, Matched: matched, Error: errText,
			ScriptErrors: match.scriptErrors, FirstError: match.firstError,
		})
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		pctx, cancel := context.WithTimeout(ctx, tailProgressEvery)
		fetches := reader.PollRecords(pctx, tailBatchCap)
		cancel()

		if err := fetches.Err0(); err != nil && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
			if !progress(nil, err.Error()) {
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

			d, ok, err := match.match(ctx, rec)
			if err != nil {
				stop = true

				return
			}

			if !ok {
				return
			}

			matched++

			m := render(rec, d, defaultValueCutoff)
			if !progress(&m, "") {
				stop = true
			}
		})

		if stop {
			return nil
		}

		if time.Since(lastProgress) >= tailProgressEvery {
			lastProgress = time.Now()

			if !progress(nil, "") {
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

	wanted := partitionSet(req.Partitions)

	out := map[int32]kgo.Offset{}

	for _, ps := range ends {
		for p, o := range ps {
			if o.Err != nil || (len(wanted) > 0 && !wanted[p]) {
				continue
			}

			switch req.Start {
			case StartOldest:
				out[p] = kgo.NewOffset().AtStart()
			case StartLast:
				// The last N of each partition, then everything new.
				out[p] = kgo.NewOffset().AtEnd().Relative(-max(req.Offset, 0))
			case StartOffset:
				// Past a partition's end the broker answers out of range and
				// the reader would rewind it to the start, replaying the
				// whole partition; from its end it just waits for new ones.
				out[p] = kgo.NewOffset().At(min(max(req.Offset, 0), o.Offset))
			case StartTimestamp:
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
