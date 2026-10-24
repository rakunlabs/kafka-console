package server

import (
	"fmt"
	"mime"
	"net/http"
	"strconv"
	"time"

	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

func (s *Server) messages(c *ada.Context) error {
	q := c.Request.URL.Query()

	p, err := parseRead(q, []string{kafka.StartNewest, kafka.StartOldest, kafka.StartOffset, kafka.StartTimestamp, kafka.StartLast},
		"offset", "timestamp", "since", "until", "limit", "timeout")
	if err != nil {
		return err
	}

	req := kafka.ConsumeRequest{
		Start:      p.start,
		Offset:     p.ints["offset"],
		Timestamp:  p.ints["timestamp"],
		Since:      p.optional("since"),
		Until:      p.optional("until"),
		Limit:      int(p.ints["limit"]),
		Filter:     p.filter,
		Search:     p.search,
		Timeout:    time.Duration(p.ints["timeout"]) * time.Second,
		Raw:        p.raw,
		Exact:      q.Get("exact") == "true",
		As:         p.as,
		Partitions: p.partitions,
	}

	if req.After, err = resumeAt(q); err != nil {
		return err
	}

	if q.Get("stream") == "true" {
		cl, err := s.client(c)
		if err != nil {
			return err
		}

		return s.messageStream(c, cl, req)
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Consume(c.Request.Context(), c.Request.PathValue("topic"), req)
	})
}

// messageStreamEvent is one line of a streamed read: "message", "progress"
// (scanned so far and where a stopped read would continue) or a final "done"
// carrying the result without its messages.
type messageStreamEvent struct {
	Type    string               `json:"type"`
	Message *kafka.Message       `json:"message,omitempty"`
	Scanned int                  `json:"scanned"`
	Next    map[int32]int64      `json:"next,omitempty"`
	Result  *kafka.ConsumeResult `json:"result,omitempty"`
	Error   string               `json:"error,omitempty"`
}

// messageStream sends each match as a newline-delimited JSON event as soon
// as it is found. Closing the connection stops the read on the server.
func (s *Server) messageStream(c *ada.Context, cl *kafka.Client, req kafka.ConsumeRequest) error {
	out := newStream(c.Response, ndjson)

	res, err := cl.ConsumeStream(c.Request.Context(), c.Request.PathValue("topic"), req, func(e kafka.ConsumeEvent) bool {
		if e.Message != nil {
			return out.line(messageStreamEvent{Type: "message", Message: e.Message, Scanned: e.Scanned})
		}

		return out.line(messageStreamEvent{Type: "progress", Scanned: e.Scanned, Next: e.Next})
	})
	if err != nil {
		return out.finish(err, func() { out.line(messageStreamEvent{Type: "error", Error: err.Error()}) })
	}

	out.line(messageStreamEvent{Type: "done", Scanned: res.Scanned, Result: res})

	return nil
}

func recordRef(c *ada.Context) (kafka.RecordRef, error) {
	partition, err := strconv.ParseInt(c.Request.PathValue("partition"), 10, 32)
	if err != nil || partition < 0 {
		return kafka.RecordRef{}, ada.NewHTTPError(http.StatusBadRequest, "partition must be a non-negative integer")
	}
	offset, err := strconv.ParseInt(c.Request.PathValue("offset"), 10, 64)
	if err != nil || offset < 0 {
		return kafka.RecordRef{}, ada.NewHTTPError(http.StatusBadRequest, "offset must be a non-negative integer")
	}

	return kafka.RecordRef{Partition: int32(partition), Offset: offset}, nil
}

func (s *Server) message(c *ada.Context) error {
	ref, err := recordRef(c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Message(c.Request.Context(), c.Request.PathValue("topic"), ref)
	})
}

// rawMessage downloads a record's key or value (?part=key) exactly as stored,
// schema framing included.
func (s *Server) rawMessage(c *ada.Context) error {
	ref, err := recordRef(c)
	if err != nil {
		return err
	}

	part := c.Request.URL.Query().Get("part")
	if part != "" && part != "key" && part != "value" {
		return ada.NewHTTPError(http.StatusBadRequest, "part must be key or value")
	}

	cl, err := s.client(c)
	if err != nil {
		return err
	}

	topic := c.Request.PathValue("topic")

	rec, err := cl.RawRecord(c.Request.Context(), topic, ref)
	if err != nil {
		return fail(err)
	}

	b := rec.Value
	if part == "key" {
		b = rec.Key
	} else {
		part = "value"
	}

	if b == nil {
		return ada.NewHTTPError(http.StatusNotFound, "the record's "+part+" is null")
	}

	name := fmt.Sprintf("%s-%d-%d-%s.bin", topic, ref.Partition, ref.Offset, part)
	w := c.Response
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)

	return nil
}

// tail streams messages as server-sent events until the client disconnects.
func (s *Server) tail(c *ada.Context) error {
	p, err := parseRead(c.Request.URL.Query(), []string{kafka.StartNewest, kafka.StartOldest, kafka.StartOffset, kafka.StartTimestamp, kafka.StartLast},
		"offset", "timestamp", "limit")
	if err != nil {
		return err
	}

	cl, err := s.client(c)
	if err != nil {
		return err
	}

	req := kafka.TailRequest{
		Start:      p.start,
		Offset:     p.ints["offset"],
		Timestamp:  p.ints["timestamp"],
		Filter:     p.filter,
		Search:     p.search,
		Raw:        p.raw,
		As:         p.as,
		Partitions: p.partitions,
	}

	limit := p.ints["limit"]
	out := newStream(c.Response, eventStream)
	sent := int64(0)

	err = cl.Tail(c.Request.Context(), c.Request.PathValue("topic"), req, func(e kafka.TailEvent) bool {
		if e.Message != nil {
			sent++
		}

		if !out.event("message", e) {
			return false
		}

		return limit <= 0 || sent < limit
	})
	if err != nil {
		return out.finish(err, func() { out.event("error", map[string]string{"error": err.Error()}) })
	}

	out.event("end", map[string]int64{"sent": sent})

	return nil
}

// ─── Writes ───────────────────────────────────────────────

func (s *Server) produce(c *ada.Context) error {
	req, err := bind[kafka.ProduceRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Produce(c.Request.Context(), c.Request.PathValue("topic"), req)
	})
}

func (s *Server) resend(c *ada.Context) error {
	req, err := bind[kafka.ResendRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Resend(c.Request.Context(), c.Request.PathValue("topic"), req)
	})
}

// replay streams newline-delimited JSON events: a plan, progress and a final
// done. Closing the connection stops the replay; records still buffered in
// the producer may or may not be written, as with any cancelled produce.
func (s *Server) replay(c *ada.Context) error {
	req, err := bind[kafka.ReplayRequest](c)
	if err != nil {
		return err
	}

	cl, err := s.client(c)
	if err != nil {
		return err
	}

	out := newStream(c.Response, ndjson)

	err = cl.Replay(c.Request.Context(), c.Request.PathValue("topic"), req, func(e kafka.ReplayEvent) bool {
		return out.line(e)
	})
	if err != nil {
		return out.finish(err, func() {
			out.line(kafka.ReplayEvent{Type: "done", StoppedBy: kafka.StopError, FirstError: err.Error()})
		})
	}

	return nil
}

func (s *Server) importRecords(c *ada.Context) error {
	req, err := bind[kafka.ImportRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Import(c.Request.Context(), c.Request.PathValue("topic"), req)
	})
}
