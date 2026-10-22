package server

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/kafka"
	"github.com/rakunlabs/kafka-console/internal/script"
)

// handle runs fn with the request's cluster and replies with its result, or
// 204 when the result is nil.
func (s *Server) handle(c *ada.Context, fn func(cl *kafka.Client) (any, error)) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	out, err := fn(cl)
	if err != nil {
		return fail(err)
	}

	if out == nil {
		return c.SendNoContent()
	}

	return c.SendJSON(out)
}

func bind[T any](c *ada.Context) (T, error) {
	var v T

	return v, bindErr(c.Bind(&v))
}

// bindErr makes a body that could not be read a 400, except one over the
// size limit: ada answers that with 413 itself.
func bindErr(err error) error {
	if err == nil {
		return nil
	}

	if _, ok := errors.AsType[*http.MaxBytesError](err); ok {
		return err
	}

	return ada.WrapHTTPError(http.StatusBadRequest, err)
}

// ─── Brokers ──────────────────────────────────────────────

func brokerID(c *ada.Context) (int32, error) {
	n, err := strconv.ParseInt(c.Request.PathValue("broker"), 10, 32)
	if err != nil {
		return 0, ada.NewHTTPError(http.StatusBadRequest, "broker must be a number")
	}

	return int32(n), nil
}

func (s *Server) broker(c *ada.Context) error {
	id, err := brokerID(c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.Broker(c.Request.Context(), id) })
}

func (s *Server) alterBrokerConfigs(c *ada.Context) error {
	id, err := brokerID(c)
	if err != nil {
		return err
	}

	req, err := bind[[]kafka.ConfigChange](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.AlterBrokerConfigs(c.Request.Context(), id, req)
	})
}

// ─── Topics ───────────────────────────────────────────────

func (s *Server) alterTopicConfigs(c *ada.Context) error {
	req, err := bind[[]kafka.ConfigChange](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.AlterTopicConfigs(c.Request.Context(), c.Request.PathValue("topic"), req)
	})
}

func (s *Server) addPartitions(c *ada.Context) error {
	req, err := bind[struct {
		Total int `json:"total"`
	}](c)
	if err != nil {
		return err
	}

	if req.Total <= 0 {
		return ada.NewHTTPError(http.StatusBadRequest, "total must be positive")
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.AddPartitions(c.Request.Context(), c.Request.PathValue("topic"), req.Total)
	})
}

func (s *Server) deleteRecords(c *ada.Context) error {
	req, err := bind[kafka.DeleteRecordsRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.DeleteRecords(c.Request.Context(), c.Request.PathValue("topic"), req)
	})
}

// checkFilter lints a message filter with the engine that will run it. It is
// cluster independent and touches no data.
func (s *Server) checkFilter(c *ada.Context) error {
	req, err := bind[struct {
		Source string `json:"source"`
	}](c)
	if err != nil {
		return err
	}

	problems := kafka.CheckFilter(req.Source)
	if problems == nil {
		problems = []script.Problem{}
	}

	return c.SendJSON(map[string]any{"problems": problems})
}

// tail streams messages as server-sent events until the client disconnects.
func (s *Server) tail(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	p, err := parseRead(c.Request.URL.Query(), []string{"newest", "oldest", "offset", "timestamp", "last"},
		"offset", "timestamp", "limit")
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
		return out.fail(err, func() { out.event("error", map[string]string{"error": err.Error()}) })
	}

	out.event("end", map[string]int64{"sent": sent})

	return nil
}

// replay streams newline-delimited JSON events: a plan, progress and a final
// done. Closing the connection stops the replay; records still buffered in
// the producer may or may not be written, as with any cancelled produce.
func (s *Server) replay(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	req, err := bind[kafka.ReplayRequest](c)
	if err != nil {
		return err
	}

	out := newStream(c.Response, ndjson)

	err = cl.Replay(c.Request.Context(), c.Request.PathValue("topic"), req, func(e kafka.ReplayEvent) bool {
		return out.line(e)
	})
	if err != nil {
		return out.fail(err, func() {
			out.line(kafka.ReplayEvent{Type: "done", StoppedBy: "error", FirstError: err.Error()})
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

// ─── Groups ───────────────────────────────────────────────

func (s *Server) deleteGroupOffsets(c *ada.Context) error {
	req, err := bind[struct {
		Topic      string  `json:"topic"`
		Partitions []int32 `json:"partitions"`
	}](c)
	if err != nil {
		return err
	}

	if req.Topic == "" {
		return ada.NewHTTPError(http.StatusBadRequest, "topic is required")
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.DeleteGroupOffsets(c.Request.Context(), c.Request.PathValue("group"), req.Topic, req.Partitions)
	})
}

// ─── Security ─────────────────────────────────────────────

func (s *Server) topicDoc(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.TopicDoc(c.Request.PathValue("topic"))
	})
}

func (s *Server) reassign(c *ada.Context) error {
	req, err := bind[kafka.ReassignRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Reassign(c.Request.Context(), c.Request.PathValue("topic"), req)
	})
}

func (s *Server) reassignments(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Reassignments(c.Request.Context(), c.Request.PathValue("topic"))
	})
}

func (s *Server) balancePlan(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.BalancePlan(c.Request.Context(), c.Request.PathValue("topic"))
	})
}

func (s *Server) electLeaders(c *ada.Context) error {
	req, err := bind[struct {
		Partitions []int32 `json:"partitions"`
	}](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.ElectPreferredLeaders(c.Request.Context(), c.Request.PathValue("topic"), req.Partitions)
	})
}

func (s *Server) acls(c *ada.Context) error {
	q := c.Request.URL.Query()
	if q.Has("resource_type") {
		r := kafka.ACLFor{Type: q.Get("resource_type"), Name: q.Get("resource_name")}

		return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.ResourceACLs(c.Request.Context(), r) })
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.ACLs(c.Request.Context()) })
}

func (s *Server) createACLs(c *ada.Context) error {
	req, err := bind[[]kafka.ACL](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return nil, cl.CreateACLs(c.Request.Context(), req) })
}

func (s *Server) deleteACL(c *ada.Context) error {
	req, err := bind[kafka.ACL](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		n, err := cl.DeleteACL(c.Request.Context(), req)
		if err != nil {
			return nil, err
		}

		return map[string]int{"deleted": n}, nil
	})
}

func (s *Server) users(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.Users(c.Request.Context()) })
}

func (s *Server) upsertUser(c *ada.Context) error {
	req, err := bind[kafka.UpsertUserRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return nil, cl.UpsertUser(c.Request.Context(), req) })
}

func (s *Server) deleteUser(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.DeleteUser(c.Request.Context(), c.Request.PathValue("user"), c.Request.URL.Query().Get("mechanism"))
	})
}

func (s *Server) quotas(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.Quotas(c.Request.Context()) })
}

func (s *Server) alterQuota(c *ada.Context) error {
	req, err := bind[kafka.AlterQuotaRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return nil, cl.AlterQuota(c.Request.Context(), req) })
}

// ─── Schema registry ──────────────────────────────────────

func (s *Server) schemaRegistry(c *ada.Context) error {
	q := c.Request.URL.Query()
	query := kafka.RegistryQuery{ShowDeleted: q.Get("deleted") == "true", Search: q.Get("q"), Type: q.Get("type"), Context: q.Get("context")}

	var err error
	if query.Offset, query.Limit, err = pageParams(q); err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Registry(c.Request.Context(), query)
	})
}

func (s *Server) subject(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Subject(c.Request.Context(), c.Request.PathValue("subject"))
	})
}

func (s *Server) createSchema(c *ada.Context) error {
	req, err := bind[kafka.CreateSchemaRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.CreateSchema(c.Request.Context(), req) })
}

func (s *Server) checkCompatibility(c *ada.Context) error {
	req, err := bind[kafka.CreateSchemaRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.CheckCompatibility(c.Request.Context(), req) })
}

type compatibilityBody struct {
	Level string `json:"level"`
}

func (s *Server) setGlobalCompatibility(c *ada.Context) error {
	req, err := bind[compatibilityBody](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.SetCompatibility(c.Request.Context(), "", req.Level)
	})
}

func (s *Server) setCompatibility(c *ada.Context) error {
	req, err := bind[compatibilityBody](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.SetCompatibility(c.Request.Context(), c.Request.PathValue("subject"), req.Level)
	})
}

func (s *Server) deleteSubject(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.DeleteSubject(c.Request.Context(), c.Request.PathValue("subject"), c.Request.URL.Query().Get("permanent") == "true")
	})
}

func (s *Server) deleteSchemaVersion(c *ada.Context) error {
	version, err := strconv.Atoi(c.Request.PathValue("version"))
	if err != nil {
		return ada.NewHTTPError(http.StatusBadRequest, "version must be a number")
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.DeleteSchemaVersion(c.Request.Context(), c.Request.PathValue("subject"), version,
			c.Request.URL.Query().Get("permanent") == "true")
	})
}
