package server

import (
	"net/http"
	"strconv"

	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

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
