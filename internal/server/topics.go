package server

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

func (s *Server) topics(c *ada.Context) error {
	q := c.Request.URL.Query()
	query := kafka.TopicQuery{
		Search:   q.Get("q"),
		Internal: q.Get("internal") == "true",
		Sort:     q.Get("sort"),
		Desc:     q.Get("desc") == "true",
	}

	switch query.Sort {
	case "":
		query.Sort = "name"
	case "name", "partitions", "messages", "size", "under_replicated":
	default:
		return ada.WrapHTTPError(http.StatusBadRequest, fmt.Errorf("unknown sort %q", query.Sort))
	}

	var err error
	if query.Offset, query.Limit, err = pageParams(q); err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.Topics(c.Request.Context(), query) })
}

func (s *Server) topic(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Topic(c.Request.Context(), c.Request.PathValue("topic"))
	})
}

func (s *Server) createTopic(c *ada.Context) error {
	req, err := bind[kafka.CreateTopicRequest](c)
	if err != nil {
		return err
	}

	if strings.TrimSpace(req.Name) == "" {
		return ada.NewHTTPError(http.StatusBadRequest, "topic name is required")
	}

	return s.created(c, func(cl *kafka.Client) (any, error) {
		return map[string]string{"name": req.Name}, cl.CreateTopic(c.Request.Context(), req)
	})
}

func (s *Server) deleteTopic(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.DeleteTopic(c.Request.Context(), c.Request.PathValue("topic"))
	})
}

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

func (s *Server) topicConsumers(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.TopicConsumers(c.Request.Context(), c.Request.PathValue("topic"))
	})
}

func (s *Server) topicDoc(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.TopicDoc(c.Request.PathValue("topic"))
	})
}
