package server

import (
	"fmt"
	"net/http"

	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

func (s *Server) groups(c *ada.Context) error {
	q := c.Request.URL.Query()
	query := kafka.GroupQuery{Search: q.Get("q"), Sort: q.Get("sort"), Desc: q.Get("desc") == "true"}

	switch query.Sort {
	case "":
		query.Sort = "name"
	case "name", "lag", "members":
	default:
		return ada.WrapHTTPError(http.StatusBadRequest, fmt.Errorf("unknown sort %q", query.Sort))
	}

	var err error
	if query.Offset, query.Limit, err = pageParams(q); err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.Groups(c.Request.Context(), query) })
}

func (s *Server) group(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Group(c.Request.Context(), c.Request.PathValue("group"))
	})
}

func (s *Server) deleteGroup(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return nil, cl.DeleteGroup(c.Request.Context(), c.Request.PathValue("group"))
	})
}

func (s *Server) createGroup(c *ada.Context) error {
	req, err := bind[kafka.CreateGroupRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.CreateGroup(c.Request.Context(), req)
	})
}

func (s *Server) addGroupTopic(c *ada.Context) error {
	req, err := bind[kafka.CreateGroupRequest](c)
	if err != nil {
		return err
	}

	req.Group = c.Request.PathValue("group")

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.AddGroupTopic(c.Request.Context(), req)
	})
}

func (s *Server) resetOffsets(c *ada.Context) error {
	req, err := bind[kafka.ResetRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		plan, err := cl.ResetOffsets(c.Request.Context(), c.Request.PathValue("group"), req)
		if err != nil {
			return nil, err
		}

		return map[string]any{"dry_run": req.DryRun, "partitions": plan}, nil
	})
}

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
