package server

import (
	"net/http"
	"strconv"

	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

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

// ─── Partition placement ──────────────────────────────────

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
