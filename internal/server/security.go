package server

import (
	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/kafka"
)

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
