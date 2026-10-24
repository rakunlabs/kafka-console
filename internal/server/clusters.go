package server

import (
	"github.com/rakunlabs/ada"

	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/rakunlabs/kafka-console/internal/kafka"
	"github.com/rakunlabs/kafka-console/internal/script"
)

type clusterInfo struct {
	Name           string   `json:"name"`
	Brokers        []string `json:"brokers"`
	ReadOnly       bool     `json:"read_only"`
	ConfirmWrites  bool     `json:"confirm_writes"`
	Color          string   `json:"color,omitempty"`
	SchemaRegistry bool     `json:"schema_registry"`
	Docs           bool     `json:"docs"`
	// Description is HTML rendered from the configured Markdown.
	Description string `json:"description,omitempty"`
	// Features are the cluster's own, with the global ones underneath.
	Features features `json:"features"`
	// MCP lists this cluster's agent endpoints; empty when MCP is off.
	MCP []mcpEndpointInfo `json:"mcp"`
}

func (s *Server) appInfo(c *ada.Context) error {
	return c.SendJSON(map[string]any{
		"info": s.info, "version": config.Version, "features": s.features,
		"max_limit": kafka.MaxLimit,
	})
}

func (s *Server) clusters(c *ada.Context) error {
	out := make([]clusterInfo, 0, len(s.registry.Names()))
	for _, name := range s.registry.Names() {
		cl, _ := s.registry.Get(name)
		out = append(out, clusterInfo{
			Name: name, Brokers: cl.Brokers(), ReadOnly: cl.ReadOnly(), ConfirmWrites: cl.ConfirmWrites(), Color: cl.Color(),
			SchemaRegistry: cl.HasRegistry(), Docs: cl.HasDocs(), Description: cl.Description(),
			Features: s.featuresOf(name),
			MCP:      nonNilEndpoints(s.mcp[name]),
		})
	}

	return c.SendJSON(out)
}

// overview and disk usage are kept in memory per cluster for a few seconds;
// ?fresh=true (the Refresh button) asks for a new read.
func (s *Server) overview(c *ada.Context) error {
	fresh := c.Request.URL.Query().Get("fresh") == "true"

	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.Overview(c.Request.Context(), fresh) })
}

func (s *Server) diskUsage(c *ada.Context) error {
	fresh := c.Request.URL.Query().Get("fresh") == "true"

	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.DiskUsage(c.Request.Context(), fresh) })
}

func (s *Server) names(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) { return cl.Names(c.Request.Context()) })
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
