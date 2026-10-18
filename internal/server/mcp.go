package server

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"slices"
	"strings"

	"github.com/denizgursoy/kafka-mcp/pkg/kafkamcp"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/rakunlabs/ada"
	mcors "github.com/rakunlabs/ada/middleware/cors"

	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/rakunlabs/kafka-console/internal/kafka"
)

// mcpEndpoints returns the configured endpoints, or one read-only-by-default
// endpoint per cluster at /mcp/<cluster>.
func mcpEndpoints(cfg *config.Config) map[string]*config.MCPEndpoint {
	if len(cfg.MCP.Endpoints) > 0 {
		return cfg.MCP.Endpoints
	}
	endpoints := make(map[string]*config.MCPEndpoint, len(cfg.Clusters))
	for name := range cfg.Clusters {
		endpoints[name] = &config.MCPEndpoint{Cluster: name, Path: "/mcp/" + url.PathEscape(name)}
	}
	return endpoints
}

// mcpClusters lends every console connection to the tools. Without
// allow_writes every cluster is read-only for MCP, so neither an endpoint's
// own cluster nor a named destination can be written.
func mcpClusters(cfg *config.Config, registry *kafka.Registry) (*kafkamcp.Clusters, error) {
	var connections []kafkamcp.Connection
	if registry != nil {
		for _, name := range registry.Names() {
			client, _ := registry.Get(name)
			connection := client.MCPConnection()
			connection.ReadOnly = connection.ReadOnly || !cfg.MCP.AllowWrites
			connections = append(connections, connection)
		}
	}
	return kafkamcp.NewClusters(connections...)
}

// mcpHandler builds one MCP server per endpoint and routes by exact path.
// Every request passes the origin and bearer-token checks.
func mcpHandler(cfg *config.Config, registry *kafka.Registry) (*mcpRoutes, error) {
	clusters, err := mcpClusters(cfg, registry)
	if err != nil {
		return nil, err
	}

	byPath := make(map[string]http.Handler)
	for name, endpoint := range mcpEndpoints(cfg) {
		local, err := mcpPath(endpoint.Path)
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", name, err)
		}
		path := cfg.Server.BasePath + local
		if path == "/healthz" {
			return nil, fmt.Errorf("endpoint %q: path %q is the health check", name, local)
		}
		if _, taken := byPath[path]; taken {
			return nil, fmt.Errorf("endpoint %q: path %q is used twice", name, local)
		}

		server := mcp.NewServer(&mcp.Implementation{Name: "kafka-console-" + name, Version: config.Version}, nil)
		if err := clusters.Register(server, endpoint.Cluster, kafkamcp.Options{
			ReadOnly:     endpoint.ReadOnly || !cfg.MCP.AllowWrites,
			Destinations: endpoint.Destinations,
			Tools:        endpoint.Tools,
			Path:         path,
			OutputDir:    cfg.MCP.OutputDir,
		}); err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", name, err)
		}
		// One transport per endpoint, so a session from one path is not
		// accepted on another with a different policy.
		byPath[path] = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server }, nil)
	}

	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		byPath[r.URL.Path].ServeHTTP(w, r)
	})
	handler = mcpGuard(cfg.MCP, handler)
	if len(cfg.MCP.AllowOrigins) > 0 {
		handler = mcors.Middleware(mcors.WithConfig(mcors.Cors{
			AllowOrigins:  cfg.MCP.AllowOrigins,
			AllowMethods:  []string{http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodOptions},
			AllowHeaders:  []string{"content-type", "accept", "authorization", "cache-control", "last-event-id", "mcp-session-id", "mcp-protocol-version"},
			ExposeHeaders: []string{"Mcp-Session-Id"},
		}))(handler)
	}

	return &mcpRoutes{byPath: byPath, handler: handler}, nil
}

// mcpRoutes serves exactly the configured endpoint paths.
type mcpRoutes struct {
	byPath  map[string]http.Handler
	handler http.Handler
}

func (m *mcpRoutes) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m == nil || m.byPath[r.URL.Path] == nil {
		http.NotFound(w, r)
		return
	}
	m.handler.ServeHTTP(w, r)
}

// mcpPath cleans an endpoint path relative to server.base_path. Any path is
// accepted except those the console itself serves: the UI root and the API.
func mcpPath(value string) (string, error) {
	value = strings.TrimSpace(value)
	if strings.ContainsAny(value, "?#") {
		return "", fmt.Errorf("path %q must be a URL path, without a query or fragment", value)
	}
	// The router reads these as wildcards, which would serve more paths than
	// the one configured.
	if strings.ContainsAny(value, "{}*") {
		return "", fmt.Errorf("path %q must be literal, without {, } or *", value)
	}
	cleaned := path.Clean("/" + strings.TrimLeft(value, "/"))
	switch {
	case cleaned == "/":
		return "", fmt.Errorf("path %q would replace the UI", value)
	case cleaned == "/api" || strings.HasPrefix(cleaned, "/api/"):
		return "", fmt.Errorf("path %q is under the console API", value)
	}
	return cleaned, nil
}

// mcpGuard refuses browser origins that are not allowed and requests without
// the bearer token. CORS alone is not enough: a refused origin's POST would
// still run the tool even though the page cannot read the answer.
func mcpGuard(settings config.MCP, next http.Handler) http.Handler {
	token := []byte(settings.AuthToken)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if origin := r.Header.Get("Origin"); origin != "" &&
			!slices.Contains(settings.AllowOrigins, "*") && !slices.Contains(settings.AllowOrigins, origin) {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		// Browsers send no credentials on a preflight, and a preflight runs no tool.
		preflight := r.Method == http.MethodOptions && r.Header.Get("Access-Control-Request-Method") != ""
		if len(token) > 0 && !preflight {
			presented, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
			if !ok || subtle.ConstantTimeCompare([]byte(presented), token) != 1 {
				w.Header().Set("WWW-Authenticate", `Bearer realm="kafka-console-mcp"`)
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func mountMCP(server *ada.Server, cfg *config.Config, registry *kafka.Registry) error {
	var routes *mcpRoutes
	if cfg.MCP.Enabled {
		var err error
		if routes, err = mcpHandler(cfg, registry); err != nil {
			return fmt.Errorf("configure MCP; %w", err)
		}
	}
	// Unknown or disabled paths in the default /mcp namespace must not fall
	// through to the UI's SPA index, which would answer an MCP client with HTML.
	root := server.Group(cfg.Server.BasePath)
	root.Handle("/mcp", routes)
	root.HandleWildcard("/mcp/", routes)
	if routes != nil {
		for full := range routes.byPath {
			local := strings.TrimPrefix(full, cfg.Server.BasePath)
			if local != "/mcp" && !strings.HasPrefix(local, "/mcp/") {
				root.Handle(local, routes)
			}
		}
	}
	return nil
}
