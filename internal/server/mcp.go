package server

import (
	"crypto/subtle"
	"fmt"
	"maps"
	"net"
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

// mcpOptIn are tools with no counterpart in the console, off unless an
// endpoint switches them on: transform_messages rewrites messages in bulk.
var mcpOptIn = []string{"transform_messages"}

// mcpFeatureTools are the MCP tools each feature gates.
var mcpFeatureTools = map[string][]string{
	config.FeatureSecurity:      {"list_acls"},
	config.FeatureCreateTopic:   {"create_topic"},
	config.FeatureDeleteTopic:   {"delete_topic"},
	config.FeatureAlterTopic:    {"alter_topic_config", "add_partitions"},
	config.FeatureDeleteRecords: {"delete_records"},
	config.FeatureDeleteGroup:   {"delete_consumer_group"},
}

// mcpTools is the endpoint's tool policy with the tools of switched-off
// features removed, so a feature that is off is off for MCP too. An endpoint
// cannot switch such a tool back on.
func mcpTools(endpoint map[string]bool, f config.Features) map[string]bool {
	tools := make(map[string]bool, len(endpoint)+len(mcpOptIn))
	for _, name := range mcpOptIn {
		tools[name] = false
	}
	for name, on := range endpoint {
		tools[name] = on
	}
	for feature, names := range mcpFeatureTools {
		if !f.Enabled(feature) {
			for _, name := range names {
				tools[name] = false
			}
		}
	}

	return tools
}

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

// mcpEndpoint is a configured endpoint with its path checked and resolved.
type mcpEndpoint struct {
	name string
	*config.MCPEndpoint
	// path is the full path the router serves, base path included; decoded
	// is the same path unescaped, the form requests are matched on.
	path, decoded string
}

// resolveMCP checks every endpoint's path once for both the routes and the
// UI, sorted by name so errors are the same on every start. It is empty
// when MCP is off.
func resolveMCP(cfg *config.Config) ([]mcpEndpoint, error) {
	if !cfg.MCP.Enabled {
		return nil, nil
	}

	configured := mcpEndpoints(cfg)
	out := make([]mcpEndpoint, 0, len(configured))
	taken := make(map[string]bool, len(configured))
	for _, name := range slices.Sorted(maps.Keys(configured)) {
		endpoint := configured[name]
		if endpoint == nil {
			return nil, fmt.Errorf("configure MCP; endpoint %q is empty", name)
		}

		local, err := mcpPath(endpoint.Path)
		if err != nil {
			return nil, fmt.Errorf("configure MCP; endpoint %q: %w", name, err)
		}

		path := cfg.Server.BasePath + local
		// Requests are matched on their decoded path, so a default endpoint
		// for a cluster named "team space" (/mcp/team%20space) is reachable.
		decoded, err := url.PathUnescape(path)
		if err != nil {
			return nil, fmt.Errorf("configure MCP; endpoint %q: path %q: %w", name, local, err)
		}

		if decoded == "/healthz" {
			return nil, fmt.Errorf("configure MCP; endpoint %q: path %q is the health check", name, local)
		}

		if taken[decoded] {
			return nil, fmt.Errorf("configure MCP; endpoint %q: path %q is used twice", name, local)
		}
		taken[decoded] = true

		out = append(out, mcpEndpoint{name: name, MCPEndpoint: endpoint, path: path, decoded: decoded})
	}

	return out, nil
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
func mcpHandler(cfg *config.Config, registry *kafka.Registry, endpoints []mcpEndpoint, proxies []*net.IPNet) (*mcpRoutes, error) {
	clusters, err := mcpClusters(cfg, registry)
	if err != nil {
		return nil, err
	}

	byPath := make(map[string]http.Handler, len(endpoints))
	files := make(map[string]http.Handler, len(endpoints))
	for _, endpoint := range endpoints {
		name, path, decoded := endpoint.name, endpoint.path, endpoint.decoded
		links, err := kafkamcp.NewFiles(decoded, mcpLinkKey(cfg.MCP))
		if err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", name, err)
		}

		server := mcp.NewServer(&mcp.Implementation{Name: "kafka-console-" + name, Version: config.Version}, nil)
		if err := clusters.Register(server, endpoint.Cluster, kafkamcp.Options{
			ReadOnly:     endpoint.ReadOnly || !cfg.MCP.AllowWrites,
			Destinations: endpoint.Destinations,
			Tools:        mcpTools(endpoint.Tools, cfg.ClusterFeatures(endpoint.Cluster)),
			Path:         path,
			Files:        links,
			User:         func(h http.Header) string { return h.Get(mcpUserHeader) },
		}); err != nil {
			return nil, fmt.Errorf("endpoint %q: %w", name, err)
		}
		// One transport per endpoint, so a session from one path is not
		// accepted on another with a different policy.
		byPath[decoded] = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return server },
			&mcp.StreamableHTTPOptions{SessionTimeout: cfg.MCP.SessionTimeout})
		files[links.Path()] = links.Handler()
	}

	// A path that is both an endpoint and another endpoint's links would
	// serve one of them by accident.
	for prefix := range files {
		for endpoint := range byPath {
			if endpoint == prefix || strings.HasPrefix(endpoint, prefix+"/") {
				return nil, fmt.Errorf("endpoint path %q is below another endpoint's file links at %q: choose another path", endpoint, prefix)
			}
		}
	}

	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		byPath[r.URL.Path].ServeHTTP(w, r)
	})
	handler = kafkamcp.Middleware(cfg.MCP.PublicURL)(handler)
	handler = mcpAuditUser(cfg.Server.UserHeader, proxies, handler)
	handler = mcpGuard(cfg.MCP, handler)

	// Download and upload links take no bearer token: the agent's curl does
	// not carry the MCP client's headers, and each link is its own one-time
	// 256-bit credential. Origins are still checked, so a page cannot drive
	// one from a browser.
	var links http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		filesOf(files, r.URL.Path).ServeHTTP(w, r)
	})
	links = mcpGuard(config.MCP{AllowOrigins: cfg.MCP.AllowOrigins}, links)
	if len(cfg.MCP.AllowOrigins) > 0 {
		handler = mcors.Middleware(mcors.WithConfig(mcors.Cors{
			AllowOrigins:  cfg.MCP.AllowOrigins,
			AllowMethods:  []string{http.MethodGet, http.MethodPost, http.MethodDelete, http.MethodOptions},
			AllowHeaders:  []string{"content-type", "accept", "authorization", "cache-control", "last-event-id", "mcp-session-id", "mcp-protocol-version"},
			ExposeHeaders: []string{"Mcp-Session-Id"},
		}))(handler)
	}

	return &mcpRoutes{byPath: byPath, files: files, handler: handler, links: links}, nil
}

// mcpLinkKey is what agent download and upload links are sealed with:
// link_key, else the bearer token every instance shares already.
func mcpLinkKey(settings config.MCP) []byte {
	if settings.LinkKey != "" {
		return []byte(settings.LinkKey)
	}

	return []byte(settings.AuthToken)
}

// filesOf returns the links handler whose prefix path is under, or nil.
func filesOf(files map[string]http.Handler, path string) http.Handler {
	for prefix, handler := range files {
		if strings.HasPrefix(path, prefix+"/") {
			return handler
		}
	}

	return nil
}

// mcpRoutes serves exactly the configured endpoint paths. byPath is keyed by
// the decoded path, the form the router matches.
type mcpRoutes struct {
	byPath  map[string]http.Handler
	files   map[string]http.Handler
	handler http.Handler
	links   http.Handler
}

func (m *mcpRoutes) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case m == nil:
		http.NotFound(w, r)
	case m.byPath[r.URL.Path] != nil:
		m.handler.ServeHTTP(w, r)
	case filesOf(m.files, r.URL.Path) != nil:
		m.links.ServeHTTP(w, r)
	default:
		http.NotFound(w, r)
	}
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
	// Plain text, not the API's JSON: MCP clients read only the status, and
	// the MCP SDK answers its own errors this way too.
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

// mcpInfo lists each cluster's MCP endpoints with the paths the router
// serves, including server.base_path. Writes are off for an endpoint
// unless MCP, the endpoint and the cluster all allow them.
func mcpInfo(cfg *config.Config, endpoints []mcpEndpoint) map[string][]mcpEndpointInfo {
	out := map[string][]mcpEndpointInfo{}
	for _, endpoint := range endpoints {
		cluster := cfg.Clusters[endpoint.Cluster]
		out[endpoint.Cluster] = append(out[endpoint.Cluster], mcpEndpointInfo{
			Name: endpoint.name,
			// The same path written escaped or not is one endpoint; show it
			// escaped so it can be pasted into a URL.
			Path:      (&url.URL{Path: endpoint.decoded}).EscapedPath(),
			ReadOnly:  !cfg.MCP.AllowWrites || endpoint.ReadOnly || cluster == nil || cluster.ReadOnly,
			AuthToken: cfg.MCP.AuthToken != "",
		})
	}

	return out
}

func nonNilEndpoints(v []mcpEndpointInfo) []mcpEndpointInfo {
	if v == nil {
		return []mcpEndpointInfo{}
	}

	return v
}

func mountMCP(server *ada.Server, cfg *config.Config, registry *kafka.Registry, endpoints []mcpEndpoint, proxies []*net.IPNet) error {
	var routes *mcpRoutes
	if cfg.MCP.Enabled {
		var err error
		if routes, err = mcpHandler(cfg, registry, endpoints, proxies); err != nil {
			return fmt.Errorf("configure MCP; %w", err)
		}
	}
	// Unknown or disabled paths in the default /mcp namespace must not fall
	// through to the UI's SPA index, which would answer an MCP client with HTML.
	root := server.Group(cfg.Server.BasePath)
	root.Handle("/mcp", routes)
	root.HandleWildcard("/mcp/", routes)
	for _, endpoint := range endpoints {
		// Routes are matched on the decoded path, so they are registered so.
		local := strings.TrimPrefix(endpoint.decoded, cfg.Server.BasePath)
		if local != "/mcp" && !strings.HasPrefix(local, "/mcp/") {
			root.Handle(local, routes)
			root.HandleWildcard(local+"/files/", routes)
		}
	}
	return nil
}
