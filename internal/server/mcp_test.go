package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/rakunlabs/ada"
	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/rakunlabs/kafka-console/internal/kafka"
)

func TestMCPDisabledDoesNotServeSPA(t *testing.T) {
	server := ada.New()
	if err := mountMCP(server, &config.Config{}, nil); err != nil {
		t.Fatalf("disabled MCP: %v", err)
	}
	server.HandleFuncWildcard("/", func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte("SPA")) })
	for _, path := range []string{"/mcp", "/mcp/local", "/mcp/nope"} {
		w := httptest.NewRecorder()
		server.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("%s: expected 404, got %d: %s", path, w.Code, w.Body.String())
		}
	}
}

func TestMCPPolicyAndProtocol(t *testing.T) {
	cfg := &config.Config{
		Server: config.Server{BasePath: "/console"},
		Clusters: map[string]*config.Cluster{
			"prod":  {Brokers: []string{"localhost:1"}, ReadOnly: true},
			"stage": {Brokers: []string{"localhost:1"}},
		},
		MCP: config.MCP{Enabled: true, AuthToken: "secret", Endpoints: map[string]*config.MCPEndpoint{
			"reader": {Cluster: "stage", Path: "/mcp", Destinations: []string{"prod"}, Tools: map[string]bool{"list_acls": false}},
		}},
	}
	server := ada.New()
	registry, err := kafka.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := mountMCP(server, cfg, registry); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(server)
	defer host.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "console-test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{
		Endpoint: host.URL + "/console/mcp", HTTPClient: &http.Client{Transport: mcpBearerTransport{token: "secret"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close() //nolint:errcheck // test cleanup
	listed, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tool := range listed.Tools {
		if tool.Name == "delete_topic" || tool.Name == "list_acls" {
			t.Fatalf("forbidden tool advertised: %s", tool.Name)
		}
	}
	out, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "server_config", Arguments: map[string]any{}})
	if err != nil || out.IsError {
		t.Fatalf("server_config: %+v, %v", out, err)
	}
	encoded, err := json.Marshal(out.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var actual struct {
		Cluster, Path  string
		Authentication string
		ReadOnly       bool `json:"read_only"`
	}
	if err := json.Unmarshal(encoded, &actual); err != nil {
		t.Fatal(err)
	}
	if actual.Cluster != "stage" || actual.Path != "/console/mcp" || !actual.ReadOnly {
		t.Fatalf("incorrect endpoint policy: %s", encoded)
	}
	if actual.Authentication != "host-managed" {
		t.Fatalf("borrowed authentication must not be misreported as none: %s", encoded)
	}
	denied, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "produce_message", Arguments: map[string]any{
		"items": []map[string]any{{"topic": "orders", "value": "must-not-be-written"}}, "confirm": true,
	}})
	if err != nil || denied.IsError {
		t.Fatalf("expected per-item write refusal in the shared batch envelope: %+v, %v", denied, err)
	}
	encoded, err = json.Marshal(denied.StructuredContent)
	if err != nil {
		t.Fatal(err)
	}
	var batch struct {
		Failed, Applied int
		Results         []struct{ Error string }
	}
	if err := json.Unmarshal(encoded, &batch); err != nil {
		t.Fatal(err)
	}
	if batch.Failed != 1 || batch.Applied != 0 || len(batch.Results) != 1 || batch.Results[0].Error == "" {
		t.Fatalf("read-only endpoint failed to refuse produce before contacting the broker: %s", encoded)
	}
	for _, path := range []string{"/console/mcp/nope", "/mcp", "/mcp/stage"} {
		w := httptest.NewRecorder()
		server.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, path, nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("unexpected route %s: %d", path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	server.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/console/mcp", nil))
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("missing bearer token accepted: %d", w.Code)
	}
}

func TestMCPWriteFloors(t *testing.T) {
	cfg := &config.Config{Clusters: map[string]*config.Cluster{
		"stage": {Brokers: []string{"localhost:1"}},
		"prod":  {Brokers: []string{"localhost:1"}, ReadOnly: true},
	}}
	registry, err := kafka.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	lent := func() map[string]bool {
		readOnly := map[string]bool{}
		for _, name := range registry.Names() {
			client, _ := registry.Get(name)
			connection := client.MCPConnection()
			readOnly[name] = connection.ReadOnly || !cfg.MCP.AllowWrites
		}
		return readOnly
	}
	if ro := lent(); !ro["stage"] || !ro["prod"] {
		t.Fatal("default MCP must deny writes to every cluster, including destinations")
	}
	cfg.MCP.AllowWrites = true
	if ro := lent(); ro["stage"] || !ro["prod"] {
		t.Fatal("allow_writes must open writable clusters only, never a read-only one")
	}
	if _, err := mcpClusters(cfg, registry); err != nil {
		t.Fatal(err)
	}
}

func TestMCPRefusesRouteAndPolicyMistakes(t *testing.T) {
	cases := map[string]*config.MCPEndpoint{
		"reserved api route":  {Cluster: "local", Path: "/api/v1"},
		"reserved health":     {Cluster: "local", Path: "/healthz"},
		"root":                {Cluster: "local", Path: "/"},
		"unknown cluster":     {Cluster: "nope", Path: "/mcp/x"},
		"unknown destination": {Cluster: "local", Path: "/mcp/x", Destinations: []string{"nope"}},
		"unknown tool":        {Cluster: "local", Path: "/mcp/x", Tools: map[string]bool{"typo": false}},
		"router wildcard":     {Cluster: "local", Path: "/abc/{name}"},
	}
	for name, endpoint := range cases {
		cfg := &config.Config{Clusters: map[string]*config.Cluster{"local": {Brokers: []string{"localhost:1"}}},
			MCP: config.MCP{Enabled: true, Endpoints: map[string]*config.MCPEndpoint{"e": endpoint}}}
		registry, err := kafka.NewRegistry(cfg)
		if err != nil {
			t.Fatal(err)
		}
		err = mountMCP(ada.New(), cfg, registry)
		registry.Close()
		if err == nil {
			t.Fatalf("%s accepted", name)
		}
	}
}

func TestMCPCrossClusterToolsAreServed(t *testing.T) {
	cfg := &config.Config{
		Clusters: map[string]*config.Cluster{
			"stage": {Brokers: []string{"localhost:1"}},
			"prod":  {Brokers: []string{"localhost:1"}},
		},
		MCP: config.MCP{Enabled: true},
	}
	registry, err := kafka.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	server := ada.New()
	if err := mountMCP(server, cfg, registry); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(server)
	defer host.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "console-test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: host.URL + "/mcp/stage"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close() //nolint:errcheck // test cleanup
	listed, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tool := range listed.Tools {
		names[tool.Name] = true
	}
	for _, want := range []string{"list_clusters", "compare_clusters", "copy_message", "produce_message"} {
		if !names[want] {
			t.Fatalf("cross-cluster tool %s missing: %v", want, names)
		}
	}
	out, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "list_clusters", Arguments: map[string]any{}})
	if err != nil || out.IsError {
		t.Fatalf("list_clusters: %+v, %v", out, err)
	}
	encoded, _ := json.Marshal(out.StructuredContent)
	for _, cluster := range []string{`"stage"`, `"prod"`} {
		if !strings.Contains(string(encoded), cluster) {
			t.Fatalf("list_clusters must report every console cluster: %s", encoded)
		}
	}
}

func TestMCPRejectsForeignOrigins(t *testing.T) {
	handler := mcpGuard(config.MCP{AllowOrigins: []string{"https://ok.example"}},
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) }))
	for origin, want := range map[string]int{"": http.StatusTeapot, "https://ok.example": http.StatusTeapot, "https://evil.example": http.StatusForbidden} {
		r := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/mcp/x", nil)
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("origin %q: got %d, want %d", origin, w.Code, want)
		}
	}
}

func TestMCPServesAnyConfiguredPath(t *testing.T) {
	for _, base := range []string{"", "/console"} {
		cfg := &config.Config{
			Server:   config.Server{BasePath: base},
			Clusters: map[string]*config.Cluster{"local": {Brokers: []string{"localhost:1"}}},
			MCP: config.MCP{Enabled: true, Endpoints: map[string]*config.MCPEndpoint{
				"odd":    {Cluster: "local", Path: "/abc/mcp/xyz"},
				"top":    {Cluster: "local", Path: "agents"},
				"nested": {Cluster: "local", Path: "/team//kafka/./tools/"},
			}},
		}
		registry, err := kafka.NewRegistry(cfg)
		if err != nil {
			t.Fatal(err)
		}
		server := ada.New()
		if err := mountMCP(server, cfg, registry); err != nil {
			t.Fatal(err)
		}
		server.HandleFuncWildcard("/", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("SPA")) })
		host := httptest.NewServer(server)

		for _, path := range []string{"/abc/mcp/xyz", "/agents", "/team/kafka/tools"} {
			client := mcp.NewClient(&mcp.Implementation{Name: "console-test", Version: "1"}, nil)
			session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: host.URL + base + path}, nil)
			if err != nil {
				t.Fatalf("%s%s: %v", base, path, err)
			}
			out, err := session.CallTool(t.Context(), &mcp.CallToolParams{Name: "server_config", Arguments: map[string]any{}})
			if err != nil || out.IsError {
				t.Fatalf("%s%s server_config: %+v, %v", base, path, out, err)
			}
			encoded, _ := json.Marshal(out.StructuredContent)
			if !strings.Contains(string(encoded), `"path":"`+base+path+`"`) {
				t.Fatalf("%s%s: server_config must report the served path: %s", base, path, encoded)
			}
			_ = session.Close()
		}

		// Near misses of a custom path belong to the UI, which must still serve them.
		w := httptest.NewRecorder()
		server.ServeHTTP(w, httptest.NewRequestWithContext(t.Context(), http.MethodGet, base+"/abc/mcp/other", nil))
		if w.Body.String() != "SPA" {
			t.Fatalf("%s: a path next to a custom endpoint must fall through to the UI, got %d %q", base, w.Code, w.Body.String())
		}
		host.Close()
		registry.Close()
	}
}

type mcpBearerTransport struct{ token string }

func (t mcpBearerTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	r.Header.Set("Authorization", "Bearer "+t.token)
	return http.DefaultTransport.RoundTrip(r)
}

func TestMCPFeaturesWithholdTools(t *testing.T) {
	off := false
	cfg := &config.Config{
		Clusters: map[string]*config.Cluster{"local": {Brokers: []string{"localhost:1"}}},
		Features: config.Features{CreateTopic: &off, DeleteGroup: &off},
		MCP:      config.MCP{Enabled: true, AllowWrites: true},
	}
	server := ada.New()
	registry, err := kafka.NewRegistry(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	// Registration validates tool names, so this also proves every name the
	// feature map switches off is a real tool.
	if err := mountMCP(server, cfg, registry); err != nil {
		t.Fatal(err)
	}
	host := httptest.NewServer(server)
	defer host.Close()
	client := mcp.NewClient(&mcp.Implementation{Name: "console-test", Version: "1"}, nil)
	session, err := client.Connect(t.Context(), &mcp.StreamableClientTransport{Endpoint: host.URL + "/mcp/local"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close() //nolint:errcheck // test cleanup
	listed, err := session.ListTools(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, tool := range listed.Tools {
		seen[tool.Name] = true
	}
	for _, name := range []string{"create_topic", "delete_consumer_group"} {
		if seen[name] {
			t.Errorf("%s advertised while its feature is off", name)
		}
	}
	for _, name := range []string{"delete_topic", "list_acls"} {
		if !seen[name] {
			t.Errorf("%s missing while its feature is on", name)
		}
	}
}
