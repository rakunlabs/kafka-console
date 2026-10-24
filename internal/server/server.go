package server

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"net/http"

	"github.com/rakunlabs/ada"
	"github.com/rakunlabs/ada/handler/folder"
	mencoding "github.com/rakunlabs/ada/middleware/encoding"
	mlog "github.com/rakunlabs/ada/middleware/log"
	mrecover "github.com/rakunlabs/ada/middleware/recover"
	mrequestid "github.com/rakunlabs/ada/middleware/requestid"
	mserver "github.com/rakunlabs/ada/middleware/server"

	"github.com/rakunlabs/kafka-console/internal/config"
	"github.com/rakunlabs/kafka-console/internal/kafka"
)

type Server struct {
	registry *kafka.Registry
	info     string
	features features
	// clusterFeatures is each cluster's features: the global ones with the
	// cluster's own on top.
	clusterFeatures map[string]features
	// mcp lists each cluster's MCP endpoints for the UI's connect panel.
	mcp map[string][]mcpEndpointInfo
}

// mcpEndpointInfo is what the UI needs to show how to connect an agent. It
// never carries the token, only whether one is required.
type mcpEndpointInfo struct {
	Name      string `json:"name"`
	Path      string `json:"path"`
	ReadOnly  bool   `json:"read_only"`
	AuthToken bool   `json:"auth_token"`
}

// features tells the UI which optional parts to show, by config key.
type features map[string]bool

func newFeatures(f config.Features) features {
	out := make(features, len(config.FeatureKeys()))
	for _, key := range config.FeatureKeys() {
		out[key] = f.Enabled(key)
	}

	return out
}

func Start(ctx context.Context, cfg *config.Config, registry *kafka.Registry, ui fs.FS) error {
	server, err := newServer(cfg, registry, ui)
	if err != nil {
		return err
	}

	return server.StartWithContext(ctx, cfg.Server.Host+":"+cfg.Server.Port,
		ada.WithHTTPServerFunc(func(hs *http.Server) *http.Server {
			hs.IdleTimeout = cfg.Server.IdleTimeout

			return hs
		}))
}

// newServer builds every route: health check, MCP, the API and the UI.
func newServer(cfg *config.Config, registry *kafka.Registry, ui fs.FS) (*ada.Server, error) {
	s := &Server{
		registry:        registry,
		info:            cfg.Info,
		features:        newFeatures(cfg.Features),
		clusterFeatures: make(map[string]features, len(cfg.Clusters)),
	}
	for name := range cfg.Clusters {
		s.clusterFeatures[name] = newFeatures(cfg.ClusterFeatures(name))
	}

	endpoints, err := resolveMCP(cfg)
	if err != nil {
		return nil, err
	}

	s.mcp = mcpInfo(cfg, endpoints)

	proxies, err := parseProxies(cfg.Server.TrustedProxies)
	if err != nil {
		return nil, fmt.Errorf("server.trusted_proxies; %w", err)
	}

	if len(proxies) == 0 {
		slog.Warn("server.trusted_proxies is not set: the user header and X-Forwarded-For in audit lines are taken from any client as they are")
	}

	server := ada.New()
	server.Use(
		mrecover.Middleware(),
		mserver.Middleware(config.ServiceName),
		mrequestid.Middleware(),
		mlog.Middleware(),
		mencoding.Middleware(),
	)
	if err := mountMCP(server, cfg, registry, endpoints, proxies); err != nil {
		return nil, err
	}

	server.GET("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	base := cfg.Server.BasePath
	root := server.Group(base, securityHeaders)

	s.routes(root.Group(apiPrefix, sameOrigin, limitBody, auditWrites(cfg.Server.UserHeader, proxies...)))

	if err := mountUI(root, base, ui); err != nil {
		return nil, err
	}

	return server, nil
}

// routes registers the JSON API on api.
func (s *Server) routes(api *ada.Mux) {
	api.GET("/info", s.appInfo)
	api.GET("/clusters", s.clusters)
	api.POST("/filter/check", s.checkFilter)

	// Switched-off features answer the API's JSON 404 for their cluster:
	// the operation is gone, not just refused.
	var (
		security      = s.feature(config.FeatureSecurity)
		createTopic   = s.feature(config.FeatureCreateTopic)
		deleteTopic   = s.feature(config.FeatureDeleteTopic)
		alterTopic    = s.feature(config.FeatureAlterTopic)
		deleteRecords = s.feature(config.FeatureDeleteRecords)
		deleteGroup   = s.feature(config.FeatureDeleteGroup)
		alterBroker   = s.feature(config.FeatureAlterBroker)
	)

	c := api.Group("/clusters/{cluster}")
	c.GET("/overview", s.overview)
	c.GET("/disk-usage", s.diskUsage)
	c.GET("/names", s.names)

	c.GET("/brokers/{broker}", s.broker)
	c.PATCH("/brokers/{broker}/configs", s.alterBrokerConfigs, alterBroker)
	c.POST("/topics/{topic}/reassign", s.reassign, alterBroker)
	c.GET("/topics/{topic}/balance-plan", s.balancePlan, alterBroker)
	c.POST("/topics/{topic}/elect-leaders", s.electLeaders, alterBroker)
	c.GET("/topics/{topic}/reassignments", s.reassignments)

	c.GET("/topics", s.topics)
	c.POST("/topics", s.createTopic, createTopic)
	c.GET("/topics/{topic}", s.topic)
	c.DELETE("/topics/{topic}", s.deleteTopic, deleteTopic)
	c.GET("/topics/{topic}/docs", s.topicDoc)
	c.PATCH("/topics/{topic}/configs", s.alterTopicConfigs, alterTopic)
	c.POST("/topics/{topic}/partitions", s.addPartitions, alterTopic)
	c.POST("/topics/{topic}/delete-records", s.deleteRecords, deleteRecords)
	c.GET("/topics/{topic}/consumers", s.topicConsumers)

	c.GET("/topics/{topic}/messages", s.messages)
	c.GET("/topics/{topic}/messages/{partition}/{offset}", s.message)
	c.GET("/topics/{topic}/messages/{partition}/{offset}/raw", s.rawMessage)
	c.GET("/topics/{topic}/tail", s.tail)
	c.POST("/topics/{topic}/messages", s.produce)
	c.POST("/topics/{topic}/resend", s.resend)
	c.POST("/topics/{topic}/replay", s.replay)
	c.POST("/topics/{topic}/import", s.importRecords)

	c.GET("/groups", s.groups)
	c.POST("/groups", s.createGroup)
	c.GET("/groups/{group}", s.group)
	c.DELETE("/groups/{group}", s.deleteGroup, deleteGroup)
	// Same Group DELETE ACL in Kafka as deleting the group.
	c.POST("/groups/{group}/delete-offsets", s.deleteGroupOffsets, deleteGroup)
	c.POST("/groups/{group}/reset", s.resetOffsets)
	c.POST("/groups/{group}/topics", s.addGroupTopic)

	c.GET("/acls", s.acls, security)
	c.POST("/acls", s.createACLs, security)
	c.POST("/acls/delete", s.deleteACL, security)
	c.GET("/users", s.users, security)
	c.POST("/users", s.upsertUser, security)
	c.DELETE("/users/{user}", s.deleteUser, security)
	c.GET("/quotas", s.quotas, security)
	c.POST("/quotas", s.alterQuota, security)

	c.GET("/schemas", s.schemaRegistry)
	c.POST("/schemas", s.createSchema)
	c.POST("/schemas/check", s.checkCompatibility)
	c.PUT("/schemas/compatibility", s.setGlobalCompatibility)
	c.GET("/schemas/subjects/{subject}", s.subject)
	c.DELETE("/schemas/subjects/{subject}", s.deleteSubject)
	c.PUT("/schemas/subjects/{subject}/compatibility", s.setCompatibility)
	c.DELETE("/schemas/subjects/{subject}/versions/{version}", s.deleteSchemaVersion)

	// Unknown API paths must not fall through to the SPA index.
	api.HandleFuncWildcard("/", notFound)
}

func notFound(w http.ResponseWriter, _ *http.Request) {
	writeJSONError(w, http.StatusNotFound, "not found")
}

// viteAsset matches Vite's content-hashed asset names, which never change
// under the same name.
const viteAsset = `-[\w-]{8}\.(js|css|woff2?)$`

// mountUI serves the built frontend under base; ui is nil when only the
// placeholder was embedded.
func mountUI(root *ada.Mux, base string, ui fs.FS) error {
	if ui == nil {
		return nil
	}

	f, err := folder.New(&folder.Config{
		Index:      true,
		SPA:        true,
		PrefixPath: base,
		// embed.FS has no modtime, so only a content hash can validate.
		ETag: folder.ETagStrong,
		CacheRegex: []*folder.RegexCacheStore{
			{Regex: viteAsset, CacheControl: "public, max-age=31536000, immutable"},
			{Regex: `.*`, CacheControl: "no-cache"},
		},
	})
	if err != nil {
		return fmt.Errorf("ui handler; %w", err)
	}

	f.SetFs(http.FS(ui))

	if base != "" {
		// "/kafka" → "/kafka/" so relative asset URLs resolve under the base.
		root.HandleFunc("", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, base+"/", http.StatusMovedPermanently)
		})
	}

	root.HandleWildcard("/", f)

	return nil
}

// featuresOf is the cluster's features; an unknown cluster gets the global
// ones and is refused by the handler.
func (s *Server) featuresOf(cluster string) features {
	if f, ok := s.clusterFeatures[cluster]; ok {
		return f
	}

	return s.features
}

// feature answers the API's JSON 404 when the request's cluster has the
// feature named key switched off.
func (s *Server) feature(key string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !s.featuresOf(r.PathValue("cluster"))[key] {
				notFound(w, r)

				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// securityHeaders keeps other sites from framing the console (clickjacking a
// delete or reset button) and browsers from sniffing a downloaded record as
// HTML. No script-src: the UI's inline handlers and styles are Svelte's own.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Frame-Options", "SAMEORIGIN")
		h.Set("Content-Security-Policy", "frame-ancestors 'self'")
		h.Set("X-Content-Type-Options", "nosniff")

		next.ServeHTTP(w, r)
	})
}

// apiPrefix is where the JSON API is mounted, below the base path.
const apiPrefix = "/api/v1"

const (
	maxBody       = 4 << 20
	maxImportBody = 64 << 20 // a batch of 500 records with 64 KiB values and keys
)

// importRoute is the one route allowed the larger body.
const importRoute = "POST /clusters/{cluster}/topics/{topic}/import"

// limitBody caps request bodies, so a huge POST is refused (413) instead
// of being decoded into memory whole. It wraps the body before the audit
// log reads it.
func limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.Body != http.NoBody {
			limit := int64(maxBody)
			if routeKey(r) == importRoute {
				limit = maxImportBody
			}

			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}

		next.ServeHTTP(w, r)
	})
}
