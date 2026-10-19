package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

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
}

// features tells the UI which optional parts to show.
type features struct {
	Security      bool `json:"security"`
	CreateTopic   bool `json:"create_topic"`
	DeleteTopic   bool `json:"delete_topic"`
	AlterTopic    bool `json:"alter_topic"`
	DeleteRecords bool `json:"delete_records"`
	DeleteGroup   bool `json:"delete_group"`
}

func newFeatures(f config.Features) features {
	return features{
		Security:      f.SecurityEnabled(),
		CreateTopic:   f.CreateTopicEnabled(),
		DeleteTopic:   f.DeleteTopicEnabled(),
		AlterTopic:    f.AlterTopicEnabled(),
		DeleteRecords: f.DeleteRecordsEnabled(),
		DeleteGroup:   f.DeleteGroupEnabled(),
	}
}

func Start(ctx context.Context, cfg *config.Config, registry *kafka.Registry, ui fs.FS) error {
	s := &Server{
		registry: registry,
		info:     cfg.Info,
		features: newFeatures(cfg.Features),
	}

	server := ada.New()
	server.Use(
		mrecover.Middleware(),
		mserver.Middleware(config.ServiceName),
		mrequestid.Middleware(),
		mlog.Middleware(),
		mencoding.Middleware(),
	)
	if err := mountMCP(server, cfg, registry); err != nil {
		return err
	}

	base := cfg.Server.BasePath

	server.GET("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	root := server.Group(base)

	api := root.Group("/api/v1", auditWrites(cfg.Server.UserHeader))
	api.GET("/info", s.appInfo)
	api.GET("/clusters", s.clusters)
	api.POST("/filter/check", s.checkFilter)

	c := api.Group("/clusters/{cluster}")
	c.GET("/overview", s.overview)
	c.GET("/names", s.names)
	c.GET("/brokers/{broker}", s.broker)
	c.PATCH("/brokers/{broker}/configs", s.alterBrokerConfigs)

	// Switched-off features are not registered, so their paths fall through
	// to the API's JSON 404: the operation is gone, not just refused.
	c.GET("/topics", s.topics)
	if s.features.CreateTopic {
		c.POST("/topics", s.createTopic)
	}
	c.GET("/topics/{topic}", s.topic)
	if s.features.DeleteTopic {
		c.DELETE("/topics/{topic}", s.deleteTopic)
	}
	if s.features.AlterTopic {
		c.PATCH("/topics/{topic}/configs", s.alterTopicConfigs)
		c.POST("/topics/{topic}/partitions", s.addPartitions)
	}
	if s.features.DeleteRecords {
		c.POST("/topics/{topic}/delete-records", s.deleteRecords)
	}
	c.GET("/topics/{topic}/consumers", s.topicConsumers)
	c.GET("/topics/{topic}/messages", s.messages)
	c.GET("/topics/{topic}/messages/{partition}/{offset}", s.message)
	c.GET("/topics/{topic}/tail", s.tail)
	c.POST("/topics/{topic}/messages", s.produce)
	c.POST("/topics/{topic}/resend", s.resend)
	c.POST("/topics/{topic}/replay", s.replay)
	c.POST("/topics/{topic}/import", s.importRecords)

	c.GET("/groups", s.groups)
	c.POST("/groups", s.createGroup)
	c.GET("/groups/{group}", s.group)
	if s.features.DeleteGroup {
		c.DELETE("/groups/{group}", s.deleteGroup)
	}
	c.POST("/groups/{group}/reset", s.resetOffsets)
	c.POST("/groups/{group}/topics", s.addGroupTopic)
	c.POST("/groups/{group}/delete-offsets", s.deleteGroupOffsets)

	if s.features.Security {
		c.GET("/acls", s.acls)
		c.POST("/acls", s.createACLs)
		c.POST("/acls/delete", s.deleteACL)
		c.GET("/users", s.users)
		c.POST("/users", s.upsertUser)
		c.DELETE("/users/{user}", s.deleteUser)
		c.GET("/quotas", s.quotas)
		c.POST("/quotas", s.alterQuota)
	}

	c.GET("/schemas", s.schemaRegistry)
	c.POST("/schemas", s.createSchema)
	c.POST("/schemas/check", s.checkCompatibility)
	c.PUT("/schemas/compatibility", s.setGlobalCompatibility)
	c.GET("/schemas/subjects/{subject}", s.subject)
	c.DELETE("/schemas/subjects/{subject}", s.deleteSubject)
	c.PUT("/schemas/subjects/{subject}/compatibility", s.setCompatibility)
	c.DELETE("/schemas/subjects/{subject}/versions/{version}", s.deleteSchemaVersion)

	// Unknown API paths must not fall through to the SPA index.
	api.HandleFuncWildcard("/", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message":"not found"}`))
	})

	if ui != nil {
		f, err := folder.New(&folder.Config{
			Index:      true,
			SPA:        true,
			PrefixPath: base,
			// embed.FS has no modtime, so only a content hash can validate.
			ETag: folder.ETagStrong,
			CacheRegex: []*folder.RegexCacheStore{
				// Vite content-hashed assets never change under the same name.
				{Regex: `-[\w-]{8}\.(js|css|woff2?)$`, CacheControl: "public, max-age=31536000, immutable"},
				{Regex: `.*`, CacheControl: "no-cache"},
			},
		})
		if err != nil {
			return fmt.Errorf("ui handler; %w", err)
		}

		f.SetFs(http.FS(ui))

		if base == "" {
			server.HandleWildcard("/", f)
		} else {
			// "/kafka" → "/kafka/" so relative asset URLs resolve under the base.
			server.HandleFunc(base, func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, base+"/", http.StatusMovedPermanently)
			})
			server.HandleWildcard(base+"/", f)
		}
	}

	return server.StartWithContext(ctx, cfg.Server.Host+":"+cfg.Server.Port)
}

type clusterInfo struct {
	Name           string   `json:"name"`
	Brokers        []string `json:"brokers"`
	ReadOnly       bool     `json:"read_only"`
	Production     bool     `json:"production"`
	SchemaRegistry bool     `json:"schema_registry"`
}

func (s *Server) appInfo(c *ada.Context) error {
	return c.SendJSON(map[string]any{"info": s.info, "features": s.features})
}

func (s *Server) clusters(c *ada.Context) error {
	out := make([]clusterInfo, 0, len(s.registry.Names()))
	for _, name := range s.registry.Names() {
		cl, _ := s.registry.Get(name)
		out = append(out, clusterInfo{
			Name: name, Brokers: cl.Brokers(), ReadOnly: cl.ReadOnly(), Production: cl.Production(),
			SchemaRegistry: cl.HasRegistry(),
		})
	}

	return c.SendJSON(out)
}

func (s *Server) client(c *ada.Context) (*kafka.Client, error) {
	name := c.Request.PathValue("cluster")

	cl, ok := s.registry.Get(name)
	if !ok {
		return nil, ada.NewHTTPError(http.StatusNotFound, fmt.Sprintf("cluster %q not found", name))
	}

	return cl, nil
}

// fail maps domain errors to HTTP errors whose text is safe to show.
func fail(err error) error {
	var invalid kafka.ErrInvalid

	switch {
	case errors.Is(err, kafka.ErrReadOnly):
		return ada.WrapHTTPError(http.StatusForbidden, err)
	case errors.Is(err, kafka.ErrNotFound):
		return ada.WrapHTTPError(http.StatusNotFound, err)
	case errors.As(err, &invalid):
		return ada.WrapHTTPError(http.StatusBadRequest, err)
	case errors.Is(err, kafka.ErrNoRegistry), errors.Is(err, kafka.ErrUnsupported):
		return ada.WrapHTTPError(http.StatusNotImplemented, err)
	default:
		// Kafka errors carry no secrets and the operator needs them.
		return ada.WrapHTTPError(http.StatusBadGateway, err)
	}
}

func (s *Server) overview(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	o, err := cl.Overview(c.Request.Context())
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(o)
}

func (s *Server) names(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	n, err := cl.Names(c.Request.Context())
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(n)
}

func (s *Server) topics(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

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

	if query.Offset, query.Limit, err = pageParams(q); err != nil {
		return err
	}

	t, err := cl.Topics(c.Request.Context(), query)
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(t)
}

// maxPage caps one page so a single request cannot ask for stats, lag or
// versions of every item in a large cluster.
const maxPage = 500

// pageParams reads offset and limit; a missing or too large limit is maxPage.
func pageParams(q url.Values) (offset, limit int, err error) {
	for key, dst := range map[string]*int{"offset": &offset, "limit": &limit} {
		if v := q.Get(key); v != "" {
			n, err := strconv.Atoi(v)
			if err != nil || n < 0 {
				return 0, 0, ada.WrapHTTPError(http.StatusBadRequest, fmt.Errorf("%s must be a non-negative integer", key))
			}
			*dst = n
		}
	}

	if limit == 0 || limit > maxPage {
		limit = maxPage
	}

	return offset, limit, nil
}

func (s *Server) topicConsumers(c *ada.Context) error {
	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.TopicConsumers(c.Request.Context(), c.Request.PathValue("topic"))
	})
}

func (s *Server) topic(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	t, err := cl.Topic(c.Request.Context(), c.Request.PathValue("topic"))
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(t)
}

func (s *Server) createTopic(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	var req kafka.CreateTopicRequest
	if err := c.Bind(&req); err != nil {
		return ada.WrapHTTPError(http.StatusBadRequest, err)
	}

	if strings.TrimSpace(req.Name) == "" {
		return ada.NewHTTPError(http.StatusBadRequest, "topic name is required")
	}

	if err := cl.CreateTopic(c.Request.Context(), req); err != nil {
		return fail(err)
	}

	return c.SetStatus(http.StatusCreated).SendJSON(map[string]string{"name": req.Name})
}

func (s *Server) deleteTopic(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	if err := cl.DeleteTopic(c.Request.Context(), c.Request.PathValue("topic")); err != nil {
		return fail(err)
	}

	return c.SendNoContent()
}

func (s *Server) messages(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	q := c.Request.URL.Query()

	req := kafka.ConsumeRequest{
		Start:     q.Get("start"),
		Offset:    queryInt(q.Get("offset")),
		Timestamp: queryInt(q.Get("timestamp")),
		Limit:     int(queryInt(q.Get("limit"))),
		MaxScan:   int(queryInt(q.Get("max_scan"))),
		Filter:    q.Get("filter"),
		Search:    q.Get("q"),
		Timeout:   time.Duration(queryInt(q.Get("timeout"))) * time.Second,
		Raw:       q.Get("raw") == "true",
	}

	if req.Partitions, err = partitions(q.Get("partitions")); err != nil {
		return err
	}

	res, err := cl.Consume(c.Request.Context(), c.Request.PathValue("topic"), req)
	if err != nil {
		if strings.HasPrefix(err.Error(), "script ") {
			return ada.WrapHTTPError(http.StatusBadRequest, err)
		}

		return fail(err)
	}

	return c.SendJSON(res)
}

func (s *Server) produce(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	var req kafka.ProduceRequest
	if err := c.Bind(&req); err != nil {
		return ada.WrapHTTPError(http.StatusBadRequest, err)
	}

	res, err := cl.Produce(c.Request.Context(), c.Request.PathValue("topic"), req)
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(res)
}

func (s *Server) message(c *ada.Context) error {
	partition, err := strconv.ParseInt(c.Request.PathValue("partition"), 10, 32)
	if err != nil || partition < 0 {
		return ada.NewHTTPError(http.StatusBadRequest, "partition must be a non-negative integer")
	}
	offset, err := strconv.ParseInt(c.Request.PathValue("offset"), 10, 64)
	if err != nil || offset < 0 {
		return ada.NewHTTPError(http.StatusBadRequest, "offset must be a non-negative integer")
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Message(c.Request.Context(), c.Request.PathValue("topic"), kafka.RecordRef{Partition: int32(partition), Offset: offset})
	})
}

func (s *Server) resend(c *ada.Context) error {
	req, err := bind[kafka.ResendRequest](c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Resend(c.Request.Context(), c.Request.PathValue("topic"), req)
	})
}

func (s *Server) groups(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	q := c.Request.URL.Query()
	query := kafka.GroupQuery{Search: q.Get("q"), SortByLag: q.Get("sort") == "lag"}

	if s := q.Get("sort"); s != "" && s != "name" && s != "lag" {
		return ada.WrapHTTPError(http.StatusBadRequest, fmt.Errorf("unknown sort %q", s))
	}

	if query.Offset, query.Limit, err = pageParams(q); err != nil {
		return err
	}

	g, err := cl.Groups(c.Request.Context(), query)
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(g)
}

func (s *Server) group(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	g, err := cl.Group(c.Request.Context(), c.Request.PathValue("group"))
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(g)
}

func (s *Server) deleteGroup(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	if err := cl.DeleteGroup(c.Request.Context(), c.Request.PathValue("group")); err != nil {
		return fail(err)
	}

	return c.SendNoContent()
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
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	var req kafka.ResetRequest
	if err := c.Bind(&req); err != nil {
		return ada.WrapHTTPError(http.StatusBadRequest, err)
	}

	plan, err := cl.ResetOffsets(c.Request.Context(), c.Request.PathValue("group"), req)
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(map[string]any{"dry_run": req.DryRun, "partitions": plan})
}

func queryInt(v string) int64 {
	n, _ := strconv.ParseInt(v, 10, 64)

	return n
}
