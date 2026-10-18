package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/rakunlabs/ada"
	"github.com/rakunlabs/ada/handler/folder"
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
}

func Start(ctx context.Context, cfg *config.Config, registry *kafka.Registry, ui fs.FS) error {
	s := &Server{registry: registry, info: cfg.Info}

	server := ada.New()
	server.Use(
		mrecover.Middleware(),
		mserver.Middleware(config.ServiceName),
		mrequestid.Middleware(),
		mlog.Middleware(),
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

	c.GET("/topics", s.topics)
	c.POST("/topics", s.createTopic)
	c.GET("/topics/{topic}", s.topic)
	c.DELETE("/topics/{topic}", s.deleteTopic)
	c.PATCH("/topics/{topic}/configs", s.alterTopicConfigs)
	c.POST("/topics/{topic}/partitions", s.addPartitions)
	c.POST("/topics/{topic}/delete-records", s.deleteRecords)
	c.GET("/topics/{topic}/consumers", s.topicConsumers)
	c.GET("/topics/{topic}/messages", s.messages)
	c.GET("/topics/{topic}/messages/{partition}/{offset}", s.message)
	c.GET("/topics/{topic}/tail", s.tail)
	c.POST("/topics/{topic}/messages", s.produce)
	c.POST("/topics/{topic}/resend", s.resend)

	c.GET("/groups", s.groups)
	c.POST("/groups", s.createGroup)
	c.GET("/groups/{group}", s.group)
	c.DELETE("/groups/{group}", s.deleteGroup)
	c.POST("/groups/{group}/reset", s.resetOffsets)
	c.POST("/groups/{group}/topics", s.addGroupTopic)
	c.POST("/groups/{group}/delete-offsets", s.deleteGroupOffsets)

	c.GET("/acls", s.acls)
	c.POST("/acls", s.createACLs)
	c.POST("/acls/delete", s.deleteACL)
	c.GET("/users", s.users)
	c.POST("/users", s.upsertUser)
	c.DELETE("/users/{user}", s.deleteUser)
	c.GET("/quotas", s.quotas)
	c.POST("/quotas", s.alterQuota)

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
		f, err := folder.New(&folder.Config{Index: true, SPA: true, PrefixPath: base})
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
	return c.SendJSON(map[string]string{"info": s.info})
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

	t, err := cl.Topics(c.Request.Context())
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(t)
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

	g, err := cl.Groups(c.Request.Context())
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
