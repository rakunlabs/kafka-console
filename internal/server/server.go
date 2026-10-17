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
}

func Start(ctx context.Context, cfg *config.Config, registry *kafka.Registry, ui fs.FS) error {
	s := &Server{registry: registry}

	server := ada.New()
	server.Use(
		mrecover.Middleware(),
		mserver.Middleware(config.ServiceName),
		mrequestid.Middleware(),
		mlog.Middleware(),
	)

	base := cfg.Server.BasePath

	server.GET("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	})

	root := server.Group(base)

	api := root.Group("/api/v1")
	api.GET("/clusters", s.clusters)

	c := api.Group("/clusters/{cluster}")
	c.GET("/overview", s.overview)
	c.GET("/topics", s.topics)
	c.POST("/topics", s.createTopic)
	c.GET("/topics/{topic}", s.topic)
	c.DELETE("/topics/{topic}", s.deleteTopic)
	c.GET("/topics/{topic}/messages", s.messages)
	c.POST("/topics/{topic}/messages", s.produce)
	c.GET("/groups", s.groups)
	c.GET("/groups/{group}", s.group)
	c.DELETE("/groups/{group}", s.deleteGroup)
	c.POST("/groups/{group}/reset", s.resetOffsets)

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
	Name       string   `json:"name"`
	Brokers    []string `json:"brokers"`
	ReadOnly   bool     `json:"read_only"`
	Production bool     `json:"production"`
}

func (s *Server) clusters(c *ada.Context) error {
	out := make([]clusterInfo, 0, len(s.registry.Names()))
	for _, name := range s.registry.Names() {
		cl, _ := s.registry.Get(name)
		out = append(out, clusterInfo{
			Name: name, Brokers: cl.Brokers(), ReadOnly: cl.ReadOnly(), Production: cl.Production(),
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
	switch {
	case errors.Is(err, kafka.ErrReadOnly):
		return ada.WrapHTTPError(http.StatusForbidden, err)
	case errors.Is(err, kafka.ErrNotFound):
		return ada.WrapHTTPError(http.StatusNotFound, err)
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
		Timeout:   time.Duration(queryInt(q.Get("timeout"))) * time.Second,
	}

	if p := q.Get("partitions"); p != "" {
		for _, v := range strings.Split(p, ",") {
			n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 32)
			if err != nil {
				return ada.NewHTTPError(http.StatusBadRequest, "partitions must be comma separated numbers")
			}

			req.Partitions = append(req.Partitions, int32(n))
		}
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
