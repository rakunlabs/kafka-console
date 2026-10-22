package server

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"mime"
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
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

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
}

// features tells the UI which optional parts to show.
type features struct {
	Security      bool `json:"security"`
	CreateTopic   bool `json:"create_topic"`
	DeleteTopic   bool `json:"delete_topic"`
	AlterTopic    bool `json:"alter_topic"`
	DeleteRecords bool `json:"delete_records"`
	DeleteGroup   bool `json:"delete_group"`
	AlterBroker   bool `json:"alter_broker"`
}

func newFeatures(f config.Features) features {
	return features{
		Security:      f.SecurityEnabled(),
		CreateTopic:   f.CreateTopicEnabled(),
		DeleteTopic:   f.DeleteTopicEnabled(),
		AlterTopic:    f.AlterTopicEnabled(),
		DeleteRecords: f.DeleteRecordsEnabled(),
		DeleteGroup:   f.DeleteGroupEnabled(),
		AlterBroker:   f.AlterBrokerEnabled(),
	}
}

func Start(ctx context.Context, cfg *config.Config, registry *kafka.Registry, ui fs.FS) error {
	s := &Server{
		registry:        registry,
		info:            cfg.Info,
		features:        newFeatures(cfg.Features),
		clusterFeatures: make(map[string]features, len(cfg.Clusters)),
	}
	for name := range cfg.Clusters {
		s.clusterFeatures[name] = newFeatures(cfg.ClusterFeatures(name))
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

	proxies, err := parseProxies(cfg.Server.TrustedProxies)
	if err != nil {
		return fmt.Errorf("server.trusted_proxies; %w", err)
	}

	api := root.Group("/api/v1", sameOrigin, limitBody, auditWrites(cfg.Server.UserHeader, proxies...))
	api.GET("/info", s.appInfo)
	api.GET("/clusters", s.clusters)
	api.POST("/filter/check", s.checkFilter)

	c := api.Group("/clusters/{cluster}")
	c.GET("/overview", s.overview)
	c.GET("/disk-usage", s.diskUsage)
	c.GET("/names", s.names)
	c.GET("/brokers/{broker}", s.broker)

	// Switched-off features answer the API's JSON 404 for their cluster:
	// the operation is gone, not just refused.
	alterBroker := s.feature(func(f features) bool { return f.AlterBroker })
	c.PATCH("/brokers/{broker}/configs", s.alterBrokerConfigs, alterBroker)
	c.POST("/topics/{topic}/reassign", s.reassign, alterBroker)
	c.GET("/topics/{topic}/balance-plan", s.balancePlan, alterBroker)
	c.POST("/topics/{topic}/elect-leaders", s.electLeaders, alterBroker)
	c.GET("/topics/{topic}/reassignments", s.reassignments)
	c.GET("/topics/{topic}/docs", s.topicDoc)
	c.GET("/topics", s.topics)
	c.POST("/topics", s.createTopic, s.feature(func(f features) bool { return f.CreateTopic }))
	c.GET("/topics/{topic}", s.topic)
	c.DELETE("/topics/{topic}", s.deleteTopic, s.feature(func(f features) bool { return f.DeleteTopic }))
	alterTopic := s.feature(func(f features) bool { return f.AlterTopic })
	c.PATCH("/topics/{topic}/configs", s.alterTopicConfigs, alterTopic)
	c.POST("/topics/{topic}/partitions", s.addPartitions, alterTopic)
	c.POST("/topics/{topic}/delete-records", s.deleteRecords, s.feature(func(f features) bool { return f.DeleteRecords }))
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
	deleteGroup := s.feature(func(f features) bool { return f.DeleteGroup })
	c.DELETE("/groups/{group}", s.deleteGroup, deleteGroup)
	// Same Group DELETE ACL in Kafka as deleting the group.
	c.POST("/groups/{group}/delete-offsets", s.deleteGroupOffsets, deleteGroup)
	c.POST("/groups/{group}/reset", s.resetOffsets)
	c.POST("/groups/{group}/topics", s.addGroupTopic)

	security := s.feature(func(f features) bool { return f.Security })
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

	return server.StartWithContext(ctx, cfg.Server.Host+":"+cfg.Server.Port,
		ada.WithHTTPServerFunc(func(hs *http.Server) *http.Server {
			hs.IdleTimeout = cfg.Server.IdleTimeout

			return hs
		}))
}

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
}

func (s *Server) appInfo(c *ada.Context) error {
	return c.SendJSON(map[string]any{
		"info": s.info, "version": config.Version, "features": s.features,
		"max_limit": kafka.MaxLimit,
	})
}

// featuresOf is the cluster's features; an unknown cluster gets the global
// ones and is refused by the handler.
func (s *Server) featuresOf(cluster string) features {
	if f, ok := s.clusterFeatures[cluster]; ok {
		return f
	}

	return s.features
}

// feature answers the API's JSON 404 when on reports the request's cluster
// has the feature switched off.
func (s *Server) feature(on func(features) bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !on(s.featuresOf(r.PathValue("cluster"))) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte(`{"message":"not found"}`))

				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

func (s *Server) clusters(c *ada.Context) error {
	out := make([]clusterInfo, 0, len(s.registry.Names()))
	for _, name := range s.registry.Names() {
		cl, _ := s.registry.Get(name)
		out = append(out, clusterInfo{
			Name: name, Brokers: cl.Brokers(), ReadOnly: cl.ReadOnly(), ConfirmWrites: cl.ConfirmWrites(), Color: cl.Color(),
			SchemaRegistry: cl.HasRegistry(), Docs: cl.HasDocs(), Description: cl.Description(),
			Features: s.featuresOf(name),
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
	var (
		invalid  kafka.ErrInvalid
		conflict kafka.ErrConflict
	)

	switch {
	case errors.Is(err, kafka.ErrReadOnly), denied(err):
		return ada.WrapHTTPError(http.StatusForbidden, err)
	case errors.Is(err, kafka.ErrNotFound):
		return ada.WrapHTTPError(http.StatusNotFound, err)
	case errors.As(err, &invalid):
		return ada.WrapHTTPError(http.StatusBadRequest, err)
	case errors.As(err, &conflict):
		return ada.WrapHTTPError(http.StatusConflict, err)
	case errors.Is(err, kafka.ErrBusy):
		return ada.WrapHTTPError(http.StatusTooManyRequests, err)
	case errors.Is(err, kafka.ErrNoRegistry), errors.Is(err, kafka.ErrUnsupported), errors.Is(err, kafka.ErrNoDocs):
		return ada.WrapHTTPError(http.StatusNotImplemented, err)
	case errors.Is(err, context.DeadlineExceeded):
		return ada.WrapHTTPError(http.StatusGatewayTimeout, err)
	case errors.Is(err, context.Canceled):
		// The caller went away; nobody reads this answer.
		return ada.WrapHTTPError(statusClientClosed, err)
	case kafkaIs(err, kafkaNotFound):
		return ada.WrapHTTPError(http.StatusNotFound, err)
	case kafkaIs(err, kafkaConflict):
		return ada.WrapHTTPError(http.StatusConflict, err)
	case kafkaIs(err, kafkaInvalid):
		return ada.WrapHTTPError(http.StatusBadRequest, err)
	default:
		// Kafka errors carry no secrets and the operator needs them.
		if hint := kafka.ExplainConnErr(err); hint != "" {
			return ada.WrapHTTPError(http.StatusBadGateway, fmt.Errorf("%w; %s", err, hint))
		}

		return ada.WrapHTTPError(http.StatusBadGateway, err)
	}
}

// overview and disk usage are kept in memory per cluster for a few seconds;
// ?fresh=true (the Refresh button) asks for a new read.
func (s *Server) overview(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	o, err := cl.Overview(c.Request.Context(), c.Request.URL.Query().Get("fresh") == "true")
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(o)
}

func (s *Server) diskUsage(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	d, err := cl.DiskUsage(c.Request.Context(), c.Request.URL.Query().Get("fresh") == "true")
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(d)
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
	if err := bindErr(c.Bind(&req)); err != nil {
		return err
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

	p, err := parseRead(q, []string{"newest", "oldest", "offset", "timestamp", "last"},
		"offset", "timestamp", "since", "until", "limit", "timeout")
	if err != nil {
		return err
	}

	req := kafka.ConsumeRequest{
		Start:      p.start,
		Offset:     p.ints["offset"],
		Timestamp:  p.ints["timestamp"],
		Since:      p.optional("since"),
		Until:      p.optional("until"),
		Limit:      int(p.ints["limit"]),
		Filter:     p.filter,
		Search:     p.search,
		Timeout:    time.Duration(p.ints["timeout"]) * time.Second,
		Raw:        p.raw,
		As:         p.as,
		Partitions: p.partitions,
	}

	if req.After, err = resumeAt(q); err != nil {
		return err
	}

	if q.Get("stream") == "true" {
		return s.messageStream(c, cl, req)
	}

	res, err := cl.Consume(c.Request.Context(), c.Request.PathValue("topic"), req)
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(res)
}

// messageStreamEvent is one line of a streamed read: "message", "progress"
// (scanned so far and where a stopped read would continue) or a final "done"
// carrying the result without its messages.
type messageStreamEvent struct {
	Type    string               `json:"type"`
	Message *kafka.Message       `json:"message,omitempty"`
	Scanned int                  `json:"scanned"`
	Next    map[int32]int64      `json:"next,omitempty"`
	Result  *kafka.ConsumeResult `json:"result,omitempty"`
	Error   string               `json:"error,omitempty"`
}

// messageStream sends each match as a newline-delimited JSON event as soon
// as it is found. Closing the connection stops the read on the server.
func (s *Server) messageStream(c *ada.Context, cl *kafka.Client, req kafka.ConsumeRequest) error {
	out := newStream(c.Response, ndjson)

	res, err := cl.ConsumeStream(c.Request.Context(), c.Request.PathValue("topic"), req, func(e kafka.ConsumeEvent) bool {
		if e.Message != nil {
			return out.line(messageStreamEvent{Type: "message", Message: e.Message, Scanned: e.Scanned})
		}

		return out.line(messageStreamEvent{Type: "progress", Scanned: e.Scanned, Next: e.Next})
	})
	if err != nil {
		return out.fail(err, func() { out.line(messageStreamEvent{Type: "error", Error: err.Error()}) })
	}

	out.line(messageStreamEvent{Type: "done", Scanned: res.Scanned, Result: res})

	return nil
}

func (s *Server) produce(c *ada.Context) error {
	cl, err := s.client(c)
	if err != nil {
		return err
	}

	var req kafka.ProduceRequest
	if err := bindErr(c.Bind(&req)); err != nil {
		return err
	}

	res, err := cl.Produce(c.Request.Context(), c.Request.PathValue("topic"), req)
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(res)
}

func recordRef(c *ada.Context) (kafka.RecordRef, error) {
	partition, err := strconv.ParseInt(c.Request.PathValue("partition"), 10, 32)
	if err != nil || partition < 0 {
		return kafka.RecordRef{}, ada.NewHTTPError(http.StatusBadRequest, "partition must be a non-negative integer")
	}
	offset, err := strconv.ParseInt(c.Request.PathValue("offset"), 10, 64)
	if err != nil || offset < 0 {
		return kafka.RecordRef{}, ada.NewHTTPError(http.StatusBadRequest, "offset must be a non-negative integer")
	}

	return kafka.RecordRef{Partition: int32(partition), Offset: offset}, nil
}

func (s *Server) message(c *ada.Context) error {
	ref, err := recordRef(c)
	if err != nil {
		return err
	}

	return s.handle(c, func(cl *kafka.Client) (any, error) {
		return cl.Message(c.Request.Context(), c.Request.PathValue("topic"), ref)
	})
}

// rawMessage downloads a record's key or value (?part=key) exactly as stored,
// schema framing included.
func (s *Server) rawMessage(c *ada.Context) error {
	ref, err := recordRef(c)
	if err != nil {
		return err
	}

	part := c.Request.URL.Query().Get("part")
	if part != "" && part != "key" && part != "value" {
		return ada.NewHTTPError(http.StatusBadRequest, "part must be key or value")
	}

	cl, err := s.client(c)
	if err != nil {
		return err
	}

	topic := c.Request.PathValue("topic")

	rec, err := cl.RawRecord(c.Request.Context(), topic, ref)
	if err != nil {
		return fail(err)
	}

	b := rec.Value
	if part == "key" {
		b = rec.Key
	} else {
		part = "value"
	}

	if b == nil {
		return ada.NewHTTPError(http.StatusNotFound, "the record's "+part+" is null")
	}

	name := fmt.Sprintf("%s-%d-%d-%s.bin", topic, ref.Partition, ref.Offset, part)
	w := c.Response
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", mime.FormatMediaType("attachment", map[string]string{"filename": name}))
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(b)

	return nil
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
	if err := bindErr(c.Bind(&req)); err != nil {
		return err
	}

	plan, err := cl.ResetOffsets(c.Request.Context(), c.Request.PathValue("group"), req)
	if err != nil {
		return fail(err)
	}

	return c.SendJSON(map[string]any{"dry_run": req.DryRun, "partitions": plan})
}

const (
	maxBody       = 4 << 20
	maxImportBody = 64 << 20 // a batch of 500 records with 64 KiB values and keys
)

// limitBody caps request bodies, so a huge POST is refused (413) instead
// of being decoded into memory whole. It wraps the body before the audit
// log reads it.
func limitBody(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil && r.Body != http.NoBody {
			limit := int64(maxBody)
			if strings.HasSuffix(r.URL.Path, "/import") {
				limit = maxImportBody
			}

			r.Body = http.MaxBytesReader(w, r.Body, limit)
		}

		next.ServeHTTP(w, r)
	})
}

// denied reports Kafka refusing the console's credentials an operation:
// the request was understood, the principal may not do it.
// statusClientClosed is nginx's code for a request its client abandoned.
const statusClientClosed = 499

// Broker refusals the caller can act on, by what they mean; anything else
// stays a 502, the broker's own failure.
var (
	kafkaNotFound = []error{kerr.UnknownTopicOrPartition, kerr.GroupIDNotFound, kerr.UnknownTopicID}
	kafkaConflict = []error{
		kerr.TopicAlreadyExists, kerr.NonEmptyGroup, kerr.GroupSubscribedToTopic,
		kerr.ReassignmentInProgress, kerr.ConcurrentTransactions,
	}
	kafkaInvalid = []error{
		kerr.InvalidPartitions, kerr.InvalidReplicationFactor, kerr.InvalidReplicaAssignment,
		kerr.InvalidConfig, kerr.InvalidTopicException, kerr.PolicyViolation, kerr.InvalidRequest,
		kerr.InvalidGroupID, kerr.MessageTooLarge, kerr.RecordListTooLarge, kerr.InvalidRecord,
		kerr.UnsupportedCompressionType, kerr.InvalidTimestamp,
	}
)

func kafkaIs(err error, kinds []error) bool {
	for _, k := range kinds {
		if errors.Is(err, k) {
			return true
		}
	}

	return false
}

func denied(err error) bool {
	if _, ok := errors.AsType[*kadm.AuthError](err); ok {
		return true
	}

	for _, e := range []error{
		kerr.TopicAuthorizationFailed, kerr.GroupAuthorizationFailed, kerr.ClusterAuthorizationFailed,
		kerr.TransactionalIDAuthorizationFailed, kerr.DelegationTokenAuthorizationFailed,
		kerr.SaslAuthenticationFailed, kafka.ErrNoTopicAccess, kafka.ErrNoGroupAccess,
	} {
		if errors.Is(err, e) {
			return true
		}
	}

	return false
}
