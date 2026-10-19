package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/tlscfg"

	"github.com/rakunlabs/kafka-console/internal/config"
)

var ErrReadOnly = errors.New("cluster is read-only")

// Client is one cluster's shared connection.
type Client struct {
	Name string
	cfg  *config.Cluster

	options []kgo.Opt
	client  *kgo.Client
	admin   *kadm.Client

	producersMu sync.Mutex
	producers   map[string]*kgo.Client

	registry *sr.Client
	codecs   *codecCache

	readers *readers
	protos  *localProto
}

func New(name string, cfg *config.Cluster) (*Client, error) {
	options := []kgo.Opt{kgo.SeedBrokers(cfg.Brokers...)}

	if cfg.TLS != nil && cfg.TLS.Enabled {
		t, err := tlsConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}

		options = append(options, kgo.DialTLSConfig(t))
	}

	if cfg.SASL != nil {
		mechanism, err := saslMechanism(cfg.SASL)
		if err != nil {
			return nil, err
		}

		options = append(options, kgo.SASL(mechanism))
	}

	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("connect to %v; %w", cfg.Brokers, err)
	}

	c := &Client{
		Name:    name,
		cfg:     cfg,
		options: options,
		client:  client,
		admin:   kadm.NewClient(client),
		readers: newReaders(),
	}

	protos, err := newLocalProto(cfg.Protobuf)
	if err != nil {
		client.Close()

		return nil, fmt.Errorf("cluster %q; %w", name, err)
	}

	c.protos = protos

	if cfg.SchemaRegistry != nil {
		registry, err := registryClient(cfg.SchemaRegistry)
		if err != nil {
			client.Close()

			return nil, err
		}

		c.registry = registry
		c.codecs = newCodecCache(registry)
	}

	return c, nil
}

func registryClient(s *config.SchemaRegistry) (*sr.Client, error) {
	opts := []sr.ClientOpt{sr.URLs(s.URLs...), sr.UserAgent(config.ServiceName)}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if s.TLS != nil && s.TLS.Enabled {
		t, err := tlsConfig(s.TLS)
		if err != nil {
			return nil, fmt.Errorf("schema registry; %w", err)
		}

		transport.TLSClientConfig = t
	}

	opts = append(opts, sr.HTTPClient(&http.Client{Timeout: 10 * time.Second, Transport: transport}))

	switch {
	case s.Token != "":
		opts = append(opts, sr.BearerToken(s.Token))
	case s.User != "":
		opts = append(opts, sr.BasicAuth(s.User, s.Password))
	}

	cl, err := sr.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("schema registry; %w", err)
	}

	return cl, nil
}

func (c *Client) Close() {
	c.client.Close()
	c.readers.close()

	c.producersMu.Lock()
	defer c.producersMu.Unlock()

	for _, p := range c.producers {
		p.Close()
	}
}

func (c *Client) ReadOnly() bool    { return c.cfg.ReadOnly }
func (c *Client) Production() bool  { return c.cfg.Production }
func (c *Client) Brokers() []string { return c.cfg.Brokers }
func (c *Client) HasRegistry() bool { return c.registry != nil }

// ErrNoRegistry means the cluster has no schema registry configured.
var ErrNoRegistry = errors.New("schema registry is not configured for this cluster")

func (c *Client) requireRegistry() error {
	if c.registry == nil {
		return ErrNoRegistry
	}

	return nil
}

func (c *Client) requireWritable() error {
	if c.cfg.ReadOnly {
		return ErrReadOnly
	}

	return nil
}

// producer returns a client for the given partitioning and compression.
// Manual partitioning needs its own client: the shared one keeps the default
// partitioner, since an unset Partition would otherwise send everything to
// partition 0. Clients are cached because each holds broker connections.
func (c *Client) producer(manual bool, codec string) (*kgo.Client, error) {
	if !manual && codec == "" {
		return c.client, nil
	}

	return c.cachedProducer(fmt.Sprintf("%t/%s", manual, codec), manual, codec)
}

// bulkProducer is producer for replays and imports. They write thousands of
// records and fill the client's buffer; on their own clients, a single
// Produce or an admin request of another user does not queue behind them.
func (c *Client) bulkProducer(manual bool, codec string) (*kgo.Client, error) {
	return c.cachedProducer(fmt.Sprintf("bulk/%t/%s", manual, codec), manual, codec)
}

func (c *Client) cachedProducer(key string, manual bool, codec string) (*kgo.Client, error) {
	c.producersMu.Lock()
	defer c.producersMu.Unlock()

	if p, ok := c.producers[key]; ok {
		return p, nil
	}

	opts := append([]kgo.Opt(nil), c.options...)
	if manual {
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}

	if codec != "" {
		opts = append(opts, kgo.ProducerBatchCompression(compressionCodec(codec)))
	}

	p, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, err
	}

	if c.producers == nil {
		c.producers = map[string]*kgo.Client{}
	}

	c.producers[key] = p

	return p, nil
}

func compressionCodec(name string) kgo.CompressionCodec {
	switch name {
	case "gzip":
		return kgo.GzipCompression()
	case "snappy":
		return kgo.SnappyCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	}

	return kgo.NoCompression()
}

// newReader opens a short-lived consumer for reading a topic by offset ranges.
func (c *Client) newReader(topic string) (*kgo.Client, error) {
	opts := append(append([]kgo.Opt(nil), c.options...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {}}),
		kgo.FetchMaxWait(time.Second),
	)

	return kgo.NewClient(opts...)
}

// newScanReader is newReader for scans that stop at a known end offset. It
// also returns transaction markers: they take offsets but carry no data, so
// a partition whose last offset is a commit marker would otherwise never
// deliver its end and the scan would wait out its empty polls. Callers skip
// them with isControl.
func (c *Client) newScanReader() (*kgo.Client, error) {
	opts := append(append([]kgo.Opt(nil), c.options...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{}),
		kgo.FetchMaxWait(time.Second),
		kgo.KeepControlRecords(),
	)

	return kgo.NewClient(opts...)
}

const (
	// maxReads bounds the reads (browse, search, resend) running at once
	// per cluster; more wait their turn instead of each opening its own
	// connections to every broker.
	maxReads = 16
	// idleReaders is how many scan readers are kept connected between
	// requests. A new one costs a dial, TLS and SASL to each broker it
	// reads from; a kept one is ready at once.
	idleReaders = 4
	// maxStreams bounds live tails and replays per cluster. Each holds a
	// consumer open for as long as it runs, so past the limit a new one is
	// refused at once rather than queued behind streams that may never end.
	maxStreams = 32
)

// ErrBusy refuses work when a cluster already runs as much of it as allowed.
var ErrBusy = errors.New("too many live tails and replays are running on this cluster; stop one and retry")

// readers hands out scan readers, at most maxReads at a time, reusing idle
// ones. A reader holds no partitions while idle: scan removes what it adds.
type readers struct {
	slots   chan struct{}
	streams chan struct{}

	mu     sync.Mutex
	idle   []*kgo.Client
	closed bool
}

func newReaders() *readers {
	return &readers{slots: make(chan struct{}, maxReads), streams: make(chan struct{}, maxStreams)}
}

// stream takes a place for a long-running consumer, or fails with ErrBusy.
func (c *Client) stream() (release func(), err error) {
	select {
	case c.readers.streams <- struct{}{}:
		return func() { <-c.readers.streams }, nil
	default:
		return nil, ErrBusy
	}
}

// scanReader returns a reader for topic and the function that gives it
// back; pass that function whether the reader is still sound (false after an
// error, so a reader in an unknown state is closed, not reused).
func (c *Client) scanReader(ctx context.Context, topic string) (*kgo.Client, func(ok bool), error) {
	r := c.readers

	select {
	case r.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	}

	r.mu.Lock()
	var cl *kgo.Client
	if n := len(r.idle); n > 0 {
		cl, r.idle = r.idle[n-1], r.idle[:n-1]
	}
	r.mu.Unlock()

	if cl == nil {
		var err error
		if cl, err = c.newScanReader(); err != nil {
			<-r.slots

			return nil, nil, err
		}
	}

	done := func(ok bool) {
		defer func() { <-r.slots }()

		// Forget the topic, so a kept reader does not keep refreshing
		// metadata for every topic it was ever lent for.
		if ok {
			cl.PurgeTopicsFromConsuming(topic)
		}

		r.mu.Lock()
		keep := ok && !r.closed && len(r.idle) < idleReaders
		if keep {
			r.idle = append(r.idle, cl)
		}
		r.mu.Unlock()

		if !keep {
			cl.Close()
		}
	}

	return cl, done, nil
}

func (r *readers) close() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.closed = true
	for _, cl := range r.idle {
		cl.Close()
	}
	r.idle = nil
}

func isControl(rec *kgo.Record) bool { return rec.Attrs.IsControl() }

func tlsConfig(s *config.TLS) (*tls.Config, error) {
	cfg, err := tlscfg.New(
		tlscfg.MaybeWithDiskKeyPair(s.CertFile, s.KeyFile),
		tlscfg.MaybeWithDiskCA(s.CAFile, tlscfg.ForClient),
		tlscfg.WithSystemCertPool(),
	)
	if err != nil {
		return nil, fmt.Errorf("configure tls; %w", err)
	}

	cfg.InsecureSkipVerify = s.InsecureSkipVerify

	return cfg, nil
}

func saslMechanism(s *config.SASL) (sasl.Mechanism, error) {
	switch s.Mechanism {
	case config.MechanismPlain:
		return plain.Auth{User: s.User, Pass: s.Password}.AsMechanism(), nil
	case config.MechanismScramSHA256:
		return scram.Auth{User: s.User, Pass: s.Password, IsToken: s.IsToken}.AsSha256Mechanism(), nil
	case config.MechanismScramSHA512:
		return scram.Auth{User: s.User, Pass: s.Password, IsToken: s.IsToken}.AsSha512Mechanism(), nil
	case config.MechanismOAuth:
		return oauthMechanism(s.OAuth)
	}

	return nil, fmt.Errorf("unsupported sasl mechanism %q", s.Mechanism)
}
