package kafka

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
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

	description string

	overviews *memo[*Overview]
	diskUsage *memo[*DiskUsage]
}

// Settings are the console-wide settings a cluster's connection uses.
type Settings struct {
	Cache  config.Cache
	Limits config.Limits
}

func New(name string, cfg *config.Cluster, settings Settings) (*Client, error) {
	cache := settings.Cache

	// Everything that can fail on configuration alone comes before the
	// connection and the caches, which start goroutines.
	protos, err := newLocalProto(cfg.Protobuf)
	if err != nil {
		return nil, err
	}

	var registry *sr.Client
	if cfg.SchemaRegistry != nil {
		if registry, err = registryClient(cfg.SchemaRegistry); err != nil {
			return nil, err
		}
	}

	options := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		// Named, so broker logs and quotas tell the console's connections
		// apart from every other client using the default name.
		kgo.ClientID(config.ServiceName),
		kgo.WithHooks(listenerHook{cluster: name, tls: cfg.TLS.On()}),
	}

	if cfg.TLS.On() {
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

	// The effective TLS and SASL setting, so a listener mismatch ("tls alert
	// record", "invalid large response") can be checked against the log.
	slog.Info("cluster connection", "cluster", name, "brokers", cfg.Brokers, "tls", cfg.TLS.On(), "sasl", cfg.SASL != nil)

	c := &Client{
		Name:    name,
		cfg:     cfg,
		options: options,
		client:  client,
		admin:   kadm.NewClient(client),
		readers: newReaders(settings.Limits),
		protos:  protos,

		overviews: newMemo[*Overview](cache.Overview, cache.Refresh, readTimeout),
		diskUsage: newMemo[*DiskUsage](cache.DiskUsage, cache.Refresh, readTimeout),
	}

	if strings.TrimSpace(cfg.Description) != "" {
		c.description = renderMarkdown([]byte(cfg.Description))
	}

	if registry != nil {
		c.registry = registry
		c.codecs = newCodecCache(registry)
	}

	return c, nil
}

// registryTimeout bounds one schema registry HTTP request.
const registryTimeout = 10 * time.Second

func registryClient(s *config.SchemaRegistry) (*sr.Client, error) {
	opts := []sr.ClientOpt{sr.URLs(s.URLs...), sr.UserAgent(config.ServiceName)}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	if s.TLS.On() {
		t, err := tlsConfig(s.TLS)
		if err != nil {
			return nil, fmt.Errorf("schema registry; %w", err)
		}

		transport.TLSClientConfig = t
	}

	opts = append(opts, sr.HTTPClient(&http.Client{Timeout: registryTimeout, Transport: transport}))

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
	c.overviews.close()
	c.diskUsage.close()

	c.producersMu.Lock()
	defer c.producersMu.Unlock()

	for _, p := range c.producers {
		p.Close()
	}
}

func (c *Client) ReadOnly() bool      { return c.cfg.ReadOnly }
func (c *Client) ConfirmWrites() bool { return c.cfg.ConfirmWrites }
func (c *Client) Color() string       { return c.cfg.Color }
func (c *Client) Brokers() []string   { return c.cfg.Brokers }
func (c *Client) HasRegistry() bool   { return c.registry != nil }

// Description is the configured cluster description rendered from Markdown
// with the same safe renderer as topic docs; empty when unset.
func (c *Client) Description() string { return c.description }

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

// How long a fetch waits for records. Scans stop at a known end, so a longer
// wait costs nothing; tails and replays report progress between polls.
const (
	scanFetchWait   = time.Second
	streamFetchWait = 500 * time.Millisecond
)

// newKafkaClient opens another client to the cluster with the shared
// connection's options plus extra.
func (c *Client) newKafkaClient(extra ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append(append([]kgo.Opt(nil), c.options...), extra...)...)
}

func (c *Client) cachedProducer(key string, manual bool, codec string) (*kgo.Client, error) {
	c.producersMu.Lock()
	defer c.producersMu.Unlock()

	if p, ok := c.producers[key]; ok {
		return p, nil
	}

	var opts []kgo.Opt
	if manual {
		opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	}

	if codec != "" {
		opts = append(opts, kgo.ProducerBatchCompression(compressionCodecs[codec]))
	}

	p, err := c.newKafkaClient(opts...)
	if err != nil {
		return nil, err
	}

	if c.producers == nil {
		c.producers = map[string]*kgo.Client{}
	}

	c.producers[key] = p

	return p, nil
}

// produceInOrder produces records and returns each one's result at its
// index in records; ProduceSync returns them in completion order.
func produceInOrder(ctx context.Context, producer *kgo.Client, records []*kgo.Record) []kgo.ProduceResult {
	at := make(map[*kgo.Record]int, len(records))
	for i, rec := range records {
		at[rec] = i
	}

	out := make([]kgo.ProduceResult, len(records))
	for _, r := range producer.ProduceSync(ctx, records...) {
		out[at[r.Record]] = r
	}

	return out
}

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
