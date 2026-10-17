package kafka

import (
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
	}

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

	key := fmt.Sprintf("%t/%s", manual, codec)

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
