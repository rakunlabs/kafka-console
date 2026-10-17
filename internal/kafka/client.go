package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"github.com/twmb/tlscfg"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"

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

	manualOnce   sync.Once
	manualClient *kgo.Client
	manualErr    error
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

	return &Client{
		Name:    name,
		cfg:     cfg,
		options: options,
		client:  client,
		admin:   kadm.NewClient(client),
	}, nil
}

func (c *Client) Close() {
	c.client.Close()

	if c.manualClient != nil {
		c.manualClient.Close()
	}
}

func (c *Client) ReadOnly() bool    { return c.cfg.ReadOnly }
func (c *Client) Production() bool  { return c.cfg.Production }
func (c *Client) Brokers() []string { return c.cfg.Brokers }

func (c *Client) requireWritable() error {
	if c.cfg.ReadOnly {
		return ErrReadOnly
	}

	return nil
}

// manual returns a producer that honours Record.Partition. The shared client
// keeps the default partitioner, since an unset Partition would otherwise
// send everything to partition 0.
func (c *Client) manual() (*kgo.Client, error) {
	c.manualOnce.Do(func() {
		opts := append(append([]kgo.Opt(nil), c.options...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
		c.manualClient, c.manualErr = kgo.NewClient(opts...)
	})

	return c.manualClient, c.manualErr
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
		return oauthMechanism(&s.OAuth), nil
	}

	return nil, fmt.Errorf("unsupported sasl mechanism %q", s.Mechanism)
}

func oauthMechanism(s *config.OAuth) sasl.Mechanism {
	if s.Token != "" {
		return oauth.Auth{Token: s.Token}.AsMechanism()
	}

	credentials := clientcredentials.Config{
		TokenURL:     s.TokenURL,
		ClientID:     s.ClientID,
		ClientSecret: s.ClientSecret,
		Scopes:       s.Scopes,
	}

	var (
		mu     sync.Mutex
		cached *oauth2.Token
	)

	return oauth.Oauth(func(ctx context.Context) (oauth.Auth, error) {
		mu.Lock()
		defer mu.Unlock()

		if !cached.Valid() {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			token, err := credentials.Token(ctx)
			if err != nil {
				// Token endpoint bodies can echo secrets; report only the status.
				var response *oauth2.RetrieveError
				if errors.As(err, &response) && response.Response != nil {
					return oauth.Auth{}, fmt.Errorf("oauth: token endpoint returned HTTP %d", response.Response.StatusCode)
				}

				return oauth.Auth{}, errors.New("oauth: token request failed")
			}

			cached = token
		}

		return oauth.Auth{Token: cached.AccessToken}, nil
	})
}
