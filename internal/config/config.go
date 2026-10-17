package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"github.com/rakunlabs/chu"
	"github.com/rakunlabs/chu/loader/loaderenv"
	"github.com/rakunlabs/logi"
)

var (
	ServiceName = "kafka-console"
	Version     = "v0.0.0"
)

type Config struct {
	LogLevel string `cfg:"log_level" default:"info"`

	Server   Server              `cfg:"server"`
	Clusters map[string]*Cluster `cfg:"clusters"`
}

type Server struct {
	Host string `cfg:"host"`
	Port string `cfg:"port" default:"8080"`
	// BasePath serves everything under a prefix, e.g. /kafka behind a proxy.
	BasePath string `cfg:"base_path"`
}

type Cluster struct {
	Brokers []string `cfg:"brokers"`
	// ReadOnly refuses every write: produce, topic create/delete, offset reset.
	ReadOnly bool `cfg:"read_only"`
	// Production only changes how the UI presents the cluster.
	Production bool `cfg:"production"`

	TLS  *TLS  `cfg:"tls"`
	SASL *SASL `cfg:"sasl"`
}

type TLS struct {
	Enabled            bool   `cfg:"enabled"`
	InsecureSkipVerify bool   `cfg:"insecure_skip_verify"`
	CAFile             string `cfg:"ca_file"`
	CertFile           string `cfg:"cert_file"`
	KeyFile            string `cfg:"key_file"`
}

const (
	MechanismPlain       = "plain"
	MechanismScramSHA256 = "scram-sha-256"
	MechanismScramSHA512 = "scram-sha-512"
	MechanismOAuth       = "oauthbearer"
)

type SASL struct {
	Mechanism    string `cfg:"mechanism"`
	User         string `cfg:"user"`
	Password     string `cfg:"password" log:"-"`
	PasswordFile string `cfg:"password_file"`
	IsToken      bool   `cfg:"is_token"`

	OAuth OAuth `cfg:"oauth"`
}

// OAuth uses either a static Token or the client credentials flow.
type OAuth struct {
	Token        string   `cfg:"token" log:"-"`
	TokenURL     string   `cfg:"token_url"`
	ClientID     string   `cfg:"client_id"`
	ClientSecret string   `cfg:"client_secret" log:"-"`
	Scopes       []string `cfg:"scopes"`
}

func Load(ctx context.Context) (*Config, error) {
	var cfg Config
	if err := chu.Load(ctx, ServiceName, &cfg,
		chu.WithLoaderOption(loaderenv.New(
			loaderenv.WithPrefix("KAFKA_CONSOLE_"),
		)),
		chu.WithVersion(Version),
	); err != nil {
		return nil, err
	}

	if err := logi.SetLogLevel(cfg.LogLevel); err != nil {
		return nil, fmt.Errorf("set log level %s; %w", cfg.LogLevel, err)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	cfg.Server.BasePath = normalizeBasePath(cfg.Server.BasePath)

	slog.Info("loaded configuration", "config", chu.MarshalMap(cfg))

	return &cfg, nil
}

func (c *Config) validate() error {
	if len(c.Clusters) == 0 {
		return fmt.Errorf("no clusters configured; add at least one under clusters")
	}

	for name, cluster := range c.Clusters {
		if cluster == nil || len(cluster.Brokers) == 0 {
			return fmt.Errorf("cluster %q has no brokers", name)
		}

		if cluster.SASL == nil {
			continue
		}

		sasl := cluster.SASL
		sasl.Mechanism = strings.ToLower(sasl.Mechanism)

		switch sasl.Mechanism {
		case MechanismPlain, MechanismScramSHA256, MechanismScramSHA512:
			if sasl.PasswordFile != "" {
				raw, err := os.ReadFile(sasl.PasswordFile)
				if err != nil {
					return fmt.Errorf("cluster %q: read password file; %w", name, err)
				}

				sasl.Password = strings.TrimSpace(string(raw))
			}
		case MechanismOAuth:
			if sasl.OAuth.Token == "" && (sasl.OAuth.TokenURL == "" || sasl.OAuth.ClientID == "") {
				return fmt.Errorf("cluster %q: oauthbearer needs a token or token_url and client_id", name)
			}
		case "":
			cluster.SASL = nil
		default:
			return fmt.Errorf("cluster %q: unsupported sasl mechanism %q", name, sasl.Mechanism)
		}
	}

	return nil
}

// normalizeBasePath returns "" for root, otherwise "/a/b" without a trailing slash.
func normalizeBasePath(p string) string {
	p = strings.Trim(strings.TrimSpace(p), "/")
	if p == "" {
		return ""
	}

	return "/" + p
}

// ClusterNames returns the configured cluster names in a stable order.
func (c *Config) ClusterNames() []string {
	names := make([]string, 0, len(c.Clusters))
	for name := range c.Clusters {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}
