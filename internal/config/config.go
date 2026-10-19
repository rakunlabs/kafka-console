package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/rakunlabs/chu"
	"github.com/rakunlabs/chu/loader/loaderenv"
	"github.com/rakunlabs/logi"

	_ "github.com/rakunlabs/chu/loader/external/loaderawssecrets"
	_ "github.com/rakunlabs/chu/loader/external/loaderawsssm"
	_ "github.com/rakunlabs/chu/loader/external/loaderazurekeyvault"
	_ "github.com/rakunlabs/chu/loader/external/loaderconsul"
	_ "github.com/rakunlabs/chu/loader/external/loadergcpparameter"
	_ "github.com/rakunlabs/chu/loader/external/loadergcpsecret"
	_ "github.com/rakunlabs/chu/loader/external/loadervault"
)

var (
	ServiceName = "kafka-console"
	Version     = "v0.0.0"
)

type Config struct {
	LogLevel string `cfg:"log_level" default:"info"`
	// Info is a short label shown beside the app name, e.g. the environment
	// ("PreProd"). Empty hides it.
	Info string `cfg:"info"`

	Server   Server              `cfg:"server"`
	Features Features            `cfg:"features"`
	Clusters map[string]*Cluster `cfg:"clusters"`
	MCP      MCP                 `cfg:"mcp"`
}

// Features switches parts of the console off with false; unset means on. A
// feature that is off is gone from both the API and the UI, for example
// when the console's credentials lack the ACL for it.
type Features struct {
	// Security is the ACL, SCRAM user and quota section.
	Security *bool `cfg:"security"`
	// CreateTopic allows creating topics.
	CreateTopic *bool `cfg:"create_topic"`
	// DeleteTopic allows deleting topics.
	DeleteTopic *bool `cfg:"delete_topic"`
	// AlterTopic allows changing topic configs and adding partitions.
	AlterTopic *bool `cfg:"alter_topic"`
	// DeleteRecords allows deleting records from the start of partitions.
	DeleteRecords *bool `cfg:"delete_records"`
	// DeleteGroup allows deleting consumer groups.
	DeleteGroup *bool `cfg:"delete_group"`
	// AlterBroker allows changing broker configs and moving partition
	// replicas between brokers.
	AlterBroker *bool `cfg:"alter_broker"`
}

func (f Features) SecurityEnabled() bool      { return enabled(f.Security) }
func (f Features) CreateTopicEnabled() bool   { return enabled(f.CreateTopic) }
func (f Features) DeleteTopicEnabled() bool   { return enabled(f.DeleteTopic) }
func (f Features) AlterTopicEnabled() bool    { return enabled(f.AlterTopic) }
func (f Features) DeleteRecordsEnabled() bool { return enabled(f.DeleteRecords) }
func (f Features) DeleteGroupEnabled() bool   { return enabled(f.DeleteGroup) }
func (f Features) AlterBrokerEnabled() bool   { return enabled(f.AlterBroker) }

func enabled(b *bool) bool { return b == nil || *b }

// MCP is opt-in and read-only unless AllowWrites is explicitly enabled.
// Cluster read_only remains a lower bound regardless of endpoint policy.
type MCP struct {
	Enabled      bool                    `cfg:"enabled"`
	AllowWrites  bool                    `cfg:"allow_writes"`
	AuthToken    string                  `cfg:"auth_token" log:"-"`
	AllowOrigins []string                `cfg:"allow_origins"`
	OutputDir    string                  `cfg:"output_dir"`
	Endpoints    map[string]*MCPEndpoint `cfg:"endpoints"`
}

// MCPEndpoint is one MCP server over a cluster. Without explicit endpoints,
// every cluster gets one at /mcp/<cluster>.
type MCPEndpoint struct {
	Cluster  string `cfg:"cluster"`
	Path     string `cfg:"path"`
	ReadOnly bool   `cfg:"read_only"`
	// Destinations are other clusters copy_message and produce_message may
	// write to from this endpoint. Default none.
	Destinations []string `cfg:"destinations"`
	// Tools switches individual tools off with false.
	Tools map[string]bool `cfg:"tools"`
}

type Server struct {
	Host string `cfg:"host"`
	Port string `cfg:"port" default:"8080"`
	// BasePath serves everything under a prefix, e.g. /kafka behind a proxy.
	BasePath string `cfg:"base_path"`
	// UserHeader names the request header that carries the caller's identity,
	// set by an authenticating proxy. It only labels audit log lines.
	UserHeader string `cfg:"user_header" default:"X-User"`
}

type Cluster struct {
	Brokers []string `cfg:"brokers"`
	// ReadOnly refuses every write: produce, topic create/delete, offset reset.
	ReadOnly bool `cfg:"read_only"`
	// Production only changes how the UI presents the cluster.
	Production bool `cfg:"production"`
	// Description is Markdown shown on the cluster overview, e.g. owners,
	// runbook links or what the cluster is for. Raw HTML is dropped.
	Description string `cfg:"description"`

	TLS  *TLS  `cfg:"tls"`
	SASL *SASL `cfg:"sasl"`

	SchemaRegistry *SchemaRegistry `cfg:"schema_registry"`

	// Protobuf decodes plain (not registry-framed) protobuf messages with
	// local .proto files.
	Protobuf *Protobuf `cfg:"protobuf"`

	// Docs shows a Markdown file per topic on its page.
	Docs *Docs `cfg:"docs"`
}

// Docs reads topic documentation from Markdown files: <path>/<topic>.md.
// The directory can be a checkout of a git repository kept current by
// something else (a sidecar or cron running git pull); files are read on
// each request, so changes show at once.
type Docs struct {
	Path string `cfg:"path"`
}

// Protobuf maps topics to message types from local .proto files, for
// producers that write protobuf without a schema registry.
type Protobuf struct {
	// Paths are directories searched for .proto files and their imports,
	// like protoc's -I. Every .proto file under them is compiled.
	Paths []string `cfg:"paths"`
	// Topics pick the message type per topic; the first match wins.
	Topics []ProtobufTopic `cfg:"topics"`
}

type ProtobufTopic struct {
	// Topic is a regular expression matched against the whole topic name.
	Topic string `cfg:"topic"`
	// Key and Value are fully qualified message names, e.g. shop.v1.Order;
	// an empty one leaves that side to the usual decoding.
	Key   string `cfg:"key"`
	Value string `cfg:"value"`
}

// SchemaRegistry is a Confluent-compatible registry used to decode and
// encode Avro, Protobuf and JSON Schema payloads.
type SchemaRegistry struct {
	URLs         []string `cfg:"urls"`
	User         string   `cfg:"user"`
	Password     string   `cfg:"password" log:"-"`
	PasswordFile string   `cfg:"password_file"`
	Token        string   `cfg:"token" log:"-"`
	TLS          *TLS     `cfg:"tls"`
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

// OAuth configures OAUTHBEARER with exactly one of: a static Token, the
// client credentials flow (secret and/or client certificate), or Google
// Cloud workload identity federation (GCP).
type OAuth struct {
	Token        string            `cfg:"token" log:"-"`
	TokenURL     string            `cfg:"token_url"`
	ClientID     string            `cfg:"client_id"`
	ClientSecret string            `cfg:"client_secret" log:"-"`
	Scopes       []string          `cfg:"scopes"`
	Zid          string            `cfg:"zid"`
	Extensions   map[string]string `cfg:"extensions"`
	// TLS is the client certificate and CA for the token endpoint. With a
	// client certificate, client_secret is optional (RFC 8705).
	TLS OAuthTLS `cfg:"tls"`
	// Proxy for token requests (http, https, socks5, socks5h). Empty uses
	// HTTPS_PROXY / HTTP_PROXY / NO_PROXY.
	Proxy string `cfg:"proxy" log:"-"`
	// Timeout bounds each token request; default 10s.
	Timeout time.Duration `cfg:"timeout"`

	GCP GCP `cfg:"gcp"`
}

// OAuthTLS is a client certificate for a token endpoint. Each value is given
// inline (PEM) or as a file, not both. Files are re-read on every token
// refresh, so rotated certificates are picked up without a restart.
type OAuthTLS struct {
	Cert               string `cfg:"cert"`
	CertFile           string `cfg:"cert_file"`
	Key                string `cfg:"key" log:"-"`
	KeyFile            string `cfg:"key_file"`
	CA                 string `cfg:"ca"`
	CAFile             string `cfg:"ca_file"`
	InsecureSkipVerify bool   `cfg:"insecure_skip_verify"`
}

// IsSet reports whether any option is configured.
func (t OAuthTLS) IsSet() bool {
	return t.InsecureSkipVerify || t.Cert != "" || t.CertFile != "" || t.Key != "" || t.KeyFile != "" || t.CA != "" || t.CAFile != ""
}

// GCP gets Google Cloud access tokens with X.509 workload identity
// federation, optionally impersonating a service account.
type GCP struct {
	Enabled bool `cfg:"enabled"`

	// Credentials is the "external_account" JSON; credential_source is ignored.
	Credentials     string `cfg:"credentials" log:"-"`
	CredentialsFile string `cfg:"credentials_file"`

	// Audience, TokenURL and ServiceAccountImpersonationURL override credentials.
	Audience                       string `cfg:"audience"`
	TokenURL                       string `cfg:"token_url"`
	ServiceAccountImpersonationURL string `cfg:"service_account_impersonation_url"`

	Cert               string `cfg:"cert"`
	CertFile           string `cfg:"cert_file"`
	Key                string `cfg:"key" log:"-"`
	KeyFile            string `cfg:"key_file"`
	CA                 string `cfg:"ca"`
	CAFile             string `cfg:"ca_file"`
	InsecureSkipVerify bool   `cfg:"insecure_skip_verify"`

	// Proxy for the Google endpoints; empty falls back to oauth.proxy.
	Proxy string `cfg:"proxy" log:"-"`

	// Scopes default to cloud-platform.
	Scopes []string `cfg:"scopes"`
	// Lifetime of the impersonated token; default 1h.
	Lifetime time.Duration `cfg:"lifetime"`
	// RefreshBefore renews this long before expiry; default 5m.
	RefreshBefore time.Duration `cfg:"refresh_before"`
	// Format is "raw" (default) or "managed_kafka" for Google Managed
	// Service for Apache Kafka.
	Format string `cfg:"format"`
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

		if sr := cluster.SchemaRegistry; sr != nil {
			if len(sr.URLs) == 0 {
				cluster.SchemaRegistry = nil
			} else if sr.PasswordFile != "" {
				raw, err := os.ReadFile(sr.PasswordFile)
				if err != nil {
					return fmt.Errorf("cluster %q: read schema registry password file; %w", name, err)
				}

				sr.Password = strings.TrimSpace(string(raw))
			}
		}

		if d := cluster.Docs; d != nil {
			if d.Path == "" {
				cluster.Docs = nil
			} else if st, err := os.Stat(d.Path); err != nil || !st.IsDir() {
				return fmt.Errorf("cluster %q: docs path %q is not a directory", name, d.Path)
			}
		}

		if pb := cluster.Protobuf; pb != nil {
			if len(pb.Paths) == 0 || len(pb.Topics) == 0 {
				return fmt.Errorf("cluster %q: protobuf needs paths and topics", name)
			}

			for i, t := range pb.Topics {
				if t.Topic == "" || t.Key == "" && t.Value == "" {
					return fmt.Errorf("cluster %q: protobuf topics[%d] needs a topic and a key or value message", name, i)
				}
			}
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
			// The full check happens when the mechanism is built.
			o := sasl.OAuth
			if o.Token == "" && !o.GCP.Enabled && (o.TokenURL == "" || o.ClientID == "") {
				return fmt.Errorf("cluster %q: oauthbearer needs a token, token_url and client_id, or gcp", name)
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
