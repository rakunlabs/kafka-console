<img align="right" height="72" src="assets/logo.svg">

# kafka-console

[![License](https://img.shields.io/github/license/rakunlabs/kafka-console?style=flat-square)](LICENSE)
[![Release](https://img.shields.io/github/v/release/rakunlabs/kafka-console?style=flat-square)](https://github.com/rakunlabs/kafka-console/releases/latest)
[![Test](https://img.shields.io/github/actions/workflow/status/rakunlabs/kafka-console/test.yml?branch=main&style=flat-square&label=test)](https://github.com/rakunlabs/kafka-console/actions/workflows/test.yml)

Web console for Apache Kafka. One binary with the UI embedded. Point it at
one or more clusters and see what is in a topic, who is consuming it and how
far behind they are.

![Messages with the JSON detail pane](assets/screenshots/messages.png)

<table>
  <tr>
    <td width="50%"><img src="assets/screenshots/overview.png" alt="Cluster overview"><br><sub>Cluster overview</sub></td>
    <td width="50%"><img src="assets/screenshots/consumer-group.png" alt="Consumer group lag per partition"><br><sub>Consumer group lag and offset reset</sub></td>
  </tr>
  <tr>
    <td width="50%"><img src="assets/screenshots/produce.png" alt="Produce a message"><br><sub>Produce</sub></td>
    <td width="50%"><img src="assets/screenshots/messages-dark.png" alt="Messages in dark theme"><br><sub>Dark theme</sub></td>
  </tr>
</table>

## Features

- **Overview:** brokers, controller, partition health, disk usage.
- **Topics:**
  - Partitions, replicas, ISR, offsets and size.
  - Create and delete topics, edit config, add partitions, delete records.
- **Messages:**
  - Browse by offset, timestamp, newest or oldest.
  - Text search, JavaScript filters, live tail, JSON tree view.
  - Resend messages to another topic.
  - Open a message in Produce to edit its key, value or headers, review changes, and send a new record to the same or another topic.
- **Produce:** key, headers, partition, compression, base64, or JSON encoded with a registry schema.
- **Consumer groups:**
  - Per-partition lag and catch-up estimate.
  - Reset offsets with a preview: earliest, latest, offset, timestamp or shift.
  - Create a group or add a topic to one.
- **Schema Registry:**
  - Avro, Protobuf and JSON Schema decoding.
  - Subjects, versions, compatibility, create and delete.
- **Security:** ACLs, SCRAM users, client quotas.
- **Operator safety:**
  - Per-cluster `read_only` and `production` flags.
  - An audit log line for every write.
  - Confirmation before destructive actions.
- **Large clusters:** works with thousands of topics and groups. Lists page, pickers load names only, and Overview reports only unhealthy topics.

## Quick start

Write a minimal `kafka-console.yaml`:

```yaml
clusters:
  local:
    brokers: ["host.docker.internal:9092"]
```

Run it:

```sh
docker run -d --name kafka-console -p 8080:8080 \
  -v "$PWD/kafka-console.yaml:/etc/kafka-console.yaml:ro" \
  ghcr.io/rakunlabs/kafka-console:latest
```

Open http://localhost:8080.

Binaries for Linux, macOS and Windows are on the
[releases page](https://github.com/rakunlabs/kafka-console/releases). Run the
binary in the same folder as `kafka-console.yaml`.

## Configuration

`kafka-console.yaml` (or `.yml`, `.json`, `.toml`) is loaded from the working
directory or `/etc`. Set `CONFIG_FILE` to use another path.

Environment variables with the `KAFKA_CONSOLE_` prefix override single values
from the file, for example `KAFKA_CONSOLE_SERVER_PORT=9000` or
`KAFKA_CONSOLE_INFO=PreProd`. Define clusters in the file: environment
variables cannot set values inside `clusters`. Read secrets from files with
`password_file`, e.g. a mounted Kubernetes secret.

```yaml
log_level: info
info: PreProd            # label shown beside the app name; omit to hide

server:
  port: "8080"
  # base_path: /kafka    # serve under a sub-path behind a reverse proxy
  # user_header: X-User  # header your auth proxy sets; logged on every write

clusters:
  local:
    brokers: ["localhost:9092"]
    schema_registry:
      urls: ["http://localhost:8081"]

  prod:
    brokers: ["kafka-1.prod:9093", "kafka-2.prod:9093"]
    production: true     # red banner and warnings in the UI
    read_only: true      # refuses produce, topic changes, offset resets
    tls:
      enabled: true
      ca_file: /etc/kafka/ca.pem
      # cert_file / key_file for mTLS, insecure_skip_verify
    sasl:
      mechanism: scram-sha-512   # plain | scram-sha-256 | scram-sha-512 | oauthbearer
      user: console
      password_file: /run/secrets/kafka-password
    schema_registry:
      urls: ["https://registry.prod:8081"]
      user: console              # basic auth, or token: <bearer>
      password_file: /run/secrets/registry-password
```

### OAUTHBEARER

With `mechanism: oauthbearer`, configure exactly one way to get a token under
`sasl.oauth`. The keys match
[wkafka](https://github.com/worldline-go/wkafka), so the same block connects
the same way in both.

**Static token.** It is never renewed.

```yaml
oauth:
  token: eyJ...
```

**Client credentials.** Tokens are cached and renewed before they expire.

```yaml
oauth:
  token_url: https://idp.example.com/oauth2/token
  client_id: kafka-console
  client_secret: ""          # optional with a client certificate (RFC 8705)
  scopes: [kafka]
  # tls:                     # mutual TLS to the token endpoint
  #   cert_file: /etc/certs/client.pem
  #   key_file: /etc/certs/client-key.pem
  #   ca_file: /etc/certs/ca.pem
  # proxy: http://proxy:3128 # http, https, socks5, socks5h; default HTTPS_PROXY
  # timeout: 10s
  # zid: ""                  # authorization identity
  # extensions: {}           # SASL extensions
```

**Google Cloud.** This uses X.509 workload identity federation, optionally
impersonating a service account. Use `format: managed_kafka` for Google
Managed Service for Apache Kafka.

```yaml
tls:
  enabled: true
sasl:
  mechanism: oauthbearer
  oauth:
    gcp:
      enabled: true
      credentials_file: /etc/certs/gcp/wif-credentials.json  # external_account JSON
      cert_file: /etc/certs/gcp/client.pem
      key_file: /etc/certs/gcp/client-key.pem
      format: managed_kafka
      # audience, token_url and service_account_impersonation_url
      # override the values in credentials
      # scopes: [https://www.googleapis.com/auth/cloud-platform]
      # lifetime: 1h             # impersonated token lifetime
      # refresh_before: 5m
      # proxy: ""                # falls back to oauth.proxy
```

Certificates, keys and CAs can be given inline (`cert`, `key`, `ca`,
`credentials`) or as files (`*_file`), but not both for the same value. Files
are re-read on each token refresh, so rotated certificates work without a
restart. If a refresh fails while the current token is still valid, the
current token keeps being used.

### Authentication and audit

kafka-console has no login of its own. Put it behind your authenticating
proxy (OAuth2 Proxy, an ingress with SSO, …) and have the proxy set a user
header. Every write request is logged with that user, the cluster, the action
and its target. Request bodies are not stored, and secrets are redacted.

```json
{"level":"INFO","msg":"audit","user":"ada@example.com","cluster":"prod","action":"groups.reset","method":"POST","status":200,"target_group":"billing"}
```

## Development

Requires Go and pnpm 11 or newer. The UI's `package.json` pins pnpm and Node.js, and pnpm downloads that Node.js version itself.

```sh
make env              # local Redpanda broker with Schema Registry
make run              # backend on :8080 with kafka-console.local.yaml
make run-ui           # UI dev server with hot reload (proxies /api to :8080)
make test             # Go tests
make lint             # golangci-lint and UI type-check
make build            # snapshot binary with goreleaser, linux/amd64 by default
make build-container  # kafka-console:test image from that binary
make snapshot         # every archive and both images, nothing published
```

Build for another platform with `make build GOOS=darwin GOARCH=arm64`.

Pushing a `v*` tag runs GoReleaser in CI. It publishes the archives and a
multi-arch image (linux/amd64 and linux/arm64) to `ghcr.io`.
