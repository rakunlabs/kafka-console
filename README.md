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
  - Move replicas between brokers, with a balance plan that evens out replicas and leaders.
  - The ACLs that apply to each topic and group.
  - Optional documentation page per topic, from Markdown files; see [Topic documentation](#topic-documentation).
- **Messages:**
  - One Start selector: newest, oldest, from an offset, the last N of each partition, a recent time range (15 minutes to 7 days), or custom from/to. Time ranges show the newest matching messages first. They use Kafka's time index by default; turn on Exact time to also find out-of-order producer timestamps, at the cost of scanning later offsets. "Load older" or "Load more" continues the same read where it stopped, even after the form is edited.
  - Live tail always shows messages that arrive from now on. Search, partition, JavaScript filter and key/value decoding still apply; Start, time range and limit do not.
  - Text search, JavaScript filters, live tail, JSON tree view. A search reads until it fills the limit, reaches the end of the range or is stopped, showing matches as they arrive. A filter gets 2 seconds per message; one that runs longer counts as a script error and the read goes on.
  - Read keys and values as JSON, text, MessagePack, CBOR, hex, base64 or an unsigned integer when the automatic choice is wrong. Protobuf without a registry, from local `.proto` files; see [Protobuf without a registry](#protobuf-without-a-registry).
  - Show fields of JSON values as columns, and times as local, UTC, relative or Unix. Both are kept in the browser.
  - Download a key or value exactly as stored, including values too large to show.
  - Resend messages to another topic.
  - Open a message in Produce to edit its key, value or headers, review changes, and send a new record to the same or another topic.
  - Replay a range by offset or time, with the same search and JS filter, to another topic or back into the same one. Review fixes the offset range; Write uses that same range, excluding records appended after review. Without a search or filter, review quickly shows an upper-bound estimate from offsets. Exact count and time check scans first, counts committed records and finds out-of-order timestamps. Shows progress while it runs.
  - Import records from a file: a JSON or CSV export from this console, JSON lines, or one value per line. Can keep partitions, timestamps and schema IDs. The file is read as a stream, so any size works. Every record is checked before any is written. A stopped or failed import can resume; if a response is lost, the batch's outcome is unknown and resending it requires acknowledging the risk of duplicates. Import does not guarantee exactly-once delivery.
  - New CSV exports preserve null keys and tombstones separately from empty strings, plus schema and truncation metadata. Older CSV files still import with their original interpretation (empty key means null, empty value means an empty string); use JSON or a new export when that distinction matters. Truncated or undecodable exports are refused rather than silently re-imported.
- **Produce:** key, headers, partition, compression, base64, or JSON encoded with a registry schema.
- **Consumer groups:**
  - Per-partition lag and catch-up estimate.
  - Reset offsets with a preview: earliest, latest, offset, timestamp or shift. Apply uses the exact reviewed offsets; changed commits or targets outside the retained range require a new preview.
  - Back up a group's offsets to a JSON file and restore them, or copy another group's offsets on the same topic.
  - Create a group or add a topic to one.
- **Schema Registry:**
  - Avro, Protobuf and JSON Schema decoding.
  - Subjects, versions, compatibility, create and delete.
  - Filter by registry context; each subject links to the topics it is named after.
- **Security:** ACLs, SCRAM users, client quotas. Editing a quota preserves its complete entity, including combined user/client-id and default components.
- **Operator safety:**
  - Per-cluster `read_only` and `confirm_writes` flags, and a `color` that tints the UI.
  - Switch off features such as topic create or delete; see [Features](#features-1).
  - An audit log line for every write.
  - Confirmation before destructive actions.
- **MCP (optional):** expose the shared [kafka-mcp](https://github.com/denizgursoy/kafka-mcp) tools on the same HTTP server, read-only by default. Includes setup examples for OpenCode, Claude Code and Codex; see [Connect an AI coding agent](#connect-an-ai-coding-agent).
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
  # trusted_proxies: ["10.0.0.0/8"]  # only these may set user_header and X-Forwarded-For
  # idle_timeout: 2m     # close idle keep-alive connections

# features:              # every feature is on unless set to false; see Features below
#   create_topic: false
#   delete_topic: false

# cache:                 # how long the overview is kept in memory and shared between viewers
#   overview: 15s        # counts, health, brokers, groups
#   disk_usage: 60s      # scans every broker's log dirs; the expensive one
#   refresh: 30s         # Refresh reads again only past this age; newer results are reused

# limits:                # per cluster connection; a cluster's own limits override these
#   reads: 16            # browse/search/resend reads at once; more wait their turn
#   idle_readers: 4      # readers kept connected between reads
#   streams: 32          # live tails and replays at once; more are refused
#   mcp_reads: 8         # readers MCP tools hold at once, apart from the UI's reads

clusters:
  local:
    brokers: ["localhost:9092"]
    schema_registry:
      urls: ["http://localhost:8081"]

  prod:
    brokers: ["kafka-1.prod:9093", "kafka-2.prod:9093"]
    order: 1             # list position, lowest first; unset ones follow by name.
                         # The first cluster opens for someone who has not picked one.
    color: "#e5484d"     # RGB hex or a CSS name like blue; tints the cluster marker, active nav item and backdrop
    confirm_writes: true # type the topic name before resend, replay, import and edited produce
    read_only: true      # refuses produce, topic changes, offset resets
    cache:               # overrides the global cache settings for this cluster
      disk_usage: 5m
    limits:              # overrides the global limits for this cluster
      mcp_reads: 2
    description: |       # Markdown, shown in a box on the cluster overview
      **Production** cluster. Owner: platform team.
      Runbook: [wiki](https://wiki.example.com/kafka-prod)
    tls:
      enabled: true      # on by default when any other tls field is set; false forces plaintext
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

### Features

Switch off parts of the console you do not want offered, for example when
its credentials lack the ACL for them. Every feature is on unless set to
`false`; leaving a key out keeps it on.

```yaml
features:
  create_topic: false
  delete_topic: false
```

A feature that is off is removed everywhere: its buttons and pages are gone
from the UI, its API routes answer 404, and its MCP tools are not offered
(an MCP endpoint's `tools` cannot switch them back on).

| Key | Turns off | MCP tools |
|---|---|---|
| `security` | The Security section: ACLs, SCRAM users, client quotas | `list_acls` |
| `create_topic` | Create topic | `create_topic` |
| `delete_topic` | Delete topic | `delete_topic` |
| `alter_topic` | Editing topic config and adding partitions | `alter_topic_config`, `add_partitions` |
| `delete_records` | Delete records from a topic | `delete_records` |
| `delete_group` | Delete consumer group | `delete_consumer_group` |
| `alter_broker` | Editing broker config and moving partition replicas | — |

Features apply to every cluster. A cluster can set its own `features` too;
a key set there wins over the global one for that cluster, and keys it leaves
out follow the global setting:

```yaml
features:
  delete_topic: false      # off everywhere...

clusters:
  dev:
    brokers: ["localhost:9092"]
    features:
      delete_topic: true   # ...except on dev
  prod:
    brokers: ["kafka-1.prod:9093"]
    features:
      security: false      # only prod hides the Security section
```

To block all writes on one cluster, use its `read_only` flag instead.
Environment variables work for the global features, for example
`KAFKA_CONSOLE_FEATURES_DELETE_TOPIC=false`.

### Protobuf without a registry

Producers that write plain protobuf (no schema registry framing) can be read
with local `.proto` files. Every `.proto` under `paths` is compiled at start
up; imports resolve against `paths` too, and the Google well-known types are
built in. Each topic rule is a regular expression matched against the whole
topic name, and the first match picks the key and value message types.

```yaml
clusters:
  local:
    brokers: ["localhost:9092"]
    protobuf:
      paths: [./protos]
      topics:
        - topic: "orders\\..*"   # orders.eu, orders.us, ...
          value: shop.v1.Order
        - topic: payments
          key: shop.v1.PaymentKey
          value: shop.v1.Payment
```

Registry-framed records still use the registry. Messages decoded this way are
read-only in Produce, since the form cannot encode them back; use Resend to
copy them byte for byte.

### Topic documentation

A topic page gets a **Docs** tab when the cluster has a `docs.path`. It shows
`<path>/<topic>.md`, rendered as Markdown. Raw HTML in the file is dropped and
links are limited to safe schemes. Files are read on each view, so a directory
kept current by `git pull` (from a cron job or a sidecar) shows changes at once.

```yaml
clusters:
  local:
    brokers: ["localhost:9092"]
    docs:
      path: ./topic-docs   # orders.created.md, payments.md, ...
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

Inside GCP, for example a GKE pod with Workload Identity, use
`source: metadata` instead. Tokens come from the metadata server for the
pod's service account, so no credential file, certificate or proxy is
needed; with `format: managed_kafka` the service account email is read from
the metadata server too. The metadata server is always reached without a
proxy.

```yaml
tls:
  enabled: true                  # system roots verify Google Managed Kafka
sasl:
  mechanism: oauthbearer
  oauth:
    gcp:
      enabled: true
      source: metadata
      format: managed_kafka
      # metadata_url: http://metadata.google.internal  # default; GCE_METADATA_HOST overrides
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

## MCP endpoint

MCP is off by default. Enable it in the same console configuration:

```yaml
mcp:
  enabled: true
  auth_token: "replace-with-a-secret" # or KAFKA_CONSOLE_MCP_AUTH_TOKEN
  # allow_writes: false             # default; separate from the UI's permissions
  # public_url: https://kafka.example.com # rarely needed: links use the address the agent connected to
  # link_key: "..."        # or KAFKA_CONSOLE_MCP_LINK_KEY; same on every instance (default: auth_token)
  # session_timeout: 30m   # close an agent's session after this long idle
```

Each configured cluster gets a Streamable HTTP endpoint at `/mcp/<cluster>`
on the console's existing port. `server.base_path` prefixes these paths too:
`base_path: /kafka` makes the local URL `/kafka/mcp/local`. Unknown MCP paths
return 404, not the UI.

### Connect an AI coding agent

When MCP is on, each cluster's **Overview** shows its MCP address with a copy
button and **Connect an agent…**, which gives ready-to-paste setup for
OpenCode, Claude Code and Codex. The address uses the host the browser reached
the console at, so it is correct behind a proxy. The token is never shown.

Build the MCP address from the console URL, `server.base_path` and the
cluster name:

```text
<console-url><base_path>/mcp/<cluster>
```

| Console setup | MCP address |
|---|---|
| `http://localhost:8080`, cluster `local` | `http://localhost:8080/mcp/local` |
| `https://kafka.example.com`, `base_path: /kafka`, cluster `prod` | `https://kafka.example.com/kafka/mcp/prod` |
| Explicit endpoint `path: /agents/prod` | `https://kafka.example.com/agents/prod` |

URL-encode cluster names that contain spaces or other reserved characters.
Use HTTPS outside localhost. Put the token in an environment variable for the
agent rather than in a project file:

```sh
export KAFKA_CONSOLE_MCP_AUTH_TOKEN="replace-with-the-console-token"
```

<details>
<summary>OpenCode</summary>

Add this to `opencode.json` or `opencode.jsonc`:

```jsonc
{
  "$schema": "https://opencode.ai/config.json",
  "mcp": {
    "kafka-local": {
      "type": "remote",
      "url": "http://localhost:8080/mcp/local",
      "oauth": false,
      "headers": {
        "Authorization": "Bearer {env:KAFKA_CONSOLE_MCP_AUTH_TOKEN}"
      },
      "enabled": true
    }
  }
}
```

Check it with `opencode mcp list`.
</details>

<details>
<summary>Claude Code</summary>

Add the server for the current project:

```sh
claude mcp add --transport http kafka-local \
  http://localhost:8080/mcp/local \
  --header "Authorization: Bearer $KAFKA_CONSOLE_MCP_AUTH_TOKEN"
```

Check it with `claude mcp get kafka-local`, or run `/mcp` inside Claude Code.
To share the address in `.mcp.json` without committing the token:

```json
{
  "mcpServers": {
    "kafka-local": {
      "type": "http",
      "url": "http://localhost:8080/mcp/local",
      "headers": {
        "Authorization": "Bearer ${KAFKA_CONSOLE_MCP_AUTH_TOKEN}"
      }
    }
  }
}
```
</details>

<details>
<summary>Codex</summary>

Add this to `~/.codex/config.toml`, or to `.codex/config.toml` in a trusted
project:

```toml
[mcp_servers.kafka-local]
url = "http://localhost:8080/mcp/local"
bearer_token_env_var = "KAFKA_CONSOLE_MCP_AUTH_TOKEN"
```

Start Codex from a shell where `KAFKA_CONSOLE_MCP_AUTH_TOKEN` is set, then use
`/mcp` to check the connection.
</details>

After connecting, ask the agent to call `server_config` first. It shows the
cluster, endpoint path and effective tool policy. If you omit
`mcp.auth_token`, remove the authorization header and keep the endpoint behind a
trusted authenticating proxy or private network.

### Files from an agent

Agents save and send files on the user's own machine; nothing is stored on the
console's host. `search_messages` and `transform_messages` with `download`,
and `produce_message` with `upload`, return a link and a `curl`
command that the agent runs where the user is working. The data streams
between that machine and Kafka while the command runs, so message bodies never
pass through the model either.

```text
Find every order over 500 in orders-dlq and save them to orders.csv
```

```sh
curl -fsS -o 'orders.csv.part' 'https://kafka.example.com/mcp/prod/files/Q2x…' && mv 'orders.csv.part' 'orders.csv'
```

A download is JSON lines, or CSV with `format: "csv"`, in this console's export
layout, so it loads on a topic's **Messages** page with **Import…**. Values are
never cut, nulls stay null, binary payloads keep their bytes, and Avro,
Protobuf and JSON Schema values keep their schema ID. A `transform_messages`
download imports too, or goes back through a `produce_message` upload.

Links are served at `<endpoint path>/files/` and expire after 10 minutes. They
need no bearer token, because the agent's `curl` does not carry the MCP
client's headers. Instead each link carries its request sealed with AES-GCM, so
it shows nothing in proxy logs and cannot be changed; treat it as a secret until
it expires, and run an upload command once. The key comes from
`mcp.link_key`, or `mcp.auth_token` when that is unset. **Running several
console instances behind one address? Give them the same key**: the link holds
the whole request, so any instance can serve a link another made, with nothing
shared between them. Browser origins are checked as for the endpoint.

Links are built on the address the agent connected to the MCP endpoint with, so
usually nothing needs setting. Behind a reverse proxy the console honours
`X-Forwarded-Proto` and `X-Forwarded-Host`. Set `mcp.public_url` only when
links would otherwise point somewhere the agent cannot reach: a proxy that
rewrites the host without sending `X-Forwarded-Host`, or one that serves the
console under a path prefix (`https://example.com/kafka/`) that
`server.base_path` does not cover.

For separate endpoint policies, configure an explicit map. Every console
cluster is visible to `list_clusters` and `compare_clusters`:

```yaml
mcp:
  enabled: true
  allow_writes: true
  endpoints:
    prod-read:
      cluster: prod
      path: /mcp/prod
      read_only: true
      destinations: [stage] # explicit cross-cluster write grant; default none
    stage:
      cluster: stage
      path: /mcp/stage
      tools:
        delete_topic: false
```

An endpoint `path` can be anything, for example `/abc/mcp/xyz` or `agents`. It
is relative to `server.base_path` and cleaned, so `/team//kafka/` becomes
`/team/kafka`. Only the paths the console itself needs are refused: `/`, the
`/api` tree, `/healthz`, and router patterns containing `{`, `}` or `*`. Only
the exact configured path is served; anything next to it still goes to the UI.
Everything under `/mcp/` that is not an endpoint returns 404. With `allow_writes: false`, **no MCP
endpoint may write to any cluster**, including named destinations. With it
enabled, a cluster's `read_only: true` still cannot be widened by an endpoint.
MCP writes use kafka-mcp's `confirm` previews, not the UI's typed confirmation;
`confirm_writes` is a UI confirmation, not an MCP permission. Call `server_config` to
verify the endpoint, cluster and effective tool policy before acting.

`transform_messages` (rewrite messages with JavaScript and write them as new
ones) has no counterpart in the console, so it is off unless an endpoint
switches it on with `tools: { transform_messages: true }`. Tools of a console
feature that is switched off (`features`) are off for MCP too.

Tool definitions, batch results, mutation guards and audit logging come directly
from `kafka-mcp/pkg/kafkamcp`, the only kafka-mcp package the console imports;
they are not reimplemented here. The console builds one MCP server per
endpoint and serves it on its own router, with its own bearer-token and origin
checks. MCP borrows the
console's existing Kafka, admin and Schema Registry clients, its cached
manual-partition producer, its bulk producer for batch writes (`produce_message`
with `upload`, `transform_messages`) and its `protobuf` topic mappings. No
broker/TLS/SASL/OAuth configuration is converted or reloaded. Message scans
borrow readers from the console's pool, at most `limits.mcp_reads` at a time
and apart from the UI's own reads, so an agent cannot starve the UI or open
unbounded connections; a call over the limit waits until its timeout. MCP
never closes the console's clients.
`server_config` reports authentication as `host-managed`, with the console's
configured brokers and SASL user; credentials are never passed on.
Tool audit records go to the console's log and identify the endpoint, configured
principal, MCP client and the user from `server.user_header`, under the same
`trusted_proxies` rules as the UI's audit lines.

The bearer token is optional only when a trusted authenticating proxy or network
boundary already protects MCP. Proxy protection must cover `/mcp` as well as
`/api`. Browser origins are rejected by default; allow a trusted browser MCP
client with `mcp.allow_origins: ["https://trusted-client.example"]`. An allowed
origin may drive all tools exposed to it. The UI's existing auth behavior is unchanged.

## Development

### Build and run

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
