# Product

<!-- impeccable:product-schema 1 -->

## Platform

web

## Stack

- Backend: Go, rakunlabs stack (`into`, `logi`, `chu`, `ada`), Kafka client `github.com/twmb/franz-go` (`kgo` + `kadm`).
- Frontend: Svelte 5 + Vite (pnpm), built from `_ui/` into `internal/server/dist` and embedded into the Go binary with `go:embed`.
- Single binary deploy; config via `chu` (file/env, prefix `KAFKA_CONSOLE_`).

## Users

Developer and operations teams working with Kafka. Developers open it to debug: find a specific message, inspect key/value/headers, produce a test message. Ops/SRE open it to check cluster health, partition/replica state, and consumer group lag, and to occasionally reset offsets or create/delete topics.

## Product Purpose

A self-hosted Kafka console that answers "what is in this topic, who is consuming it, and how far behind are they" quickly, and lets authorized people make the small administrative changes without a CLI.

## Operating Context

- Multiple clusters (e.g. local, staging, prod) defined in config, each with brokers and optional SASL/TLS. The UI switches between them.
- Used alongside terminals and logs during incidents and debugging sessions; long sessions, dense data.

## Capabilities and Constraints

- Cluster overview: brokers (leader/replica counts, version guess), controller, topic/partition counts, disk usage. Broker detail: log dirs, editable broker configs.
- Topics: list (size, cleanup policy), detail (partitions, leader, replicas, ISR, offsets, size, config), create, delete, edit/reset configs, add partitions, delete records (truncate).
- Messages: one Start selector for newest, oldest, offset, last N per partition, time presets and custom from/to. Time ranges show newest matches first and use Kafka's time index by default; optional Exact time scans later offsets for out-of-order producer timestamps. Search/filter reads stream until limit, end of range, Stop or 30 min, no scan cap; key/value/headers; JSON pretty print; JS filter; live tail over SSE always starts at now while keeping search, partition, filter and decoding; JSON/CSV export with explicit null metadata.
- Schema Registry (optional per cluster): automatic Avro/Protobuf/JSON Schema decoding of Confluent-framed keys and values; subjects, versions, diff, create with compatibility check, compatibility levels, soft/hard delete.
- Produce: key, value, headers, target partition, compression, base64 binary, or JSON encoded through a registry schema.
- Edit & resend: load a full record into Produce; edit key/value/headers and target topic/partition, preserving binary/null data and original schema IDs/message types by default. Review changes before sending a new record; the original stays unchanged. `confirm_writes` clusters require typing the target topic; read-only clusters block writes. Byte-for-byte Resend remains separate.
- Bulk: range replay (offset/time bounds, partition, search, JS filter; fixed reviewed range, read-committed, in order per partition; quick predicate-free estimate or optional exact count and timestamp check; streamed progress; stops on the first failed write or when the browser disconnects; up to 1M records / 30 min). File import (console JSON/CSV export, JSON lines, plain lines; payload text kept verbatim; optional partition, timestamp and schema-ID preservation; file streamed in the browser, never held whole, so size is unbounded; dry-run validation of every record before ordered batches of 500; stop/failure reports the first unwritten item; lost responses mark the batch as unknown and resume requires acknowledging possible duplicates). Both refuse read-only clusters and need the typed topic name on clusters with `confirm_writes`.
- Consumer groups: list, state, members, per-partition lag; offset reset (earliest/latest/specific/timestamp/shift) applies only the exact previewed offsets and asks for a new preview if commits or retained ranges changed; delete offsets per topic.
- Security: ACLs (list/create/delete with presets), SCRAM users, client quotas; editing a quota preserves its complete compound/default entity.
- Optional MCP: shared kafka-mcp tools on the console HTTP server at `/mcp/<cluster>` by default or any configured path (under `server.base_path` when set), borrowing the console's live Kafka/admin/registry clients and reader/producer factories without translating connection config; off by default, read-only unless explicitly enabled for writes, with cluster read-only as a lower bound and explicit cross-cluster destinations.
- Destructive operations (delete topic, delete records, offset reset/delete, hard delete schema) need explicit confirmation.
- Open decision: authentication / per-cluster read-only mode.

## Product Principles

1. Data first: offsets, lag and payloads are the content; chrome stays out of the way.
2. Prod is dangerous: always make the current cluster obvious and destructive actions deliberate.
3. Fast to the answer: every view reachable in one or two clicks, URLs are shareable.
4. One binary, zero setup beyond a config file.
