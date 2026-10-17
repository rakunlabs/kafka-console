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

- Cluster overview: brokers, controller, topic/partition counts.
- Topics: list, detail (partitions, leader, replicas, ISR, offsets, config), create, delete.
- Messages: browse by partition, offset, timestamp, newest/oldest; key/value/headers; JSON pretty print.
- Produce: key, value, headers, target partition.
- Consumer groups: list, state, members, per-partition lag; offset reset (earliest/latest/specific/timestamp).
- Destructive operations (delete topic, offset reset) need explicit confirmation.
- Open decision: authentication / per-cluster read-only mode.

## Product Principles

1. Data first: offsets, lag and payloads are the content; chrome stays out of the way.
2. Prod is dangerous: always make the current cluster obvious and destructive actions deliberate.
3. Fast to the answer: every view reachable in one or two clicks, URLs are shareable.
4. One binary, zero setup beyond a config file.
