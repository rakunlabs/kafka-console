BINARY := kafka-console
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo v0.0.0)
COMMIT  ?= $(shell git rev-parse --short HEAD 2>/dev/null || echo -)
DATE    ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ)
LDFLAGS := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT) -X main.date=$(DATE)

.DEFAULT_GOAL := help

.PHONY: build-ui
build-ui: ## Build the frontend into internal/server/dist
	cd _ui && pnpm install --frozen-lockfile && pnpm build

.PHONY: build
build: build-ui ## Build the single binary with the embedded UI
	CGO_ENABLED=0 go build -trimpath -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/$(BINARY)

.PHONY: run
run: ## Run the backend with kafka-console.local.yaml
	CONFIG_FILE=kafka-console.local.yaml go run ./cmd/$(BINARY)

.PHONY: dev-ui
dev-ui: ## Run the Vite dev server (proxies /api to :8080)
	cd _ui && pnpm dev

.PHONY: env-up
env-up: ## Start a local Redpanda broker
	docker compose -f env/docker-compose.yaml up -d

.PHONY: env-down
env-down: ## Stop the local broker
	docker compose -f env/docker-compose.yaml down -v

.PHONY: test
test: ## Run Go tests
	go test ./...

.PHONY: lint
lint: ## Vet Go code and type-check the UI
	go vet ./...
	cd _ui && pnpm check

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'
