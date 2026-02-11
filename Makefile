.PHONY: all build test lint clean proto generate docker-build docker-up docker-down
.PHONY: test-unit test-integration test-e2e test-connector test-connectors bench
.PHONY: build-api build-worker build-mcp-gateway build-cli

# ─── Variables ───────────────────────────────────────────────
GO         := go
GOFLAGS    := -trimpath
LDFLAGS    := -s -w -X main.version=$(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
BINDIR     := bin
MODULE     := github.com/flowforge/flowforge
GOTEST     := $(GO) test -race -count=1
GOLINT     := golangci-lint run

# All Go source directories
PKGS := $(shell $(GO) list ./... 2>/dev/null | grep -v /vendor/)

# ─── Build ───────────────────────────────────────────────────
all: lint test build

build: build-api build-worker build-mcp-gateway build-cli

build-api:
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BINDIR)/flowforge-api ./cmd/flowforge-api

build-worker:
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BINDIR)/flowforge-worker ./cmd/flowforge-worker

build-mcp-gateway:
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BINDIR)/flowforge-mcp-gateway ./cmd/flowforge-mcp-gateway

build-cli:
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BINDIR)/flowforge-cli ./cmd/flowforge-cli

# ─── Test ────────────────────────────────────────────────────
test: test-unit

test-unit:
	$(GOTEST) ./...

test-integration:
	$(GOTEST) -tags=integration ./tests/integration/...

test-e2e:
	$(GOTEST) -tags=e2e ./tests/e2e/...

test-connector:
	@if [ -z "$(CONNECTOR)" ]; then echo "Usage: make test-connector CONNECTOR=salesforce"; exit 1; fi
	$(GOTEST) ./connectors/$(CONNECTOR)/...

test-connectors:
	$(GOTEST) ./connectors/...

bench:
	$(GO) test -bench=. -benchmem ./...

# ─── Lint ────────────────────────────────────────────────────
lint:
	@if command -v golangci-lint >/dev/null 2>&1; then $(GOLINT) ./...; else echo "golangci-lint not installed, skipping"; fi

# ─── Code Generation ────────────────────────────────────────
generate:
	$(GO) generate ./...

proto: proto-all

proto-all: proto-go proto-ts proto-openapi proto-zod

proto-go:
	@echo "Generating Go protobuf + Connect-Go code..."
	buf generate

proto-ts:
	@echo "Generating TypeScript types + Connect client stubs..."
	buf generate

proto-openapi:
	@echo "Generating OpenAPI spec..."
	buf generate

proto-zod:
	@echo "Generating Zod schemas from OpenAPI..."
	./scripts/generate-zod.sh

# ─── Docker ──────────────────────────────────────────────────
docker-build:
	docker build -t flowforge-api:latest --target api .
	docker build -t flowforge-worker:latest --target worker .
	docker build -t flowforge-mcp-gateway:latest --target mcp-gateway .

docker-up:
	docker compose -f deploy/docker-compose/docker-compose.yml up -d

docker-down:
	docker compose -f deploy/docker-compose/docker-compose.yml down

# ─── Clean ───────────────────────────────────────────────────
clean:
	rm -rf $(BINDIR)
	$(GO) clean -cache -testcache

# ─── Dev Setup ───────────────────────────────────────────────
dev-setup:
	$(GO) install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	$(GO) install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	$(GO) install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-grpc-gateway@latest
	$(GO) install github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2@latest
	$(GO) install connectrpc.com/connect/cmd/protoc-gen-connect-go@latest
	@echo "Installing TypeScript proto codegen plugins..."
	cd packages/proto-ts && npm install
