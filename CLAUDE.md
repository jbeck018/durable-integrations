# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FlowForge is an open-source integration platform (ETL, Reverse ETL, MCP servers, embedded iPaaS) built on Temporal.io durable execution. Primary language is Go; secondary is Python. The UI is a React + TypeScript component library.

## Build & Development Commands

### Go (primary)

```bash
make build              # Build all binaries (api, worker, mcp-gateway, cli) → bin/
make build-api          # Build only the API server
make build-worker       # Build only the Temporal worker
make test               # Run unit tests (alias for test-unit)
make test-unit          # go test -race -count=1 ./...
make test-integration   # go test -tags=integration ./tests/integration/...
make test-e2e           # go test -tags=e2e ./tests/e2e/...
make test-connector CONNECTOR=salesforce  # Test a single connector
make test-connectors    # Test all connectors
make bench              # Run benchmarks with -benchmem
make lint               # Run golangci-lint (skips if not installed)
make proto              # Generate protobuf code via buf
make generate           # go generate ./...
make dev-setup          # Install protoc-gen-go, grpc-gateway, etc.
```

Run a single Go test:
```bash
go test -race -count=1 -run TestName ./path/to/package/...
```

### UI (React + TypeScript)

```bash
cd ui && npm run dev        # Vite dev server
cd ui && npm run build      # tsc && vite build
cd ui && npm run typecheck  # tsc --noEmit
```

### Docker

```bash
make docker-build       # Build all three service images (api, worker, mcp-gateway)
make docker-up          # docker compose up with deploy/docker-compose/docker-compose.yml
make docker-down        # docker compose down
```

Infrastructure services in Docker Compose: PostgreSQL 16, Redis 7, Temporal 1.24, MinIO (S3), Vault 1.17, Prometheus, Grafana, Temporal UI.

## Architecture

### 6-Layer Design

1. **API Gateway** (`internal/api/`) — REST (gRPC-Gateway) + GraphQL + WebSocket. HTTP router uses `go-chi/chi`. Auth via JWT/API key/OIDC.
2. **Control Plane** (`internal/controlplane/`) — Scheduler, Config Manager, Tenant Manager, Schema Manager. Configures _what_ and _when_, does not process data.
3. **Orchestration Engine** (`internal/orchestration/`) — Temporal.io workflows and activities. Every sync is a Temporal workflow.
4. **Worker Pool** (`internal/worker/`) — Extract, Transform, Load, MCP workers. Each scales independently.
5. **Connector Runtime** (`internal/connector/`) — Native plugins, OCI containers, WASM (experimental). Protocol handler and registry.
6. **Data Layer** (`internal/storage/`) — PostgreSQL repos, Redis cache/pubsub/ratelimiter, S3-compatible blob storage.

### Four Service Binaries

All in `cmd/`, each `main.go` is a thin bootstrap:

- **flowforge-api** (`:8080` REST, `:9090` gRPC) — HTTP server with graceful shutdown
- **flowforge-worker** (`:9091` metrics) — Temporal worker on `flowforge-sync` task queue
- **flowforge-mcp-gateway** (`:8090`) — MCP protocol server (SSE + WebSocket transports)
- **flowforge-cli** — Developer CLI for scaffolding and management

### Temporal Workflow Patterns

The core sync pipeline uses parent-child workflows (`internal/orchestration/workflows/`):

- **SyncOrchestrator** (parent) → spawns Extract → Transform → Load as child workflows
- Each child has independent retry policies and can fail without re-running earlier phases
- Fan-out/fan-in for parallel extraction of high-volume sources
- Continue-as-new for scheduled recurring syncs (prevents unbounded event history)
- Signal-based control: Pause/Resume/Cancel/Modify signals for runtime control
- Query handler (`sync:status`) for real-time progress monitoring
- Saga compensation for bidirectional sync failure rollback

Task queues: `flowforge-sync` (main), `flowforge-system` (housekeeping). Per-connector queues planned: `sync-extract-{type}`, `sync-load-{type}`.

### Connector Development Kit (CDK)

Public interfaces in `pkg/cdk/`:

- `Source` — Spec, Check, Discover, Read (channel-based output)
- `Destination` — Spec, Check, Write (channel-based input), Capabilities
- `Bidirectional` — extends both + Resolve (conflict resolution) + WebhookHandler

Connectors register via `init()` calling `cdk.RegisterSource`, `cdk.RegisterDestination`, or `cdk.RegisterBidirectional`. Each connector lives in `connectors/{name}/` with a standard structure: connector.go, auth, source/destination impls, spec.json, streams, tests.

### Internal Data Protocol

Message types (Protobuf internally, JSON externally): RECORD, STATE, SCHEMA, LOG, CONTROL, SPEC, CHECK, DISCOVER, READ, WRITE, MCP_TOOL_CALL, MCP_TOOL_RESULT. Defined in `proto/flowforge/protocol/protocol.proto`.

### MCP Framework

`internal/mcp/` — Gateway server with SSE/WebSocket/stdio transports, tool registry that auto-generates MCP tools from connector capabilities, per-tool auth and rate limiting, custom MCP server builder.

## Configuration

All config is via environment variables, loaded in `internal/common/config.go` via `common.LoadConfig()`. Key prefixes:
- `FLOWFORGE_*` — service config (port, log level, env, concurrency, batch size)
- `POSTGRES_*` — database
- `REDIS_*` — cache
- `TEMPORAL_*` — orchestration (host, port, namespace)
- `S3_*` / `MINIO_*` — object storage
- `VAULT_*` — secrets
- `MCP_GATEWAY_*` — MCP server
- `OTEL_EXPORTER_OTLP_ENDPOINT` — tracing

## Key Conventions

- Go module: `github.com/flowforge/flowforge`
- `internal/` packages are private platform code; `pkg/` is the public API for connector developers
- Shared types between workflows and activities live in `internal/orchestration/types/` to avoid import cycles
- Connectors use `connectors/common/` for shared HTTP client, OAuth, and pagination utilities
- Protobuf definitions in `proto/`, generated code goes to `gen/go/` (via `buf generate`)
- Database migrations in `migrations/postgres/` (sequential numbered SQL files)
- Structured JSON logging via `log/slog` (API server) and `internal/observability/logging` (worker)
- Observability: OpenTelemetry tracing, Prometheus metrics, structured logging with correlation IDs

## CI

GitHub Actions (`.github/workflows/ci.yml`): lint, test, build across Go 1.22/1.23 matrix. CI test job runs with Postgres and Redis service containers. Docker images built after lint+test+build pass.
