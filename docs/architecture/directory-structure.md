---
title: Directory Structure
layout: default
parent: Architecture
nav_order: 4
description: "Complete guide to every directory in the FlowForge repository."
permalink: /architecture/directory-structure/
---

# Directory Structure

```
flowforge/
├── cmd/           → Binary entry points (main packages)
├── internal/      → Private Go packages (platform internals)
├── pkg/           → Public Go packages (importable by connectors/plugins)
├── connectors/    → First-class connector implementations
├── packages/      → UI monorepo packages
├── python/        → Python CDK and Python connectors
├── ui/            → React component library (FlowForge-UI)
├── proto/         → Protobuf definitions (API, protocol, internal RPC)
├── deploy/        → Deployment configurations (Docker, Helm, Operator, Terraform)
├── migrations/    → Database migration files
├── docs/          → Documentation (this site)
├── scripts/       → Build and development scripts
└── .github/       → GitHub Actions workflows
```

---

## `cmd/` — Binary Entry Points

Each subdirectory produces one compiled binary:

| Binary | Port | Purpose |
|:--|:--|:--|
| `flowforge-api` | `:8080` REST, `:9090` gRPC | API server with graceful shutdown |
| `flowforge-worker` | `:9091` metrics | Temporal worker on `flowforge-sync` task queue |
| `flowforge-mcp-gateway` | `:8090` | MCP protocol server (SSE + WebSocket) |
| `flowforge-cli` | — | Developer CLI for scaffolding and management |

---

## `internal/` — Private Platform Packages

| Package | Purpose |
|:--|:--|
| `api/rest/` | gRPC-Gateway REST handlers |
| `api/connect/` | Connect-RPC handler for type-safe browser clients |
| `api/graphql/` | GraphQL schema and resolvers |
| `api/middleware/` | Auth, rate limiting, CORS, logging middleware |
| `controlplane/scheduler/` | Cron, webhook, and event-driven sync scheduling |
| `controlplane/config/` | Config storage, validation, tenant overrides |
| `controlplane/tenant/` | Multi-tenant management, Neon provisioning, quotas |
| `controlplane/schema/` | Schema detection, drift, versioning, catalog |
| `orchestration/` | Temporal client, workflows, activities, signals |
| `worker/extract/` | Extract workers: API polling, CDC, file reads |
| `worker/transform/` | Transform workers: mapping, coercion, dedup |
| `worker/load/` | Load workers: bulk writes, streaming inserts |
| `worker/mcp/` | MCP workers: tool execution, context assembly |
| `connector/` | Connector runtime: native plugins, OCI, WASM |
| `mapping/` | Auto-mapping, expression language, templates |
| `mcp/` | MCP gateway, tool registry, security |
| `embedded/` | Embedded iPaaS: lifecycle, OAuth, white-label |
| `security/` | Auth, RBAC, secrets, encryption, audit |
| `storage/postgres/` | PostgreSQL repositories, tenant router |
| `storage/redis/` | Redis cache, rate limiter, pub/sub |
| `storage/blob/` | S3-compatible blob storage |
| `ratelimit/` | Distributed rate limiting (Redis-backed) |
| `observability/` | Metrics, tracing, structured logging |

---

## `pkg/` — Public Go Packages

These are importable by external code — connector developers use these.

| Package | Purpose |
|:--|:--|
| `cdk/` | Connector Development Kit: Source, Destination, Bidirectional interfaces |
| `cdk/testing/` | Acceptance test framework for connectors |
| `protocol/` | Public protocol types: RECORD, STATE, SCHEMA |
| `arrow/` | Arrow buffer utilities for columnar data |

---

## `connectors/` — First-Class Connectors

Each connector follows a standard structure:

```
connectors/{name}/
├── connector.go    # Registration and metadata
├── auth.go         # Authentication (OAuth, API key, JWT)
├── source.go       # Source interface implementation
├── destination.go  # Destination interface implementation
├── spec.json       # Configuration JSON Schema
├── streams.go      # Stream definitions and schemas
└── *_test.go       # Unit and acceptance tests
```

---

## `packages/` — UI Monorepo

| Package | Purpose |
|:--|:--|
| `proto-ts/` | Auto-generated TypeScript types and Connect client from proto |
| `ui-core/` | Framework-agnostic state management and business logic |
| `ui-react/` | React adapter components |
| `ui-angular/` | Angular adapter components |
| `ui-svelte/` | Svelte adapter components |

---

## `deploy/` — Deployment Configurations

```
deploy/
├── docker-compose/   # Development/evaluation
├── helm/flowforge/   # Kubernetes Helm chart
├── operator/         # Kubernetes Operator (CRDs + controllers)
└── terraform/        # Cloud infrastructure (AWS, GCP, Azure)
```
