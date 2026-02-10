# FlowForge Implementation Plan

> **The Open-Source, High-Performance Integration Platform**
> ETL · Reverse ETL · MCP Servers · Embedded iPaaS — Powered by Temporal.io

---

## Table of Contents

1. [Overview](#1-overview)
2. [Repository Structure](#2-repository-structure)
3. [Phase 1: Alpha (Months 1–3)](#3-phase-1-alpha-months-13)
4. [Phase 2: Beta (Months 4–6)](#4-phase-2-beta-months-46)
5. [Phase 3: GA v1.0 (Months 7–9)](#5-phase-3-ga-v10-months-79)
6. [Phase 4: v1.1 (Months 10–12)](#6-phase-4-v11-months-1012)
7. [Phase 5: v2.0 (Months 13–18)](#7-phase-5-v20-months-1318)
8. [Technology Decisions](#8-technology-decisions)
9. [Cross-Cutting Concerns](#9-cross-cutting-concerns)
10. [Risk Register](#10-risk-register)
11. [Success Metrics](#11-success-metrics)
12. [Document Index](#12-document-index)

---

## 1. Overview

FlowForge unifies five fragmented integration categories — ETL, Reverse ETL, MCP server hosting, embedded iPaaS, and app-to-app workflows — into a single open-source platform built on **Temporal.io durable execution**.

### Core Principles

| Principle | What It Means |
|---|---|
| **Extreme Performance** | Go-based workers, Apache Arrow columnar buffers, 50K+ records/sec per node |
| **Radical Simplicity** | 3-tier CDK (No-Code → Low-Code → Full-Code), build a REST connector in <30 min |
| **MCP-Native** | Every connector auto-exposes MCP tools; AI agents are first-class consumers |

### What Ships in v1.0

- **10 first-class connectors**: Salesforce, HubSpot, BigQuery, Snowflake, S3, Gong, Chorus, Zoom, Zendesk, Intercom
- **Full ETL + Reverse ETL** with bidirectional sync support
- **MCP Gateway** for AI agent connectivity
- **Embedded iPaaS APIs** for SaaS vendor white-labeling
- **Kubernetes-native deployment** with auto-scaling
- **Full observability stack** (OpenTelemetry, Prometheus, Grafana)

---

## 2. Repository Structure

```
flowforge/
├── cmd/                          # Binary entry points
│   ├── flowforge-api/            # API server
│   ├── flowforge-worker/         # Worker process (all worker types)
│   ├── flowforge-mcp-gateway/    # MCP gateway server
│   └── flowforge-cli/            # CLI tool
├── internal/                     # Private Go packages (not importable externally)
│   ├── api/                      # API layer
│   │   ├── rest/                 # gRPC-Gateway REST handlers
│   │   ├── graphql/              # GraphQL resolvers
│   │   └── middleware/           # Auth, rate-limiting, logging middleware
│   ├── controlplane/             # Control plane services
│   │   ├── scheduler/            # Sync scheduling (cron, webhook, event, API-trigger)
│   │   ├── config/               # Configuration management
│   │   ├── tenant/               # Multi-tenant namespace management
│   │   └── schema/               # Schema detection, drift handling, versioning
│   ├── orchestration/            # Temporal workflow definitions
│   │   ├── workflows/            # Workflow implementations
│   │   │   ├── sync_orchestrator.go
│   │   │   ├── extract.go
│   │   │   ├── transform.go
│   │   │   ├── load.go
│   │   │   └── mcp_request.go
│   │   ├── activities/           # Temporal activity implementations
│   │   └── signals/              # Signal and query definitions
│   ├── worker/                   # Worker pool management
│   │   ├── extract/              # Extract worker logic
│   │   ├── transform/            # Transform worker logic
│   │   ├── load/                 # Load worker logic
│   │   ├── mcp/                  # MCP worker logic
│   │   └── pool/                 # Connection pooling, backpressure
│   ├── connector/                # Connector runtime
│   │   ├── protocol/             # Internal data protocol (RECORD, STATE, SCHEMA, etc.)
│   │   ├── runtime/              # Container/WASM connector execution
│   │   └── registry/             # Connector registry and discovery
│   ├── mapping/                  # Data mapping engine
│   │   ├── auto/                 # Automatic mapping (Levenshtein, type, semantic)
│   │   ├── expression/           # JSONPath expression language
│   │   ├── templates/            # Pre-built mapping templates
│   │   └── visual/               # Visual mapper backend
│   ├── mcp/                      # MCP framework
│   │   ├── gateway/              # MCP gateway (SSE, WebSocket)
│   │   ├── tools/                # Auto-generated tool registry
│   │   ├── security/             # Per-tool auth, data filtering, audit
│   │   └── builder/              # Custom MCP server builder
│   ├── embedded/                 # Embedded iPaaS
│   │   ├── lifecycle/            # Integration lifecycle API
│   │   ├── oauth/                # OAuth flow management for end-customers
│   │   └── whitelabel/           # White-label configuration
│   ├── security/                 # Security layer
│   │   ├── auth/                 # OIDC/SAML SSO, API keys, JWT
│   │   ├── rbac/                 # Role-based access control
│   │   ├── secrets/              # Vault/AWS SM/GCP SM integration
│   │   ├── encryption/           # At-rest encryption (AES-256)
│   │   └── audit/                # Immutable audit logging
│   ├── observability/            # Observability
│   │   ├── metrics/              # Prometheus metrics
│   │   ├── tracing/              # OpenTelemetry tracing
│   │   └── logging/              # Structured JSON logging
│   └── storage/                  # Data layer
│       ├── postgres/             # PostgreSQL repositories
│       ├── redis/                # Redis cache layer
│       └── blob/                 # S3-compatible blob storage
├── pkg/                          # Public Go packages (importable by connectors)
│   ├── cdk/                      # Connector Development Kit
│   │   ├── source.go             # Source connector interface
│   │   ├── destination.go        # Destination connector interface
│   │   ├── bidirectional.go      # Bidirectional connector interface
│   │   ├── testing/              # Connector acceptance test framework
│   │   └── examples/             # Example connectors
│   ├── protocol/                 # Public protocol types (RECORD, STATE, etc.)
│   └── arrow/                    # Arrow buffer utilities
├── connectors/                   # First-class connector implementations
│   ├── salesforce/
│   ├── hubspot/
│   ├── bigquery/
│   ├── snowflake/
│   ├── s3/
│   ├── gong/
│   ├── chorus/
│   ├── zoom/
│   ├── zendesk/
│   └── intercom/
├── python/                       # Python CDK and connectors
│   ├── flowforge_cdk/            # Python CDK package
│   ├── connectors/               # Python connector implementations
│   └── tests/
├── ui/                           # React UI component library (FlowForge-UI)
│   ├── src/
│   │   ├── components/           # Embeddable React components
│   │   │   ├── ConnectorSelector/
│   │   │   ├── FieldMapper/
│   │   │   ├── SyncMonitor/
│   │   │   └── SchemaViewer/
│   │   └── lib/                  # Shared utilities
│   ├── package.json
│   └── tsconfig.json
├── proto/                        # Protobuf definitions
│   ├── flowforge/
│   │   ├── api/v1/               # Public API protos
│   │   ├── internal/             # Internal RPC protos
│   │   └── protocol/             # Data protocol protos
│   └── buf.yaml
├── deploy/                       # Deployment configurations
│   ├── docker-compose/           # Docker Compose (dev/eval)
│   ├── helm/                     # Helm chart
│   │   └── flowforge/
│   ├── operator/                 # Kubernetes Operator
│   └── terraform/                # Cloud infrastructure modules
├── migrations/                   # Database migrations
│   └── postgres/
├── docs/                         # Documentation
│   ├── architecture/             # Architecture docs
│   ├── guides/                   # Developer and operator guides
│   ├── api/                      # API reference (auto-generated from proto)
│   └── connectors/               # Per-connector documentation
├── scripts/                      # Build and dev scripts
│   ├── dev-setup.sh
│   ├── generate-proto.sh
│   └── run-acceptance-tests.sh
├── .github/                      # GitHub Actions CI/CD
│   └── workflows/
├── dagger/                       # Dagger CI pipeline definitions
├── go.mod
├── go.sum
├── Makefile
├── Dockerfile
├── CLAUDE.md                     # Claude Code project instructions
└── README.md
```

---

## 3. Phase 1: Alpha (Months 1–3)

**Goal**: Core platform running end-to-end with Salesforce + BigQuery connectors.

### Sprint 1 (Weeks 1–2): Foundation

| # | Task | Details | Output |
|---|---|---|---|
| 1.1 | **Project scaffolding** | Initialize Go module, directory structure, Makefile, CI pipeline, linting (golangci-lint), pre-commit hooks | Buildable repo with `make build`, `make test`, `make lint` |
| 1.2 | **Protobuf definitions** | Define core protos: data protocol messages (RECORD, STATE, SCHEMA, LOG, CONTROL), API service definitions, connector interfaces | `proto/` with `buf generate` working |
| 1.3 | **Database schema v1** | PostgreSQL schema for: connectors, connections, syncs, sync_runs, streams, state_checkpoints, tenants | `migrations/postgres/001_initial.sql` |
| 1.4 | **Docker Compose dev environment** | Temporal server, PostgreSQL, Redis, MinIO (S3-compat), Vault (dev mode) | `docker compose up` gets full dev stack running |

### Sprint 2 (Weeks 3–4): Temporal Integration

| # | Task | Details | Output |
|---|---|---|---|
| 2.1 | **Temporal client setup** | Temporal SDK integration, namespace creation, search attribute registration | `internal/orchestration/client.go` |
| 2.2 | **SyncOrchestrator workflow** | Parent workflow: spawns Extract → Transform → Load child workflows, handles signals (pause/resume/cancel), continue-as-new for schedules | `internal/orchestration/workflows/sync_orchestrator.go` |
| 2.3 | **Extract workflow** | Child workflow for extraction: pagination, cursor-based incremental, checkpointing via STATE messages, fan-out for parallel partitions | `internal/orchestration/workflows/extract.go` |
| 2.4 | **Transform workflow** | Child workflow: applies field mappings, type coercion, dedup. Arrow-based columnar buffers for batch processing | `internal/orchestration/workflows/transform.go` |
| 2.5 | **Load workflow** | Child workflow: batched writes with retry, conflict resolution (append/upsert/merge), destination-specific optimizations | `internal/orchestration/workflows/load.go` |
| 2.6 | **Task queue strategy** | Implement dedicated task queues: `sync-extract-{type}`, `sync-transform`, `sync-load-{type}`, `system-maintenance` | Queue routing in worker registration |
| 2.7 | **Temporal test harness** | Deterministic workflow testing with Temporal's test framework, replay tests, signal tests | `internal/orchestration/workflows/*_test.go` |

### Sprint 3 (Weeks 5–6): CDK and Protocol

| # | Task | Details | Output |
|---|---|---|---|
| 3.1 | **Data protocol implementation** | Go types for all protocol messages: SPEC, CHECK, DISCOVER, READ, WRITE, RECORD, STATE, SCHEMA, LOG, CONTROL | `pkg/protocol/` |
| 3.2 | **CDK interfaces (Go)** | Source, Destination, and Bidirectional connector interfaces with all required methods (Spec, Check, Discover, Read, Write, Resolve, WebhookHandler) | `pkg/cdk/` |
| 3.3 | **Connector runtime** | Native plugin connector execution (compiled into worker binary), protocol message serialization/deserialization, lifecycle management | `internal/connector/runtime/` |
| 3.4 | **Connector test framework** | Acceptance test suite: spec validation, connection check, discovery, read/write compliance, idempotency, performance baseline | `pkg/cdk/testing/` |
| 3.5 | **Arrow buffer layer** | Apache Arrow columnar buffer for inter-phase data transfer. Batch accumulation, memory management, zero-copy where possible | `pkg/arrow/` |

### Sprint 4 (Weeks 7–8): Salesforce Connector

| # | Task | Details | Output |
|---|---|---|---|
| 4.1 | **Salesforce auth** | OAuth 2.0 (JWT Bearer + Web Server flow), API Token auth, token refresh | `connectors/salesforce/auth.go` |
| 4.2 | **Salesforce source** | Spec, Check, Discover (via Describe API), Read (REST API for small, Bulk API 2.0 for large). Full refresh + incremental (SystemModstamp). All standard + custom objects | `connectors/salesforce/source.go` |
| 4.3 | **Salesforce destination** | Write (Insert, Update, Upsert by External ID, Delete, Soft Delete). Bulk API 2.0 for batch, REST for real-time <200 records | `connectors/salesforce/destination.go` |
| 4.4 | **Salesforce advanced** | Compound field support, relationship traversal, formula field reads, rate limit handling (429/503 retry, daily limit tracking) | Advanced features integrated |
| 4.5 | **Salesforce tests** | Full acceptance test suite, integration tests against Salesforce sandbox, performance benchmark (target: 100K read/min, 50K write/min) | `connectors/salesforce/*_test.go` |

### Sprint 5 (Weeks 9–10): BigQuery Connector

| # | Task | Details | Output |
|---|---|---|---|
| 5.1 | **BigQuery auth** | Service Account JSON, Application Default Credentials, Workload Identity | `connectors/bigquery/auth.go` |
| 5.2 | **BigQuery source** | Full table scan, incremental (partition-based or column cursor), query-based extraction. Nested/repeated field handling (STRUCT, ARRAY) | `connectors/bigquery/source.go` |
| 5.3 | **BigQuery destination** | Append, Truncate-and-Load, Merge (MERGE DML), Streaming Insert. Stage to GCS for bulk (500K+ records/min) | `connectors/bigquery/destination.go` |
| 5.4 | **BigQuery optimization** | Partition pruning, column projection pushdown, result caching, slot reservation awareness | Optimizations integrated |
| 5.5 | **BigQuery tests** | Acceptance suite, integration tests, performance benchmarks | `connectors/bigquery/*_test.go` |

### Sprint 6 (Weeks 11–12): API and Integration

| # | Task | Details | Output |
|---|---|---|---|
| 6.1 | **gRPC service definitions** | API protos for: connectors CRUD, connections, syncs CRUD, sync runs, streams, health | `proto/flowforge/api/v1/` |
| 6.2 | **gRPC-Gateway REST layer** | Auto-generated REST endpoints from proto annotations, OpenAPI spec generation | `internal/api/rest/` |
| 6.3 | **Core API endpoints** | POST/GET/PUT/DELETE for connectors, connections, syncs. GET sync runs with filtering. Health and readiness probes | REST API functional |
| 6.4 | **Worker process** | Single binary that registers all worker types (extract, transform, load) with appropriate task queues. Graceful shutdown, health reporting | `cmd/flowforge-worker/` |
| 6.5 | **API server process** | HTTP server with gRPC-Gateway, middleware (logging, request ID, recovery), config loading | `cmd/flowforge-api/` |
| 6.6 | **End-to-end integration test** | Full pipeline: create connection → discover → configure sync → run sync → verify data in destination. Salesforce→BigQuery and BigQuery→Salesforce | `tests/e2e/` |
| 6.7 | **Docker Compose deployment** | Production-like Docker Compose with all services, health checks, volume mounts, `.env` template | `deploy/docker-compose/` |

### Alpha Exit Criteria

- [ ] Salesforce → BigQuery full refresh sync works end-to-end
- [ ] BigQuery → Salesforce reverse ETL works end-to-end
- [ ] Incremental sync with checkpointing works for both connectors
- [ ] Sync survives worker crash and resumes via Temporal replay
- [ ] API can create connections, configure syncs, and monitor runs
- [ ] Docker Compose deployment works on a single machine
- [ ] All connector acceptance tests pass
- [ ] CI pipeline builds, tests, and lints on every PR

---

## 4. Phase 2: Beta (Months 4–6)

**Goal**: All Tier 1 connectors, Python CDK, MCP gateway (beta), Kubernetes deployment, visual mapping UI.

### Sprint 7 (Weeks 13–14): Snowflake + S3 Connectors

| # | Task | Details | Output |
|---|---|---|---|
| 7.1 | **Snowflake connector** | Bidirectional. Auth (username/password, key pair, OAuth). Read: full/incremental/change tracking. Write: COPY INTO from staged files (1M+ records/min), merge, stream-based CDC. Variant/Object/Array handling, Time Travel | `connectors/snowflake/` |
| 7.2 | **S3 connector** | Bidirectional. Auth (IAM Role, Access Key, STS). Read: full/prefix/event-triggered/incremental. Write: append/overwrite/partitioned. Formats: CSV, JSON, JSONL, Parquet, Avro, ORC. Multipart upload, S3 Select, compression | `connectors/s3/` |
| 7.3 | **Zero-copy warehouse transfers** | Staged file transfer path: BigQuery → GCS → Snowflake (bypasses worker data plane). S3 as intermediary for cross-cloud | `internal/worker/transfer/` |

### Sprint 8 (Weeks 15–16): HubSpot Connector + Python CDK

| # | Task | Details | Output |
|---|---|---|---|
| 8.1 | **HubSpot connector** | Bidirectional. OAuth 2.0 + Private App Token. Objects: Contacts, Companies, Deals, Tickets, Products, Line Items, Custom Objects, Engagements, Associations. Batch API, webhook CDC. Target: 80K read/min, 40K write/min | `connectors/hubspot/` |
| 8.2 | **Python CDK** | Python package with abstract base classes mirroring Go CDK interfaces. Temporal Python SDK integration. gRPC bridge for Go↔Python interop. Type-safe protocol message classes | `python/flowforge_cdk/` |
| 8.3 | **Python CDK testing** | Python connector acceptance test framework. Pytest-based, mirrors Go test suite | `python/flowforge_cdk/testing/` |
| 8.4 | **Transform worker Python support** | Embedded Python subprocess pool via gRPC for complex transforms. Arrow-based interop between Go and Python | `internal/worker/transform/python/` |

### Sprint 9 (Weeks 17–18): MCP Gateway (Beta)

| # | Task | Details | Output |
|---|---|---|---|
| 9.1 | **MCP Gateway server** | Multi-tenant gateway accepting MCP client connections (SSE + WebSocket). Authentication, routing, rate limiting | `internal/mcp/gateway/` |
| 9.2 | **MCP Tool Registry** | Auto-generates MCP tool definitions from connector capabilities. JSON Schema input/output for each tool. Per-connector tool catalog | `internal/mcp/tools/` |
| 9.3 | **MCP Worker** | Dedicated Temporal workers executing MCP tool calls as activities. Full durability and retry | `internal/worker/mcp/` |
| 9.4 | **MCP task queues** | Per-MCP-server queues (`mcp-request-{server_id}`) for fair scheduling across tenants | Queue routing |
| 9.5 | **MCP security (basic)** | Per-tool authorization (enable/disable per API key), input validation (JSON Schema), secrets isolation | `internal/mcp/security/` |
| 9.6 | **MCP Gateway binary** | Standalone MCP gateway process | `cmd/flowforge-mcp-gateway/` |
| 9.7 | **Auto-generated tools for Tier 1** | MCP tools for Salesforce (salesforce_query, salesforce_get_record, etc.), HubSpot, BigQuery, Snowflake, S3 per PRD table | Tool definitions for all 5 Tier 1 connectors |

### Sprint 10 (Weeks 19–20): Kubernetes Deployment

| # | Task | Details | Output |
|---|---|---|---|
| 10.1 | **Helm chart** | Full Helm chart: API deployment, Temporal StatefulSet (via subchart), worker deployments (per type), PostgreSQL, Redis, Vault, Ingress with TLS | `deploy/helm/flowforge/` |
| 10.2 | **HPA configuration** | Horizontal Pod Autoscaler: API by request rate, workers by Temporal task queue depth (custom metrics adapter) | HPA manifests in Helm chart |
| 10.3 | **Custom metrics adapter** | Kubernetes metrics adapter exposing Temporal task queue depth as a custom metric | `deploy/helm/flowforge/templates/metrics-adapter.yaml` |
| 10.4 | **Predictive scaling** | Pre-scale workers 2 minutes before known scheduled sync high-load periods | `internal/controlplane/scheduler/prescale.go` |
| 10.5 | **Scale-to-zero** | Worker deployments for infrequently-used connectors scale to zero and spin up on demand (<5s cold start for Go) | KEDA ScaledObject or custom controller |
| 10.6 | **Resource quotas** | Per-tenant resource quotas in multi-tenant deployments | `internal/controlplane/tenant/quotas.go` |

### Sprint 11 (Weeks 21–22): Data Mapping Engine + Visual UI

| # | Task | Details | Output |
|---|---|---|---|
| 11.1 | **Automatic mapping** | Schema matching: Levenshtein distance, type compatibility, semantic analysis. Standard field recognition (email, phone, name, address, created_at). AI-assisted mapping (optional LLM) | `internal/mapping/auto/` |
| 11.2 | **Expression language** | JSONPath-based expressions: concatenation, splitting, date formatting, conditional logic, lookup tables. Computed fields from multiple sources | `internal/mapping/expression/` |
| 11.3 | **Mapping templates** | Pre-built templates per PRD: Salesforce↔HubSpot Contact, BigQuery→Salesforce, Zendesk↔Intercom, Gong→BigQuery, S3→Snowflake | `internal/mapping/templates/` |
| 11.4 | **React UI scaffold** | Initialize React + TypeScript project with build tooling (Vite), component library setup, Storybook | `ui/` |
| 11.5 | **ConnectorSelector component** | Embeddable React component for browsing and selecting connectors with search, categories, and icons | `ui/src/components/ConnectorSelector/` |
| 11.6 | **FieldMapper component** | Drag-and-drop visual field mapping UI. Displays source→destination schemas, supports expressions, type coercion indicators | `ui/src/components/FieldMapper/` |
| 11.7 | **SyncMonitor component** | Real-time sync progress display: record counts, throughput, errors, phase indicators (Extract/Transform/Load) | `ui/src/components/SyncMonitor/` |

### Sprint 12 (Weeks 23–24): Scheduling, Webhooks, Polish

| # | Task | Details | Output |
|---|---|---|---|
| 12.1 | **Cron scheduler** | Standard cron expression support with timezone awareness. Minimum interval: 1 minute. Uses Temporal schedules | `internal/controlplane/scheduler/cron.go` |
| 12.2 | **Webhook triggers** | Inbound webhooks triggering immediate syncs. Signature verification for Salesforce, HubSpot, Zendesk, Stripe | `internal/controlplane/scheduler/webhook.go` |
| 12.3 | **Event-driven triggers** | Subscribe to platform events (Salesforce Platform Events, HubSpot Webhooks) for near-real-time CDC | `internal/controlplane/scheduler/events.go` |
| 12.4 | **API-triggered syncs** | Programmatic sync initiation via REST API. Dependency chains (Sync B after Sync A) | `internal/controlplane/scheduler/api_trigger.go` |
| 12.5 | **Structured logging** | JSON logging with correlation IDs across all components. Configurable log levels per connector | `internal/observability/logging/` |
| 12.6 | **Beta integration tests** | Full test coverage across all Tier 1 connectors, MCP gateway, scheduling, Kubernetes deployment | `tests/` |

### Beta Exit Criteria

- [ ] All 5 Tier 1 connectors (Salesforce, HubSpot, BigQuery, Snowflake, S3) pass acceptance tests
- [ ] Python CDK can build and run connectors
- [ ] MCP Gateway accepts connections and auto-generated tools work for all Tier 1 connectors
- [ ] Kubernetes Helm chart deploys successfully with auto-scaling
- [ ] Visual mapping UI components render and function correctly
- [ ] Webhook and cron scheduling triggers syncs reliably
- [ ] Bidirectional sync works for all Tier 1 connectors

---

## 5. Phase 3: GA v1.0 (Months 7–9)

**Goal**: Production-ready platform with all connectors, enterprise security, full observability, embedded APIs.

### Sprint 13 (Weeks 25–26): Tier 2 Connectors (Call Recording)

| # | Task | Details | Output |
|---|---|---|---|
| 13.1 | **Gong connector** | Source (read-only). OAuth 2.0 / API Key. Streams: Calls, Users, Deals, Emails, Transcripts, Participants, Trackers, Scorecards, Stats. Transcript extraction with speaker diarization. Target: 10K calls/hour | `connectors/gong/` |
| 13.2 | **Chorus connector** | Source (read-only). OAuth 2.0 via ZoomInfo. Streams: Meetings, Recordings, Transcripts, Participants, Trackers, Deal Intelligence, Coaching Moments. Full transcript with timestamps | `connectors/chorus/` |
| 13.3 | **Zoom connector** | Source (read-only + webhook receiver). OAuth 2.0 (S2S), JWT legacy. Streams: Meetings, Recordings, Participants, Cloud Recording Files, Transcripts, Webinars, Registration. Recording file download | `connectors/zoom/` |
| 13.4 | **MCP tools for Tier 2** | Auto-generated: gong_search_calls, gong_get_transcript, gong_list_deals, etc. | MCP tool definitions |

### Sprint 14 (Weeks 27–28): Tier 3 Connectors (Support)

| # | Task | Details | Output |
|---|---|---|---|
| 14.1 | **Zendesk connector** | Bidirectional. OAuth 2.0 / API Token. Read: Tickets, Users, Orgs, Comments, Satisfaction Ratings (incremental cursor via Export API). Write: Tickets (create/update), Users, Orgs, Tags. Target: 100K tickets/hour read, 10K updates/hour write | `connectors/zendesk/` |
| 14.2 | **Intercom connector** | Bidirectional. OAuth 2.0 / Access Token. Read: Contacts, Companies, Conversations, Tags, Segments (Scroll API for large). Write: Contacts, Companies, Tags, Conversations (reply/assign/close), Events | `connectors/intercom/` |
| 14.3 | **MCP tools for Tier 3** | Auto-generated: zendesk_search_tickets, zendesk_get_ticket, zendesk_create_ticket, intercom_search_contacts, intercom_get_conversation, intercom_send_message, etc. | MCP tool definitions |

### Sprint 15 (Weeks 29–30): Security and RBAC

| # | Task | Details | Output |
|---|---|---|---|
| 15.1 | **OIDC/SAML SSO** | OpenID Connect and SAML 2.0 authentication. Integration with Okta, Auth0, Azure AD | `internal/security/auth/sso.go` |
| 15.2 | **API key management** | API key generation, rotation, scoping, revocation. JWT-based service accounts | `internal/security/auth/apikey.go` |
| 15.3 | **RBAC** | Role-based access control: Admin, Editor, Viewer predefined roles + custom roles. Resource-level permissions | `internal/security/rbac/` |
| 15.4 | **Secrets management** | Integration with HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager. Dynamic secret generation. No plaintext credential storage | `internal/security/secrets/` |
| 15.5 | **Encryption** | TLS 1.3 for all network traffic. AES-256 encryption at rest for credentials and sync state | `internal/security/encryption/` |
| 15.6 | **Audit logging** | Immutable audit trail: config changes, sync executions, admin actions. Structured, queryable, exportable | `internal/security/audit/` |
| 15.7 | **Data residency** | Configurable routing ensuring records stay within specified geographic regions during processing | `internal/security/residency/` |

### Sprint 16 (Weeks 31–32): Production MCP Framework

| # | Task | Details | Output |
|---|---|---|---|
| 16.1 | **MCP data filtering** | Row-level and column-level security policies on MCP tool results. AI agents only see authorized data | `internal/mcp/security/filtering.go` |
| 16.2 | **MCP audit logging** | Every MCP tool invocation logged: agent identity, parameters, response summary | `internal/mcp/security/audit.go` |
| 16.3 | **MCP rate limiting** | Per-agent, per-tool rate limits preventing runaway AI agents from exhausting API quotas | `internal/mcp/security/ratelimit.go` |
| 16.4 | **Custom MCP server builder** | Composite tools (join data across connectors), semantic tools (NL descriptions for LLM selection), RAG-enabled tools, workflows-as-tools | `internal/mcp/builder/` |
| 16.5 | **MCP load testing** | Performance testing: target <2s P95 tool call latency. Concurrent agent simulation | `tests/mcp/load/` |

### Sprint 17 (Weeks 33–34): Embedded iPaaS APIs

| # | Task | Details | Output |
|---|---|---|---|
| 17.1 | **Integration lifecycle API** | REST endpoints per PRD: POST /v1/integrations, POST /connect, GET /catalog, PUT /mappings, POST /syncs, GET /syncs/{id}, POST /pause, POST /webhooks, POST /mcp | `internal/embedded/lifecycle/` |
| 17.2 | **Multi-tenant isolation** | Per-customer namespaces: separate credentials, sync schedules, data boundaries. Configurable resource limits per tenant | `internal/embedded/tenant/` |
| 17.3 | **White-label configuration** | Configurable error messages, webhook URLs, OAuth redirect URIs per embedding vendor | `internal/embedded/whitelabel/` |
| 17.4 | **GraphQL API** | GraphQL schema mirroring REST API for flexible frontend querying. Subscriptions for real-time sync updates | `internal/api/graphql/` |
| 17.5 | **Embedded OAuth flows** | Managed OAuth for end-customers: authorization URL generation, callback handling, token storage, refresh management | `internal/embedded/oauth/` |

### Sprint 18 (Weeks 35–36): Observability + Kubernetes Operator

| # | Task | Details | Output |
|---|---|---|---|
| 18.1 | **Prometheus metrics** | Metrics: sync throughput, latency, error rates, queue depth, worker utilization. `/metrics` endpoint on all services | `internal/observability/metrics/` |
| 18.2 | **OpenTelemetry tracing** | Distributed tracing from API request → Temporal workflow → connector activity. Correlation IDs | `internal/observability/tracing/` |
| 18.3 | **Grafana dashboards** | Pre-built dashboards: system overview, per-connector performance, sync history, MCP metrics, worker pool status | `deploy/helm/flowforge/dashboards/` |
| 18.4 | **Alerting rules** | Prometheus alerting: sync failures, SLA breaches, schema drift, resource exhaustion. PagerDuty, Slack, email, webhook integrations | `deploy/helm/flowforge/alerts/` |
| 18.5 | **Schema management** | Auto-detection, drift handling (new columns: auto-add/ignore/alert; removed: soft-delete/hard-delete/alert; type changes: coerce/fail/alert), schema versioning with diffs and rollback, Catalog API | `internal/controlplane/schema/` |
| 18.6 | **Kubernetes Operator** | CRDs: FlowForgeCluster, FlowForgeSync, FlowForgeConnector. Reconciliation loops for lifecycle management, auto-scaling policies, rolling upgrades | `deploy/operator/` |

### GA v1.0 Exit Criteria

- [ ] All 10 connectors pass acceptance tests and performance benchmarks
- [ ] RBAC, SSO, audit logging, and encryption fully functional
- [ ] MCP Gateway handles concurrent AI agent connections with <2s P95 latency
- [ ] Embedded iPaaS APIs support full integration lifecycle
- [ ] Kubernetes Operator manages cluster lifecycle
- [ ] Full observability stack (metrics, tracing, logging, dashboards, alerts)
- [ ] Schema detection and drift handling works across all connectors
- [ ] SOC 2 controls documented

---

## 6. Phase 4: v1.1 (Months 10–12)

**Goal**: No-code connector builder, WASM runtime, AI-assisted mapping, connector marketplace.

### Key Deliverables

| # | Task | Details |
|---|---|---|
| v1.1-1 | **No-code connector builder** | Visual UI generating declarative YAML manifests. Handles: REST endpoint config, auth (API key, OAuth 2.0, Basic), pagination (offset, cursor, page, link-header, keyset), response parsing (JSONPath, flattening, unwinding), rate limiting, incremental sync |
| v1.1-2 | **Low-code CDK** | YAML manifest + custom Python/Go functions. Bridges no-code and full-code. Target: 1–4 hours for complex connectors |
| v1.1-3 | **WASM runtime** | WebAssembly module execution for ultra-lightweight connectors. Sub-millisecond cold start, strong sandboxing. Experimental |
| v1.1-4 | **Schema evolution automation** | Automatic handling of schema changes across sync pipelines. Migration generation, backward compatibility checks |
| v1.1-5 | **AI-assisted mapping** | LLM-powered mapping suggestions understanding field semantics beyond string matching. Integrated into FieldMapper UI component |
| v1.1-6 | **Connector marketplace** | Community registry (npm-style): publish, version, search, install connectors. Quality ratings, download counts, maintainer verification |
| v1.1-7 | **CLI tool** | `flowforge` CLI: init project, scaffold connector, run local tests, deploy, sync management, logs tailing | `cmd/flowforge-cli/` |

---

## 7. Phase 5: v2.0 (Months 13–18)

**Goal**: Real-time streaming, managed cloud, multi-region, advanced CDC.

### Key Deliverables

| # | Task | Details |
|---|---|---|
| v2.0-1 | **Kafka streaming integration** | Real-time sub-second streaming via Kafka. Source and sink connectors for Kafka topics. Kafka Connect compatibility layer |
| v2.0-2 | **Debezium CDC** | Advanced change data capture using Debezium for database sources. Log-based CDC for PostgreSQL, MySQL, MongoDB |
| v2.0-3 | **Managed cloud offering** | SaaS deployment: tenant provisioning, billing integration, usage metering, SLA enforcement |
| v2.0-4 | **Multi-region replication** | Cross-region data routing, geo-aware worker scheduling, data residency enforcement at infrastructure level |
| v2.0-5 | **SCIM provisioning** | Automated user/group provisioning via SCIM 2.0 for enterprise identity providers |

---

## 8. Technology Decisions

### Primary Stack

| Component | Technology | Rationale |
|---|---|---|
| **Primary Language** | Go | Max performance, goroutine concurrency, native Temporal SDK, single binary deploy |
| **Secondary Language** | Python | CDK support, data science ecosystem, Temporal Python SDK for transform workers |
| **Orchestration** | Temporal.io (self-hosted) | Durable execution, workflow versioning, signals/queries, battle-tested (Uber, Netflix, Snap) |
| **Metadata DB** | PostgreSQL | Reliable, JSON support, also Temporal backend |
| **Cache** | Redis | Low-latency config cache, rate limiter, pub/sub for real-time events |
| **Object Storage** | S3-compatible (MinIO for dev) | Staged transfers, connector artifacts, audit archival |
| **Secrets** | HashiCorp Vault | Dynamic secrets, encryption-as-a-service |
| **API Framework** | gRPC + gRPC-Gateway | High-perf internal RPC; auto-generated REST + OpenAPI |
| **UI** | React + TypeScript | Embeddable component library, modern ecosystem |
| **Observability** | OpenTelemetry + Prometheus + Grafana | Vendor-neutral telemetry, industry standard |
| **CI/CD** | GitHub Actions + Dagger | Reproducible containerized CI pipelines |
| **Deployment** | Helm + K8s Operator | Production-grade with auto-scaling and lifecycle management |
| **Testing** | Go testing + Temporal Test Framework | Deterministic replay testing |
| **Internal Data Format** | Apache Arrow (columnar) | Efficient memory, vectorized transforms, zero-copy IPC |

### Key Architecture Decisions

1. **Go as primary language**: Performance-critical data movement benefits from Go's goroutine model and low GC overhead. Temporal's Go SDK is the most mature. Single binary deployment simplifies operations.

2. **Temporal over custom orchestration**: Building durable execution from scratch would take months. Temporal provides: automatic retries, saga compensation, long-running workflow support, visibility, and versioning out of the box.

3. **gRPC internally, REST externally**: gRPC for high-throughput internal communication between services. gRPC-Gateway auto-generates REST endpoints from proto definitions — one source of truth.

4. **Apache Arrow for internal buffers**: Columnar format dramatically reduces memory for typical ETL workloads (many rows, few columns accessed per transform). Enables vectorized operations and zero-copy transfer between phases.

5. **Native connectors first, containers later**: First-class connectors compile into the worker binary for zero overhead. Container and WASM isolation for community/third-party connectors.

6. **Multi-tenant from day one**: Tenant isolation is architectural, not bolted on. Separate Temporal namespaces, credential storage, and resource quotas per tenant.

---

## 9. Cross-Cutting Concerns

### Testing Strategy

| Level | Scope | Tools | Run Frequency |
|---|---|---|---|
| **Unit** | Individual functions, protocol serialization, mapping expressions | Go `testing`, Python `pytest` | Every commit |
| **Workflow** | Temporal workflow logic, signal handling, failure scenarios | Temporal Test Framework (deterministic replay) | Every commit |
| **Integration** | Connector ↔ external system, API ↔ database | Testcontainers, sandbox accounts | Per PR |
| **Acceptance** | Full connector compliance (spec, check, discover, read, write, idempotency, perf) | CDK testing framework | Per PR for connector changes |
| **E2E** | Full pipeline: API → Temporal → Worker → Connector → Destination | Docker Compose test environment | Nightly + release |
| **Load** | Throughput benchmarks, concurrent sync simulation, MCP latency | k6, custom Go benchmarks | Weekly + release |

### Performance Targets

| Metric | Target |
|---|---|
| Records/sec per worker node | 50,000+ |
| Small sync latency (1K records) | <30 seconds P95 |
| Go worker cold start | <5 seconds |
| MCP tool call latency | <2 seconds P95 |
| Salesforce read throughput | 100K records/minute |
| BigQuery bulk load | 500K+ records/minute |
| Snowflake COPY INTO | 1M+ records/minute |

### Documentation Plan

All documentation lives in `docs/` and is published as a static site (e.g., Docusaurus or MkDocs).

| Document | Audience | Location |
|---|---|---|
| Architecture Overview | Engineers | `docs/architecture/overview.md` |
| System Architecture Deep-Dive | Engineers | `docs/architecture/system-architecture.md` |
| Directory Structure | Engineers | `docs/architecture/directory-structure.md` |
| Temporal Patterns Guide | Engineers | `docs/architecture/temporal-patterns.md` |
| Connector Development Guide | Connector developers | `docs/guides/connector-development.md` |
| MCP Framework Guide | Platform/AI engineers | `docs/guides/mcp-framework.md` |
| Embedded iPaaS Guide | SaaS vendor engineers | `docs/guides/embedded-ipaas.md` |
| Deployment Guide | DevOps/SRE | `docs/guides/deployment.md` |
| API Reference | All developers | `docs/api/` (auto-generated from proto) |
| Per-Connector Docs | Integration engineers | `docs/connectors/{name}.md` |
| Contributing Guide | OSS contributors | `CONTRIBUTING.md` |

---

## 10. Risk Register

| Risk | Severity | Likelihood | Mitigation |
|---|---|---|---|
| Temporal operational complexity | Medium | Medium | Opinionated Helm charts with sane defaults; K8s Operator for lifecycle; consider Temporal Cloud |
| Connector maintenance burden | High | High | CDK generates quality boilerplate; automated acceptance tests in CI; community bounty program |
| MCP protocol evolution | Medium | Medium | Abstract MCP behind internal adapter layer; follow Anthropic spec; participate in standards |
| Airbyte adds Reverse ETL | Medium | High | Move faster on MCP + embedded; differentiate on Go performance vs Java; build community early |
| Go ecosystem for data transforms | Low | Medium | Python subprocess pool via gRPC; Arrow-based interop; WASM for sandboxed functions |
| Enterprise adoption without cloud | High | Medium | Partner with cloud providers; ship K8s Operator; plan managed offering for v2 |

---

## 11. Success Metrics

| Category | Metric | 12-Month Target |
|---|---|---|
| Performance | Records/sec per worker | 50,000+ |
| Ecosystem | Total connectors | 50+ (15 first-class) |
| Adoption | GitHub stars | 5,000+ |
| Adoption | Monthly active installations | 1,000+ |
| Developer UX | Time to build basic REST connector | <30 minutes |
| MCP | Active MCP servers | 500+ |
| MCP | Tool call latency P95 | <2 seconds |
| Enterprise | SOC 2 compliance | By month 9 |
| Embedded | SaaS vendors using embedded | 20+ |
| Reliability | Sync success rate | >99.5% |
| Reliability | Data accuracy | 100% row-level |
| Reliability | Control plane uptime | 99.9% |

---

## 12. Document Index

Detailed guides accompanying this plan:

| Document | Description |
|---|---|
| [`docs/architecture/system-architecture.md`](docs/architecture/system-architecture.md) | Deep-dive into the 6-layer architecture, data flow paths, and Temporal workflow patterns |
| [`docs/architecture/directory-structure.md`](docs/architecture/directory-structure.md) | Detailed explanation of every directory and file's purpose |
| [`docs/architecture/temporal-patterns.md`](docs/architecture/temporal-patterns.md) | Temporal workflow patterns: parent-child, fan-out/fan-in, continue-as-new, signal-based control |
| [`docs/guides/connector-development.md`](docs/guides/connector-development.md) | Step-by-step guide to building connectors with the CDK (all 3 tiers) |
| [`docs/guides/mcp-framework.md`](docs/guides/mcp-framework.md) | MCP gateway architecture, auto-generated tools, security model, custom server builder |
| [`docs/guides/embedded-ipaas.md`](docs/guides/embedded-ipaas.md) | Embedding FlowForge in SaaS products: API reference, multi-tenant setup, white-labeling |
| [`docs/guides/deployment.md`](docs/guides/deployment.md) | Deployment guide: Docker Compose (dev), Helm (prod), Operator (enterprise), auto-scaling |
