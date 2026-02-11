# FlowForge Directory Structure

This document explains the purpose and contents of every directory in the FlowForge repository.

---

## Top-Level Layout

```
flowforge/
├── cmd/           → Binary entry points (main packages)
├── internal/      → Private Go packages (platform internals)
├── pkg/           → Public Go packages (importable by connectors/plugins)
├── connectors/    → First-class connector implementations
├── python/        → Python CDK and Python connectors
├── ui/            → React component library (FlowForge-UI)
├── proto/         → Protobuf definitions (API, protocol, internal RPC)
├── deploy/        → Deployment configurations (Docker, Helm, Operator, Terraform)
├── migrations/    → Database migration files
├── docs/          → Documentation
├── scripts/       → Build and development scripts
├── .github/       → GitHub Actions workflows
├── dagger/        → Dagger CI pipeline definitions
└── tests/         → Integration and E2E tests
```

---

## `cmd/` — Binary Entry Points

Each subdirectory produces one compiled binary:

| Binary | Purpose | Key Dependencies |
|---|---|---|
| `flowforge-api` | API server (REST + GraphQL + WebSocket) | gRPC-Gateway, GraphQL resolvers, middleware |
| `flowforge-worker` | Worker process (registers all worker types) | Temporal SDK, connector runtime, Arrow buffers |
| `flowforge-mcp-gateway` | MCP gateway server (SSE + WebSocket) | MCP protocol, tool registry, security |
| `flowforge-cli` | Developer CLI tool | API client, connector scaffolding |

Each `main.go` is minimal — it wires dependencies and starts the service:

```
cmd/
├── flowforge-api/
│   └── main.go              # HTTP server startup, config loading, middleware chain
├── flowforge-worker/
│   └── main.go              # Temporal worker registration, task queue assignment
├── flowforge-mcp-gateway/
│   └── main.go              # MCP gateway startup, transport listeners
└── flowforge-cli/
    └── main.go              # CLI commands (init, scaffold, deploy, sync)
```

---

## `internal/` — Private Platform Packages

These packages are NOT importable by external code (Go enforces this). This is where all platform logic lives.

### `internal/api/` — API Layer

```
internal/api/
├── rest/                    # gRPC-Gateway generated REST handlers
│   ├── server.go            # HTTP server setup, route registration
│   ├── connectors.go        # Connector CRUD handlers
│   ├── connections.go       # Connection management handlers
│   ├── syncs.go             # Sync CRUD and execution handlers
│   └── health.go            # Health and readiness probes
├── graphql/                 # GraphQL schema and resolvers
│   ├── schema.graphql       # GraphQL type definitions
│   ├── resolver.go          # Root resolver
│   ├── sync_resolver.go     # Sync-related queries and mutations
│   └── subscription.go      # Real-time sync status subscriptions
└── middleware/              # HTTP middleware
    ├── auth.go              # JWT/API key validation
    ├── ratelimit.go         # Per-tenant rate limiting
    ├── logging.go           # Request/response logging with correlation IDs
    ├── recovery.go          # Panic recovery
    └── cors.go              # CORS configuration
```

### `internal/controlplane/` — Control Plane Services

```
internal/controlplane/
├── scheduler/               # Sync scheduling
│   ├── cron.go              # Cron expression parsing, Temporal schedule creation
│   ├── webhook.go           # Inbound webhook trigger handling
│   ├── events.go            # Platform event subscription (Salesforce, HubSpot)
│   ├── api_trigger.go       # Programmatic sync initiation
│   ├── dependency.go        # Sync dependency chain management
│   └── prescale.go          # Predictive worker pre-scaling
├── config/                  # Configuration management
│   ├── store.go             # Config storage and retrieval
│   ├── validator.go         # Config validation against JSON Schema
│   └── resolver.go          # Config resolution with tenant overrides
├── tenant/                  # Multi-tenant management
│   ├── manager.go           # Tenant CRUD, namespace provisioning
│   ├── isolation.go         # Credential and data boundary enforcement
│   └── quotas.go            # Per-tenant resource quota management
└── schema/                  # Schema management
    ├── detector.go          # Automatic schema inference
    ├── drift.go             # Schema drift detection and policy enforcement
    ├── versioning.go        # Schema history, diffs, rollback
    └── catalog.go           # Schema catalog API
```

### `internal/orchestration/` — Temporal Workflows

```
internal/orchestration/
├── client.go                # Temporal client initialization, namespace setup
├── workflows/               # Workflow implementations
│   ├── sync_orchestrator.go # Parent sync workflow (Extract → Transform → Load)
│   ├── extract.go           # Extraction child workflow (pagination, fan-out)
│   ├── transform.go         # Transform child workflow (mapping, coercion)
│   ├── load.go              # Load child workflow (batched writes, conflict resolution)
│   ├── mcp_request.go       # MCP tool call workflow
│   ├── scheduled_sync.go    # Scheduled sync with continue-as-new
│   └── bidirectional.go     # Bidirectional sync with saga compensation
├── activities/              # Activity implementations
│   ├── extract_activities.go
│   ├── transform_activities.go
│   ├── load_activities.go
│   ├── mcp_activities.go
│   └── system_activities.go # Housekeeping, metrics, health checks
└── signals/                 # Signal and query definitions
    ├── signals.go           # Pause, Resume, Cancel, Modify signal types
    └── queries.go           # Status, Progress query handlers
```

### `internal/worker/` — Worker Pool

```
internal/worker/
├── extract/                 # Extract worker
│   ├── worker.go            # Worker registration, task queue binding
│   ├── api_poller.go        # REST API extraction with pagination
│   ├── bulk_reader.go       # Bulk API reading (Salesforce Bulk 2.0, etc.)
│   ├── cdc_listener.go      # Change data capture listeners
│   └── file_reader.go       # S3/file-based extraction
├── transform/               # Transform worker
│   ├── worker.go            # Worker registration
│   ├── mapper.go            # Field mapping execution
│   ├── coercer.go           # Type coercion
│   ├── dedup.go             # Deduplication
│   └── python/              # Python subprocess pool for custom transforms
│       ├── pool.go          # gRPC bridge to Python processes
│       └── executor.go      # Python transform execution
├── load/                    # Load worker
│   ├── worker.go            # Worker registration
│   ├── batch_writer.go      # Batched write accumulation
│   ├── bulk_writer.go       # Bulk API writers (Salesforce, BigQuery, etc.)
│   ├── stream_writer.go     # Streaming insert writers
│   └── conflict.go          # Conflict resolution for upserts
├── mcp/                     # MCP worker
│   ├── worker.go            # Worker registration, gRPC streaming
│   ├── executor.go          # MCP tool call execution
│   └── context.go           # Context assembly for tool responses
└── pool/                    # Shared worker utilities
    ├── connection.go        # Connection pooling (HTTP/2, database)
    ├── backpressure.go      # Backpressure signaling via heartbeats
    └── metrics.go           # Worker utilization metrics
```

### `internal/connector/` — Connector Runtime

```
internal/connector/
├── protocol/                # Internal data protocol
│   ├── messages.go          # RECORD, STATE, SCHEMA, LOG, CONTROL types
│   ├── serializer.go        # Protobuf serialization/deserialization
│   └── validator.go         # Message validation
├── runtime/                 # Connector execution runtime
│   ├── native.go            # Native plugin execution (compiled into binary)
│   ├── container.go         # OCI container execution
│   ├── wasm.go              # WASM module execution (experimental)
│   └── lifecycle.go         # Connector lifecycle management (init, health, shutdown)
└── registry/                # Connector discovery
    ├── registry.go          # Connector registry (local + remote)
    ├── resolver.go          # Connector version resolution
    └── catalog.go           # Connector catalog API
```

### `internal/mapping/` — Data Mapping Engine

```
internal/mapping/
├── auto/                    # Automatic mapping
│   ├── schema_match.go      # Levenshtein distance, type compatibility
│   ├── standard_fields.go   # Common field recognition (email, phone, etc.)
│   └── ai_assist.go         # LLM-powered semantic mapping (optional)
├── expression/              # Expression language
│   ├── parser.go            # JSONPath expression parser
│   ├── evaluator.go         # Expression evaluation engine
│   ├── functions.go         # Built-in functions (concat, split, date_format, etc.)
│   └── computed.go          # Computed field definitions
├── templates/               # Pre-built mapping templates
│   ├── salesforce_hubspot.go
│   ├── bigquery_salesforce.go
│   ├── zendesk_intercom.go
│   ├── gong_bigquery.go
│   └── s3_snowflake.go
└── visual/                  # Visual mapper backend
    ├── handler.go           # API handlers for visual mapper UI
    └── suggestion.go        # Mapping suggestion generation
```

### `internal/mcp/` — MCP Framework

```
internal/mcp/
├── gateway/                 # MCP gateway
│   ├── server.go            # Gateway server (SSE + WebSocket transports)
│   ├── router.go            # Tool call routing to appropriate workers
│   ├── session.go           # Client session management
│   └── transport/           # Transport implementations
│       ├── sse.go           # Server-Sent Events transport
│       └── websocket.go     # WebSocket transport
├── tools/                   # Tool registry
│   ├── registry.go          # Tool registration and discovery
│   ├── generator.go         # Auto-generate tools from connector capabilities
│   └── schema.go            # JSON Schema input/output definitions
├── security/                # MCP security
│   ├── auth.go              # Per-tool authorization
│   ├── filtering.go         # Row/column-level data filtering
│   ├── audit.go             # Tool invocation audit logging
│   ├── ratelimit.go         # Per-agent, per-tool rate limits
│   └── validation.go        # Input validation against JSON Schema
└── builder/                 # Custom MCP server builder
    ├── composite.go         # Composite tools (multi-connector)
    ├── semantic.go          # Semantic tool definitions for LLM selection
    ├── rag.go               # RAG-enabled tools
    └── workflow.go          # Expose Temporal workflows as MCP tools
```

### `internal/embedded/` — Embedded iPaaS

```
internal/embedded/
├── lifecycle/               # Integration lifecycle API
│   ├── handler.go           # REST handlers for /v1/integrations/*
│   ├── service.go           # Business logic for integration management
│   └── webhook.go           # Webhook notification delivery
├── oauth/                   # OAuth flow management
│   ├── initiator.go         # Authorization URL generation
│   ├── callback.go          # OAuth callback handling
│   └── token.go             # Token storage and refresh
└── whitelabel/              # White-label configuration
    ├── config.go            # Per-vendor customization
    ├── branding.go          # Error messages, URLs per vendor
    └── redirect.go          # Custom OAuth redirect URIs
```

### `internal/security/` — Security Layer

```
internal/security/
├── auth/                    # Authentication
│   ├── sso.go               # OIDC/SAML SSO integration
│   ├── apikey.go            # API key management
│   ├── jwt.go               # JWT issuance and validation
│   └── service_account.go   # Service account management
├── rbac/                    # Authorization
│   ├── roles.go             # Role definitions (Admin, Editor, Viewer, custom)
│   ├── permissions.go       # Permission checks
│   └── policy.go            # Policy evaluation engine
├── secrets/                 # Secrets management
│   ├── vault.go             # HashiCorp Vault integration
│   ├── aws_sm.go            # AWS Secrets Manager integration
│   └── gcp_sm.go            # GCP Secret Manager integration
├── encryption/              # Encryption
│   ├── tls.go               # TLS configuration
│   └── aes.go               # AES-256 at-rest encryption
└── audit/                   # Audit logging
    ├── logger.go            # Immutable audit trail
    └── exporter.go          # Audit log export
```

### `internal/observability/` — Observability

```
internal/observability/
├── metrics/                 # Prometheus metrics
│   ├── collector.go         # Metric definitions and collection
│   └── exporter.go          # /metrics endpoint
├── tracing/                 # Distributed tracing
│   ├── otel.go              # OpenTelemetry setup
│   └── propagator.go        # Context propagation across services
└── logging/                 # Structured logging
    ├── logger.go            # JSON structured logger
    └── correlation.go       # Correlation ID management
```

### `internal/storage/` — Data Layer

```
internal/storage/
├── postgres/                # PostgreSQL repositories
│   ├── connector_repo.go    # Connector CRUD
│   ├── connection_repo.go   # Connection CRUD
│   ├── sync_repo.go         # Sync CRUD and run history
│   ├── state_repo.go        # Checkpoint state storage
│   ├── tenant_repo.go       # Tenant management
│   └── schema_repo.go       # Schema versioning storage
├── redis/                   # Redis cache
│   ├── config_cache.go      # Connector config caching
│   ├── schema_cache.go      # Schema caching
│   ├── ratelimiter.go       # Rate limiter state
│   └── pubsub.go            # Real-time event pub/sub
└── blob/                    # S3-compatible blob storage
    ├── client.go            # S3 client abstraction
    ├── staging.go           # Staged file transfer management
    └── archival.go          # Audit log archival
```

---

## `pkg/` — Public Go Packages

These are importable by external code — connector developers use these.

```
pkg/
├── cdk/                     # Connector Development Kit
│   ├── source.go            # Source connector interface
│   ├── destination.go       # Destination connector interface
│   ├── bidirectional.go     # Bidirectional connector interface
│   ├── testing/             # Acceptance test framework
│   │   ├── suite.go         # Test suite runner
│   │   ├── spec_test.go     # Spec validation tests
│   │   ├── check_test.go    # Connection check tests
│   │   ├── discover_test.go # Discovery tests
│   │   ├── read_test.go     # Read compliance tests
│   │   ├── write_test.go    # Write compliance tests
│   │   ├── idempotency_test.go # Idempotency tests
│   │   └── perf_test.go     # Performance baseline tests
│   └── examples/            # Example connector implementations
│       ├── rest_source/     # Simple REST API source
│       └── db_bidirectional/ # Database bidirectional connector
├── protocol/                # Public protocol types
│   ├── record.go            # RECORD message type
│   ├── state.go             # STATE message type
│   ├── schema.go            # SCHEMA message type
│   └── catalog.go           # Stream catalog types
└── arrow/                   # Arrow buffer utilities
    ├── buffer.go            # Arrow record batch management
    ├── convert.go           # Type conversion helpers
    └── ipc.go               # Zero-copy IPC utilities
```

---

## `connectors/` — First-Class Connectors

Each connector follows the same structure:

```
connectors/
├── salesforce/
│   ├── connector.go         # Connector registration and metadata
│   ├── auth.go              # Authentication (OAuth 2.0, API Token)
│   ├── source.go            # Source interface implementation
│   ├── destination.go       # Destination interface implementation
│   ├── spec.json            # Configuration JSON Schema
│   ├── streams.go           # Stream definitions and schemas
│   ├── bulk.go              # Bulk API 2.0 operations
│   ├── rate_limiter.go      # Salesforce-specific rate limit handling
│   └── *_test.go            # Unit and acceptance tests
├── hubspot/                 # (same structure)
├── bigquery/                # (same structure)
├── snowflake/               # (same structure)
├── s3/                      # (same structure)
├── gong/                    # (source only — no destination.go)
├── chorus/                  # (source only)
├── zoom/                    # (source only)
├── zendesk/                 # (bidirectional)
└── intercom/                # (bidirectional)
```

---

## `deploy/` — Deployment Configurations

```
deploy/
├── docker-compose/          # Development/evaluation
│   ├── docker-compose.yml   # All services defined
│   ├── .env.example         # Environment variable template
│   └── init/                # Initialization scripts
│       ├── postgres-init.sql
│       └── vault-init.sh
├── helm/                    # Kubernetes Helm chart
│   └── flowforge/
│       ├── Chart.yaml
│       ├── values.yaml      # Default configuration
│       ├── values-production.yaml
│       ├── templates/
│       │   ├── api-deployment.yaml
│       │   ├── worker-deployment.yaml
│       │   ├── mcp-gateway-deployment.yaml
│       │   ├── temporal-statefulset.yaml
│       │   ├── hpa.yaml
│       │   ├── metrics-adapter.yaml
│       │   └── ingress.yaml
│       ├── dashboards/      # Grafana dashboards as ConfigMaps
│       └── alerts/          # Prometheus alerting rules
├── operator/                # Kubernetes Operator
│   ├── api/v1/              # CRD type definitions
│   ├── controllers/         # Reconciliation controllers
│   └── config/              # Operator configuration
└── terraform/               # Cloud infrastructure modules
    ├── aws/                 # AWS-specific (RDS, ElastiCache, EKS)
    ├── gcp/                 # GCP-specific (Cloud SQL, Memorystore, GKE)
    └── azure/               # Azure-specific
```

---

## Other Top-Level Directories

```
proto/                       # Protobuf definitions
├── flowforge/
│   ├── api/v1/              # Public API service definitions
│   │   ├── connector.proto
│   │   ├── sync.proto
│   │   ├── integration.proto
│   │   └── mcp.proto
│   ├── internal/            # Internal RPC definitions
│   └── protocol/            # Data protocol messages
│       ├── record.proto
│       ├── state.proto
│       └── control.proto
└── buf.yaml                 # Buf configuration for proto management

python/                      # Python ecosystem
├── flowforge_cdk/           # Python CDK package
│   ├── source.py            # Source connector abstract base class
│   ├── destination.py       # Destination connector ABC
│   ├── bidirectional.py     # Bidirectional connector ABC
│   ├── protocol.py          # Protocol message types
│   └── testing/             # Python acceptance test framework
├── connectors/              # Python connector implementations
├── tests/                   # Python CDK tests
├── pyproject.toml
└── setup.py

ui/                          # React component library
├── src/
│   ├── components/
│   │   ├── ConnectorSelector/ # Connector browsing and selection
│   │   ├── FieldMapper/       # Visual drag-and-drop field mapping
│   │   ├── SyncMonitor/       # Real-time sync progress display
│   │   └── SchemaViewer/      # Schema inspection component
│   └── lib/                   # Shared utilities, API client, hooks
├── package.json
├── tsconfig.json
└── vite.config.ts

migrations/                  # Database migrations
└── postgres/
    ├── 001_initial.sql      # Core tables
    ├── 002_rbac.sql         # RBAC tables
    └── 003_audit.sql        # Audit log tables

scripts/                     # Build and dev scripts
├── dev-setup.sh             # One-command dev environment setup
├── generate-proto.sh        # Protobuf code generation
├── run-acceptance-tests.sh  # Run connector acceptance tests
└── benchmark.sh             # Run performance benchmarks

tests/                       # Cross-cutting tests
├── e2e/                     # End-to-end pipeline tests
├── integration/             # Service integration tests
└── load/                    # Load and performance tests
```
