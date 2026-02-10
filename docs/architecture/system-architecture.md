# FlowForge System Architecture

## Overview

FlowForge is a 6-layer architecture where each layer is independently scalable. All data movement is orchestrated by Temporal.io durable workflows, ensuring no data loss even under infrastructure failures.

---

## 1. Architecture Layers

```
┌─────────────────────────────────────────────────────────────┐
│                     API Gateway Layer                        │
│         REST (gRPC-Gateway) · GraphQL · WebSocket            │
├─────────────────────────────────────────────────────────────┤
│                     Control Plane Layer                       │
│     Scheduler · Config Manager · Tenant Manager · Schema     │
├─────────────────────────────────────────────────────────────┤
│                  Orchestration Engine Layer                   │
│              Temporal.io Server Cluster                       │
│       Workflows · Activities · Task Queues · Signals         │
├─────────────────────────────────────────────────────────────┤
│                     Worker Pool Layer                         │
│   Extract Workers · Transform Workers · Load Workers         │
│                  MCP Workers · Connector Workers              │
├─────────────────────────────────────────────────────────────┤
│                   Connector Runtime Layer                     │
│     Native Plugins · OCI Containers · WASM Sandboxes         │
│          CDK Runtime · OAuth Manager · Protocol              │
├─────────────────────────────────────────────────────────────┤
│                      Data Layer                              │
│    PostgreSQL · Redis · S3-Compatible · HashiCorp Vault       │
└─────────────────────────────────────────────────────────────┘
```

### Layer 1: API Gateway

**Components**: REST API, GraphQL API, WebSocket events
**Technology**: Go + gRPC-Gateway
**Scaling**: Horizontal, stateless pods behind Ingress with TLS termination

The API Gateway is the external interface. All client interactions (UI, CLI, embedded SDKs, MCP clients) enter through this layer. gRPC-Gateway auto-generates REST endpoints from Protobuf service definitions, ensuring a single source of truth between internal RPC and external REST/OpenAPI.

Key responsibilities:
- Request authentication and authorization (JWT, API key, OIDC)
- Rate limiting (per-tenant, per-endpoint)
- Request validation against Protobuf schemas
- OpenAPI spec serving for API documentation
- WebSocket connections for real-time sync status updates
- GraphQL resolvers for flexible data querying

### Layer 2: Control Plane

**Components**: Scheduler, Config Manager, Tenant Manager, Schema Manager
**Technology**: Go microservices
**Scaling**: Leader-elected, HA pair

The Control Plane manages the "what" and "when" of data movement. It does not process data itself — it configures and triggers Temporal workflows.

- **Scheduler**: Manages cron schedules, webhook triggers, event subscriptions, API-triggered syncs, and dependency chains. Converts schedules into Temporal workflow executions.
- **Config Manager**: Stores and validates connector configurations, sync configurations, and field mappings. Serves config to workers at execution time.
- **Tenant Manager**: Manages multi-tenant isolation: namespaces, credentials, resource quotas, and data boundaries. Each tenant gets an isolated Temporal namespace.
- **Schema Manager**: Handles schema detection, drift policies, versioning, and the Catalog API. Monitors for schema changes and applies configured policies.

### Layer 3: Orchestration Engine

**Components**: Temporal Server cluster
**Technology**: Temporal.io (self-hosted)
**Scaling**: Multi-node with PostgreSQL backend

Temporal is the nervous system. Every sync operation is a Temporal Workflow with discrete Activities. This provides:

- **Durable Execution**: Worker crash mid-sync → Temporal replays from last committed state
- **Automatic Retries**: Per-activity retry policies with exponential backoff, jitter, max attempts
- **Saga Compensation**: Bidirectional sync failures trigger compensation activities
- **Long-Running Workflows**: Hours/days-long backfills are first-class citizens
- **Visibility**: Full event history, search attributes for filtering by connector/tenant/sync/error
- **Versioning**: Update workflow code without breaking in-flight syncs

### Layer 4: Worker Pool

**Components**: Extract, Transform, Load, MCP, Connector workers
**Technology**: Go + Python (polyglot via Temporal SDK)
**Scaling**: Horizontal auto-scaling by task queue depth

Workers execute the actual data processing. Each worker type is a separate Kubernetes Deployment with its own HPA:

| Worker Type | Language | Concurrency | Resources | Responsibilities |
|---|---|---|---|---|
| Extract | Go | 100–500 goroutines | 2-4 CPU, 4-8 GB | API polling, CDC, file reads, schema detection |
| Transform | Go + Python | Goroutines + Python subprocess pool | 4-8 CPU, 8-16 GB | Field mapping, type coercion, custom transforms, dedup |
| Load | Go | Goroutines + batched writes | 2-4 CPU, 4-8 GB | Bulk API writes, upserts, conflict resolution |
| MCP | Go | gRPC streaming, event loop | 1-2 CPU, 2-4 GB | MCP protocol, tool execution, context assembly |
| Connector | Polyglot | Isolated per invocation | Per-connector | Custom connector logic, third-party SDKs |

### Layer 5: Connector Runtime

**Components**: Connector processes, CDK runtime, OAuth manager
**Technology**: OCI containers, WASM sandboxes, native plugins
**Scaling**: Per-connector scaling

The Connector Runtime executes connector code in isolation:

- **Native Plugins**: First-class connectors compiled into the worker binary for zero-overhead execution
- **OCI Containers**: Community/third-party connectors run in lightweight containers pulled from a registry
- **WASM Modules** (experimental): Ultra-lightweight connectors with sub-millisecond cold start

### Layer 6: Data Layer

**Components**: PostgreSQL, Redis, S3-compatible, HashiCorp Vault
**Technology**: Standard HA patterns
**Scaling**: Per-component standard scaling

- **PostgreSQL**: Metadata DB — connectors, connections, syncs, runs, state checkpoints, tenants, schemas, audit logs. Also serves as Temporal's persistence backend.
- **Redis**: Low-latency cache for connector configs, schema cache, rate limiter state, pub/sub for real-time events.
- **S3-Compatible**: Staged file transfers (warehouse-to-warehouse), connector artifact storage, audit log archival. MinIO for dev, real S3/GCS for production.
- **HashiCorp Vault**: Secrets management — connector credentials, OAuth tokens, encryption keys. Dynamic secret generation.

---

## 2. Data Flow Paths

### ETL Path (Ingestion)

```
Source Systems
    │
    ▼
Extract Workers ─── (Temporal Activities) ───▶ Arrow Buffer
    │                                              │
    │  • API polling / Bulk API                    │
    │  • CDC listeners                             ▼
    │  • Cursor-based incremental          Transform Workers
    │  • Fan-out for parallel partitions           │
    │                                              │  • Field mapping
                                                   │  • Type coercion
                                                   │  • Deduplication
                                                   │  • Custom transforms
                                                   ▼
                                             Arrow Buffer
                                                   │
                                                   ▼
                                             Load Workers
                                                   │
                                                   │  • Batched writes (Bulk API 2.0)
                                                   │  • Streaming inserts
                                                   │  • COPY INTO from staged files
                                                   ▼
                                            Data Warehouse
                                     (BigQuery, Snowflake, S3)
```

### Reverse ETL Path (Activation)

```
Data Warehouse
    │
    ▼
Query Workers ─── (Temporal Activities) ───▶ Arrow Buffer
    │                                              │
    │  • SQL query extraction                      ▼
    │  • Partition-based reads            Transform Workers
    │  • Change detection                          │
    │                                              │  • Schema mapping
                                                   │  • Field transforms
                                                   ▼
                                             Arrow Buffer
                                                   │
                                                   ▼
                                             Load Workers
                                                   │
                                                   │  • CRM upserts
                                                   │  • Ticket creation
                                                   │  • API writes with rate limiting
                                                   ▼
                                          Operational Systems
                                    (Salesforce, HubSpot, Zendesk)
```

### MCP Path (AI Agent)

```
AI Agent (Claude, ChatGPT, IDE Plugin)
    │
    │  MCP Protocol (SSE / WebSocket)
    ▼
MCP Gateway
    │
    │  • Authentication
    │  • Tool routing
    │  • Rate limiting
    ▼
MCP Worker ─── (Temporal Activity) ───▶ Connector Activity
    │                                         │
    │  • Tool execution                       │  • API call to external system
    │  • Context assembly                     │  • Query execution
    │                                         │  • Data retrieval/mutation
    ▼                                         ▼
MCP Gateway ◄─────────── Response ───────────┘
    │
    │  MCP Protocol
    ▼
AI Agent
```

### Embedded iPaaS Path

```
SaaS Application
    │
    │  REST / GraphQL API
    ▼
FlowForge API ───▶ Control Plane
    │                    │
    │  • Tenant context  │  • Config resolution
    │  • Auth validation │  • Schedule management
    │                    ▼
    │              Temporal Orchestrator
    │                    │
    │                    ▼
    │              Worker Pool
    │                    │
    │                    ▼
    │           Customer's Connected Systems
    │           (via tenant-scoped credentials)
    ▼
Webhook Callback to SaaS Application
    (sync completed, failed, schema changed)
```

---

## 3. Temporal Workflow Patterns

### 3.1 Parent-Child Sync Workflow

The primary pattern for all sync operations:

```
SyncOrchestrator (Parent Workflow)
    │
    ├──▶ ExtractWorkflow (Child)
    │       ├── PaginateActivity
    │       ├── CheckpointActivity
    │       └── EmitRecordsActivity
    │
    ├──▶ TransformWorkflow (Child)
    │       ├── MapFieldsActivity
    │       ├── CoerceTypesActivity
    │       └── DeduplicateActivity
    │
    └──▶ LoadWorkflow (Child)
            ├── BatchAccumulateActivity
            ├── BulkWriteActivity
            └── ConfirmWriteActivity
```

The parent coordinates state across phases. Each child handles pagination, checkpointing, and error recovery independently. If a child fails, only that phase retries — not the entire sync.

### 3.2 Fan-Out/Fan-In for Parallel Extraction

For high-volume sources (millions of records):

```
ExtractWorkflow (Parent)
    │
    ├──▶ PartitionWorkflow-1 (records 0-100K)
    ├──▶ PartitionWorkflow-2 (records 100K-200K)
    ├──▶ PartitionWorkflow-3 (records 200K-300K)
    └──▶ PartitionWorkflow-N (records ...)
           │
           │  Temporal Signals (progress reports)
           ▼
    ExtractWorkflow collects results
           │
           ▼
    Feed to TransformWorkflow
```

### 3.3 Continue-As-New for Scheduled Syncs

Recurring syncs avoid unbounded event history:

```
SyncScheduleWorkflow
    │
    ├── Execute sync cycle
    ├── Record results
    ├── Calculate next run time
    └── Continue-As-New (fresh execution, no history buildup)
         │
         └── SyncScheduleWorkflow (new execution)
              └── ... (repeats)
```

### 3.4 Signal-Based Control

Real-time operational control without killing workflows:

```
Running Sync Workflow
    │
    ├── ◄── PauseSignal ──── User pauses sync
    │       (workflow blocks until ResumeSignal)
    │
    ├── ◄── ResumeSignal ─── User resumes sync
    │       (workflow continues from paused state)
    │
    ├── ◄── CancelSignal ─── User cancels sync
    │       (workflow runs compensation activities)
    │
    └── ◄── ModifySignal ─── User changes config
            (workflow applies new config on next activity)
```

---

## 4. Task Queue Strategy

Dedicated task queues ensure workload isolation:

| Queue Pattern | Purpose | Isolation Benefit |
|---|---|---|
| `sync-extract-{connector_type}` | Per-source-connector extraction | Slow Salesforce API doesn't block HubSpot |
| `sync-transform` | Shared transformation | CPU-bound work benefits from pooling |
| `sync-load-{connector_type}` | Per-destination loading | Rate-limited destinations don't block others |
| `mcp-request-{server_id}` | Per-MCP-server requests | Fair scheduling across tenants |
| `system-maintenance` | Housekeeping | Low priority, never competes with data movement |

---

## 5. Internal Data Protocol

FlowForge defines a protocol inspired by Airbyte's but extended for bidirectional operation and MCP:

| Message Type | Direction | Purpose |
|---|---|---|
| `SPEC` | Platform → Connector | Request connector configuration schema |
| `CHECK` | Platform → Connector | Validate credentials and connectivity |
| `DISCOVER` | Platform → Connector | Enumerate streams and schemas |
| `READ` | Platform → Source | Extract records with optional incremental state |
| `WRITE` | Platform → Destination | Load records with upsert/append semantics |
| `RECORD` | Connector → Platform | Data record with stream name, payload, timestamp |
| `STATE` | Connector → Platform | Checkpoint for incremental sync resumption |
| `SCHEMA` | Connector → Platform | Schema declaration or change notification |
| `LOG` | Connector → Platform | Structured log (DEBUG, INFO, WARN, ERROR) |
| `CONTROL` | Bidirectional | Rate limit signals, backpressure, pause/resume |
| `MCP_TOOL_CALL` | MCP Client → Platform | Tool invocation from AI agent |
| `MCP_TOOL_RESULT` | Platform → MCP Client | Tool result returned to AI agent |

All messages are serialized as Protobuf for internal communication and JSON for external APIs.

---

## 6. Performance Optimizations

| Optimization | How It Works |
|---|---|
| **Batch Processing** | All loads use batched writes: Salesforce Bulk API 2.0, BigQuery streaming with batch accumulation, Snowflake COPY INTO |
| **Connection Pooling** | Persistent pools per destination. HTTP/2 multiplexing for REST. DB pools sized by worker count |
| **Backpressure** | Workers use Temporal activity heartbeats. Rate-limited destination → signal orchestrator to slow extraction |
| **Zero-Copy Transfers** | Warehouse-to-warehouse (BigQuery→Snowflake) stages through S3, bypasses worker data plane |
| **Incremental Processing** | All connectors support cursor-based incremental. State persisted per-stream, recovered on restart |
| **Columnar Buffering** | Apache Arrow columnar format for internal buffers. Efficient memory, vectorized transforms |
