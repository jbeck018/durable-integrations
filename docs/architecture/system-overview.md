---
title: System Overview
layout: default
parent: Architecture
nav_order: 1
description: "Detailed walkthrough of FlowForge's 6-layer architecture and data flow paths."
permalink: /architecture/system-overview/
---

# System Overview

FlowForge is a 6-layer architecture where each layer is independently scalable. All data movement is orchestrated by Temporal.io durable workflows, ensuring no data loss even under infrastructure failures.

---

## 1. Architecture Layers

### Layer 1: API Gateway

**Components**: REST API, Connect-RPC, GraphQL API, WebSocket events
**Technology**: Go + gRPC-Gateway + Connect-RPC
**Scaling**: Horizontal, stateless pods behind Ingress with TLS termination

The API Gateway is the external interface. All client interactions (UI, CLI, embedded SDKs, MCP clients) enter through this layer. gRPC-Gateway auto-generates REST endpoints from Protobuf service definitions, and Connect-RPC provides type-safe JSON-over-HTTP for browser clients.

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

- **Scheduler**: Manages cron schedules, webhook triggers, event subscriptions, API-triggered syncs, and dependency chains
- **Config Manager**: Stores and validates connector configurations, sync configurations, and field mappings
- **Tenant Manager**: Manages multi-tenant isolation: namespaces, credentials, resource quotas, and data boundaries. Each tenant gets an isolated Neon Postgres database and Temporal namespace
- **Schema Manager**: Handles schema detection, drift policies, versioning, and the Catalog API

### Layer 3: Orchestration Engine

**Components**: Temporal Server cluster
**Technology**: Temporal.io (self-hosted)
**Scaling**: Multi-node with PostgreSQL backend

Temporal is the nervous system. Every sync operation is a Temporal Workflow with discrete Activities. This provides:

- **Durable Execution**: Worker crash mid-sync → Temporal replays from last committed state
- **Automatic Retries**: Per-activity retry policies with exponential backoff, jitter, max attempts
- **Saga Compensation**: Bidirectional sync failures trigger compensation activities
- **Long-Running Workflows**: Hours/days-long backfills are first-class citizens
- **Visibility**: Full event history, search attributes for filtering
- **Versioning**: Update workflow code without breaking in-flight syncs

### Layer 4: Worker Pool

**Components**: Extract, Transform, Load, MCP, Connector workers
**Technology**: Go + Python (polyglot via Temporal SDK)
**Scaling**: Horizontal auto-scaling by task queue depth

| Worker Type | Language | Concurrency | Resources | Responsibilities |
|:--|:--|:--|:--|:--|
| Extract | Go | 100-500 goroutines | 2-4 CPU, 4-8 GB | API polling, CDC, file reads, schema detection |
| Transform | Go + Python | Goroutines + Python subprocess pool | 4-8 CPU, 8-16 GB | Field mapping, type coercion, custom transforms, dedup |
| Load | Go | Goroutines + batched writes | 2-4 CPU, 4-8 GB | Bulk API writes, upserts, conflict resolution |
| MCP | Go | gRPC streaming, event loop | 1-2 CPU, 2-4 GB | MCP protocol, tool execution, context assembly |
| Connector | Polyglot | Isolated per invocation | Per-connector | Custom connector logic, third-party SDKs |

### Layer 5: Connector Runtime

**Components**: Connector processes, CDK runtime, OAuth manager
**Technology**: OCI containers, WASM sandboxes, native plugins

- **Native Plugins**: First-class connectors compiled into the worker binary for zero-overhead execution
- **OCI Containers**: Community/third-party connectors run in lightweight containers pulled from a registry
- **WASM Modules** (experimental): Ultra-lightweight connectors with sub-millisecond cold start

### Layer 6: Data Layer

**Components**: PostgreSQL, Redis, S3-compatible, HashiCorp Vault

- **PostgreSQL**: Metadata DB — connectors, connections, syncs, runs, state checkpoints, tenants, schemas, audit logs. Also serves as Temporal's persistence backend
- **Redis**: Low-latency cache for connector configs, schema cache, rate limiter state, pub/sub for real-time events
- **S3-Compatible**: Staged file transfers, connector artifact storage, audit log archival. MinIO for dev, real S3/GCS for production
- **HashiCorp Vault**: Secrets management — connector credentials, OAuth tokens, encryption keys

---

## 2. Data Flow Paths

### ETL Path (Ingestion)

```
Source Systems → Extract Workers → Arrow Buffer → Transform Workers → Arrow Buffer → Load Workers → Data Warehouse
```

1. Extract Workers poll APIs, read CDC streams, or scan files
2. Records are buffered in Apache Arrow columnar format
3. Transform Workers apply field mapping, type coercion, deduplication
4. Load Workers execute batched writes (Bulk API 2.0, streaming inserts, COPY INTO)

### Reverse ETL Path (Activation)

```
Data Warehouse → Query Workers → Arrow Buffer → Transform Workers → Arrow Buffer → Load Workers → Operational Systems
```

1. Query Workers extract data via SQL or partition-based reads
2. Transform Workers map warehouse schemas to operational system fields
3. Load Workers write to CRMs, support tools, and APIs with rate limiting

### MCP Path (AI Agent)

```
AI Agent → MCP Gateway (auth + routing) → MCP Worker (Temporal Activity) → Connector → External System
```

### Embedded iPaaS Path

```
SaaS Application → FlowForge API → Control Plane → Temporal Orchestrator → Workers → Customer's Connected Systems
```

---

## 3. Performance Optimizations

| Optimization | How It Works |
|:--|:--|
| **Batch Processing** | All loads use batched writes: Salesforce Bulk API 2.0, BigQuery streaming, Snowflake COPY INTO |
| **Connection Pooling** | Persistent pools per destination. HTTP/2 multiplexing for REST. DB pools sized by worker count |
| **Backpressure** | Workers use Temporal activity heartbeats. Rate-limited destination → signal orchestrator to slow extraction |
| **Zero-Copy Transfers** | Warehouse-to-warehouse stages through S3, bypasses worker data plane |
| **Incremental Processing** | All connectors support cursor-based incremental. State persisted per-stream, recovered on restart |
| **Columnar Buffering** | Apache Arrow columnar format for internal buffers. Efficient memory, vectorized transforms |
