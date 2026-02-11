---
title: Home
layout: home
nav_order: 1
description: "FlowForge — The open-source, high-performance integration platform built on Temporal.io."
permalink: /
---

# FlowForge

**The Open-Source, High-Performance Integration Platform**
{: .fs-6 .fw-300 }

ETL · Reverse ETL · MCP Servers · Embedded iPaaS — Powered by Temporal.io Durable Execution
{: .fs-5 .fw-300 }

[Get Started](#quick-start){: .btn .btn-primary .fs-5 .mb-4 .mb-md-0 .mr-2 }
[View on GitHub](https://github.com/jbeck018/durable-integrations){: .btn .fs-5 .mb-4 .mb-md-0 }

---

FlowForge unifies ETL, Reverse ETL, MCP server hosting, and embedded iPaaS into a single platform built on [Temporal.io](https://temporal.io). Instead of stitching together Fivetran + Hightouch + Paragon + custom MCP servers, FlowForge provides one framework for all data movement.

## Key Capabilities

| Capability | Description |
|:--|:--|
| **ETL/ELT** | Extract from any source, transform with Arrow-powered columnar buffers, load into warehouses |
| **Reverse ETL** | Activate warehouse data back into CRMs, support tools, and operational systems |
| **MCP Servers** | Every connector auto-exposes MCP tools — AI agents query and write to any system |
| **Embedded iPaaS** | Headless APIs + embeddable UI components for SaaS vendors to white-label integrations |
| **Durable Execution** | Every sync is a Temporal workflow — survives crashes, retries automatically, scales horizontally |

## Architecture Overview

```
API Gateway (REST + Connect-RPC + GraphQL + WebSocket)
    ↓
Control Plane (Scheduler · Config · Tenants · Schema)
    ↓
Temporal.io Orchestration (Workflows · Activities · Signals)
    ↓
Worker Pool (Extract · Transform · Load · MCP)
    ↓
Connector Runtime (Native · OCI Containers · WASM)
    ↓
Data Layer (PostgreSQL · Redis · S3 · Vault)
```

## First-Class Connectors (v1.0)

| Tier | Connectors | Direction |
|:--|:--|:--|
| **Tier 1: Bidirectional** | Salesforce, HubSpot, BigQuery, Snowflake, S3 | Read + Write |
| **Tier 2: Call Recording** | Gong, Chorus, Zoom | Read-only |
| **Tier 3: Support** | Zendesk, Intercom | Bidirectional |

## Quick Start

```bash
# Clone the repository
git clone https://github.com/jbeck018/durable-integrations.git
cd durable-integrations

# Start all infrastructure services
make docker-up

# Build all binaries
make build

# Run the API server
./bin/flowforge-api

# Run the worker
./bin/flowforge-worker
```

See the [Deployment Guide]({{ site.baseurl }}/deployment/) for full setup instructions including Docker Compose, Kubernetes Helm, and Kubernetes Operator deployments.

## Tech Stack

| Component | Technology |
|:--|:--|
| Primary Language | Go |
| Secondary Language | Python |
| Orchestration | Temporal.io |
| API | Connect-RPC + gRPC-Gateway (auto-generated REST) |
| Data Format | Apache Arrow (columnar) |
| UI | React / Angular / Svelte (headless core) |
| Observability | OpenTelemetry + Prometheus + Grafana |
| Deployment | Docker Compose / Helm / K8s Operator |
| Tenant Isolation | Neon project-per-tenant Postgres |
