# FlowForge

**The Open-Source, High-Performance Integration Platform**

ETL · Reverse ETL · MCP Servers · Embedded iPaaS — Powered by Temporal.io Durable Execution

---

FlowForge unifies ETL, Reverse ETL, MCP server hosting, and embedded iPaaS into a single platform built on [Temporal.io](https://temporal.io). Instead of stitching together Fivetran + Hightouch + Paragon + custom MCP servers, FlowForge provides one framework for all data movement.

## Key Capabilities

- **ETL/ELT**: Extract from any source, transform with Arrow-powered columnar buffers, load into warehouses
- **Reverse ETL**: Activate warehouse data back into CRMs, support tools, and operational systems
- **MCP Servers**: Every connector auto-exposes MCP tools — AI agents query and write to any system
- **Embedded iPaaS**: Headless APIs + embeddable React components for SaaS vendors to white-label integrations
- **Durable Execution**: Every sync is a Temporal workflow — survives crashes, retries automatically, scales horizontally

## Architecture

```
API Gateway (REST + GraphQL + WebSocket)
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
|---|---|---|
| **Tier 1: Bidirectional** | Salesforce, HubSpot, BigQuery, Snowflake, S3 | Read + Write |
| **Tier 2: Call Recording** | Gong, Chorus, Zoom | Read-only |
| **Tier 3: Support** | Zendesk, Intercom | Bidirectional |

## Tech Stack

| Component | Technology |
|---|---|
| Primary Language | Go |
| Secondary Language | Python |
| Orchestration | Temporal.io |
| API | gRPC + gRPC-Gateway (auto-generated REST) |
| Data Format | Apache Arrow (columnar) |
| UI | React + TypeScript |
| Observability | OpenTelemetry + Prometheus + Grafana |
| Deployment | Docker Compose / Helm / K8s Operator |

## Documentation

| Document | Description |
|---|---|
| [Implementation Plan](IMPLEMENTATION_PLAN.md) | Full phased plan with sprint-level tasks |
| [System Architecture](docs/architecture/system-architecture.md) | 6-layer architecture, data flows, performance |
| [Temporal Patterns](docs/architecture/temporal-patterns.md) | Workflow patterns: parent-child, fan-out, signals, sagas |
| [Directory Structure](docs/architecture/directory-structure.md) | Every directory and file explained |
| [Connector Development](docs/guides/connector-development.md) | Build connectors: No-Code, Low-Code, Full-Code CDK |
| [MCP Framework](docs/guides/mcp-framework.md) | MCP gateway, auto-generated tools, security, custom servers |
| [Deployment](docs/guides/deployment.md) | Docker Compose, Helm, K8s Operator, auto-scaling |
| [Embedded iPaaS](docs/guides/embedded-ipaas.md) | Multi-tenant APIs, white-labeling, React components |

## Roadmap

| Phase | Timeline | Scope |
|---|---|---|
| Alpha | Months 1-3 | Core Temporal orchestration, Go CDK, Salesforce + BigQuery, API, Docker Compose |
| Beta | Months 4-6 | All Tier 1 connectors, Python CDK, MCP gateway, K8s Helm, visual mapping UI |
| GA v1.0 | Months 7-9 | All connectors, RBAC, audit, MCP production, embedded APIs, K8s Operator |
| v1.1 | Months 10-12 | No-code builder, WASM runtime, AI mapping, connector marketplace |
| v2.0 | Months 13-18 | Real-time streaming (Kafka), managed cloud, multi-region |

## License

Apache 2.0