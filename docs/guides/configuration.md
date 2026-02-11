---
title: Configuration
layout: default
parent: Guides
nav_order: 5
description: "Environment variables, service configuration, and encryption setup."
permalink: /guides/configuration/
---

# Configuration Reference

All FlowForge configuration is via environment variables, loaded at startup.

---

## Service Configuration

| Variable | Default | Description |
|:--|:--|:--|
| `FLOWFORGE_API_PORT` | `8080` | REST API port |
| `FLOWFORGE_GRPC_PORT` | `9090` | gRPC port |
| `FLOWFORGE_LOG_LEVEL` | `info` | Log level (debug, info, warn, error) |
| `FLOWFORGE_ENVIRONMENT` | `development` | Environment (development, staging, production) |
| `FLOWFORGE_CONCURRENCY` | `10` | Default worker concurrency |
| `FLOWFORGE_BATCH_SIZE` | `1000` | Default batch size for writes |
| `FLOWFORGE_ENCRYPTION_KEY` | — | **Required.** AES-256 encryption key for field-level encryption |
| `FLOWFORGE_HKDF_SALT` | built-in | Optional HKDF salt override |

---

## Database (PostgreSQL)

| Variable | Default | Description |
|:--|:--|:--|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_USER` | `flowforge` | Database user |
| `POSTGRES_PASSWORD` | — | Database password |
| `POSTGRES_DB` | `flowforge` | Database name |
| `POSTGRES_SSLMODE` | `disable` | SSL mode |

---

## Cache (Redis)

| Variable | Default | Description |
|:--|:--|:--|
| `REDIS_HOST` | `localhost` | Redis host |
| `REDIS_PORT` | `6379` | Redis port |
| `REDIS_PASSWORD` | — | Redis password |
| `REDIS_DB` | `0` | Redis database number |

---

## Orchestration (Temporal)

| Variable | Default | Description |
|:--|:--|:--|
| `TEMPORAL_HOST` | `localhost` | Temporal server host |
| `TEMPORAL_PORT` | `7233` | Temporal gRPC port |
| `TEMPORAL_NAMESPACE` | `flowforge` | Default Temporal namespace |

---

## Object Storage (S3)

| Variable | Default | Description |
|:--|:--|:--|
| `S3_ENDPOINT` | — | S3-compatible endpoint (e.g., MinIO) |
| `S3_BUCKET` | `flowforge-staging` | Default bucket name |
| `S3_REGION` | `us-east-1` | AWS region |
| `S3_ACCESS_KEY` | — | Access key |
| `S3_SECRET_KEY` | — | Secret key |

---

## Secrets (Vault)

| Variable | Default | Description |
|:--|:--|:--|
| `VAULT_ADDR` | `http://localhost:8200` | Vault address |
| `VAULT_TOKEN` | — | Vault token |

---

## MCP Gateway

| Variable | Default | Description |
|:--|:--|:--|
| `MCP_GATEWAY_PORT` | `8090` | MCP gateway port |
| `MCP_GATEWAY_TRANSPORT` | `sse,websocket` | Enabled transports |

---

## Tenant Isolation (Neon)

| Variable | Default | Description |
|:--|:--|:--|
| `NEON_API_KEY` | — | Neon API key for database provisioning |
| `NEON_REGION` | `aws-us-east-1` | Neon region for new projects |

---

## Observability

| Variable | Default | Description |
|:--|:--|:--|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | — | OpenTelemetry collector endpoint |

---

## Encryption Setup

FlowForge uses AES-256-GCM with HKDF-SHA256 key derivation for field-level encryption (connection URIs, OAuth tokens, etc.).

### Generate an encryption key

```bash
openssl rand -hex 32
```

Set this as `FLOWFORGE_ENCRYPTION_KEY`.

### Custom HKDF salt (optional)

```bash
export FLOWFORGE_HKDF_SALT=$(openssl rand -hex 16)
```

The salt is used in HKDF key derivation to bind the derived key to your deployment. If not set, a built-in default is used.
