---
title: Deployment
layout: default
parent: Guides
nav_order: 2
description: "Deploy FlowForge with Docker Compose, Kubernetes Helm, or the Kubernetes Operator."
permalink: /guides/deployment/
---

# Deployment Guide

FlowForge supports three deployment modes.

| Mode | Use Case | Minimum Resources |
|:--|:--|:--|
| **Docker Compose** | Development, evaluation | 4 CPU, 8 GB RAM, 50 GB disk |
| **Kubernetes (Helm)** | Production, multi-tenant | 3 nodes, 8 CPU each, 32 GB RAM |
| **Kubernetes (Operator)** | Enterprise, auto-scaling | 3+ nodes with auto-scaling |

---

## Docker Compose (Development)

### Quick Start

```bash
git clone https://github.com/jbeck018/durable-integrations.git
cd durable-integrations

cp deploy/docker-compose/.env.example deploy/docker-compose/.env
# Edit .env with your configuration

make docker-up
```

### Services

| Service | Port | Purpose |
|:--|:--|:--|
| `flowforge-api` | 8080 | REST + GraphQL API |
| `flowforge-worker` | — | Worker process |
| `flowforge-mcp-gateway` | 8090 | MCP gateway |
| `temporal-server` | 7233 | Temporal gRPC |
| `temporal-web` | 8088 | Temporal Web UI |
| `postgresql` | 5432 | Metadata + Temporal |
| `redis` | 6379 | Cache + rate limiter |
| `minio` | 9000/9001 | S3-compatible storage |
| `vault` | 8200 | Secrets management |
| `prometheus` | 9090 | Metrics |
| `grafana` | 3000 | Dashboards |

### Environment Configuration

```bash
FLOWFORGE_API_PORT=8080
FLOWFORGE_LOG_LEVEL=info
FLOWFORGE_ENCRYPTION_KEY=<openssl rand -hex 32>
POSTGRES_HOST=postgresql
POSTGRES_PORT=5432
POSTGRES_USER=flowforge
POSTGRES_PASSWORD=<strong-password>
POSTGRES_DB=flowforge
REDIS_HOST=redis
TEMPORAL_HOST=temporal-server
TEMPORAL_PORT=7233
TEMPORAL_NAMESPACE=flowforge
S3_ENDPOINT=http://minio:9000
S3_BUCKET=flowforge-staging
VAULT_ADDR=http://vault:8200
NEON_API_KEY=<your-neon-api-key>       # For tenant isolation
NEON_REGION=aws-us-east-1              # Neon region
```

---

## Kubernetes (Helm Chart)

### Installation

```bash
helm repo add flowforge https://charts.flowforge.io
helm repo update

kubectl create namespace flowforge

helm install flowforge flowforge/flowforge \
  --namespace flowforge \
  --values my-values.yaml
```

### Key Configuration (values.yaml)

```yaml
api:
  replicas: 2
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10

workers:
  extract:
    autoscaling:
      enabled: true
      minReplicas: 1
      maxReplicas: 20
  transform:
    autoscaling:
      enabled: true
      minReplicas: 1
      maxReplicas: 15
  load:
    autoscaling:
      enabled: true
      minReplicas: 1
      maxReplicas: 20

temporal:
  enabled: true
  server:
    replicas: 3

postgresql:
  enabled: true
  primary:
    persistence:
      size: 100Gi

redis:
  enabled: true
  architecture: replication
```

### Verifying Installation

```bash
kubectl get pods -n flowforge
kubectl get svc -n flowforge
kubectl port-forward -n flowforge svc/flowforge-api 8080:8080
```

---

## Auto-Scaling

FlowForge implements intelligent auto-scaling at multiple levels:

- **Task Queue Depth Scaling**: Custom metrics adapter exposes Temporal queue depth to HPA
- **Predictive Scaling**: Pre-scales workers 2 minutes before scheduled high-load periods
- **Scale-to-Zero**: Infrequently-used worker deployments scale to zero and spin up on demand

---

## Monitoring

### Pre-Built Grafana Dashboards

| Dashboard | Metrics |
|:--|:--|
| System Overview | Total syncs, records/sec, error rate, queue depth |
| Connector Performance | Per-connector throughput, latency, API quota usage |
| MCP Metrics | Tool call latency, calls/sec, error rates |
| Worker Pool | CPU/memory utilization, task processing rate |

### Alerting

```yaml
- alert: SyncFailureRate
  expr: rate(flowforge_sync_failures_total[5m]) > 0.05
  for: 10m
  labels:
    severity: warning

- alert: WorkerQueueBacklog
  expr: temporal_task_queue_depth > 100
  for: 5m
  labels:
    severity: critical
```

---

## Backup and Recovery

- **PostgreSQL**: Daily pg_dump or managed database snapshots
- **Temporal**: Automatic — all workflow state persists in its database. Worker crashes and cluster restarts are fully recoverable
- **Vault**: `vault operator raft snapshot save`
