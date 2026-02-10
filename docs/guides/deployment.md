# Deployment Guide

FlowForge supports three deployment modes, from single-machine development to production Kubernetes clusters.

---

## Deployment Modes

| Mode | Infrastructure | Use Case | Minimum Resources |
|---|---|---|---|
| **Docker Compose** | Single machine | Development, evaluation, small team (<10 syncs) | 4 CPU, 8 GB RAM, 50 GB disk |
| **Kubernetes (Helm)** | K8s cluster | Production, multi-tenant, high availability | 3 nodes, 8 CPU each, 32 GB RAM each |
| **Kubernetes (Operator)** | K8s cluster | Enterprise, auto-scaling, GitOps managed | 3+ nodes with auto-scaling policies |

---

## 1. Docker Compose (Development / Evaluation)

### Prerequisites

- Docker Engine 24+ with Docker Compose v2
- 4 CPU cores and 8 GB RAM available
- 50 GB free disk space

### Quick Start

```bash
# Clone the repository
git clone https://github.com/flowforge/flowforge.git
cd flowforge

# Copy environment template
cp deploy/docker-compose/.env.example deploy/docker-compose/.env

# Edit configuration (API keys, secrets)
vi deploy/docker-compose/.env

# Start all services
docker compose -f deploy/docker-compose/docker-compose.yml up -d

# Verify all services are healthy
docker compose -f deploy/docker-compose/docker-compose.yml ps

# View logs
docker compose -f deploy/docker-compose/docker-compose.yml logs -f flowforge-api
```

### Services

The Docker Compose stack includes:

| Service | Port | Purpose |
|---|---|---|
| `flowforge-api` | 8080 | REST + GraphQL API |
| `flowforge-worker` | — | Worker process (all types) |
| `flowforge-mcp-gateway` | 8090 | MCP gateway (SSE + WebSocket) |
| `temporal-server` | 7233 | Temporal gRPC |
| `temporal-web` | 8088 | Temporal Web UI |
| `postgresql` | 5432 | Metadata + Temporal persistence |
| `redis` | 6379 | Cache + rate limiter |
| `minio` | 9000/9001 | S3-compatible object storage |
| `vault` | 8200 | Secrets management (dev mode) |
| `prometheus` | 9090 | Metrics collection |
| `grafana` | 3000 | Dashboards |

### Environment Configuration

```bash
# deploy/docker-compose/.env

# FlowForge
FLOWFORGE_API_PORT=8080
FLOWFORGE_LOG_LEVEL=info
FLOWFORGE_ENCRYPTION_KEY=<generate-with: openssl rand -hex 32>

# PostgreSQL
POSTGRES_HOST=postgresql
POSTGRES_PORT=5432
POSTGRES_USER=flowforge
POSTGRES_PASSWORD=<strong-password>
POSTGRES_DB=flowforge

# Redis
REDIS_HOST=redis
REDIS_PORT=6379

# Temporal
TEMPORAL_HOST=temporal-server
TEMPORAL_PORT=7233
TEMPORAL_NAMESPACE=flowforge

# MinIO (S3-compatible)
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=<strong-password>
S3_ENDPOINT=http://minio:9000
S3_BUCKET=flowforge-staging

# Vault
VAULT_ADDR=http://vault:8200
VAULT_TOKEN=<dev-token>

# MCP Gateway
MCP_GATEWAY_PORT=8090
MCP_GATEWAY_TRANSPORT=sse,websocket
```

### Development Workflow

```bash
# Rebuild after code changes
docker compose -f deploy/docker-compose/docker-compose.yml up -d --build flowforge-api flowforge-worker

# Run database migrations
docker compose -f deploy/docker-compose/docker-compose.yml exec flowforge-api /flowforge-api migrate

# Access Temporal Web UI
open http://localhost:8088

# Access Grafana dashboards
open http://localhost:3000  # admin/admin

# Tail worker logs
docker compose -f deploy/docker-compose/docker-compose.yml logs -f flowforge-worker

# Stop everything
docker compose -f deploy/docker-compose/docker-compose.yml down

# Stop and remove volumes (clean reset)
docker compose -f deploy/docker-compose/docker-compose.yml down -v
```

---

## 2. Kubernetes (Helm Chart)

### Prerequisites

- Kubernetes 1.28+
- Helm 3.12+
- kubectl configured for your cluster
- PersistentVolume provisioner (for PostgreSQL, Temporal)
- Ingress controller (nginx-ingress or similar)
- cert-manager (for TLS)

### Installation

```bash
# Add FlowForge Helm repository
helm repo add flowforge https://charts.flowforge.io
helm repo update

# Create namespace
kubectl create namespace flowforge

# Install with default values
helm install flowforge flowforge/flowforge \
  --namespace flowforge \
  --values my-values.yaml

# Or install from local chart during development
helm install flowforge deploy/helm/flowforge \
  --namespace flowforge \
  --values my-values.yaml
```

### values.yaml Configuration

```yaml
# my-values.yaml

global:
  domain: flowforge.example.com
  storageClass: gp3  # AWS EBS, adjust for your cloud

# API Server
api:
  replicas: 2
  resources:
    requests:
      cpu: 500m
      memory: 512Mi
    limits:
      cpu: 2000m
      memory: 2Gi
  autoscaling:
    enabled: true
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilization: 70
  ingress:
    enabled: true
    className: nginx
    annotations:
      cert-manager.io/cluster-issuer: letsencrypt-prod
    hosts:
      - host: api.flowforge.example.com
        paths:
          - path: /
            pathType: Prefix
    tls:
      - secretName: flowforge-api-tls
        hosts:
          - api.flowforge.example.com

# Workers
workers:
  extract:
    replicas: 2
    resources:
      requests: { cpu: 1000m, memory: 2Gi }
      limits: { cpu: 4000m, memory: 8Gi }
    autoscaling:
      enabled: true
      minReplicas: 1
      maxReplicas: 20
      # Custom metric: Temporal task queue depth
      metrics:
        - type: External
          external:
            metric:
              name: temporal_task_queue_depth
              selector:
                matchLabels:
                  queue_type: sync-extract
            target:
              type: AverageValue
              averageValue: 10

  transform:
    replicas: 2
    resources:
      requests: { cpu: 2000m, memory: 4Gi }
      limits: { cpu: 8000m, memory: 16Gi }
    autoscaling:
      enabled: true
      minReplicas: 1
      maxReplicas: 15

  load:
    replicas: 2
    resources:
      requests: { cpu: 1000m, memory: 2Gi }
      limits: { cpu: 4000m, memory: 8Gi }
    autoscaling:
      enabled: true
      minReplicas: 1
      maxReplicas: 20

  mcp:
    replicas: 1
    resources:
      requests: { cpu: 500m, memory: 1Gi }
      limits: { cpu: 2000m, memory: 4Gi }
    autoscaling:
      enabled: true
      minReplicas: 1
      maxReplicas: 10

# MCP Gateway
mcpGateway:
  replicas: 2
  resources:
    requests: { cpu: 500m, memory: 512Mi }
    limits: { cpu: 2000m, memory: 2Gi }
  ingress:
    enabled: true
    hosts:
      - host: mcp.flowforge.example.com

# Temporal
temporal:
  enabled: true  # Deploy Temporal as subchart
  server:
    replicas: 3
    resources:
      requests: { cpu: 1000m, memory: 2Gi }
  persistence:
    default:
      driver: sql
      sql:
        driver: postgres
        # Uses the shared PostgreSQL instance
  web:
    enabled: true
    ingress:
      enabled: true
      hosts:
        - host: temporal.flowforge.example.com

# PostgreSQL
postgresql:
  enabled: true  # Deploy in-cluster (or set to false for managed)
  auth:
    postgresPassword: <strong-password>
    database: flowforge
  primary:
    persistence:
      size: 100Gi
  readReplicas:
    replicaCount: 1

# External PostgreSQL (if postgresql.enabled=false)
externalPostgresql:
  host: my-rds-instance.region.rds.amazonaws.com
  port: 5432
  database: flowforge
  existingSecret: flowforge-pg-credentials

# Redis
redis:
  enabled: true
  architecture: replication
  auth:
    password: <strong-password>
  master:
    persistence:
      size: 10Gi

# Vault
vault:
  enabled: true
  server:
    ha:
      enabled: true
      replicas: 3

# Observability
prometheus:
  enabled: true
  server:
    retention: 30d
    persistentVolume:
      size: 50Gi

grafana:
  enabled: true
  adminPassword: <strong-password>
  dashboardProviders:
    - name: flowforge
      folder: FlowForge
      type: file
      options:
        path: /var/lib/grafana/dashboards/flowforge
  dashboards:
    flowforge:
      system-overview:
        file: dashboards/system-overview.json
      connector-performance:
        file: dashboards/connector-performance.json
      mcp-metrics:
        file: dashboards/mcp-metrics.json
```

### Verifying the Installation

```bash
# Check all pods are running
kubectl get pods -n flowforge

# Check services
kubectl get svc -n flowforge

# Check ingress
kubectl get ingress -n flowforge

# View API logs
kubectl logs -n flowforge -l app=flowforge-api -f

# View worker logs
kubectl logs -n flowforge -l app=flowforge-worker-extract -f

# Port-forward for local access
kubectl port-forward -n flowforge svc/flowforge-api 8080:8080
kubectl port-forward -n flowforge svc/temporal-web 8088:8088
kubectl port-forward -n flowforge svc/grafana 3000:3000
```

---

## 3. Kubernetes Operator (Enterprise)

The FlowForge Operator manages the full lifecycle of FlowForge clusters via Custom Resource Definitions (CRDs).

### CRDs

**FlowForgeCluster** — Manages the entire FlowForge installation:

```yaml
apiVersion: flowforge.io/v1
kind: FlowForgeCluster
metadata:
  name: production
  namespace: flowforge
spec:
  version: 1.0.0

  api:
    replicas: 3
    resources:
      requests: { cpu: 1, memory: 1Gi }

  workers:
    extract:
      minReplicas: 2
      maxReplicas: 50
      scalePolicy:
        queueDepthThreshold: 10
        scaleUpStabilization: 30s
        scaleDownStabilization: 5m
    transform:
      minReplicas: 2
      maxReplicas: 30
    load:
      minReplicas: 2
      maxReplicas: 50
    mcp:
      minReplicas: 1
      maxReplicas: 20
      scaleToZero: true
      scaleUpTrigger: queueDepth > 0

  temporal:
    replicas: 3
    persistence:
      type: postgresql
      size: 200Gi

  security:
    tls:
      enabled: true
      issuer: letsencrypt-prod
    rbac:
      enabled: true
    audit:
      enabled: true
      retention: 90d

  observability:
    metrics:
      enabled: true
    tracing:
      enabled: true
      samplingRate: 0.1
    logging:
      level: info
```

**FlowForgeSync** — Declarative sync configuration:

```yaml
apiVersion: flowforge.io/v1
kind: FlowForgeSync
metadata:
  name: salesforce-to-bigquery
  namespace: flowforge
spec:
  source:
    connector: salesforce
    connectionRef: salesforce-prod
    streams:
      - name: contacts
        syncMode: incremental
      - name: opportunities
        syncMode: incremental
  destination:
    connector: bigquery
    connectionRef: bigquery-analytics
    dataset: salesforce_mirror
  schedule:
    cron: "*/15 * * * *"
    timezone: America/New_York
  mapping:
    template: salesforce-to-bigquery-default
```

### Installing the Operator

```bash
# Install CRDs
kubectl apply -f deploy/operator/config/crd/bases/

# Install the operator
kubectl apply -f deploy/operator/config/manager/

# Create a FlowForgeCluster
kubectl apply -f my-cluster.yaml

# Watch the operator reconcile
kubectl logs -n flowforge-system -l app=flowforge-operator -f
```

---

## Auto-Scaling Strategy

FlowForge implements intelligent auto-scaling at multiple levels:

### Task Queue Depth Scaling

A custom Kubernetes metrics adapter exposes Temporal task queue depth:

```
Temporal Task Queue Depth → Custom Metrics Adapter → HPA → Scale Workers
```

When queue depth exceeds threshold (default: 10 pending tasks), HPA scales the corresponding worker deployment.

### Predictive Scaling

For scheduled syncs, FlowForge pre-scales workers 2 minutes before known high-load periods:

```
Schedule: "0 */6 * * *" (every 6 hours)
    │
    ▼
Pre-scale at xx:58 → Workers ready by xx:00 → Sync starts immediately
```

### Scale-to-Zero

Worker deployments for infrequently-used connectors scale to zero and spin up on demand:

- Go workers: <5 second cold start
- Container connectors: <10 second cold start
- Uses KEDA ScaledObject or custom controller

### Resource Quotas

Per-tenant resource quotas prevent monopolization in multi-tenant deployments:

```yaml
tenantQuotas:
  default:
    maxConcurrentSyncs: 10
    maxWorkersPerSync: 5
    maxRecordsPerSync: 10000000
  enterprise:
    maxConcurrentSyncs: 100
    maxWorkersPerSync: 20
    maxRecordsPerSync: unlimited
```

---

## Infrastructure Requirements

### AWS Reference Architecture

```
┌─────────────────────────────────────────┐
│                  VPC                     │
│  ┌───────────────────────────────────┐  │
│  │           EKS Cluster              │  │
│  │  ┌──────┐ ┌──────┐ ┌──────┐      │  │
│  │  │ API  │ │Worker│ │ MCP  │      │  │
│  │  │ Pods │ │ Pods │ │ Pods │      │  │
│  │  └──────┘ └──────┘ └──────┘      │  │
│  └───────────────────────────────────┘  │
│                                          │
│  ┌──────────┐  ┌──────────┐             │
│  │  RDS     │  │ElastiCache│             │
│  │PostgreSQL│  │  Redis    │             │
│  │ (Multi-  │  │(Cluster)  │             │
│  │  AZ)     │  │           │             │
│  └──────────┘  └──────────┘             │
│                                          │
│  ┌──────────┐  ┌──────────┐             │
│  │   S3     │  │  Secrets  │             │
│  │ Bucket   │  │ Manager   │             │
│  └──────────┘  └──────────┘             │
│                                          │
│  ┌──────────┐                            │
│  │  ALB /   │                            │
│  │  NLB     │                            │
│  └──────────┘                            │
└─────────────────────────────────────────┘
```

| Service | AWS | GCP | Azure |
|---|---|---|---|
| Kubernetes | EKS | GKE | AKS |
| PostgreSQL | RDS PostgreSQL | Cloud SQL | Azure Database for PostgreSQL |
| Redis | ElastiCache | Memorystore | Azure Cache for Redis |
| Object Storage | S3 | GCS | Blob Storage |
| Secrets | Secrets Manager | Secret Manager | Key Vault |
| Load Balancer | ALB/NLB | Cloud Load Balancing | Azure Load Balancer |
| DNS | Route53 | Cloud DNS | Azure DNS |
| TLS | ACM | Managed Certificates | App Gateway |

---

## Monitoring and Observability

### Grafana Dashboards

FlowForge ships with pre-built dashboards:

| Dashboard | Metrics |
|---|---|
| **System Overview** | Total syncs running, records/sec, error rate, queue depth, worker count |
| **Connector Performance** | Per-connector throughput, latency, error rates, API quota usage |
| **Sync History** | Sync duration trends, success/fail ratios, data volume over time |
| **MCP Metrics** | Tool call latency, calls/sec, error rates, agent activity |
| **Worker Pool** | CPU/memory utilization, task processing rate, queue wait times |
| **Temporal Health** | Workflow/activity latency, failure rates, namespace stats |

### Alerting

Pre-configured Prometheus alert rules:

```yaml
groups:
  - name: flowforge-alerts
    rules:
      - alert: SyncFailureRate
        expr: rate(flowforge_sync_failures_total[5m]) > 0.05
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Sync failure rate above 5%"

      - alert: WorkerQueueBacklog
        expr: temporal_task_queue_depth > 100
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Task queue backlog exceeding 100"

      - alert: MCPLatencyHigh
        expr: histogram_quantile(0.95, flowforge_mcp_tool_call_duration_seconds) > 2
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "MCP tool call P95 latency above 2 seconds"
```

---

## Backup and Recovery

### PostgreSQL Backup

```bash
# Automated daily backup (in Docker Compose)
docker compose exec postgresql pg_dump -U flowforge flowforge > backup_$(date +%Y%m%d).sql

# For Kubernetes, use the PostgreSQL operator's backup CRD
# or configure managed database backups (RDS snapshots, etc.)
```

### Temporal Recovery

Temporal persists all workflow state in its database. Recovery is automatic:
- Worker crashes → Temporal replays workflows from last state
- Temporal server crashes → Restarts and resumes from PostgreSQL
- Full cluster restart → All in-flight syncs resume automatically

### Vault Backup

```bash
# Vault snapshot (Raft storage)
vault operator raft snapshot save backup_$(date +%Y%m%d).snap
```
