---
title: Tenant Isolation
layout: default
parent: Architecture
nav_order: 3
description: "Per-tenant isolation with Neon project-per-tenant Postgres, Temporal namespaces, S3 prefixes, and Redis key scoping."
permalink: /architecture/tenant-isolation/
---

# Multi-Tenant Isolation

FlowForge provides full per-tenant isolation across all infrastructure layers. Each tenant gets its own database, Temporal namespace, blob storage prefix, and cache keyspace.

---

## Isolation Layers

| Layer | Isolation Method | Key Pattern |
|:--|:--|:--|
| **Database** | Neon project-per-tenant | Each tenant gets a dedicated Neon Postgres project with scale-to-zero compute |
| **Temporal** | Namespace-per-tenant | `tenant-{tenant_id}` namespace prevents workflow interference |
| **Blob Storage** | S3 prefix-per-tenant | `tenants/{tenant_id}/` prefix on all S3 operations |
| **Cache** | Redis key prefix | `flowforge:{tenant_id}:{purpose}:{key}` pattern |
| **Rate Limiting** | Per-tenant counters | `flowforge:{tenant_id}:ratelimit:{connector}:{window}` |

---

## Neon Project-per-Tenant Database

Each tenant gets an isolated Postgres database provisioned via the [Neon API](https://neon.tech). This eliminates noisy-neighbor problems at the database level.

### How It Works

1. When `CreateTenant` is called, the `NeonProvisioner` creates a new Neon project
2. The project is configured with scale-to-zero compute (suspends after 5 minutes idle)
3. The connection URI is encrypted and stored in the catalog database
4. A `TenantRouter` maintains a per-tenant connection pool cache with lazy initialization

### TenantRouter

The `TenantRouter` provides per-tenant database connections with:

- **Lazy initialization**: Connection pools created on first access
- **Double-check locking**: Safe concurrent pool creation
- **Idle cleanup**: Pools unused for 5+ minutes are closed (Neon handles compute suspension)

```go
// Get a database connection for a specific tenant
db, err := tenantRouter.ForTenant(ctx, tenantID)
if err != nil {
    return err
}
// Use db for tenant-scoped queries
```

### Connection URI Encryption

Tenant connection URIs are stored encrypted using AES-256-GCM with HKDF key derivation:

```go
encryptor, _ := encryption.NewEncryptor(os.Getenv("FLOWFORGE_ENCRYPTION_KEY"))
resolver := postgres.NewCatalogTenantURIResolver(catalogDB, encryptor)
router := postgres.NewTenantRouter(postgres.TenantRouterConfig{
    Catalog:  catalogDB,
    Resolver: resolver,
})
```

### Neon Configuration

| Setting | Value | Purpose |
|:--|:--|:--|
| `pg_version` | 16 | PostgreSQL version |
| `autoscaling_limit_min_cu` | 0.25 | Minimum compute (scale-to-zero) |
| `autoscaling_limit_max_cu` | 2 | Maximum compute |
| `suspend_timeout_seconds` | 300 | Idle suspend after 5 minutes |

---

## Per-Tenant Temporal Namespaces

Each tenant gets its own Temporal namespace. This prevents one tenant's heavy workflow load from affecting another's scheduling.

```go
// Register a namespace for a new tenant
client.EnsureTenantNamespace(ctx, tenantID)

// Start a workflow in a tenant's namespace
client.StartTenantSyncWorkflow(ctx, tenantID, workflowID, params)
```

The namespace naming convention is `tenant-{tenant_id}`. Namespace registration is idempotent.

---

## Per-Tenant S3 Prefix Isolation

All blob storage operations are scoped to a tenant prefix:

```go
// Create a tenant-scoped blob client
tenantBlob := blobClient.WithTenantPrefix(tenantID)

// All operations are automatically prefixed with tenants/{tenant_id}/
tenantBlob.Upload(ctx, "data/file.json", reader)
// Actually stores at: tenants/{tenant_id}/data/file.json
```

---

## Per-Tenant Redis Key Isolation

All Redis keys follow the pattern: `flowforge:{tenant_id}:{purpose}:{key}`

```go
// Create tenant-scoped caches
configCache := redis.NewTenantConfigCache(redisClient, tenantID)
schemaCache := redis.NewTenantSchemaCache(redisClient, tenantID)

// Rate limiter keys
ratelimitKey := ratelimit.BuildKeyWithWindow(tenantID, "salesforce", "minute")
// → "flowforge:{tenant_id}:ratelimit:salesforce:minute"
```

---

## Resource Quotas

Each tenant has configurable resource quotas enforced by the `TenantManager`:

```go
type ResourceQuotas struct {
    MaxConnectors      int   // Default: 10
    MaxSyncs           int   // Default: 20
    MaxRecordsPerMonth int64 // Default: 10,000,000
    MaxStorageBytes    int64 // Default: 10 GB
}
```

Quota checks are performed before creating connectors, starting syncs, and processing records:

```go
err := tenantManager.CheckQuota(ctx, tenantID, "connectors")
if err != nil {
    // Quota exceeded — return 429 to client
}
```
