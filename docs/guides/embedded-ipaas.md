# Embedded iPaaS Guide

This guide covers how SaaS vendors embed FlowForge's integration capabilities into their own products, offering native integrations to their customers.

---

## Overview

FlowForge's embedded iPaaS mode provides:

- **Headless API**: Every operation available via REST and GraphQL — no UI dependency
- **Multi-Tenant Isolation**: Each end-customer gets an isolated namespace
- **White-Label Ready**: Configurable error messages, webhook URLs, and OAuth redirects
- **Embeddable UI Components**: Optional React component library for drop-in UX

```
┌──────────────────────────────────────┐
│         Your SaaS Application         │
│                                       │
│  ┌─────────────┐  ┌───────────────┐  │
│  │ Your Auth   │  │ FlowForge-UI  │  │
│  │ System      │  │ Components    │  │
│  └──────┬──────┘  └───────┬───────┘  │
│         │                  │          │
│         └──────┬───────────┘          │
│                │                      │
│         ┌──────▼──────┐               │
│         │ Your Backend │               │
│         └──────┬──────┘               │
└────────────────┼──────────────────────┘
                 │
                 │ REST / GraphQL API
                 ▼
┌──────────────────────────────────────┐
│         FlowForge Backend             │
│                                       │
│  ┌──────────┐  ┌──────────────────┐  │
│  │ Tenant   │  │ Connector Runtime │  │
│  │ Manager  │  │ (Temporal Workers)│  │
│  └──────────┘  └──────────────────┘  │
│                                       │
│  ┌──────────┐  ┌──────────────────┐  │
│  │ OAuth    │  │ Sync Engine      │  │
│  │ Manager  │  │ (Temporal WFs)   │  │
│  └──────────┘  └──────────────────┘  │
└──────────────────────────────────────┘
```

---

## Integration Lifecycle API

The full integration lifecycle is managed through a RESTful API.

### Endpoints

| Endpoint | Method | Purpose |
|---|---|---|
| `/v1/integrations` | POST | Create a new integration for an end-customer |
| `/v1/integrations/{id}/connect` | POST | Initiate OAuth flow or credential validation |
| `/v1/integrations/{id}/catalog` | GET | Return discoverable streams/objects |
| `/v1/integrations/{id}/mappings` | PUT | Configure field mappings |
| `/v1/integrations/{id}/syncs` | POST | Create and start a sync job |
| `/v1/integrations/{id}/syncs/{syncId}` | GET | Get sync status, progress, errors |
| `/v1/integrations/{id}/syncs/{syncId}/pause` | POST | Pause a running sync |
| `/v1/integrations/{id}/webhooks` | POST | Register webhooks for sync events |
| `/v1/integrations/{id}/mcp` | POST | Provision an MCP server |

### Step-by-Step: Setting Up an Integration

#### 1. Create an Integration

```bash
curl -X POST https://flowforge.example.com/v1/integrations \
  -H "Authorization: Bearer $VENDOR_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "tenant_id": "customer-123",
    "connector_type": "salesforce",
    "display_name": "Salesforce CRM",
    "metadata": {
      "customer_plan": "enterprise",
      "region": "us-east-1"
    }
  }'
```

Response:
```json
{
  "id": "int_abc123",
  "tenant_id": "customer-123",
  "connector_type": "salesforce",
  "status": "pending_connection",
  "created_at": "2026-02-10T15:00:00Z"
}
```

#### 2. Connect (OAuth)

```bash
curl -X POST https://flowforge.example.com/v1/integrations/int_abc123/connect \
  -H "Authorization: Bearer $VENDOR_API_KEY" \
  -d '{
    "auth_type": "oauth2",
    "redirect_uri": "https://your-app.com/integrations/callback"
  }'
```

Response:
```json
{
  "authorization_url": "https://login.salesforce.com/services/oauth2/authorize?client_id=...&redirect_uri=...&state=...",
  "state": "oauth_state_xyz",
  "expires_in": 600
}
```

Your app redirects the end-customer to the `authorization_url`. After they authorize, Salesforce redirects back to your `redirect_uri` with an auth code. FlowForge handles the token exchange automatically.

#### 3. Discover Available Streams

```bash
curl https://flowforge.example.com/v1/integrations/int_abc123/catalog \
  -H "Authorization: Bearer $VENDOR_API_KEY"
```

Response:
```json
{
  "streams": [
    {
      "name": "contacts",
      "display_name": "Contacts",
      "sync_modes": ["full_refresh", "incremental"],
      "default_cursor_field": ["SystemModstamp"],
      "primary_key": [["Id"]],
      "schema": {
        "type": "object",
        "properties": {
          "Id": { "type": "string" },
          "FirstName": { "type": "string" },
          "LastName": { "type": "string" },
          "Email": { "type": "string" },
          "Phone": { "type": "string" },
          "AccountId": { "type": "string" },
          "SystemModstamp": { "type": "string", "format": "date-time" }
        }
      }
    },
    {
      "name": "opportunities",
      "display_name": "Opportunities",
      "sync_modes": ["full_refresh", "incremental"],
      "schema": { ... }
    }
  ]
}
```

#### 4. Configure Field Mappings

```bash
curl -X PUT https://flowforge.example.com/v1/integrations/int_abc123/mappings \
  -H "Authorization: Bearer $VENDOR_API_KEY" \
  -d '{
    "stream": "contacts",
    "mappings": [
      {
        "source_field": "Email",
        "destination_field": "email_address",
        "transform": null
      },
      {
        "source_field": ["FirstName", "LastName"],
        "destination_field": "full_name",
        "transform": "concat(source.FirstName, \" \", source.LastName)"
      },
      {
        "source_field": "Phone",
        "destination_field": "phone",
        "transform": "normalize_phone(source.Phone, \"E164\")"
      }
    ]
  }'
```

#### 5. Create and Start a Sync

```bash
curl -X POST https://flowforge.example.com/v1/integrations/int_abc123/syncs \
  -H "Authorization: Bearer $VENDOR_API_KEY" \
  -d '{
    "streams": [
      {
        "name": "contacts",
        "sync_mode": "incremental"
      }
    ],
    "destination": {
      "type": "webhook",
      "url": "https://your-app.com/api/integrations/data",
      "auth_header": "Bearer your-internal-token"
    },
    "schedule": {
      "cron": "*/15 * * * *",
      "timezone": "America/New_York"
    }
  }'
```

Response:
```json
{
  "sync_id": "sync_def456",
  "status": "running",
  "next_run": "2026-02-10T15:15:00Z",
  "workflow_id": "sync-def456-run-001"
}
```

#### 6. Monitor Sync Status

```bash
curl https://flowforge.example.com/v1/integrations/int_abc123/syncs/sync_def456 \
  -H "Authorization: Bearer $VENDOR_API_KEY"
```

Response:
```json
{
  "sync_id": "sync_def456",
  "status": "completed",
  "last_run": {
    "started_at": "2026-02-10T15:15:00Z",
    "completed_at": "2026-02-10T15:15:28Z",
    "records_extracted": 5432,
    "records_loaded": 5432,
    "errors": 0,
    "duration_seconds": 28
  },
  "next_run": "2026-02-10T15:30:00Z",
  "total_runs": 47,
  "success_rate": 0.978
}
```

#### 7. Register Webhooks

```bash
curl -X POST https://flowforge.example.com/v1/integrations/int_abc123/webhooks \
  -H "Authorization: Bearer $VENDOR_API_KEY" \
  -d '{
    "url": "https://your-app.com/api/integrations/events",
    "events": ["sync.completed", "sync.failed", "schema.changed"],
    "secret": "webhook-signing-secret"
  }'
```

FlowForge sends webhook payloads signed with HMAC-SHA256:

```json
{
  "event": "sync.completed",
  "integration_id": "int_abc123",
  "sync_id": "sync_def456",
  "timestamp": "2026-02-10T15:15:28Z",
  "data": {
    "records_synced": 5432,
    "duration_seconds": 28
  }
}
```

---

## Multi-Tenant Isolation

Each end-customer tenant gets full isolation:

### Namespace Isolation

```
Tenant: customer-123
├── Temporal Namespace: flowforge-customer-123
├── Credentials: vault/tenants/customer-123/*
├── Sync State: postgres/state WHERE tenant_id = 'customer-123'
├── Resource Quota: max 10 concurrent syncs, 5 workers
└── Data Boundary: US-East only
```

### Resource Limits

Configure per-tenant resource quotas:

```bash
curl -X PUT https://flowforge.example.com/v1/tenants/customer-123/quotas \
  -H "Authorization: Bearer $VENDOR_API_KEY" \
  -d '{
    "max_concurrent_syncs": 10,
    "max_records_per_sync": 1000000,
    "max_workers": 5,
    "max_integrations": 20,
    "max_mcp_servers": 5,
    "rate_limit_requests_per_minute": 100
  }'
```

### Data Boundaries

Ensure customer data stays within geographic regions:

```json
{
  "tenant_id": "customer-123",
  "data_residency": {
    "processing_region": "us-east-1",
    "storage_region": "us-east-1",
    "allow_cross_region": false
  }
}
```

---

## White-Label Configuration

Customize FlowForge's customer-facing surfaces for your brand:

```bash
curl -X PUT https://flowforge.example.com/v1/vendors/your-company/branding \
  -H "Authorization: Bearer $VENDOR_API_KEY" \
  -d '{
    "display_name": "YourApp Integrations",
    "oauth_redirect_base": "https://your-app.com/integrations/oauth",
    "webhook_base_url": "https://hooks.your-app.com",
    "error_messages": {
      "connection_failed": "Unable to connect to {connector}. Please check your credentials in YourApp Settings.",
      "sync_failed": "Data sync encountered an issue. Contact support@your-app.com for help.",
      "rate_limited": "The {connector} API rate limit was reached. YourApp will retry automatically."
    },
    "support_url": "https://your-app.com/support",
    "docs_url": "https://docs.your-app.com/integrations"
  }'
```

---

## Embeddable React UI Components

FlowForge-UI provides drop-in React components for common integration UX patterns.

### Installation

```bash
npm install @flowforge/ui
```

### ConnectorSelector

Browse and select connectors:

```tsx
import { ConnectorSelector } from '@flowforge/ui';

function IntegrationSetup() {
  return (
    <ConnectorSelector
      apiBaseUrl="https://flowforge.example.com"
      apiKey={vendorApiKey}
      tenantId={currentCustomerId}
      categories={['crm', 'warehouse', 'support']}  // Filter categories
      onSelect={(connector) => {
        // User selected a connector — initiate connection
        startOAuthFlow(connector.id);
      }}
      theme={{
        primaryColor: '#your-brand-color',
        borderRadius: '8px',
      }}
    />
  );
}
```

### FieldMapper

Visual drag-and-drop field mapping:

```tsx
import { FieldMapper } from '@flowforge/ui';

function MappingConfiguration({ integrationId }) {
  return (
    <FieldMapper
      apiBaseUrl="https://flowforge.example.com"
      apiKey={vendorApiKey}
      integrationId={integrationId}
      sourceStream="contacts"
      destinationSchema={yourAppSchema}
      onSave={(mappings) => {
        // User configured mappings — save and start sync
        saveMappings(integrationId, mappings);
      }}
      features={{
        autoMapping: true,      // Enable AI-assisted mapping
        expressions: true,      // Allow JSONPath expressions
        typeCoercion: true,     // Show type casting options
      }}
    />
  );
}
```

### SyncMonitor

Real-time sync progress and history:

```tsx
import { SyncMonitor } from '@flowforge/ui';

function IntegrationDashboard({ integrationId }) {
  return (
    <SyncMonitor
      apiBaseUrl="https://flowforge.example.com"
      apiKey={vendorApiKey}
      integrationId={integrationId}
      showHistory={true}        // Show past sync runs
      showNextRun={true}        // Show countdown to next scheduled run
      onPause={(syncId) => pauseSync(syncId)}
      onResume={(syncId) => resumeSync(syncId)}
    />
  );
}
```

### SchemaViewer

Inspect discovered schemas:

```tsx
import { SchemaViewer } from '@flowforge/ui';

function SchemaInspection({ integrationId }) {
  return (
    <SchemaViewer
      apiBaseUrl="https://flowforge.example.com"
      apiKey={vendorApiKey}
      integrationId={integrationId}
      showDrift={true}           // Highlight schema changes
      showVersionHistory={true}  // Show schema version timeline
    />
  );
}
```

---

## GraphQL API

For flexible frontend querying, FlowForge provides a GraphQL API alongside REST:

```graphql
# Query integration status with nested data
query GetIntegration($id: ID!) {
  integration(id: $id) {
    id
    connectorType
    status
    connection {
      authenticated
      lastChecked
    }
    syncs {
      id
      status
      schedule {
        cron
        timezone
        nextRun
      }
      lastRun {
        startedAt
        completedAt
        recordsExtracted
        recordsLoaded
        errors
      }
    }
    catalog {
      streams {
        name
        syncModes
        schema
      }
    }
  }
}

# Subscription: real-time sync progress
subscription WatchSync($syncId: ID!) {
  syncProgress(syncId: $syncId) {
    phase
    recordsProcessed
    totalRecords
    throughputPerSecond
    estimatedCompletion
    errors {
      message
      recordIndex
    }
  }
}
```

---

## Data Mapping Engine

FlowForge's mapping engine operates at multiple levels.

### Automatic Mapping

When schemas are discovered, FlowForge automatically suggests mappings:

1. **Name Matching**: Levenshtein distance between field names (`first_name` ↔ `FirstName`)
2. **Type Compatibility**: Fields with matching types score higher
3. **Standard Field Recognition**: Built-in patterns for common fields:
   - Email: `email`, `email_address`, `EmailAddress`, `e_mail`
   - Phone: `phone`, `phone_number`, `PhoneNumber`, `tel`
   - Name: `name`, `full_name`, `FullName`, `display_name`
   - Created: `created_at`, `CreatedDate`, `created_date`, `creation_time`
4. **AI-Assisted** (optional): LLM-powered semantic understanding

### Expression Language

JSONPath-based expressions for complex transforms:

| Expression | Description | Example |
|---|---|---|
| `source.field` | Direct field reference | `source.Email` |
| `concat(a, b)` | String concatenation | `concat(source.FirstName, " ", source.LastName)` |
| `split(str, sep, idx)` | Split and select | `split(source.FullName, " ", 0)` → first name |
| `date_format(field, fmt)` | Date formatting | `date_format(source.CreatedDate, "YYYY-MM-DD")` |
| `if(cond, then, else)` | Conditional | `if(source.Status == "Active", true, false)` |
| `lookup(table, key)` | Lookup table | `lookup("country_codes", source.Country)` |
| `coalesce(a, b, c)` | First non-null | `coalesce(source.WorkEmail, source.PersonalEmail)` |
| `normalize_phone(p, fmt)` | Phone normalization | `normalize_phone(source.Phone, "E164")` |
| `lowercase(str)` | Lowercase | `lowercase(source.Email)` |
| `regex_extract(str, pat)` | Regex extraction | `regex_extract(source.URL, "https?://([^/]+)")` |

### Mapping Templates

Pre-built templates for common integration patterns:

```bash
# List available templates
curl https://flowforge.example.com/v1/mapping-templates \
  -H "Authorization: Bearer $VENDOR_API_KEY"

# Apply a template
curl -X POST https://flowforge.example.com/v1/integrations/int_abc123/mappings/apply-template \
  -d '{"template": "salesforce-contact-to-hubspot-contact"}'
```

Available templates:
- Salesforce Contact ↔ HubSpot Contact
- BigQuery analytics → Salesforce custom objects
- Zendesk tickets ↔ Intercom conversations
- Gong call transcripts → BigQuery analytics
- S3 event data → Snowflake staging tables

---

## Destination Modes

When embedding, you can send synced data to several destination types:

### Webhook Destination

FlowForge sends records to your app via HTTP:

```json
{
  "destination": {
    "type": "webhook",
    "url": "https://your-app.com/api/integrations/data",
    "auth_header": "Bearer your-token",
    "batch_size": 100,
    "format": "jsonl"
  }
}
```

### Direct Database Write

Write directly to your database:

```json
{
  "destination": {
    "type": "postgresql",
    "connection_string": "postgres://user:pass@host/db",
    "schema": "integrations",
    "table": "salesforce_contacts",
    "sync_mode": "upsert",
    "upsert_key": ["external_id"]
  }
}
```

### Warehouse Destination

For analytics use cases:

```json
{
  "destination": {
    "type": "bigquery",
    "connection_ref": "bq-analytics",
    "dataset": "customer_data",
    "sync_mode": "merge"
  }
}
```

---

## Security Considerations

### API Authentication

All embedded API calls require a vendor API key:

```
Authorization: Bearer ff_vendor_key_abc123...
```

Vendor keys are scoped:
- **Full Access**: All tenant operations
- **Tenant-Scoped**: Operations for specific tenants only
- **Read-Only**: Monitoring and status queries only

### Credential Handling

End-customer credentials (OAuth tokens, API keys) are:
- Stored in HashiCorp Vault, encrypted at rest
- Never returned in API responses
- Automatically refreshed when expired
- Scoped to the tenant namespace (cross-tenant access impossible)

### Webhook Security

All webhook deliveries include an HMAC-SHA256 signature:

```
X-FlowForge-Signature: sha256=<hmac>
X-FlowForge-Timestamp: 1707580528
```

Verify in your app:

```python
import hmac, hashlib

def verify_webhook(payload: bytes, signature: str, timestamp: str, secret: str) -> bool:
    expected = hmac.new(
        secret.encode(),
        f"{timestamp}.{payload.decode()}".encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(f"sha256={expected}", signature)
```
