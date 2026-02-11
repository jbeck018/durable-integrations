---
title: Embedded iPaaS
layout: default
parent: Guides
nav_order: 3
description: "Embed FlowForge's integration capabilities into your SaaS application."
permalink: /guides/embedded-ipaas/
---

# Embedded iPaaS Guide

Embed FlowForge's integration capabilities into your SaaS application. Offer native integrations to your customers with a headless API and optional UI components.

---

## Overview

FlowForge's embedded iPaaS mode provides:

- **Headless API**: Every operation available via REST and GraphQL
- **Multi-Tenant Isolation**: Each end-customer gets an isolated namespace with a dedicated Neon Postgres database
- **White-Label Ready**: Configurable error messages, webhook URLs, and OAuth redirects
- **Embeddable UI Components**: React, Angular, and Svelte component libraries

---

## Integration Lifecycle API

| Endpoint | Method | Purpose |
|:--|:--|:--|
| `/v1/integrations` | POST | Create a new integration |
| `/v1/integrations/{id}/connect` | POST | Initiate OAuth flow |
| `/v1/integrations/{id}/catalog` | GET | Return discoverable streams |
| `/v1/integrations/{id}/mappings` | PUT | Configure field mappings |
| `/v1/integrations/{id}/syncs` | POST | Create and start a sync |
| `/v1/integrations/{id}/syncs/{syncId}` | GET | Get sync status |
| `/v1/integrations/{id}/webhooks` | POST | Register webhooks |
| `/v1/integrations/{id}/mcp` | POST | Provision an MCP server |

### Step 1: Create an Integration

```bash
curl -X POST https://flowforge.example.com/v1/integrations \
  -H "Authorization: Bearer $VENDOR_API_KEY" \
  -d '{
    "tenant_id": "customer-123",
    "connector_type": "salesforce",
    "display_name": "Salesforce CRM"
  }'
```

### Step 2: Connect (OAuth)

```bash
curl -X POST https://flowforge.example.com/v1/integrations/int_abc123/connect \
  -d '{
    "auth_type": "oauth2",
    "redirect_uri": "https://your-app.com/integrations/callback"
  }'
```

Your app redirects the end-customer to the `authorization_url`. FlowForge handles the token exchange via a secure Backend-for-Frontend (BFF) pattern with PKCE.

### Step 3: Discover Streams

```bash
curl https://flowforge.example.com/v1/integrations/int_abc123/catalog
```

### Step 4: Configure Mappings

```bash
curl -X PUT https://flowforge.example.com/v1/integrations/int_abc123/mappings \
  -d '{
    "stream": "contacts",
    "mappings": [
      { "source_field": "Email", "destination_field": "email_address" },
      { "source_field": ["FirstName", "LastName"], "destination_field": "full_name",
        "transform": "concat(source.FirstName, \" \", source.LastName)" }
    ]
  }'
```

### Step 5: Start a Sync

```bash
curl -X POST https://flowforge.example.com/v1/integrations/int_abc123/syncs \
  -d '{
    "streams": [{ "name": "contacts", "sync_mode": "incremental" }],
    "destination": {
      "type": "webhook",
      "url": "https://your-app.com/api/integrations/data"
    },
    "schedule": { "cron": "*/15 * * * *" }
  }'
```

### Step 6: Register Webhooks

```bash
curl -X POST https://flowforge.example.com/v1/integrations/int_abc123/webhooks \
  -d '{
    "url": "https://your-app.com/api/integrations/events",
    "events": ["sync.completed", "sync.failed", "schema.changed"],
    "secret": "webhook-signing-secret"
  }'
```

---

## Embeddable UI Components

FlowForge provides UI components for React, Angular, and Svelte via a headless core architecture.

### React

```bash
npm install @flowforge/ui-react
```

```tsx
import { ConnectorSelector, FieldMapper, SyncMonitor } from '@flowforge/ui-react';

<ConnectorSelector
  apiBaseUrl="https://flowforge.example.com"
  apiKey={vendorApiKey}
  tenantId={customerId}
  onSelect={(connector) => startOAuthFlow(connector.id)}
/>

<FieldMapper
  integrationId={integrationId}
  sourceStream="contacts"
  destinationSchema={yourAppSchema}
  onSave={(mappings) => saveMappings(mappings)}
/>

<SyncMonitor
  integrationId={integrationId}
  showHistory={true}
  onPause={(syncId) => pauseSync(syncId)}
/>
```

### Angular

```bash
npm install @flowforge/ui-angular
```

### Svelte

```bash
npm install @flowforge/ui-svelte
```

All three frameworks share the same `@flowforge/ui-core` state management layer, ensuring identical behavior.

---

## Multi-Tenant Isolation

Each end-customer tenant gets full isolation:

- **Database**: Dedicated Neon Postgres project (scale-to-zero)
- **Temporal**: Isolated namespace (`tenant-{id}`)
- **Blob Storage**: S3 prefix (`tenants/{id}/`)
- **Cache**: Redis key prefix (`flowforge:{id}:`)
- **Resource Quotas**: Configurable limits per tenant

---

## Security

### API Authentication

All embedded API calls require a vendor API key scoped by access level:
- **Full Access**: All tenant operations
- **Tenant-Scoped**: Operations for specific tenants only
- **Read-Only**: Monitoring and status queries only

### Credential Handling

End-customer credentials (OAuth tokens, API keys) are:
- Stored in HashiCorp Vault, encrypted at rest
- Never returned in API responses
- Automatically refreshed when expired
- Scoped to the tenant namespace

### Webhook Security

All webhook deliveries include HMAC-SHA256 signatures:

```
X-FlowForge-Signature: sha256=<hmac>
X-FlowForge-Timestamp: 1707580528
```
