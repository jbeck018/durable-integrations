---
title: Connector Development
layout: default
parent: Guides
nav_order: 1
description: "Build FlowForge connectors at three tiers: No-Code, Low-Code, and Full-Code CDK."
permalink: /guides/connector-development/
---

# Connector Development Guide

This guide walks you through building FlowForge connectors at all three tiers: No-Code, Low-Code, and Full-Code.

---

## Choosing Your Tier

| Tier | Time to Build | Best For | Language |
|:--|:--|:--|:--|
| **No-Code Builder** | 10-30 minutes | REST APIs with standard auth | YAML (declarative) |
| **Low-Code CDK** | 1-4 hours | APIs with complex pagination, auth, or transforms | YAML + Python/Go |
| **Full-Code CDK** | 1-3 days | Databases, protocols, complex bidirectional syncs | Go, Python, TypeScript, Rust |

---

## Tier 1: No-Code Connector Builder

The No-Code builder generates a declarative YAML manifest interpreted by the FlowForge runtime. No programming required.

### YAML Manifest Structure

```yaml
apiVersion: flowforge.io/v1
kind: Connector
metadata:
  name: github-issues
  version: 1.0.0
  type: source
  category: developer-tools
  description: "Extract issues from GitHub repositories"

spec:
  config:
    properties:
      repository:
        type: string
        description: "Repository in owner/repo format"
        pattern: "^[\\w.-]+/[\\w.-]+$"
      token:
        type: string
        description: "GitHub personal access token"
        secret: true

  auth:
    type: bearer_token
    token_field: token

  streams:
    - name: issues
      endpoint:
        url: "https://api.github.com/repos/{% raw %}{{config.repository}}{% endraw %}/issues"
        method: GET
        headers:
          Accept: "application/vnd.github.v3+json"
      pagination:
        type: link_header
      schema:
        primary_key: [id]
        properties:
          id: { type: integer }
          number: { type: integer }
          title: { type: string }
          state: { type: string }
          created_at: { type: string, format: date-time }
      incremental:
        cursor_field: updated_at
        cursor_param: since
      rate_limit:
        requests_per_second: 10
        retry_on: [429, 503]
```

### Auth Types

| Type | Description |
|:--|:--|
| `bearer_token` | Authorization: Bearer {token} |
| `api_key` | Custom header with API key |
| `oauth2` | OAuth 2.0 Authorization Code or Client Credentials |
| `basic` | HTTP Basic Authentication |
| `custom_header` | Arbitrary custom headers |

### Pagination Types

| Type | Description |
|:--|:--|
| `offset` | Offset-based (limit/offset params) |
| `cursor` | Cursor-based (opaque cursor token) |
| `page_number` | Page number with total pages |
| `keyset` | Keyset pagination (ordered by key field) |
| `link_header` | HTTP Link header (GitHub-style) |

---

## Tier 2: Low-Code CDK

Extend YAML manifests with custom Python functions for complex cases.

```yaml
spec:
  auth:
    type: custom
    handler: auth.py:get_auth_headers

  streams:
    - name: contacts
      pagination:
        type: custom
        handler: pagination.py:get_next_page
      response:
        transform: transforms.py:parse_contacts
```

```python
# auth.py
def get_auth_headers(config: dict) -> dict:
    import hmac, hashlib, time
    timestamp = str(int(time.time()))
    signature = hmac.new(
        config["api_key"].encode(),
        timestamp.encode(),
        hashlib.sha256
    ).hexdigest()
    return {
        "X-API-Key": config["api_key"],
        "X-Timestamp": timestamp,
        "X-Signature": signature,
    }
```

---

## Tier 3: Full-Code CDK (Go)

### Source Interface

```go
type Source interface {
    Spec() (*ConnectorSpec, error)
    Check(ctx context.Context, config json.RawMessage) (*CheckResult, error)
    Discover(ctx context.Context, config json.RawMessage) (*Catalog, error)
    Read(ctx context.Context, config json.RawMessage, catalog *ConfiguredCatalog,
         state State, output chan<- Message) error
}
```

### Destination Interface

```go
type Destination interface {
    Spec() (*ConnectorSpec, error)
    Check(ctx context.Context, config json.RawMessage) (*CheckResult, error)
    Write(ctx context.Context, config json.RawMessage, catalog *ConfiguredCatalog,
          input <-chan Message) (*WriteResult, error)
    Capabilities() []SyncMode
}
```

### Bidirectional Interface

```go
type Bidirectional interface {
    Source
    Destination
    Resolve(ctx context.Context, conflicts []Conflict) ([]Resolution, error)
    WebhookHandler(ctx context.Context, event WebhookEvent) error
}
```

### Registering Your Connector

```go
func init() {
    cdk.RegisterSource("myapi", myapi.New())
}
```

---

## Testing

```bash
# Run acceptance tests for a specific connector
make test-connector CONNECTOR=salesforce

# Run all connector tests
make test-connectors

# Run with integration (requires credentials)
make test-connector CONNECTOR=salesforce INTEGRATION=true
```

### Acceptance Test Categories

| Test | What It Validates |
|:--|:--|
| **Spec Validation** | Config schema is valid JSON Schema |
| **Connection Check** | Connects with valid creds, fails gracefully with invalid |
| **Discovery** | Returns at least one stream with valid schema |
| **Read Compliance** | Full refresh + incremental produce valid records |
| **Write Compliance** | Append, upsert, delete produce expected results |
| **Idempotency** | Re-running same sync produces identical results |
| **Performance Baseline** | Meets minimum throughput for its category |

---

## Packaging

### Native Plugin (First-Class)

Compiled directly into the worker binary:

```go
import _ "github.com/flowforge/flowforge/connectors/salesforce"
```

### OCI Container Image

```dockerfile
FROM flowforge/connector-base:latest
COPY myconnector /usr/local/bin/myconnector
ENTRYPOINT ["/usr/local/bin/myconnector"]
```

### WASM Module (Experimental)

```bash
GOOS=wasip1 GOARCH=wasm go build -o myconnector.wasm ./myconnector
flowforge connector publish myconnector.wasm --version 1.0.0
```
