# Connector Development Guide

This guide walks you through building FlowForge connectors at all three tiers: No-Code, Low-Code, and Full-Code.

---

## Overview

FlowForge connectors are modular components that interface with external systems. Every connector implements a standard protocol (SPEC, CHECK, DISCOVER, READ, WRITE) and is tested against an automated acceptance suite.

### Choosing Your Tier

| Tier | Time to Build | Best For | Language |
|---|---|---|---|
| **No-Code Builder** | 10–30 minutes | REST APIs with standard auth | YAML (declarative) |
| **Low-Code CDK** | 1–4 hours | APIs with complex pagination, auth, or transforms | YAML + Python/Go |
| **Full-Code CDK** | 1–3 days | Databases, protocols, complex bidirectional syncs | Go, Python, TypeScript, Rust |

---

## Tier 1: No-Code Connector Builder

The No-Code builder generates a declarative YAML manifest interpreted by the FlowForge runtime. No programming required.

### YAML Manifest Structure

```yaml
# connector.yaml — Example: GitHub Issues Source Connector
apiVersion: flowforge.io/v1
kind: Connector
metadata:
  name: github-issues
  version: 1.0.0
  type: source
  category: developer-tools
  icon: github
  description: "Extract issues from GitHub repositories"

spec:
  # Configuration schema (what the user fills in)
  config:
    properties:
      repository:
        type: string
        description: "Repository in owner/repo format"
        pattern: "^[\\w.-]+/[\\w.-]+$"
        examples: ["octocat/Hello-World"]
      token:
        type: string
        description: "GitHub personal access token"
        secret: true
      state_filter:
        type: string
        enum: [open, closed, all]
        default: all

  # Authentication
  auth:
    type: bearer_token
    token_field: token

  # Streams (data objects to extract)
  streams:
    - name: issues
      # API endpoint
      endpoint:
        url: "https://api.github.com/repos/{{config.repository}}/issues"
        method: GET
        headers:
          Accept: "application/vnd.github.v3+json"
        params:
          state: "{{config.state_filter}}"
          per_page: 100

      # Pagination
      pagination:
        type: link_header  # Follows GitHub's Link header
        # Other types: offset, cursor, page_number, keyset

      # Schema
      schema:
        primary_key: [id]
        properties:
          id: { type: integer }
          number: { type: integer }
          title: { type: string }
          body: { type: string }
          state: { type: string }
          user:
            type: object
            properties:
              login: { type: string }
              id: { type: integer }
          labels:
            type: array
            items:
              type: object
              properties:
                name: { type: string }
          created_at: { type: string, format: date-time }
          updated_at: { type: string, format: date-time }

      # Incremental sync
      incremental:
        cursor_field: updated_at
        cursor_param: since
        cursor_format: iso8601

      # Response parsing
      response:
        records_path: "$"  # Root array
        # For nested: "$.data.items"

      # Rate limiting
      rate_limit:
        requests_per_second: 10
        retry_on: [429, 503]
        backoff:
          initial: 1s
          multiplier: 2
          max: 60s

    - name: comments
      endpoint:
        url: "https://api.github.com/repos/{{config.repository}}/issues/comments"
        method: GET
      pagination:
        type: link_header
      schema:
        primary_key: [id]
        properties:
          id: { type: integer }
          body: { type: string }
          user:
            type: object
            properties:
              login: { type: string }
          created_at: { type: string, format: date-time }
          updated_at: { type: string, format: date-time }
      incremental:
        cursor_field: updated_at
        cursor_param: since
```

### Auth Types

```yaml
# API Key in header
auth:
  type: api_key
  header_name: X-API-Key
  key_field: api_key

# OAuth 2.0 Authorization Code
auth:
  type: oauth2
  grant_type: authorization_code
  authorization_url: "https://example.com/oauth/authorize"
  token_url: "https://example.com/oauth/token"
  scopes: [read, write]
  client_id_field: client_id
  client_secret_field: client_secret
  refresh_enabled: true

# OAuth 2.0 Client Credentials
auth:
  type: oauth2
  grant_type: client_credentials
  token_url: "https://example.com/oauth/token"
  scopes: [api.read]

# Basic Auth
auth:
  type: basic
  username_field: username
  password_field: password

# Custom Header
auth:
  type: custom_header
  headers:
    Authorization: "Token {{config.api_token}}"
    X-Custom-Auth: "{{config.custom_key}}"
```

### Pagination Types

```yaml
# Offset-based
pagination:
  type: offset
  limit_param: limit
  offset_param: offset
  page_size: 100

# Cursor-based
pagination:
  type: cursor
  cursor_param: cursor
  cursor_path: "$.meta.next_cursor"
  has_more_path: "$.meta.has_more"

# Page number
pagination:
  type: page_number
  page_param: page
  page_size: 50
  total_path: "$.total_pages"

# Keyset
pagination:
  type: keyset
  key_field: id
  key_param: after
  order: asc
  page_size: 100
```

---

## Tier 2: Low-Code CDK

The Low-Code tier extends the YAML manifest with custom functions for cases the declarative format can't handle.

### Python Custom Functions

```yaml
# connector.yaml
apiVersion: flowforge.io/v1
kind: Connector
metadata:
  name: custom-crm
  version: 1.0.0
  type: source

spec:
  config:
    properties:
      api_url: { type: string }
      api_key: { type: string, secret: true }

  auth:
    type: custom
    handler: auth.py:get_auth_headers  # Custom Python function

  streams:
    - name: contacts
      endpoint:
        url: "{{config.api_url}}/api/v2/contacts"
      pagination:
        type: custom
        handler: pagination.py:get_next_page  # Custom pagination
      response:
        transform: transforms.py:parse_contacts  # Custom response parsing
      incremental:
        handler: state.py:get_cursor  # Custom state management
```

```python
# auth.py
def get_auth_headers(config: dict) -> dict:
    """Generate authentication headers with HMAC signing."""
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

```python
# pagination.py
def get_next_page(response: dict, current_params: dict) -> dict | None:
    """Custom pagination: decode opaque cursor from response."""
    cursor = response.get("_pagination", {}).get("next")
    if not cursor:
        return None  # No more pages
    return {**current_params, "cursor": cursor, "limit": 200}
```

```python
# transforms.py
def parse_contacts(response: dict) -> list[dict]:
    """Flatten nested contact structure."""
    contacts = []
    for item in response.get("data", {}).get("contacts", []):
        contact = {
            "id": item["id"],
            "email": item["email_addresses"][0]["value"] if item.get("email_addresses") else None,
            "name": f"{item.get('first_name', '')} {item.get('last_name', '')}".strip(),
            "company": item.get("organization", {}).get("name"),
            "created_at": item["created_at"],
            "updated_at": item["updated_at"],
        }
        contacts.append(contact)
    return contacts
```

---

## Tier 3: Full-Code CDK (Go)

For complex connectors requiring full control.

### Source Connector Interface

```go
package cdk

// Source defines the interface for source connectors.
type Source interface {
    // Spec returns the JSON Schema for connector configuration.
    Spec() (*ConnectorSpec, error)

    // Check validates connectivity with the given configuration.
    Check(ctx context.Context, config json.RawMessage) (*CheckResult, error)

    // Discover returns a catalog of available streams with schemas.
    Discover(ctx context.Context, config json.RawMessage) (*Catalog, error)

    // Read yields records from selected streams.
    // state contains the last checkpoint for incremental sync.
    // output is the channel for emitting RECORD, STATE, and LOG messages.
    Read(ctx context.Context, config json.RawMessage, catalog *ConfiguredCatalog, state State, output chan<- Message) error
}
```

### Destination Connector Interface

```go
package cdk

// Destination defines the interface for destination connectors.
type Destination interface {
    // Spec returns the JSON Schema for destination configuration.
    Spec() (*ConnectorSpec, error)

    // Check validates write access with the given configuration.
    Check(ctx context.Context, config json.RawMessage) (*CheckResult, error)

    // Write accepts batches of records and returns write results.
    Write(ctx context.Context, config json.RawMessage, catalog *ConfiguredCatalog, input <-chan Message) (*WriteResult, error)

    // Capabilities declares supported sync modes.
    Capabilities() []SyncMode // append, upsert, merge, soft_delete
}
```

### Bidirectional Connector Interface

```go
package cdk

// Bidirectional extends both Source and Destination with conflict resolution.
type Bidirectional interface {
    Source
    Destination

    // Resolve handles conflicts when the same record is modified in both systems.
    Resolve(ctx context.Context, conflicts []Conflict) ([]Resolution, error)

    // WebhookHandler processes inbound events for real-time CDC.
    WebhookHandler(ctx context.Context, event WebhookEvent) error
}
```

### Example: Building a Full-Code Source Connector

```go
package myapi

import (
    "context"
    "encoding/json"
    "fmt"
    "net/http"
    "time"

    "github.com/flowforge/flowforge/pkg/cdk"
    "github.com/flowforge/flowforge/pkg/protocol"
)

// Config holds the connector configuration.
type Config struct {
    BaseURL string `json:"base_url"`
    APIKey  string `json:"api_key"`
    // ... other fields
}

// MyAPISource implements cdk.Source.
type MyAPISource struct {
    client *http.Client
}

func New() *MyAPISource {
    return &MyAPISource{
        client: &http.Client{Timeout: 30 * time.Second},
    }
}

func (s *MyAPISource) Spec() (*cdk.ConnectorSpec, error) {
    return &cdk.ConnectorSpec{
        DocumentationURL: "https://docs.myapi.com",
        ConfigSchema: json.RawMessage(`{
            "type": "object",
            "required": ["base_url", "api_key"],
            "properties": {
                "base_url": {
                    "type": "string",
                    "description": "Base URL of the API"
                },
                "api_key": {
                    "type": "string",
                    "description": "API key for authentication",
                    "airbyte_secret": true
                }
            }
        }`),
    }, nil
}

func (s *MyAPISource) Check(ctx context.Context, rawConfig json.RawMessage) (*cdk.CheckResult, error) {
    var config Config
    if err := json.Unmarshal(rawConfig, &config); err != nil {
        return &cdk.CheckResult{Status: cdk.StatusFailed, Message: "Invalid config: " + err.Error()}, nil
    }

    req, _ := http.NewRequestWithContext(ctx, "GET", config.BaseURL+"/health", nil)
    req.Header.Set("Authorization", "Bearer "+config.APIKey)

    resp, err := s.client.Do(req)
    if err != nil {
        return &cdk.CheckResult{Status: cdk.StatusFailed, Message: "Connection failed: " + err.Error()}, nil
    }
    defer resp.Body.Close()

    if resp.StatusCode != 200 {
        return &cdk.CheckResult{Status: cdk.StatusFailed, Message: fmt.Sprintf("API returned %d", resp.StatusCode)}, nil
    }

    return &cdk.CheckResult{Status: cdk.StatusSucceeded}, nil
}

func (s *MyAPISource) Discover(ctx context.Context, rawConfig json.RawMessage) (*cdk.Catalog, error) {
    return &cdk.Catalog{
        Streams: []cdk.Stream{
            {
                Name:      "users",
                Namespace: "myapi",
                Schema: json.RawMessage(`{
                    "type": "object",
                    "properties": {
                        "id":         {"type": "integer"},
                        "email":      {"type": "string"},
                        "name":       {"type": "string"},
                        "created_at": {"type": "string", "format": "date-time"},
                        "updated_at": {"type": "string", "format": "date-time"}
                    }
                }`),
                SupportedSyncModes: []cdk.SyncMode{cdk.FullRefresh, cdk.Incremental},
                DefaultCursorField: []string{"updated_at"},
                PrimaryKey:         [][]string{{"id"}},
            },
        },
    }, nil
}

func (s *MyAPISource) Read(ctx context.Context, rawConfig json.RawMessage, catalog *cdk.ConfiguredCatalog, state cdk.State, output chan<- protocol.Message) error {
    var config Config
    if err := json.Unmarshal(rawConfig, &config); err != nil {
        return err
    }

    for _, stream := range catalog.Streams {
        if err := s.readStream(ctx, config, stream, state, output); err != nil {
            return fmt.Errorf("reading stream %s: %w", stream.Stream.Name, err)
        }
    }

    return nil
}

func (s *MyAPISource) readStream(ctx context.Context, config Config, stream cdk.ConfiguredStream, state cdk.State, output chan<- protocol.Message) error {
    cursor := ""
    if v, ok := state[stream.Stream.Name]; ok {
        cursor = v.Cursor
    }

    page := 1
    for {
        url := fmt.Sprintf("%s/api/v1/%s?page=%d&per_page=100", config.BaseURL, stream.Stream.Name, page)
        if cursor != "" && stream.SyncMode == cdk.Incremental {
            url += "&updated_since=" + cursor
        }

        req, _ := http.NewRequestWithContext(ctx, "GET", url, nil)
        req.Header.Set("Authorization", "Bearer "+config.APIKey)

        resp, err := s.client.Do(req)
        if err != nil {
            return err
        }

        var result struct {
            Data    []json.RawMessage `json:"data"`
            HasMore bool              `json:"has_more"`
        }
        if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
            resp.Body.Close()
            return err
        }
        resp.Body.Close()

        // Emit records
        for _, record := range result.Data {
            output <- protocol.Message{
                Type: protocol.MessageTypeRecord,
                Record: &protocol.Record{
                    Stream:    stream.Stream.Name,
                    Data:      record,
                    EmittedAt: time.Now(),
                },
            }
        }

        // Emit state checkpoint every page
        output <- protocol.Message{
            Type: protocol.MessageTypeState,
            State: &protocol.State{
                Stream: stream.Stream.Name,
                Data:   map[string]interface{}{"page": page, "cursor": time.Now().Format(time.RFC3339)},
            },
        }

        if !result.HasMore {
            break
        }
        page++
    }

    return nil
}
```

### Registering Your Connector

```go
package main

import (
    "github.com/flowforge/flowforge/pkg/cdk"
    "github.com/your-org/myapi-connector/myapi"
)

func init() {
    cdk.RegisterSource("myapi", myapi.New())
}
```

---

## Connector Testing

Every connector must pass the standardized acceptance test suite.

### Running Tests

```bash
# Run acceptance tests for a specific connector
make test-connector CONNECTOR=salesforce

# Run all connector acceptance tests
make test-connectors

# Run with integration (requires credentials)
make test-connector CONNECTOR=salesforce INTEGRATION=true
```

### Acceptance Test Categories

| Test | What It Validates |
|---|---|
| **Spec Validation** | Config schema is valid JSON Schema, all required fields present, generates usable form |
| **Connection Check** | Connects with valid creds, fails gracefully with invalid creds |
| **Discovery** | Returns at least one stream with valid schema |
| **Read Compliance** | Full refresh + incremental produce valid RECORD messages. STATE checkpoints emitted |
| **Write Compliance** | Append, upsert, delete produce expected results |
| **Idempotency** | Re-running same sync produces identical results (critical for Temporal replay) |
| **Performance Baseline** | Meets minimum throughput for its category |

### Writing Custom Tests

```go
package myapi_test

import (
    "testing"

    "github.com/flowforge/flowforge/pkg/cdk/testing"
    "github.com/your-org/myapi-connector/myapi"
)

func TestMyAPIConnector(t *testing.T) {
    connector := myapi.New()

    suite := testing.NewAcceptanceSuite(t, connector, testing.SuiteConfig{
        ConfigPath:         "testdata/config.json",
        InvalidConfigPath:  "testdata/invalid_config.json",
        ExpectedStreams:     []string{"users", "orders"},
        PerformanceTarget:  10000, // records per minute minimum
    })

    suite.Run()
}
```

---

## Connector Packaging

### Native Plugin (First-Class)

First-class connectors are compiled directly into the worker binary:

```go
// cmd/flowforge-worker/main.go
import (
    _ "github.com/flowforge/flowforge/connectors/salesforce"
    _ "github.com/flowforge/flowforge/connectors/hubspot"
    _ "github.com/flowforge/flowforge/connectors/bigquery"
    // ... all first-class connectors auto-register via init()
)
```

### OCI Container Image

Community connectors are packaged as containers:

```dockerfile
FROM flowforge/connector-base:latest
COPY myconnector /usr/local/bin/myconnector
ENTRYPOINT ["/usr/local/bin/myconnector"]
```

```bash
# Build and publish
docker build -t registry.flowforge.io/connectors/myapi:1.0.0 .
docker push registry.flowforge.io/connectors/myapi:1.0.0
```

### WASM Module (Experimental)

Ultra-lightweight connectors as WebAssembly:

```bash
# Build Go connector to WASM
GOOS=wasip1 GOARCH=wasm go build -o myconnector.wasm ./myconnector

# Publish to registry
flowforge connector publish myconnector.wasm --version 1.0.0
```
