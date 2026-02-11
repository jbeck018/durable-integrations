# MCP (Model Context Protocol) Framework Guide

FlowForge is the first integration platform with native MCP support. Every connector automatically becomes accessible to AI agents through the MCP protocol.

---

## Overview

The MCP framework consists of three components:

1. **MCP Gateway**: Multi-tenant gateway accepting MCP client connections (Claude, ChatGPT, IDE plugins) over SSE or WebSocket
2. **MCP Tool Registry**: Auto-generates MCP tool definitions from connector capabilities
3. **MCP Worker Pool**: Dedicated Temporal workers executing MCP tool calls with full durability

```
AI Agent (Claude, ChatGPT, IDE Plugin)
    │
    │  MCP Protocol (SSE / WebSocket)
    ▼
┌─────────────────────────┐
│      MCP Gateway         │
│  ┌───────────────────┐  │
│  │ Auth │ Route │ Rate│  │
│  └───────────────────┘  │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│    MCP Tool Registry     │
│  ┌────┐ ┌────┐ ┌────┐  │
│  │ SF │ │ BQ │ │ HB │  │  (auto-generated from connectors)
│  └────┘ └────┘ └────┘  │
└─────────┬───────────────┘
          │
          ▼
┌─────────────────────────┐
│    MCP Worker Pool       │
│  (Temporal Activities)   │
│  Full durability/retry   │
└─────────┬───────────────┘
          │
          ▼
    External Systems
  (Salesforce, BigQuery, ...)
```

---

## Auto-Generated MCP Tools

When a connector is configured, FlowForge automatically generates MCP tools. No additional configuration required.

### Generated Tool Catalog

| Connector | Auto-Generated Tools |
|---|---|
| **Salesforce** | `salesforce_query` (SOQL), `salesforce_get_record`, `salesforce_create_record`, `salesforce_update_record`, `salesforce_search`, `salesforce_describe_object` |
| **HubSpot** | `hubspot_search_contacts`, `hubspot_get_deal`, `hubspot_create_contact`, `hubspot_update_deal`, `hubspot_list_properties` |
| **BigQuery** | `bigquery_query` (SQL), `bigquery_list_tables`, `bigquery_get_schema`, `bigquery_insert_rows` |
| **Snowflake** | `snowflake_query` (SQL), `snowflake_list_databases`, `snowflake_get_table_schema` |
| **S3** | `s3_list_objects`, `s3_read_file`, `s3_write_file`, `s3_get_presigned_url` |
| **Zendesk** | `zendesk_search_tickets`, `zendesk_get_ticket`, `zendesk_create_ticket`, `zendesk_add_comment` |
| **Gong** | `gong_search_calls`, `gong_get_transcript`, `gong_list_deals` |
| **Intercom** | `intercom_search_contacts`, `intercom_get_conversation`, `intercom_send_message` |

### Tool Definition Example

Each tool gets a proper JSON Schema definition optimized for LLM tool selection:

```json
{
  "name": "salesforce_query",
  "description": "Execute a SOQL query against the connected Salesforce instance. Returns matching records. Use for searching, filtering, and retrieving Salesforce data. Supports all standard SOQL syntax including WHERE, ORDER BY, LIMIT, and relationship queries.",
  "input_schema": {
    "type": "object",
    "required": ["query"],
    "properties": {
      "query": {
        "type": "string",
        "description": "SOQL query to execute. Example: SELECT Id, Name, Email FROM Contact WHERE CreatedDate > 2024-01-01T00:00:00Z LIMIT 100"
      },
      "include_deleted": {
        "type": "boolean",
        "default": false,
        "description": "Include soft-deleted records in results (queryAll)"
      }
    }
  },
  "output_schema": {
    "type": "object",
    "properties": {
      "total_size": { "type": "integer" },
      "records": {
        "type": "array",
        "items": { "type": "object" }
      },
      "done": { "type": "boolean" }
    }
  }
}
```

### How Auto-Generation Works

```go
// internal/mcp/tools/generator.go

func GenerateToolsFromConnector(connector cdk.Source, config json.RawMessage) ([]MCPTool, error) {
    catalog, err := connector.Discover(ctx, config)
    if err != nil {
        return nil, err
    }

    var tools []MCPTool

    for _, stream := range catalog.Streams {
        // Generate read tool
        tools = append(tools, MCPTool{
            Name:        fmt.Sprintf("%s_get_%s", connectorName, singularize(stream.Name)),
            Description: fmt.Sprintf("Retrieve a single %s by ID from %s", singularize(stream.Name), connectorName),
            InputSchema: generateGetSchema(stream),
        })

        // Generate search/list tool
        tools = append(tools, MCPTool{
            Name:        fmt.Sprintf("%s_search_%s", connectorName, stream.Name),
            Description: fmt.Sprintf("Search %s in %s with filters", stream.Name, connectorName),
            InputSchema: generateSearchSchema(stream),
        })

        // Generate write tools (if destination)
        if dest, ok := connector.(cdk.Destination); ok {
            for _, mode := range dest.Capabilities() {
                tools = append(tools, generateWriteTool(connectorName, stream, mode))
            }
        }
    }

    return tools, nil
}
```

---

## MCP Gateway Architecture

### Transport Layer

The gateway supports two MCP transport protocols:

**Server-Sent Events (SSE)** — For HTTP-based clients:
```
Client                          Gateway
  │                                │
  │  GET /mcp/sse                  │
  │  Authorization: Bearer <token> │
  │──────────────────────────────▶│
  │                                │
  │  SSE: event: endpoint          │
  │  data: /mcp/messages/{id}      │
  │◀──────────────────────────────│
  │                                │
  │  POST /mcp/messages/{id}       │
  │  {"method": "tools/call", ...} │
  │──────────────────────────────▶│
  │                                │
  │  SSE: event: message           │
  │  data: {"result": {...}}       │
  │◀──────────────────────────────│
```

**WebSocket** — For bidirectional streaming:
```
Client                          Gateway
  │                                │
  │  WS /mcp/ws                    │
  │  Authorization: Bearer <token> │
  │══════════════════════════════▶│
  │                                │
  │  {"method": "tools/call", ...} │
  │──────────────────────────────▶│
  │                                │
  │  {"result": {...}}             │
  │◀──────────────────────────────│
```

### Multi-Tenant Routing

Each MCP connection is scoped to a tenant. The gateway resolves the tenant from the authentication token and routes tool calls to the appropriate connector configuration:

```
AI Agent connects with tenant-scoped API key
    │
    ▼
Gateway resolves tenant → loads connector configs
    │
    ▼
Tool call "salesforce_query" → routes to tenant's Salesforce connection
    │
    ▼
MCP Worker executes using tenant's OAuth credentials (from Vault)
    │
    ▼
Result filtered by tenant's data policies → returned to agent
```

### Task Queue Isolation

Each provisioned MCP server gets its own Temporal task queue:

```
mcp-request-{server_id}
```

This ensures:
- Fair scheduling across tenants (one tenant's heavy usage doesn't block another)
- Independent scaling per MCP server
- Isolated rate limiting

---

## Security Model

Enterprise MCP deployments require robust security. FlowForge implements defense-in-depth:

### Per-Tool Authorization

Each MCP tool can be independently enabled/disabled per API key or user role:

```json
{
  "api_key": "mcp_key_abc123",
  "tenant_id": "tenant-456",
  "allowed_tools": [
    "salesforce_query",
    "salesforce_get_record",
    "salesforce_search"
  ],
  "denied_tools": [
    "salesforce_create_record",
    "salesforce_update_record"
  ]
}
```

An AI agent can be granted read-only Salesforce access without write permissions.

### Data Filtering

Row-level and column-level security policies filter MCP tool results:

```json
{
  "tool": "salesforce_query",
  "policies": {
    "row_filter": "OwnerId = '{current_user_id}'",
    "allowed_columns": ["Id", "Name", "Email", "Phone"],
    "denied_columns": ["SSN", "CreditCardNumber"],
    "max_rows": 1000
  }
}
```

### Audit Logging

Every MCP tool invocation is logged:

```json
{
  "timestamp": "2026-02-10T15:30:00Z",
  "event": "mcp_tool_call",
  "agent_identity": "claude-desktop-user-789",
  "tenant_id": "tenant-456",
  "tool": "salesforce_query",
  "parameters": {
    "query": "SELECT Id, Name FROM Account LIMIT 10"
  },
  "result_summary": {
    "records_returned": 10,
    "execution_time_ms": 450
  },
  "data_policies_applied": ["row_filter", "column_mask"]
}
```

### Rate Limiting

Per-agent, per-tool rate limits prevent abuse:

```json
{
  "agent_id": "claude-desktop-user-789",
  "limits": {
    "global": { "requests_per_minute": 60 },
    "per_tool": {
      "salesforce_query": { "requests_per_minute": 20 },
      "bigquery_query": { "requests_per_minute": 10 }
    }
  }
}
```

### Input Validation

All MCP tool inputs are validated against JSON Schema before execution:

```go
func (w *MCPWorker) ExecuteToolCall(ctx context.Context, call ToolCall) (ToolResult, error) {
    // 1. Validate input against tool's JSON Schema
    if err := validateInput(call.Tool.InputSchema, call.Parameters); err != nil {
        return ToolResult{}, fmt.Errorf("invalid input: %w", err)
    }

    // 2. Check authorization
    if !isAuthorized(ctx, call.AgentID, call.Tool.Name) {
        return ToolResult{}, ErrUnauthorized
    }

    // 3. Execute via connector
    result, err := executeConnectorActivity(ctx, call)
    if err != nil {
        return ToolResult{}, err
    }

    // 4. Apply data filtering policies
    filtered := applyDataPolicies(ctx, call.Tool.Name, result)

    // 5. Audit log
    auditLog(ctx, call, filtered)

    return filtered, nil
}
```

### Secrets Isolation

AI agents never receive raw API credentials. All authentication is handled by the FlowForge backend:

```
AI Agent: "salesforce_query: SELECT Id FROM Account"
    │
    ▼
MCP Worker: retrieves Salesforce OAuth token from Vault
    │         (agent never sees the token)
    ▼
Salesforce API: executes query with worker's token
    │
    ▼
MCP Worker: returns data to agent (no credential exposure)
```

---

## Custom MCP Server Builder

Beyond auto-generated tools, you can build custom MCP servers that combine data across connectors.

### Composite Tools

Join data from multiple connectors in a single MCP call:

```yaml
# custom-mcp-server.yaml
name: sales-intelligence
description: "Combined sales data from Salesforce and Gong"

tools:
  - name: get_deal_with_calls
    description: "Get a Salesforce deal with associated Gong call recordings and transcripts"
    steps:
      - connector: salesforce
        action: query
        query: "SELECT Id, Name, Amount, StageName FROM Opportunity WHERE Id = '{{input.deal_id}}'"
        output_as: deal

      - connector: gong
        action: search_calls
        filters:
          deal_id: "{{input.deal_id}}"
        output_as: calls

      - transform: |
          {
            "deal": deal.records[0],
            "calls": calls.map(c => ({
              "date": c.started_at,
              "duration": c.duration,
              "participants": c.participants,
              "summary": c.ai_summary
            })),
            "total_calls": calls.length
          }
```

### Semantic Tools

Tools with descriptions optimized for LLM tool selection:

```yaml
tools:
  - name: find_customer_context
    description: |
      Find comprehensive context about a customer for a sales conversation.
      Returns company info, recent deals, support tickets, and call history.
      Use this before customer meetings to prepare talking points.
    semantic_hints:
      - "customer research"
      - "meeting preparation"
      - "account overview"
    steps:
      - connector: salesforce
        action: query
        query: "SELECT ... FROM Account WHERE Name LIKE '%{{input.company_name}}%'"
      - connector: zendesk
        action: search_tickets
        query: "organization:{{company_name}}"
      - connector: gong
        action: search_calls
        filters: { company: "{{company_name}}" }
```

### RAG-Enabled Tools

Tools that combine retrieval with connector data:

```yaml
tools:
  - name: search_knowledge_base
    description: "Search internal knowledge base with semantic search, returning relevant documents with context"
    steps:
      - connector: s3
        action: list_objects
        prefix: "knowledge-base/"
        filter: "*.md"

      - transform: vector_search
        query: "{{input.question}}"
        top_k: 5

      - transform: |
          results.map(doc => ({
            "title": doc.metadata.title,
            "relevance": doc.score,
            "excerpt": doc.content.substring(0, 500),
            "source": doc.s3_key
          }))
```

### Workflows as MCP Tools

Expose Temporal workflows as MCP tools for complex multi-step processes:

```yaml
tools:
  - name: onboard_new_customer
    description: "Trigger the full customer onboarding workflow. Creates accounts in Salesforce, Zendesk, and sets up initial sync."
    type: workflow
    workflow: customer_onboarding
    input_schema:
      type: object
      required: [company_name, contact_email]
      properties:
        company_name: { type: string }
        contact_email: { type: string }
        plan: { type: string, enum: [starter, professional, enterprise] }
    async: true  # Returns workflow ID immediately, agent can check status later
```

---

## Provisioning MCP Servers

### Via API

```bash
# Create an MCP server for a tenant's integrations
curl -X POST https://flowforge.example.com/v1/integrations/int_123/mcp \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "sales-tools",
    "tools": ["salesforce_*", "hubspot_*", "gong_*"],
    "rate_limits": {
      "requests_per_minute": 60
    },
    "allowed_roles": ["sales-agent", "sales-manager"]
  }'

# Response
{
  "mcp_server_id": "mcp_abc123",
  "endpoint": "https://flowforge.example.com/mcp/sse/mcp_abc123",
  "websocket_endpoint": "wss://flowforge.example.com/mcp/ws/mcp_abc123",
  "api_key": "mcp_key_xyz789"
}
```

### Connecting from Claude Desktop

```json
// claude_desktop_config.json
{
  "mcpServers": {
    "flowforge-sales": {
      "url": "https://flowforge.example.com/mcp/sse/mcp_abc123",
      "headers": {
        "Authorization": "Bearer mcp_key_xyz789"
      }
    }
  }
}
```

### Connecting Programmatically

```python
from mcp import ClientSession
from mcp.client.sse import sse_client

async def main():
    async with sse_client(
        url="https://flowforge.example.com/mcp/sse/mcp_abc123",
        headers={"Authorization": "Bearer mcp_key_xyz789"}
    ) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # List available tools
            tools = await session.list_tools()
            print(f"Available tools: {[t.name for t in tools]}")

            # Call a tool
            result = await session.call_tool(
                "salesforce_query",
                {"query": "SELECT Id, Name FROM Account LIMIT 5"}
            )
            print(result)
```
