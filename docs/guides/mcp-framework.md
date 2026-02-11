---
title: MCP Framework
layout: default
parent: Guides
nav_order: 4
description: "Native MCP support — every connector becomes accessible to AI agents."
permalink: /guides/mcp-framework/
---

# MCP (Model Context Protocol) Framework

FlowForge is the first integration platform with native MCP support. Every connector automatically becomes accessible to AI agents through the MCP protocol.

---

## Overview

The MCP framework consists of three components:

1. **MCP Gateway**: Multi-tenant gateway accepting MCP client connections over SSE or WebSocket
2. **MCP Tool Registry**: Auto-generates MCP tool definitions from connector capabilities
3. **MCP Worker Pool**: Dedicated Temporal workers executing tool calls with full durability

---

## Auto-Generated MCP Tools

When a connector is configured, FlowForge automatically generates MCP tools:

| Connector | Auto-Generated Tools |
|:--|:--|
| **Salesforce** | `salesforce_query`, `salesforce_get_record`, `salesforce_create_record`, `salesforce_update_record`, `salesforce_search` |
| **HubSpot** | `hubspot_search_contacts`, `hubspot_get_deal`, `hubspot_create_contact`, `hubspot_update_deal` |
| **BigQuery** | `bigquery_query`, `bigquery_list_tables`, `bigquery_get_schema`, `bigquery_insert_rows` |
| **Snowflake** | `snowflake_query`, `snowflake_list_databases`, `snowflake_get_table_schema` |
| **S3** | `s3_list_objects`, `s3_read_file`, `s3_write_file`, `s3_get_presigned_url` |
| **Zendesk** | `zendesk_search_tickets`, `zendesk_get_ticket`, `zendesk_create_ticket` |
| **Gong** | `gong_search_calls`, `gong_get_transcript`, `gong_list_deals` |
| **Intercom** | `intercom_search_contacts`, `intercom_get_conversation`, `intercom_send_message` |

---

## Connecting from Claude Desktop

```json
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

## Connecting Programmatically (Python)

```python
from mcp import ClientSession
from mcp.client.sse import sse_client

async with sse_client(
    url="https://flowforge.example.com/mcp/sse/mcp_abc123",
    headers={"Authorization": "Bearer mcp_key_xyz789"}
) as (read, write):
    async with ClientSession(read, write) as session:
        await session.initialize()
        tools = await session.list_tools()
        result = await session.call_tool(
            "salesforce_query",
            {"query": "SELECT Id, Name FROM Account LIMIT 5"}
        )
```

---

## Security Model

### Per-Tool Authorization

Each MCP tool can be independently enabled/disabled per API key:

```json
{
  "api_key": "mcp_key_abc123",
  "allowed_tools": ["salesforce_query", "salesforce_get_record"],
  "denied_tools": ["salesforce_create_record", "salesforce_update_record"]
}
```

### Data Filtering

Row-level and column-level security policies filter tool results.

### Rate Limiting

Per-agent, per-tool rate limits prevent abuse.

### Secrets Isolation

AI agents never receive raw API credentials. All authentication is handled server-side.

### Audit Logging

Every tool invocation is logged with agent identity, parameters, results, and applied policies.

---

## Custom MCP Server Builder

### Composite Tools

Join data from multiple connectors:

```yaml
tools:
  - name: get_deal_with_calls
    description: "Get a Salesforce deal with Gong call recordings"
    steps:
      - connector: salesforce
        action: query
        output_as: deal
      - connector: gong
        action: search_calls
        output_as: calls
      - transform: "{ deal: deal.records[0], calls: calls }"
```

### Workflows as MCP Tools

Expose Temporal workflows as MCP tools:

```yaml
tools:
  - name: onboard_new_customer
    type: workflow
    workflow: customer_onboarding
    async: true
```

---

## Provisioning MCP Servers

```bash
curl -X POST https://flowforge.example.com/v1/integrations/int_123/mcp \
  -H "Authorization: Bearer $API_KEY" \
  -d '{
    "name": "sales-tools",
    "tools": ["salesforce_*", "hubspot_*"],
    "rate_limits": { "requests_per_minute": 60 }
  }'
```

Returns the SSE and WebSocket endpoints for connecting MCP clients.
