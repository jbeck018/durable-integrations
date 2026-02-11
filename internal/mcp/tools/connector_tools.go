package tools

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// GenerateConnectorTools creates MCP tool definitions for a registered connector.
// For each connector, it generates: {connector}_discover, {connector}_read,
// {connector}_write, {connector}_check. The set of tools generated depends on
// the connector type (source, destination, bidirectional).
func GenerateConnectorTools(connectorName string, meta cdk.ConnectorMeta) []ToolDefinition {
	var tools []ToolDefinition

	switch meta.Type {
	case "source":
		tools = append(tools, makeDiscoverTool(connectorName, meta))
		tools = append(tools, makeReadTool(connectorName, meta))
		tools = append(tools, makeCheckTool(connectorName, meta))
	case "destination":
		tools = append(tools, makeWriteTool(connectorName, meta))
		tools = append(tools, makeCheckTool(connectorName, meta))
	case "bidirectional":
		tools = append(tools, makeDiscoverTool(connectorName, meta))
		tools = append(tools, makeReadTool(connectorName, meta))
		tools = append(tools, makeWriteTool(connectorName, meta))
		tools = append(tools, makeCheckTool(connectorName, meta))
	}

	return tools
}

// GenerateAllConnectorTools iterates over all registered connectors in the CDK
// registry and generates MCP tools for each one.
func GenerateAllConnectorTools() []ToolDefinition {
	connectors := cdk.ListConnectors()
	var allTools []ToolDefinition
	for _, meta := range connectors {
		tools := GenerateConnectorTools(meta.Name, meta)
		allTools = append(allTools, tools...)
	}
	return allTools
}

func makeDiscoverTool(connectorName string, meta cdk.ConnectorMeta) ToolDefinition {
	return ToolDefinition{
		Name:        connectorName + "_discover",
		Description: fmt.Sprintf("Discover available streams and schemas from the %s connector", meta.DisplayName),
		Category:    "connector",
		Connector:   connectorName,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"config": {
					"type": "object",
					"description": "Connector configuration"
				}
			},
			"required": ["config"]
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				Config json.RawMessage `json:"config"`
			}
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, fmt.Errorf("invalid parameters: %w", err)
			}

			src, err := cdk.GetSource(connectorName)
			if err != nil {
				return nil, fmt.Errorf("connector %s not available as source: %w", connectorName, err)
			}

			catalog, err := src.Discover(ctx, input.Config)
			if err != nil {
				return nil, fmt.Errorf("discover failed for %s: %w", connectorName, err)
			}

			return json.Marshal(catalog)
		},
	}
}

func makeReadTool(connectorName string, meta cdk.ConnectorMeta) ToolDefinition {
	return ToolDefinition{
		Name:        connectorName + "_read",
		Description: fmt.Sprintf("Read data records from the %s connector", meta.DisplayName),
		Category:    "connector",
		Connector:   connectorName,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"config": {
					"type": "object",
					"description": "Connector configuration"
				},
				"streams": {
					"type": "array",
					"description": "Configured streams to read from",
					"items": {
						"type": "object"
					}
				},
				"state": {
					"type": "object",
					"description": "State checkpoint for incremental reads"
				}
			},
			"required": ["config", "streams"]
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				Config  json.RawMessage            `json:"config"`
				Streams []protocol.ConfiguredStream `json:"streams"`
				State   map[string]json.RawMessage  `json:"state"`
			}
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, fmt.Errorf("invalid parameters: %w", err)
			}

			src, err := cdk.GetSource(connectorName)
			if err != nil {
				return nil, fmt.Errorf("connector %s not available as source: %w", connectorName, err)
			}

			catalog := &protocol.ConfiguredCatalog{Streams: input.Streams}
			output := make(chan protocol.Message, 256)
			var records []protocol.Record

			readDone := make(chan error, 1)
			go func() {
				readDone <- src.Read(ctx, input.Config, catalog, input.State, output)
				close(output)
			}()

			for msg := range output {
				if msg.Record != nil {
					records = append(records, *msg.Record)
				}
			}

			if err := <-readDone; err != nil {
				return nil, fmt.Errorf("read failed for %s: %w", connectorName, err)
			}

			result := struct {
				Records []protocol.Record `json:"records"`
				Count   int               `json:"count"`
			}{
				Records: records,
				Count:   len(records),
			}
			return json.Marshal(result)
		},
	}
}

func makeWriteTool(connectorName string, meta cdk.ConnectorMeta) ToolDefinition {
	return ToolDefinition{
		Name:        connectorName + "_write",
		Description: fmt.Sprintf("Write data records to the %s connector", meta.DisplayName),
		Category:    "connector",
		Connector:   connectorName,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"config": {
					"type": "object",
					"description": "Connector configuration"
				},
				"streams": {
					"type": "array",
					"description": "Configured streams for writing",
					"items": {
						"type": "object"
					}
				},
				"records": {
					"type": "array",
					"description": "Records to write",
					"items": {
						"type": "object"
					}
				}
			},
			"required": ["config", "streams", "records"]
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				Config  json.RawMessage            `json:"config"`
				Streams []protocol.ConfiguredStream `json:"streams"`
				Records []protocol.Record           `json:"records"`
			}
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, fmt.Errorf("invalid parameters: %w", err)
			}

			dest, err := cdk.GetDestination(connectorName)
			if err != nil {
				return nil, fmt.Errorf("connector %s not available as destination: %w", connectorName, err)
			}

			catalog := &protocol.ConfiguredCatalog{Streams: input.Streams}
			inputCh := make(chan protocol.Message, len(input.Records))
			for _, r := range input.Records {
				inputCh <- protocol.Message{
					Type:   protocol.MessageTypeRecord,
					Record: &r,
				}
			}
			close(inputCh)

			writeResult, err := dest.Write(ctx, input.Config, catalog, inputCh)
			if err != nil {
				return nil, fmt.Errorf("write failed for %s: %w", connectorName, err)
			}

			return json.Marshal(writeResult)
		},
	}
}

func makeCheckTool(connectorName string, meta cdk.ConnectorMeta) ToolDefinition {
	return ToolDefinition{
		Name:        connectorName + "_check",
		Description: fmt.Sprintf("Check connectivity and credentials for the %s connector", meta.DisplayName),
		Category:    "connector",
		Connector:   connectorName,
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"config": {
					"type": "object",
					"description": "Connector configuration to validate"
				}
			},
			"required": ["config"]
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				Config json.RawMessage `json:"config"`
			}
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, fmt.Errorf("invalid parameters: %w", err)
			}

			// Try source first, then destination
			if src, err := cdk.GetSource(connectorName); err == nil {
				result, checkErr := src.Check(ctx, input.Config)
				if checkErr != nil {
					return nil, fmt.Errorf("check failed for %s: %w", connectorName, checkErr)
				}
				return json.Marshal(result)
			}

			dest, err := cdk.GetDestination(connectorName)
			if err != nil {
				return nil, fmt.Errorf("connector %s not registered: %w", connectorName, err)
			}

			result, err := dest.Check(ctx, input.Config)
			if err != nil {
				return nil, fmt.Errorf("check failed for %s: %w", connectorName, err)
			}
			return json.Marshal(result)
		},
	}
}
