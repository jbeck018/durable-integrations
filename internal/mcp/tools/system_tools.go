package tools

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// syncState tracks the in-memory status of triggered syncs.
// In a production system this would be backed by a persistent store,
// but for the MCP gateway it serves as an operational cache.
type syncState struct {
	mu    sync.RWMutex
	syncs map[string]*SyncStatus
}

// SyncStatus describes the current state of a sync operation.
type SyncStatus struct {
	ID          string    `json:"id"`
	Connector   string    `json:"connector"`
	Stream      string    `json:"stream"`
	Status      string    `json:"status"`
	RecordCount int64     `json:"record_count"`
	StartedAt   time.Time `json:"started_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Error       string    `json:"error,omitempty"`
}

var globalSyncState = &syncState{
	syncs: make(map[string]*SyncStatus),
}

// RegisterSystemTools adds all built-in system tools to the given registry.
func RegisterSystemTools(registry *ToolRegistry) {
	registry.RegisterTool(makeListConnectorsTool())
	registry.RegisterTool(makeSyncStatusTool())
	registry.RegisterTool(makeTriggerSyncTool())
	registry.RegisterTool(makeSchemaInfoTool())
	registry.RegisterTool(makeFieldMappingSuggestTool())
}

func makeListConnectorsTool() ToolDefinition {
	return ToolDefinition{
		Name:        "list_connectors",
		Description: "List all registered connectors with their metadata, type, and capabilities",
		Category:    "system",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"type": {
					"type": "string",
					"description": "Filter by connector type: source, destination, or bidirectional",
					"enum": ["source", "destination", "bidirectional"]
				},
				"category": {
					"type": "string",
					"description": "Filter by connector category"
				}
			}
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var filter struct {
				Type     string `json:"type"`
				Category string `json:"category"`
			}
			if len(params) > 0 {
				if err := json.Unmarshal(params, &filter); err != nil {
					return nil, fmt.Errorf("invalid parameters: %w", err)
				}
			}

			connectors := cdk.ListConnectors()
			var filtered []cdk.ConnectorMeta
			for _, c := range connectors {
				if filter.Type != "" && c.Type != filter.Type {
					continue
				}
				if filter.Category != "" && c.Category != filter.Category {
					continue
				}
				filtered = append(filtered, c)
			}

			sort.Slice(filtered, func(i, j int) bool {
				return filtered[i].Name < filtered[j].Name
			})

			result := struct {
				Connectors []cdk.ConnectorMeta `json:"connectors"`
				Count      int                 `json:"count"`
			}{
				Connectors: filtered,
				Count:      len(filtered),
			}
			return json.Marshal(result)
		},
	}
}

func makeSyncStatusTool() ToolDefinition {
	return ToolDefinition{
		Name:        "sync_status",
		Description: "Get the current status of a sync operation by its ID",
		Category:    "system",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"sync_id": {
					"type": "string",
					"description": "The unique identifier of the sync operation"
				}
			},
			"required": ["sync_id"]
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				SyncID string `json:"sync_id"`
			}
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, fmt.Errorf("invalid parameters: %w", err)
			}
			if input.SyncID == "" {
				return nil, fmt.Errorf("sync_id is required")
			}

			globalSyncState.mu.RLock()
			status, ok := globalSyncState.syncs[input.SyncID]
			globalSyncState.mu.RUnlock()

			if !ok {
				return json.Marshal(map[string]interface{}{
					"error":   "not_found",
					"message": fmt.Sprintf("sync %s not found", input.SyncID),
				})
			}
			return json.Marshal(status)
		},
	}
}

func makeTriggerSyncTool() ToolDefinition {
	return ToolDefinition{
		Name:        "trigger_sync",
		Description: "Trigger a new sync operation for a connector and stream",
		Category:    "system",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"connector": {
					"type": "string",
					"description": "Name of the connector to sync"
				},
				"stream": {
					"type": "string",
					"description": "Name of the stream to sync"
				},
				"sync_mode": {
					"type": "string",
					"description": "Sync mode: full_refresh or incremental",
					"enum": ["full_refresh", "incremental"],
					"default": "full_refresh"
				}
			},
			"required": ["connector", "stream"]
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				Connector string `json:"connector"`
				Stream    string `json:"stream"`
				SyncMode  string `json:"sync_mode"`
			}
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, fmt.Errorf("invalid parameters: %w", err)
			}
			if input.Connector == "" || input.Stream == "" {
				return nil, fmt.Errorf("connector and stream are required")
			}
			if input.SyncMode == "" {
				input.SyncMode = "full_refresh"
			}

			// Verify connector exists
			if _, err := cdk.GetMeta(input.Connector); err != nil {
				return nil, fmt.Errorf("connector %s not found: %w", input.Connector, err)
			}

			syncID := fmt.Sprintf("sync_%s_%s_%d", input.Connector, input.Stream, time.Now().UnixNano())
			now := time.Now()
			status := &SyncStatus{
				ID:        syncID,
				Connector: input.Connector,
				Stream:    input.Stream,
				Status:    "running",
				StartedAt: now,
				UpdatedAt: now,
			}

			globalSyncState.mu.Lock()
			globalSyncState.syncs[syncID] = status
			globalSyncState.mu.Unlock()

			// Launch the sync asynchronously
			go runSync(ctx, input.Connector, input.Stream, input.SyncMode, status)

			return json.Marshal(map[string]interface{}{
				"sync_id":    syncID,
				"status":     "running",
				"started_at": now,
			})
		},
	}
}

// runSync executes a sync operation in the background and updates status.
func runSync(ctx context.Context, connectorName, stream, syncMode string, status *SyncStatus) {
	src, err := cdk.GetSource(connectorName)
	if err != nil {
		globalSyncState.mu.Lock()
		status.Status = "failed"
		status.Error = err.Error()
		status.UpdatedAt = time.Now()
		globalSyncState.mu.Unlock()
		return
	}

	config, err := json.Marshal(map[string]interface{}{})
	if err != nil {
		globalSyncState.mu.Lock()
		status.Status = "failed"
		status.Error = err.Error()
		status.UpdatedAt = time.Now()
		globalSyncState.mu.Unlock()
		return
	}

	catalog, err := src.Discover(ctx, config)
	if err != nil {
		globalSyncState.mu.Lock()
		status.Status = "failed"
		status.Error = fmt.Sprintf("discover failed: %v", err)
		status.UpdatedAt = time.Now()
		globalSyncState.mu.Unlock()
		return
	}

	// Find the requested stream in the catalog
	var configuredStreams []protocol.ConfiguredStream
	for _, s := range catalog.Streams {
		if s.Name == stream {
			mode := protocol.SyncModeFullRefresh
			if syncMode == "incremental" {
				mode = protocol.SyncModeIncremental
			}
			configuredStreams = append(configuredStreams, protocol.ConfiguredStream{
				Stream:   s,
				SyncMode: mode,
			})
			break
		}
	}

	if len(configuredStreams) == 0 {
		globalSyncState.mu.Lock()
		status.Status = "failed"
		status.Error = fmt.Sprintf("stream %s not found in connector %s", stream, connectorName)
		status.UpdatedAt = time.Now()
		globalSyncState.mu.Unlock()
		return
	}

	configuredCatalog := &protocol.ConfiguredCatalog{Streams: configuredStreams}
	output := make(chan protocol.Message, 256)

	readDone := make(chan error, 1)
	go func() {
		readDone <- src.Read(ctx, config, configuredCatalog, nil, output)
		close(output)
	}()

	var count int64
	for msg := range output {
		if msg.Record != nil {
			count++
			if count%100 == 0 {
				globalSyncState.mu.Lock()
				status.RecordCount = count
				status.UpdatedAt = time.Now()
				globalSyncState.mu.Unlock()
			}
		}
	}

	readErr := <-readDone

	globalSyncState.mu.Lock()
	status.RecordCount = count
	status.UpdatedAt = time.Now()
	if readErr != nil {
		status.Status = "failed"
		status.Error = readErr.Error()
	} else {
		status.Status = "completed"
	}
	globalSyncState.mu.Unlock()
}

func makeSchemaInfoTool() ToolDefinition {
	return ToolDefinition{
		Name:        "schema_info",
		Description: "Get the schema definition for a specific stream of a connector",
		Category:    "system",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"connector": {
					"type": "string",
					"description": "Name of the connector"
				},
				"stream": {
					"type": "string",
					"description": "Name of the stream to get schema for"
				}
			},
			"required": ["connector", "stream"]
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				Connector string `json:"connector"`
				Stream    string `json:"stream"`
			}
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, fmt.Errorf("invalid parameters: %w", err)
			}
			if input.Connector == "" || input.Stream == "" {
				return nil, fmt.Errorf("connector and stream are required")
			}

			src, err := cdk.GetSource(input.Connector)
			if err != nil {
				return nil, fmt.Errorf("connector %s not available: %w", input.Connector, err)
			}

			config, err := json.Marshal(map[string]interface{}{})
			if err != nil {
				return nil, err
			}

			catalog, err := src.Discover(ctx, config)
			if err != nil {
				return nil, fmt.Errorf("discover failed: %w", err)
			}

			for _, s := range catalog.Streams {
				if s.Name == input.Stream {
					result := struct {
						Connector   string          `json:"connector"`
						Stream      string          `json:"stream"`
						Schema      json.RawMessage `json:"schema"`
						SyncModes   []string        `json:"supported_sync_modes"`
						PrimaryKey  [][]string      `json:"primary_key,omitempty"`
						CursorField []string        `json:"default_cursor_field,omitempty"`
					}{
						Connector:   input.Connector,
						Stream:      s.Name,
						Schema:      s.Schema,
						PrimaryKey:  s.PrimaryKey,
						CursorField: s.DefaultCursorField,
					}
					for _, m := range s.SupportedSyncModes {
						result.SyncModes = append(result.SyncModes, string(m))
					}
					return json.Marshal(result)
				}
			}

			return json.Marshal(map[string]interface{}{
				"error":   "not_found",
				"message": fmt.Sprintf("stream %s not found in connector %s", input.Stream, input.Connector),
			})
		},
	}
}

func makeFieldMappingSuggestTool() ToolDefinition {
	return ToolDefinition{
		Name:        "field_mapping_suggest",
		Description: "Suggest field mappings between a source and destination stream based on schema similarity",
		Category:    "system",
		InputSchema: json.RawMessage(`{
			"type": "object",
			"properties": {
				"source_schema": {
					"type": "object",
					"description": "JSON Schema of the source stream"
				},
				"dest_schema": {
					"type": "object",
					"description": "JSON Schema of the destination stream"
				}
			},
			"required": ["source_schema", "dest_schema"]
		}`),
		Handler: func(ctx context.Context, params json.RawMessage) (json.RawMessage, error) {
			var input struct {
				SourceSchema json.RawMessage `json:"source_schema"`
				DestSchema   json.RawMessage `json:"dest_schema"`
			}
			if err := json.Unmarshal(params, &input); err != nil {
				return nil, fmt.Errorf("invalid parameters: %w", err)
			}

			sourceFields := extractFields(input.SourceSchema)
			destFields := extractFields(input.DestSchema)

			type FieldMapping struct {
				SourceField string  `json:"source_field"`
				DestField   string  `json:"dest_field"`
				Confidence  float64 `json:"confidence"`
				Reason      string  `json:"reason"`
			}

			var mappings []FieldMapping

			// Exact match pass
			destUsed := make(map[string]bool)
			for _, sf := range sourceFields {
				for _, df := range destFields {
					if destUsed[df.Name] {
						continue
					}
					if sf.Name == df.Name {
						conf := 0.9
						reason := "exact name match"
						if sf.Type == df.Type {
							conf = 1.0
							reason = "exact name and type match"
						}
						mappings = append(mappings, FieldMapping{
							SourceField: sf.Name,
							DestField:   df.Name,
							Confidence:  conf,
							Reason:      reason,
						})
						destUsed[df.Name] = true
						break
					}
				}
			}

			// Normalized match pass (case-insensitive, underscore/camel normalization)
			for _, sf := range sourceFields {
				matched := false
				for _, m := range mappings {
					if m.SourceField == sf.Name {
						matched = true
						break
					}
				}
				if matched {
					continue
				}

				normSource := normalizeFieldName(sf.Name)
				for _, df := range destFields {
					if destUsed[df.Name] {
						continue
					}
					normDest := normalizeFieldName(df.Name)
					if normSource == normDest {
						conf := 0.7
						reason := "normalized name match"
						if sf.Type == df.Type {
							conf = 0.8
							reason = "normalized name and type match"
						}
						mappings = append(mappings, FieldMapping{
							SourceField: sf.Name,
							DestField:   df.Name,
							Confidence:  conf,
							Reason:      reason,
						})
						destUsed[df.Name] = true
						break
					}
				}
			}

			// Type-based suggestion for unmatched fields
			for _, sf := range sourceFields {
				matched := false
				for _, m := range mappings {
					if m.SourceField == sf.Name {
						matched = true
						break
					}
				}
				if matched {
					continue
				}

				for _, df := range destFields {
					if destUsed[df.Name] {
						continue
					}
					if sf.Type == df.Type && sf.Type != "" {
						mappings = append(mappings, FieldMapping{
							SourceField: sf.Name,
							DestField:   df.Name,
							Confidence:  0.3,
							Reason:      "type match only",
						})
						destUsed[df.Name] = true
						break
					}
				}
			}

			sort.Slice(mappings, func(i, j int) bool {
				return mappings[i].Confidence > mappings[j].Confidence
			})

			result := struct {
				Mappings []FieldMapping `json:"mappings"`
				Count    int            `json:"count"`
			}{
				Mappings: mappings,
				Count:    len(mappings),
			}
			return json.Marshal(result)
		},
	}
}

type schemaField struct {
	Name string
	Type string
}

// extractFields parses a JSON Schema "properties" map and returns field names with types.
func extractFields(schema json.RawMessage) []schemaField {
	var s struct {
		Properties map[string]struct {
			Type string `json:"type"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(schema, &s); err != nil {
		return nil
	}

	fields := make([]schemaField, 0, len(s.Properties))
	for name, prop := range s.Properties {
		fields = append(fields, schemaField{Name: name, Type: prop.Type})
	}
	sort.Slice(fields, func(i, j int) bool {
		return fields[i].Name < fields[j].Name
	})
	return fields
}

// normalizeFieldName converts a field name to a canonical lowercase form,
// stripping underscores, hyphens, and converting camelCase.
func normalizeFieldName(name string) string {
	// Insert underscores before uppercase letters for camelCase splitting
	var buf strings.Builder
	buf.Grow(len(name) + 4)
	for i, r := range name {
		if i > 0 && r >= 'A' && r <= 'Z' {
			buf.WriteByte('_')
		}
		buf.WriteRune(r)
	}
	// Lowercase and strip separators
	s := strings.ToLower(buf.String())
	s = strings.ReplaceAll(s, "-", "")
	s = strings.ReplaceAll(s, "_", "")
	s = strings.TrimSpace(s)
	return s
}
