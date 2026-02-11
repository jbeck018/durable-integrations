// Package schema provides schema management for FlowForge streams.
// It handles schema discovery, versioned storage, comparison, and
// breaking change detection to protect sync pipelines from unexpected
// schema evolution.
package schema

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/internal/observability/logging"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// ChangeType identifies the kind of schema change between two versions.
type ChangeType string

const (
	ChangeAddedField   ChangeType = "added_field"
	ChangeRemovedField ChangeType = "removed_field"
	ChangeTypeChange   ChangeType = "type_change"
	ChangeNullable     ChangeType = "nullable_change"
)

// SchemaChange records a single difference between two schema versions.
type SchemaChange struct {
	Type       ChangeType `json:"type"`
	Field      string     `json:"field"`
	OldValue   string     `json:"old_value,omitempty"`
	NewValue   string     `json:"new_value,omitempty"`
	IsBreaking bool       `json:"is_breaking"`
}

// SchemaDiff is the result of comparing two schema versions.
type SchemaDiff struct {
	Changes      []SchemaChange `json:"changes"`
	IsCompatible bool           `json:"is_compatible"`
}

// StreamRepository provides persistence for stream schemas and versions.
type StreamRepository interface {
	GetStreamByID(ctx context.Context, streamID string) (*StreamRecord, error)
	UpsertStream(ctx context.Context, record *StreamRecord) (*StreamRecord, error)
	ListStreamsByConnection(ctx context.Context, connectionID string) ([]*StreamRecord, error)
	CreateSchemaVersion(ctx context.Context, sv *SchemaVersionRecord) (*SchemaVersionRecord, error)
	GetSchemaVersion(ctx context.Context, streamID string, version int) (*SchemaVersionRecord, error)
	GetLatestSchemaVersion(ctx context.Context, streamID string) (*SchemaVersionRecord, error)
}

// StreamRecord is the database representation of a stream.
type StreamRecord struct {
	ID                 string          `json:"id"`
	ConnectionID       string          `json:"connection_id"`
	Name               string          `json:"name"`
	Namespace          string          `json:"namespace"`
	Schema             json.RawMessage `json:"schema"`
	SupportedSyncModes []string        `json:"supported_sync_modes"`
}

// SchemaVersionRecord is the database representation of a schema version.
type SchemaVersionRecord struct {
	ID       string          `json:"id"`
	StreamID string          `json:"stream_id"`
	Version  int             `json:"version"`
	Schema   json.RawMessage `json:"schema"`
	Diff     json.RawMessage `json:"diff"`
}

// SchemaCache provides caching for stream schemas.
type SchemaCache interface {
	Get(ctx context.Context, streamID string) (json.RawMessage, bool, error)
	Set(ctx context.Context, streamID string, schema json.RawMessage) error
	Invalidate(ctx context.Context, streamID string) error
}

// SchemaManager provides schema discovery, storage, comparison, and
// breaking change detection for FlowForge data streams.
type SchemaManager struct {
	streamRepo StreamRepository
	cache      SchemaCache
	logger     *logging.Logger
}

// NewSchemaManager creates a SchemaManager. The cache parameter may be nil
// if caching is not desired.
func NewSchemaManager(streamRepo StreamRepository, cache SchemaCache) *SchemaManager {
	return &SchemaManager{
		streamRepo: streamRepo,
		cache:      cache,
		logger:     logging.Global().WithField("component", "schema_manager"),
	}
}

// DiscoverAndStore persists the streams from a discovered catalog into the
// schema repository. For each stream, it creates or updates the stream record
// and creates a new schema version if the schema has changed.
func (sm *SchemaManager) DiscoverAndStore(ctx context.Context, connectionID string, catalog *protocol.Catalog) error {
	if connectionID == "" {
		return fmt.Errorf("connectionID is required: %w", common.ErrInvalidConfig)
	}
	if catalog == nil || len(catalog.Streams) == 0 {
		return fmt.Errorf("catalog is empty: %w", common.ErrInvalidConfig)
	}

	for _, stream := range catalog.Streams {
		syncModes := make([]string, len(stream.SupportedSyncModes))
		for i, m := range stream.SupportedSyncModes {
			syncModes[i] = string(m)
		}

		record := &StreamRecord{
			ConnectionID:       connectionID,
			Name:               stream.Name,
			Namespace:          stream.Namespace,
			Schema:             stream.Schema,
			SupportedSyncModes: syncModes,
		}

		upserted, err := sm.streamRepo.UpsertStream(ctx, record)
		if err != nil {
			return fmt.Errorf("failed to upsert stream %q: %w", stream.Name, err)
		}

		// Check if schema changed by comparing with the latest version.
		latestVersion, err := sm.streamRepo.GetLatestSchemaVersion(ctx, upserted.ID)
		schemaChanged := false
		if err != nil {
			// No existing version — this is a new stream, so always create a version.
			schemaChanged = true
		} else {
			// Compare the schemas.
			changes, detectErr := DetectBreakingChanges(latestVersion.Schema, stream.Schema)
			if detectErr != nil {
				sm.logger.WithContext(ctx).Warn("schema comparison failed, creating new version",
					"stream", stream.Name,
					"error", detectErr,
				)
				schemaChanged = true
			} else if len(changes) > 0 {
				schemaChanged = true
			}
		}

		if schemaChanged {
			var diffJSON json.RawMessage
			if latestVersion != nil {
				changes, _ := DetectBreakingChanges(latestVersion.Schema, stream.Schema)
				diffBytes, marshalErr := json.Marshal(changes)
				if marshalErr == nil {
					diffJSON = diffBytes
				}
			}
			if diffJSON == nil {
				diffJSON = json.RawMessage("[]")
			}

			_, err := sm.streamRepo.CreateSchemaVersion(ctx, &SchemaVersionRecord{
				StreamID: upserted.ID,
				Schema:   stream.Schema,
				Diff:     diffJSON,
			})
			if err != nil {
				return fmt.Errorf("failed to create schema version for stream %q: %w", stream.Name, err)
			}

			// Invalidate cache for this stream.
			if sm.cache != nil {
				_ = sm.cache.Invalidate(ctx, upserted.ID)
			}
		}

		sm.logger.WithContext(ctx).Debug("stream processed",
			"stream", stream.Name,
			"schema_changed", schemaChanged,
		)
	}

	sm.logger.WithContext(ctx).Info("catalog discovery stored",
		"connection_id", connectionID,
		"streams", len(catalog.Streams),
	)

	return nil
}

// GetSchema retrieves the latest schema for a stream by ID. It checks the
// cache first, then falls back to the persistent store.
func (sm *SchemaManager) GetSchema(ctx context.Context, streamID string) (json.RawMessage, error) {
	if streamID == "" {
		return nil, fmt.Errorf("streamID is required: %w", common.ErrInvalidConfig)
	}

	// Check cache.
	if sm.cache != nil {
		cached, found, err := sm.cache.Get(ctx, streamID)
		if err != nil {
			sm.logger.WithContext(ctx).Warn("schema cache read failed",
				"stream_id", streamID,
				"error", err,
			)
		} else if found {
			return cached, nil
		}
	}

	// Fetch the stream record which contains the current schema.
	record, err := sm.streamRepo.GetStreamByID(ctx, streamID)
	if err != nil {
		return nil, fmt.Errorf("failed to get stream %s: %w", streamID, err)
	}

	// Populate cache.
	if sm.cache != nil {
		if cacheErr := sm.cache.Set(ctx, streamID, record.Schema); cacheErr != nil {
			sm.logger.WithContext(ctx).Warn("schema cache write failed",
				"stream_id", streamID,
				"error", cacheErr,
			)
		}
	}

	return record.Schema, nil
}

// CompareSchemas compares two specific schema versions of a stream and
// returns a SchemaDiff describing the changes between them.
func (sm *SchemaManager) CompareSchemas(ctx context.Context, streamID string, v1, v2 int) (*SchemaDiff, error) {
	if streamID == "" {
		return nil, fmt.Errorf("streamID is required: %w", common.ErrInvalidConfig)
	}
	if v1 == v2 {
		return &SchemaDiff{Changes: nil, IsCompatible: true}, nil
	}

	version1, err := sm.streamRepo.GetSchemaVersion(ctx, streamID, v1)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema version %d for stream %s: %w", v1, streamID, err)
	}

	version2, err := sm.streamRepo.GetSchemaVersion(ctx, streamID, v2)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema version %d for stream %s: %w", v2, streamID, err)
	}

	changes, err := DetectBreakingChanges(version1.Schema, version2.Schema)
	if err != nil {
		return nil, fmt.Errorf("failed to compare schemas: %w", err)
	}

	isCompatible := true
	for _, change := range changes {
		if change.IsBreaking {
			isCompatible = false
			break
		}
	}

	return &SchemaDiff{
		Changes:      changes,
		IsCompatible: isCompatible,
	}, nil
}

// DetectBreakingChanges compares two JSON schemas (represented as raw JSON)
// and returns a list of changes, each annotated with whether it is breaking.
//
// Breaking changes:
//   - removed_field: a field present in old is absent in new
//   - type_change: the type of a field changed between versions
//   - nullable_change: a field changed from nullable to non-nullable
//
// Non-breaking changes:
//   - added_field: a new field was added in the new schema
//   - nullable_change: a field changed from non-nullable to nullable
func DetectBreakingChanges(oldSchema, newSchema json.RawMessage) ([]SchemaChange, error) {
	oldProps, err := extractProperties(oldSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse old schema: %w", err)
	}

	newProps, err := extractProperties(newSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to parse new schema: %w", err)
	}

	var changes []SchemaChange

	// Collect all field names from both schemas.
	allFields := make(map[string]bool)
	for field := range oldProps {
		allFields[field] = true
	}
	for field := range newProps {
		allFields[field] = true
	}

	// Sort for deterministic output.
	sortedFields := make([]string, 0, len(allFields))
	for field := range allFields {
		sortedFields = append(sortedFields, field)
	}
	sort.Strings(sortedFields)

	for _, field := range sortedFields {
		oldProp, inOld := oldProps[field]
		newProp, inNew := newProps[field]

		if inOld && !inNew {
			// Field removed: breaking.
			changes = append(changes, SchemaChange{
				Type:       ChangeRemovedField,
				Field:      field,
				OldValue:   oldProp.Type,
				IsBreaking: true,
			})
			continue
		}

		if !inOld && inNew {
			// Field added: non-breaking.
			changes = append(changes, SchemaChange{
				Type:       ChangeAddedField,
				Field:      field,
				NewValue:   newProp.Type,
				IsBreaking: false,
			})
			continue
		}

		// Field exists in both — check for type changes.
		if oldProp.Type != newProp.Type {
			changes = append(changes, SchemaChange{
				Type:       ChangeTypeChange,
				Field:      field,
				OldValue:   oldProp.Type,
				NewValue:   newProp.Type,
				IsBreaking: true,
			})
		}

		// Check for nullable changes.
		if oldProp.Nullable != newProp.Nullable {
			// Becoming nullable is non-breaking; becoming non-nullable is breaking.
			isBreaking := oldProp.Nullable && !newProp.Nullable
			changes = append(changes, SchemaChange{
				Type:       ChangeNullable,
				Field:      field,
				OldValue:   fmt.Sprintf("nullable=%v", oldProp.Nullable),
				NewValue:   fmt.Sprintf("nullable=%v", newProp.Nullable),
				IsBreaking: isBreaking,
			})
		}
	}

	return changes, nil
}

// propertyInfo holds the type and nullability of a schema property.
type propertyInfo struct {
	Type     string
	Nullable bool
}

// extractProperties parses a JSON Schema and returns a map of field names
// to their type information. It supports the standard JSON Schema structure
// with a "properties" object and optional "type" arrays for nullable fields.
func extractProperties(schema json.RawMessage) (map[string]propertyInfo, error) {
	if len(schema) == 0 {
		return make(map[string]propertyInfo), nil
	}

	var parsed struct {
		Properties map[string]json.RawMessage `json:"properties"`
	}
	if err := json.Unmarshal(schema, &parsed); err != nil {
		// Try treating the entire schema as a flat field map.
		var flat map[string]json.RawMessage
		if flatErr := json.Unmarshal(schema, &flat); flatErr != nil {
			return nil, fmt.Errorf("unable to parse schema: %w", err)
		}
		// If there are no properties, treat top-level keys as fields.
		if _, hasProps := flat["properties"]; !hasProps {
			return extractFlatProperties(flat)
		}
		return nil, err
	}

	if parsed.Properties == nil {
		return make(map[string]propertyInfo), nil
	}

	result := make(map[string]propertyInfo, len(parsed.Properties))
	for name, propJSON := range parsed.Properties {
		info := parsePropertyType(propJSON)
		result[name] = info
	}

	return result, nil
}

// extractFlatProperties handles schemas where the top-level keys are field names
// (no "properties" wrapper).
func extractFlatProperties(flat map[string]json.RawMessage) (map[string]propertyInfo, error) {
	result := make(map[string]propertyInfo, len(flat))
	for name, propJSON := range flat {
		// Skip JSON Schema keywords that are not field definitions.
		switch name {
		case "type", "$schema", "$id", "title", "description", "required",
			"additionalProperties", "definitions", "$defs":
			continue
		}
		info := parsePropertyType(propJSON)
		result[name] = info
	}
	return result, nil
}

// parsePropertyType extracts type and nullability from a property definition.
func parsePropertyType(propJSON json.RawMessage) propertyInfo {
	var prop struct {
		Type interface{} `json:"type"`
	}
	if err := json.Unmarshal(propJSON, &prop); err != nil {
		return propertyInfo{Type: "unknown"}
	}

	switch t := prop.Type.(type) {
	case string:
		return propertyInfo{Type: t, Nullable: false}
	case []interface{}:
		// JSON Schema nullable pattern: {"type": ["string", "null"]}
		nullable := false
		primaryType := "unknown"
		for _, v := range t {
			if s, ok := v.(string); ok {
				if s == "null" {
					nullable = true
				} else {
					primaryType = s
				}
			}
		}
		return propertyInfo{Type: primaryType, Nullable: nullable}
	default:
		return propertyInfo{Type: "unknown"}
	}
}
