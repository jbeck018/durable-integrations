// Package protocol defines the FlowForge internal data protocol.
// All message types exchanged between the platform and connectors are defined here.
// This is the single source of truth for the protocol — connectors and internal
// components both import from this package.
package protocol

import (
	"encoding/json"
	"time"
)

// MessageType enumerates all protocol message types.
type MessageType string

const (
	MessageTypeSpec          MessageType = "SPEC"
	MessageTypeCheck         MessageType = "CHECK"
	MessageTypeDiscover      MessageType = "DISCOVER"
	MessageTypeRead          MessageType = "READ"
	MessageTypeWrite         MessageType = "WRITE"
	MessageTypeRecord        MessageType = "RECORD"
	MessageTypeState         MessageType = "STATE"
	MessageTypeSchema        MessageType = "SCHEMA"
	MessageTypeLog           MessageType = "LOG"
	MessageTypeControl       MessageType = "CONTROL"
	MessageTypeMCPToolCall   MessageType = "MCP_TOOL_CALL"
	MessageTypeMCPToolResult MessageType = "MCP_TOOL_RESULT"
)

// Message is the envelope for all protocol communication.
// Only one of the payload fields will be non-nil for any given message.
type Message struct {
	Type          MessageType    `json:"type"`
	Record        *Record        `json:"record,omitempty"`
	State         *State         `json:"state,omitempty"`
	Schema        *SchemaMessage `json:"schema,omitempty"`
	Log           *Log           `json:"log,omitempty"`
	Control       *Control       `json:"control,omitempty"`
	Spec          *Spec          `json:"spec,omitempty"`
	MCPToolCall   *MCPToolCall   `json:"mcp_tool_call,omitempty"`
	MCPToolResult *MCPToolResult `json:"mcp_tool_result,omitempty"`
}

// Record represents a single data record from a stream.
type Record struct {
	Stream    string          `json:"stream"`
	Namespace string          `json:"namespace,omitempty"`
	Data      json.RawMessage `json:"data"`
	EmittedAt time.Time       `json:"emitted_at"`
}

// State represents a checkpoint for incremental sync resumption.
type State struct {
	Type   StateType       `json:"type"`
	Stream string          `json:"stream,omitempty"`
	Data   json.RawMessage `json:"data"`
}

// StateType identifies the scope of a state checkpoint.
type StateType string

const (
	StateTypeStream StateType = "STREAM"
	StateTypeGlobal StateType = "GLOBAL"
)

// SchemaMessage signals a stream schema declaration or change.
type SchemaMessage struct {
	Stream string          `json:"stream"`
	Schema json.RawMessage `json:"schema"`
	Change SchemaChange    `json:"change,omitempty"`
}

// SchemaChange indicates what kind of schema event occurred.
type SchemaChange string

const (
	SchemaChangeNone          SchemaChange = ""
	SchemaChangeNewColumn     SchemaChange = "NEW_COLUMN"
	SchemaChangeRemovedColumn SchemaChange = "REMOVED_COLUMN"
	SchemaChangeTypeChange    SchemaChange = "TYPE_CHANGE"
)

// LogLevel enumerates log severities.
type LogLevel string

const (
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
)

// Log represents a structured log message from a connector.
type Log struct {
	Level     LogLevel        `json:"level"`
	Message   string          `json:"message"`
	Timestamp time.Time       `json:"timestamp"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// ControlType enumerates control signal types.
type ControlType string

const (
	ControlTypeRateLimit    ControlType = "RATE_LIMIT"
	ControlTypeBackpressure ControlType = "BACKPRESSURE"
	ControlTypePause        ControlType = "PAUSE"
	ControlTypeResume       ControlType = "RESUME"
)

// Control represents a bidirectional control signal.
type Control struct {
	Type    ControlType     `json:"type"`
	Message string          `json:"message,omitempty"`
	Data    json.RawMessage `json:"data,omitempty"`
}

// Spec holds a connector's configuration JSON Schema.
type Spec struct {
	DocumentationURL string          `json:"documentation_url,omitempty"`
	ConfigSchema     json.RawMessage `json:"config_schema"`
}

// MCPToolCall represents a tool invocation from an AI agent.
type MCPToolCall struct {
	ID         string          `json:"id"`
	Tool       string          `json:"tool"`
	Parameters json.RawMessage `json:"parameters"`
	AgentID    string          `json:"agent_id,omitempty"`
	TenantID   string          `json:"tenant_id,omitempty"`
}

// MCPToolResult contains the result of an MCP tool execution.
type MCPToolResult struct {
	CallID  string          `json:"call_id"`
	Content json.RawMessage `json:"content"`
	IsError bool            `json:"is_error,omitempty"`
}

// SyncMode represents the supported sync modes for a stream.
type SyncMode string

const (
	SyncModeFullRefresh SyncMode = "full_refresh"
	SyncModeIncremental SyncMode = "incremental"
)

// DestinationSyncMode represents destination write semantics.
type DestinationSyncMode string

const (
	DestSyncModeAppend     DestinationSyncMode = "append"
	DestSyncModeUpsert     DestinationSyncMode = "upsert"
	DestSyncModeMerge      DestinationSyncMode = "merge"
	DestSyncModeSoftDelete DestinationSyncMode = "soft_delete"
)

// Stream describes a data stream exposed by a connector.
type Stream struct {
	Name               string          `json:"name"`
	Namespace          string          `json:"namespace,omitempty"`
	DisplayName        string          `json:"display_name,omitempty"`
	Schema             json.RawMessage `json:"json_schema"`
	SupportedSyncModes []SyncMode      `json:"supported_sync_modes"`
	DefaultCursorField []string        `json:"default_cursor_field,omitempty"`
	SourceDefinedPK    bool            `json:"source_defined_primary_key,omitempty"`
	PrimaryKey         [][]string      `json:"primary_key,omitempty"`
}

// ConfiguredStream describes a user-selected stream with sync configuration.
type ConfiguredStream struct {
	Stream              Stream              `json:"stream"`
	SyncMode            SyncMode            `json:"sync_mode"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode,omitempty"`
	CursorField         []string            `json:"cursor_field,omitempty"`
	PrimaryKey          [][]string          `json:"primary_key,omitempty"`
}

// Catalog contains the full set of streams a connector exposes.
type Catalog struct {
	Streams []Stream `json:"streams"`
}

// ConfiguredCatalog contains the user-selected streams and their configurations.
type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
}

// CheckStatus indicates the result of a connectivity check.
type CheckStatus string

const (
	CheckStatusSucceeded CheckStatus = "SUCCEEDED"
	CheckStatusFailed    CheckStatus = "FAILED"
)

// CheckResult holds the result of a connector check operation.
type CheckResult struct {
	Status  CheckStatus `json:"status"`
	Message string      `json:"message,omitempty"`
}

// WriteResult holds the result of a write operation.
type WriteResult struct {
	RecordsWritten int64        `json:"records_written"`
	Errors         []WriteError `json:"errors,omitempty"`
	StateMessages  []State      `json:"state_messages,omitempty"`
}

// WriteError records a single write failure.
type WriteError struct {
	Message   string          `json:"message"`
	Record    json.RawMessage `json:"record,omitempty"`
	ErrorCode string          `json:"error_code,omitempty"`
}
