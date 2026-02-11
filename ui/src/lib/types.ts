/**
 * FlowForge shared TypeScript types.
 * These mirror the Go protocol types defined in pkg/protocol/protocol.go
 * and the REST API resource models.
 */

// ---------------------------------------------------------------------------
// Protocol enums
// ---------------------------------------------------------------------------

export type MessageType =
  | "SPEC"
  | "CHECK"
  | "DISCOVER"
  | "READ"
  | "WRITE"
  | "RECORD"
  | "STATE"
  | "SCHEMA"
  | "LOG"
  | "CONTROL"
  | "MCP_TOOL_CALL"
  | "MCP_TOOL_RESULT";

export type SyncMode = "full_refresh" | "incremental";

export type DestinationSyncMode = "append" | "upsert" | "merge" | "soft_delete";

export type CheckStatus = "SUCCEEDED" | "FAILED";

export type StateType = "STREAM" | "GLOBAL";

export type SchemaChangeType = "" | "NEW_COLUMN" | "REMOVED_COLUMN" | "TYPE_CHANGE";

export type LogLevel = "DEBUG" | "INFO" | "WARN" | "ERROR";

export type ControlType = "RATE_LIMIT" | "BACKPRESSURE" | "PAUSE" | "RESUME";

export type SyncStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "paused";

export type SyncRunPhase = "extract" | "transform" | "load";

export type ConnectorType = "source" | "destination" | "bidirectional";

export type DiffStatus = "added" | "removed" | "changed" | "unchanged";

// ---------------------------------------------------------------------------
// Core domain models
// ---------------------------------------------------------------------------

export interface Connector {
  id: string;
  name: string;
  display_name: string;
  type: ConnectorType;
  category: string;
  version: string;
  icon: string;
  description: string;
  documentation_url: string;
  config_schema: JSONSchema;
  created_at: string;
  updated_at: string;
}

export interface Connection {
  id: string;
  connector_id: string;
  connector_name: string;
  tenant_id: string;
  name: string;
  config: Record<string, unknown>;
  status: CheckStatus;
  status_message: string;
  created_at: string;
  updated_at: string;
}

export interface Sync {
  id: string;
  name: string;
  source_connection_id: string;
  destination_connection_id: string;
  tenant_id: string;
  schedule: string;
  status: SyncStatus;
  catalog: ConfiguredCatalog;
  field_mappings: FieldMapping[];
  created_at: string;
  updated_at: string;
}

export interface SyncRun {
  id: string;
  sync_id: string;
  status: SyncStatus;
  phase: SyncRunPhase;
  started_at: string;
  completed_at: string | null;
  records_extracted: number;
  records_loaded: number;
  records_rejected: number;
  bytes_processed: number;
  error_count: number;
  errors: SyncRunError[];
  duration_ms: number;
}

export interface SyncRunError {
  message: string;
  record?: Record<string, unknown>;
  error_code: string;
  timestamp: string;
}

export interface Stream {
  name: string;
  namespace: string;
  display_name: string;
  json_schema: JSONSchema;
  supported_sync_modes: SyncMode[];
  default_cursor_field: string[];
  source_defined_primary_key: boolean;
  primary_key: string[][];
}

export interface ConfiguredStream {
  stream: Stream;
  sync_mode: SyncMode;
  destination_sync_mode: DestinationSyncMode;
  cursor_field: string[];
  primary_key: string[][];
}

export interface ConfiguredCatalog {
  streams: ConfiguredStream[];
}

export interface Catalog {
  streams: Stream[];
}

export interface Tenant {
  id: string;
  name: string;
  slug: string;
  settings: Record<string, unknown>;
  created_at: string;
  updated_at: string;
}

export interface FieldMapping {
  id: string;
  sync_id: string;
  source_stream: string;
  source_field: string;
  destination_stream: string;
  destination_field: string;
  transform: FieldTransform | null;
  is_auto_mapped: boolean;
}

export interface FieldTransform {
  type: string;
  expression: string;
  config: Record<string, unknown>;
}

export interface MCPServer {
  id: string;
  name: string;
  url: string;
  tenant_id: string;
  tools: MCPTool[];
  status: "active" | "inactive" | "error";
  created_at: string;
  updated_at: string;
}

export interface MCPTool {
  name: string;
  description: string;
  input_schema: JSONSchema;
}

// ---------------------------------------------------------------------------
// JSON Schema (subset sufficient for UI rendering)
// ---------------------------------------------------------------------------

export interface JSONSchema {
  type?: string | string[];
  properties?: Record<string, JSONSchema>;
  items?: JSONSchema;
  required?: string[];
  description?: string;
  title?: string;
  enum?: unknown[];
  default?: unknown;
  format?: string;
  nullable?: boolean;
  oneOf?: JSONSchema[];
  anyOf?: JSONSchema[];
  allOf?: JSONSchema[];
  $ref?: string;
  additionalProperties?: boolean | JSONSchema;
}

/**
 * Flattened representation of a schema field for UI rendering.
 */
export interface SchemaField {
  path: string;
  name: string;
  type: string;
  required: boolean;
  nullable: boolean;
  description: string;
  children: SchemaField[];
  depth: number;
  schema: JSONSchema;
}

// ---------------------------------------------------------------------------
// API response wrappers
// ---------------------------------------------------------------------------

export interface PaginatedResponse<T> {
  data: T[];
  total: number;
  page: number;
  page_size: number;
  has_next: boolean;
  has_prev: boolean;
}

export interface APIError {
  status: number;
  code: string;
  message: string;
  details?: Record<string, unknown>;
  request_id?: string;
}

export interface APIResponse<T> {
  data: T;
  request_id: string;
}

// ---------------------------------------------------------------------------
// WebSocket message types for real-time sync monitoring
// ---------------------------------------------------------------------------

export interface SyncProgressEvent {
  type: "sync_progress";
  sync_id: string;
  run_id: string;
  phase: SyncRunPhase;
  records_extracted: number;
  records_loaded: number;
  records_rejected: number;
  bytes_processed: number;
  error_count: number;
  status: SyncStatus;
  timestamp: string;
}

export interface SyncErrorEvent {
  type: "sync_error";
  sync_id: string;
  run_id: string;
  error: SyncRunError;
  timestamp: string;
}

export interface SyncStatusEvent {
  type: "sync_status";
  sync_id: string;
  run_id: string;
  status: SyncStatus;
  phase: SyncRunPhase;
  timestamp: string;
}

export type WebSocketEvent = SyncProgressEvent | SyncErrorEvent | SyncStatusEvent;

// ---------------------------------------------------------------------------
// Component-specific types
// ---------------------------------------------------------------------------

export interface ConnectorFilter {
  search: string;
  type: ConnectorType | "all";
  category: string;
}

export interface MappingPair {
  id: string;
  sourceField: string;
  destinationField: string;
  transform: FieldTransform | null;
  isAutoMapped: boolean;
  status: "valid" | "error" | "warning";
}

export interface SortConfig<T> {
  key: keyof T;
  direction: "asc" | "desc";
}
