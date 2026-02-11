/**
 * Framework-agnostic types for FlowForge UI.
 * Re-exports from @flowforge/proto-ts where available,
 * plus UI-specific types that aren't in the proto layer.
 */

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
// Connector types
// ---------------------------------------------------------------------------

export type ConnectorType = "source" | "destination" | "bidirectional";

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

export interface ConnectorFilter {
  search: string;
  type: ConnectorType | "all";
  category: string;
}

// ---------------------------------------------------------------------------
// Field mapping types
// ---------------------------------------------------------------------------

export interface FieldTransform {
  type: string;
  expression: string;
  config: Record<string, unknown>;
}

export interface MappingPair {
  id: string;
  sourceField: string;
  destinationField: string;
  transform: FieldTransform | null;
  isAutoMapped: boolean;
  status: "valid" | "error" | "warning";
}

// ---------------------------------------------------------------------------
// Sync types
// ---------------------------------------------------------------------------

export type SyncStatus =
  | "pending"
  | "running"
  | "completed"
  | "failed"
  | "cancelled"
  | "paused";

export type SyncRunPhase = "extract" | "transform" | "load";

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
// Schema diff
// ---------------------------------------------------------------------------

export type DiffStatus = "added" | "removed" | "changed" | "unchanged";

// ---------------------------------------------------------------------------
// Drag state for field mapper
// ---------------------------------------------------------------------------

export interface DragState {
  sourceField: SchemaField | null;
  side: "source" | "destination";
  startX: number;
  startY: number;
  currentX: number;
  currentY: number;
}

export interface LinePosition {
  sx: number;
  sy: number;
  dx: number;
  dy: number;
}
