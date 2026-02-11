/**
 * @flowforge/ui-core — framework-agnostic core logic for FlowForge UI.
 *
 * This package contains all business logic, state management, and utilities
 * extracted from the React components. Framework adapters (React, Angular,
 * Svelte) import from here and wire the logic to their own reactivity systems.
 */

// Types
export type {
  Connector,
  ConnectorFilter,
  ConnectorType,
  DiffStatus,
  DragState,
  FieldTransform,
  JSONSchema,
  LinePosition,
  MappingPair,
  SchemaField,
  SyncErrorEvent,
  SyncProgressEvent,
  SyncRun,
  SyncRunError,
  SyncRunPhase,
  SyncStatus,
  SyncStatusEvent,
  WebSocketEvent,
} from "./types/index.js";

// State management
export { Store } from "./state/store.js";
export type { Listener, Selector, Updater } from "./state/store.js";

export { createConnectorSelectorStore } from "./state/connector-selector.js";
export type { ConnectorSelectorState, ConnectorSelectorStore } from "./state/connector-selector.js";

export { createFieldMapperStore } from "./state/field-mapper.js";
export type { FieldMapperState, FieldMapperStore } from "./state/field-mapper.js";

export { createSyncMonitorStore } from "./state/sync-monitor.js";
export type { SyncMonitorState, SyncMonitorStore } from "./state/sync-monitor.js";

export { createSchemaViewerStore } from "./state/schema-viewer.js";
export type { SchemaViewerState, SchemaViewerStore } from "./state/schema-viewer.js";

// Logic
export { flattenSchema, countFields } from "./logic/schema-flatten.js";
export { extractCategories, matchesFilter, fieldMatchesSearch } from "./logic/field-matching.js";
export { bezierPath, bezierMidpoint, calculateLinePositions } from "./logic/svg-paths.js";
export { formatDuration, calculateElapsed, formatNumber } from "./logic/duration-format.js";
export { easeOutCubic, animateCounter } from "./logic/counter-animation.js";
export { getPhaseProgress, PHASE_ORDER } from "./logic/phase-progress.js";

// Auth
export { OAuthManager } from "./auth/oauth-manager.js";
export type { OAuthManagerConfig, OAuthCompleteMessage, OAuthInitiateResponse } from "./auth/oauth-manager.js";
export { TokenStore } from "./auth/token-store.js";
export type { OAuthSession } from "./auth/token-store.js";
export { generateCodeVerifier, generateCodeChallenge } from "./auth/pkce.js";
