/**
 * @flowforge/ui-svelte — Svelte adapter for FlowForge UI components.
 */

// Svelte stores
export { storeToSvelteReadable, storeSelectToSvelteReadable } from "./stores/flowforge-store.js";
export { createOAuthStore } from "./stores/oauth-store.js";
export type { OAuthStore, OAuthStoreValue } from "./stores/oauth-store.js";
export { createWebSocketStore } from "./stores/websocket-store.js";
export type { WebSocketStore, WebSocketStoreValue } from "./stores/websocket-store.js";

// Re-export core for convenience
export {
  Store,
  createConnectorSelectorStore,
  createFieldMapperStore,
  createSyncMonitorStore,
  createSchemaViewerStore,
  flattenSchema,
  countFields,
  extractCategories,
  matchesFilter,
  fieldMatchesSearch,
  bezierPath,
  bezierMidpoint,
  calculateLinePositions,
  formatDuration,
  calculateElapsed,
  formatNumber,
  animateCounter,
  easeOutCubic,
  getPhaseProgress,
  PHASE_ORDER,
  OAuthManager,
  TokenStore,
  generateCodeVerifier,
  generateCodeChallenge,
} from "@flowforge/ui-core";

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
  ConnectorSelectorStore,
  FieldMapperStore,
  SyncMonitorStore,
  SchemaViewerStore,
  OAuthManagerConfig,
  OAuthCompleteMessage,
  OAuthSession,
} from "@flowforge/ui-core";
