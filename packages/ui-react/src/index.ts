/**
 * @flowforge/ui-react — React adapter for FlowForge UI components.
 *
 * Re-exports hooks that bridge ui-core state to React,
 * plus the FlowForge context provider.
 */

// Hooks
export { useStore, useStoreSelector } from "./hooks/use-store.js";
export { useOAuth } from "./hooks/use-oauth.js";
export { useDebounce } from "./hooks/use-debounce.js";
export { useDurationTimer } from "./hooks/use-duration-timer.js";
export { useAnimatedCounter } from "./hooks/use-animated-counter.js";
export { useWebSocket } from "./hooks/use-websocket.js";

// Provider
export { FlowForgeProvider, useFlowForgeConfig } from "./provider/FlowForgeProvider.js";
export type { FlowForgeConfig, FlowForgeProviderProps } from "./provider/FlowForgeProvider.js";

// Re-export core for convenience
export {
  // State stores
  createConnectorSelectorStore,
  createFieldMapperStore,
  createSyncMonitorStore,
  createSchemaViewerStore,
  // Logic
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
  getPhaseProgress,
  PHASE_ORDER,
  // Auth
  OAuthManager,
  TokenStore,
  generateCodeVerifier,
  generateCodeChallenge,
} from "@flowforge/ui-core";

export type {
  // Types
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
  // Store types
  ConnectorSelectorStore,
  FieldMapperStore,
  SyncMonitorStore,
  SchemaViewerStore,
  OAuthManagerConfig,
  OAuthCompleteMessage,
  OAuthSession,
} from "@flowforge/ui-core";
