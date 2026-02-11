/**
 * @flowforge/ui-angular — Angular adapter for FlowForge UI components.
 */

// Services
export { FlowForgeService } from "./services/flowforge.service.js";
export { FlowForgeOAuthService } from "./services/oauth.service.js";
export { storeToSignal, storeToObservable, storeSelectToSignal } from "./services/store.service.js";

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
