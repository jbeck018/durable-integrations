/**
 * Sync monitor state — manages WebSocket connection, live progress updates,
 * and sync run data. Framework-agnostic.
 */

import type { SyncProgressEvent, SyncRun, WebSocketEvent } from "../types/index.js";
import { Store } from "./store.js";

export interface SyncMonitorState {
  runs: SyncRun[];
  progressMap: Map<string, SyncProgressEvent>;
  connected: boolean;
  loading: boolean;
}

export function createSyncMonitorStore() {
  const store = new Store<SyncMonitorState>({
    runs: [],
    progressMap: new Map(),
    connected: false,
    loading: true,
  });

  return {
    store,

    setRuns(runs: SyncRun[]) {
      store.setState((s) => ({ ...s, runs, loading: false }));
    },

    setLoading(loading: boolean) {
      store.setState((s) => ({ ...s, loading }));
    },

    setConnected(connected: boolean) {
      store.setState((s) => ({ ...s, connected }));
    },

    handleWebSocketEvent(event: WebSocketEvent) {
      if (event.type === "sync_progress") {
        store.setState((s) => {
          const next = new Map(s.progressMap);
          next.set(event.run_id, event as SyncProgressEvent);
          return { ...s, progressMap: next };
        });
      }
    },

    getProgress(runId: string): SyncProgressEvent | null {
      return store.getState().progressMap.get(runId) ?? null;
    },
  };
}

export type SyncMonitorStore = ReturnType<typeof createSyncMonitorStore>;
