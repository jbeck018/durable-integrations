/**
 * Svelte store for WebSocket connections with auto-reconnect.
 */

import { writable, type Readable } from "svelte/store";
import type { WebSocketEvent } from "@flowforge/ui-core";

export interface WebSocketStoreValue {
  connected: boolean;
  lastEvent: WebSocketEvent | null;
  error: Error | null;
}

export interface WebSocketStore extends Readable<WebSocketStoreValue> {
  send: (data: unknown) => void;
  reconnect: () => void;
  close: () => void;
}

const MAX_RECONNECT_DELAY = 30000;
const BASE_RECONNECT_DELAY = 1000;

export function createWebSocketStore(url: string | null): WebSocketStore {
  const { subscribe, set, update } = writable<WebSocketStoreValue>({
    connected: false,
    lastEvent: null,
    error: null,
  });

  let ws: WebSocket | null = null;
  let reconnectTimeout: ReturnType<typeof setTimeout> | null = null;
  let reconnectAttempts = 0;
  let destroyed = false;

  function connect() {
    if (!url || destroyed) return;

    if (ws) {
      ws.close();
      ws = null;
    }

    try {
      ws = new WebSocket(url);

      ws.onopen = () => {
        if (!destroyed) {
          update((s) => ({ ...s, connected: true, error: null }));
          reconnectAttempts = 0;
        }
      };

      ws.onmessage = (event) => {
        if (!destroyed) {
          try {
            const parsed = JSON.parse(event.data) as WebSocketEvent;
            update((s) => ({ ...s, lastEvent: parsed }));
          } catch {
            update((s) => ({ ...s, error: new Error("Failed to parse WebSocket message") }));
          }
        }
      };

      ws.onerror = () => {
        if (!destroyed) {
          update((s) => ({ ...s, error: new Error("WebSocket connection error") }));
        }
      };

      ws.onclose = (event) => {
        if (!destroyed) {
          update((s) => ({ ...s, connected: false }));
          if (!event.wasClean) {
            const delay = Math.min(
              BASE_RECONNECT_DELAY * Math.pow(2, reconnectAttempts),
              MAX_RECONNECT_DELAY,
            );
            reconnectAttempts += 1;
            reconnectTimeout = setTimeout(() => {
              if (!destroyed) connect();
            }, delay);
          }
        }
      };
    } catch (err) {
      if (!destroyed) {
        update((s) => ({
          ...s,
          error: err instanceof Error ? err : new Error(String(err)),
        }));
      }
    }
  }

  connect();

  return {
    subscribe,

    send(data: unknown) {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
      }
    },

    reconnect() {
      reconnectAttempts = 0;
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
      connect();
    },

    close() {
      destroyed = true;
      if (reconnectTimeout) clearTimeout(reconnectTimeout);
      if (ws) {
        ws.close();
        ws = null;
      }
    },
  };
}
