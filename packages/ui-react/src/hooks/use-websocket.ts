/**
 * React hook for WebSocket connections with auto-reconnect.
 * Framework-specific wrapper — the reconnection logic lives here
 * since it requires React lifecycle management.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type { WebSocketEvent } from "@flowforge/ui-core";

interface UseWebSocketResult {
  connected: boolean;
  lastEvent: WebSocketEvent | null;
  error: Error | null;
  send: (data: unknown) => void;
  reconnect: () => void;
}

const MAX_RECONNECT_DELAY = 30000;
const BASE_RECONNECT_DELAY = 1000;

export function useWebSocket(url: string | null): UseWebSocketResult {
  const [connected, setConnected] = useState(false);
  const [lastEvent, setLastEvent] = useState<WebSocketEvent | null>(null);
  const [error, setError] = useState<Error | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);
  const reconnectAttemptsRef = useRef(0);

  const connect = useCallback(() => {
    if (!url) return;

    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }

    try {
      const ws = new WebSocket(url);
      wsRef.current = ws;

      ws.onopen = () => {
        if (mountedRef.current) {
          setConnected(true);
          setError(null);
          reconnectAttemptsRef.current = 0;
        }
      };

      ws.onmessage = (event) => {
        if (mountedRef.current) {
          try {
            const parsed = JSON.parse(event.data) as WebSocketEvent;
            setLastEvent(parsed);
          } catch {
            setError(new Error("Failed to parse WebSocket message"));
          }
        }
      };

      ws.onerror = () => {
        if (mountedRef.current) {
          setError(new Error("WebSocket connection error"));
        }
      };

      ws.onclose = (event) => {
        if (mountedRef.current) {
          setConnected(false);
          if (!event.wasClean) {
            const attempt = reconnectAttemptsRef.current;
            const delay = Math.min(
              BASE_RECONNECT_DELAY * Math.pow(2, attempt),
              MAX_RECONNECT_DELAY,
            );
            reconnectAttemptsRef.current = attempt + 1;
            reconnectTimeoutRef.current = setTimeout(() => {
              if (mountedRef.current) connect();
            }, delay);
          }
        }
      };
    } catch (err) {
      if (mountedRef.current) {
        setError(err instanceof Error ? err : new Error(String(err)));
      }
    }
  }, [url]);

  useEffect(() => {
    mountedRef.current = true;
    connect();
    return () => {
      mountedRef.current = false;
      if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
      if (wsRef.current) {
        wsRef.current.close();
        wsRef.current = null;
      }
    };
  }, [connect]);

  const send = useCallback((data: unknown) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(JSON.stringify(data));
    }
  }, []);

  const reconnect = useCallback(() => {
    reconnectAttemptsRef.current = 0;
    if (reconnectTimeoutRef.current) clearTimeout(reconnectTimeoutRef.current);
    connect();
  }, [connect]);

  return useMemo(
    () => ({ connected, lastEvent, error, send, reconnect }),
    [connected, lastEvent, error, send, reconnect],
  );
}
