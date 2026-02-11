/**
 * FlowForge shared React hooks.
 * Provides data fetching, pagination, WebSocket, and domain-specific hooks.
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { getClient } from "./api";
import type {
  Connection,
  Connector,
  PaginatedResponse,
  Stream,
  Sync,
  SyncRun,
  WebSocketEvent,
} from "./types";

// ---------------------------------------------------------------------------
// useAPI<T> — generic data fetching with loading/error/refetch
// ---------------------------------------------------------------------------

interface UseAPIState<T> {
  data: T | null;
  loading: boolean;
  error: Error | null;
}

interface UseAPIResult<T> extends UseAPIState<T> {
  refetch: () => void;
}

export function useAPI<T>(fetcher: (signal: AbortSignal) => Promise<T>): UseAPIResult<T> {
  const [state, setState] = useState<UseAPIState<T>>({
    data: null,
    loading: true,
    error: null,
  });

  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;

  const mountedRef = useRef(true);
  const controllerRef = useRef<AbortController | null>(null);

  const execute = useCallback(() => {
    if (controllerRef.current) {
      controllerRef.current.abort();
    }

    const controller = new AbortController();
    controllerRef.current = controller;

    setState((prev) => ({ ...prev, loading: true, error: null }));

    fetcherRef.current(controller.signal)
      .then((data) => {
        if (mountedRef.current && !controller.signal.aborted) {
          setState({ data, loading: false, error: null });
        }
      })
      .catch((err: unknown) => {
        if (mountedRef.current && !controller.signal.aborted) {
          const error = err instanceof Error ? err : new Error(String(err));
          setState({ data: null, loading: false, error });
        }
      });
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    execute();
    return () => {
      mountedRef.current = false;
      controllerRef.current?.abort();
    };
  }, [execute]);

  return useMemo(
    () => ({
      ...state,
      refetch: execute,
    }),
    [state, execute],
  );
}

// ---------------------------------------------------------------------------
// usePagination — paginated data fetching
// ---------------------------------------------------------------------------

interface UsePaginationResult<T> {
  data: T[];
  total: number;
  page: number;
  pageSize: number;
  hasNext: boolean;
  hasPrev: boolean;
  loading: boolean;
  error: Error | null;
  goToPage: (page: number) => void;
  nextPage: () => void;
  prevPage: () => void;
  setPageSize: (size: number) => void;
  refetch: () => void;
}

export function usePagination<T>(
  fetcher: (page: number, pageSize: number, signal: AbortSignal) => Promise<PaginatedResponse<T>>,
  initialPageSize: number = 20,
): UsePaginationResult<T> {
  const [page, setPage] = useState(1);
  const [pageSize, setPageSizeState] = useState(initialPageSize);
  const [state, setState] = useState<{
    data: T[];
    total: number;
    hasNext: boolean;
    hasPrev: boolean;
    loading: boolean;
    error: Error | null;
  }>({
    data: [],
    total: 0,
    hasNext: false,
    hasPrev: false,
    loading: true,
    error: null,
  });

  const fetcherRef = useRef(fetcher);
  fetcherRef.current = fetcher;
  const mountedRef = useRef(true);
  const controllerRef = useRef<AbortController | null>(null);

  const execute = useCallback(() => {
    if (controllerRef.current) {
      controllerRef.current.abort();
    }

    const controller = new AbortController();
    controllerRef.current = controller;

    setState((prev) => ({ ...prev, loading: true, error: null }));

    fetcherRef.current(page, pageSize, controller.signal)
      .then((result) => {
        if (mountedRef.current && !controller.signal.aborted) {
          setState({
            data: result.data,
            total: result.total,
            hasNext: result.has_next,
            hasPrev: result.has_prev,
            loading: false,
            error: null,
          });
        }
      })
      .catch((err: unknown) => {
        if (mountedRef.current && !controller.signal.aborted) {
          const error = err instanceof Error ? err : new Error(String(err));
          setState((prev) => ({ ...prev, loading: false, error }));
        }
      });
  }, [page, pageSize]);

  useEffect(() => {
    mountedRef.current = true;
    execute();
    return () => {
      mountedRef.current = false;
      controllerRef.current?.abort();
    };
  }, [execute]);

  const goToPage = useCallback((p: number) => {
    setPage(Math.max(1, p));
  }, []);

  const nextPage = useCallback(() => {
    if (state.hasNext) {
      setPage((prev) => prev + 1);
    }
  }, [state.hasNext]);

  const prevPage = useCallback(() => {
    if (state.hasPrev) {
      setPage((prev) => Math.max(1, prev - 1));
    }
  }, [state.hasPrev]);

  const setPageSize = useCallback((size: number) => {
    setPageSizeState(size);
    setPage(1);
  }, []);

  return useMemo(
    () => ({
      ...state,
      page,
      pageSize,
      goToPage,
      nextPage,
      prevPage,
      setPageSize,
      refetch: execute,
    }),
    [state, page, pageSize, goToPage, nextPage, prevPage, setPageSize, execute],
  );
}

// ---------------------------------------------------------------------------
// useWebSocket — real-time event stream
// ---------------------------------------------------------------------------

interface UseWebSocketResult {
  connected: boolean;
  lastEvent: WebSocketEvent | null;
  error: Error | null;
  send: (data: unknown) => void;
  reconnect: () => void;
}

export function useWebSocket(url: string | null): UseWebSocketResult {
  const [connected, setConnected] = useState(false);
  const [lastEvent, setLastEvent] = useState<WebSocketEvent | null>(null);
  const [error, setError] = useState<Error | null>(null);

  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mountedRef = useRef(true);
  const reconnectAttemptsRef = useRef(0);

  const MAX_RECONNECT_DELAY = 30000;
  const BASE_RECONNECT_DELAY = 1000;

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
              if (mountedRef.current) {
                connect();
              }
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
      if (reconnectTimeoutRef.current) {
        clearTimeout(reconnectTimeoutRef.current);
      }
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
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current);
    }
    connect();
  }, [connect]);

  return useMemo(
    () => ({ connected, lastEvent, error, send, reconnect }),
    [connected, lastEvent, error, send, reconnect],
  );
}

// ---------------------------------------------------------------------------
// Domain-specific hooks
// ---------------------------------------------------------------------------

export function useConnectors(params?: { type?: string; category?: string; search?: string }) {
  const client = getClient();
  const stableKey = JSON.stringify(params ?? {});

  return usePagination<Connector>(
    useCallback(
      (page, pageSize, signal) =>
        client.listConnectors({ page, page_size: pageSize, ...params }, signal),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [stableKey],
    ),
  );
}

export function useConnections(params?: { tenant_id?: string; connector_id?: string }) {
  const client = getClient();
  const stableKey = JSON.stringify(params ?? {});

  return usePagination<Connection>(
    useCallback(
      (page, pageSize, signal) =>
        client.listConnections({ page, page_size: pageSize, ...params }, signal),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [stableKey],
    ),
  );
}

export function useSyncs(params?: { tenant_id?: string; status?: string }) {
  const client = getClient();
  const stableKey = JSON.stringify(params ?? {});

  return usePagination<Sync>(
    useCallback(
      (page, pageSize, signal) =>
        client.listSyncs({ page, page_size: pageSize, ...params }, signal),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [stableKey],
    ),
  );
}

export function useSyncRuns(syncId: string, params?: { status?: string }) {
  const client = getClient();
  const stableKey = JSON.stringify({ syncId, ...(params ?? {}) });

  return usePagination<SyncRun>(
    useCallback(
      (page, pageSize, signal) =>
        client.listSyncRuns(syncId, { page, page_size: pageSize, ...params }, signal),
      // eslint-disable-next-line react-hooks/exhaustive-deps
      [stableKey],
    ),
  );
}

export function useStreams(connectionId: string) {
  const client = getClient();

  return useAPI<Stream[]>(
    useCallback(
      (signal: AbortSignal) =>
        client
          .discoverStreams(connectionId, signal)
          .then((response) => response.data.streams),
      [client, connectionId],
    ),
  );
}

// ---------------------------------------------------------------------------
// useDebounce — utility for search inputs
// ---------------------------------------------------------------------------

export function useDebounce<T>(value: T, delayMs: number): T {
  const [debounced, setDebounced] = useState(value);

  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(timer);
  }, [value, delayMs]);

  return debounced;
}

// ---------------------------------------------------------------------------
// useElementRect — tracks element dimensions for SVG positioning
// ---------------------------------------------------------------------------

export function useElementRect(
  elementRef: React.RefObject<HTMLElement | null>,
): DOMRect | null {
  const [rect, setRect] = useState<DOMRect | null>(null);

  useEffect(() => {
    const el = elementRef.current;
    if (!el) return;

    const observer = new ResizeObserver(() => {
      setRect(el.getBoundingClientRect());
    });

    observer.observe(el);
    setRect(el.getBoundingClientRect());

    return () => observer.disconnect();
  }, [elementRef]);

  return rect;
}
