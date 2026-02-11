/**
 * SyncMonitor — real-time sync dashboard.
 * Displays active syncs with live progress bars (extract/transform/load phases),
 * animated record counters, error details, duration timer, and control buttons.
 * Uses WebSocket for real-time updates.
 */

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  SyncProgressEvent,
  SyncRun,
  SyncRunError,
  SyncRunPhase,
  SyncStatus,
  WebSocketEvent,
} from "../../lib/types";
import { useAPI, useWebSocket } from "../../lib/hooks";
import { getClient } from "../../lib/api";

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "16px",
    width: "100%",
  } as React.CSSProperties,
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: "12px",
  } as React.CSSProperties,
  title: {
    fontSize: "18px",
    fontWeight: 600,
    color: "#111827",
    margin: 0,
  } as React.CSSProperties,
  connectionDot: (connected: boolean): React.CSSProperties => ({
    width: "8px",
    height: "8px",
    borderRadius: "50%",
    background: connected ? "#22c55e" : "#ef4444",
    display: "inline-block",
    marginRight: "6px",
  }),
  connectionStatus: {
    fontSize: "12px",
    color: "#6b7280",
    display: "flex",
    alignItems: "center",
  } as React.CSSProperties,
  card: {
    border: "1px solid #e5e7eb",
    borderRadius: "12px",
    background: "#ffffff",
    overflow: "hidden",
  } as React.CSSProperties,
  cardHeader: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    padding: "12px 16px",
    borderBottom: "1px solid #f3f4f6",
  } as React.CSSProperties,
  cardBody: {
    padding: "16px",
    display: "flex",
    flexDirection: "column" as const,
    gap: "16px",
  } as React.CSSProperties,
  syncName: {
    fontSize: "14px",
    fontWeight: 600,
    color: "#111827",
  } as React.CSSProperties,
  statusBadge: (status: SyncStatus): React.CSSProperties => {
    const colorMap: Record<SyncStatus, { bg: string; text: string }> = {
      pending: { bg: "#f3f4f6", text: "#6b7280" },
      running: { bg: "#dbeafe", text: "#1d4ed8" },
      completed: { bg: "#dcfce7", text: "#166534" },
      failed: { bg: "#fef2f2", text: "#dc2626" },
      cancelled: { bg: "#fef3c7", text: "#92400e" },
      paused: { bg: "#fef9c3", text: "#854d0e" },
    };
    const c = colorMap[status] ?? { bg: "#f3f4f6", text: "#6b7280" };
    return {
      fontSize: "11px",
      fontWeight: 600,
      padding: "2px 8px",
      borderRadius: "9999px",
      background: c.bg,
      color: c.text,
      textTransform: "capitalize",
    };
  },
  phaseSection: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "8px",
  } as React.CSSProperties,
  phaseRow: {
    display: "flex",
    alignItems: "center",
    gap: "12px",
  } as React.CSSProperties,
  phaseLabel: (isActive: boolean): React.CSSProperties => ({
    fontSize: "12px",
    fontWeight: isActive ? 600 : 400,
    color: isActive ? "#111827" : "#9ca3af",
    width: "70px",
    textTransform: "capitalize" as const,
  }),
  progressBarOuter: {
    flex: 1,
    height: "8px",
    background: "#f3f4f6",
    borderRadius: "4px",
    overflow: "hidden",
    position: "relative" as const,
  } as React.CSSProperties,
  progressBarInner: (percent: number, isActive: boolean): React.CSSProperties => ({
    height: "100%",
    width: `${Math.min(100, Math.max(0, percent))}%`,
    background: isActive
      ? "linear-gradient(90deg, #3b82f6, #60a5fa)"
      : "#22c55e",
    borderRadius: "4px",
    transition: "width 0.5s ease",
  }),
  statsRow: {
    display: "flex",
    gap: "24px",
    flexWrap: "wrap" as const,
  } as React.CSSProperties,
  stat: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "2px",
  } as React.CSSProperties,
  statLabel: {
    fontSize: "11px",
    color: "#9ca3af",
    textTransform: "uppercase" as const,
    letterSpacing: "0.05em",
  } as React.CSSProperties,
  statValue: {
    fontSize: "20px",
    fontWeight: 700,
    color: "#111827",
    fontVariantNumeric: "tabular-nums" as const,
  } as React.CSSProperties,
  statValueError: {
    fontSize: "20px",
    fontWeight: 700,
    color: "#ef4444",
    fontVariantNumeric: "tabular-nums" as const,
  } as React.CSSProperties,
  controls: {
    display: "flex",
    gap: "8px",
    alignItems: "center",
  } as React.CSSProperties,
  button: (variant: "primary" | "secondary" | "danger" | "warning"): React.CSSProperties => {
    const colors = {
      primary: { bg: "#3b82f6", text: "#fff", border: "#3b82f6" },
      secondary: { bg: "#fff", text: "#374151", border: "#d1d5db" },
      danger: { bg: "#ef4444", text: "#fff", border: "#ef4444" },
      warning: { bg: "#f59e0b", text: "#fff", border: "#f59e0b" },
    };
    const c = colors[variant];
    return {
      padding: "6px 14px",
      fontSize: "12px",
      fontWeight: 500,
      borderRadius: "6px",
      border: `1px solid ${c.border}`,
      background: c.bg,
      color: c.text,
      cursor: "pointer",
    };
  },
  errorsSection: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "4px",
  } as React.CSSProperties,
  errorToggle: {
    fontSize: "12px",
    color: "#ef4444",
    cursor: "pointer",
    background: "none",
    border: "none",
    padding: "4px 0",
    fontWeight: 500,
    textAlign: "left" as const,
  } as React.CSSProperties,
  errorItem: {
    fontSize: "12px",
    color: "#6b7280",
    padding: "6px 8px",
    background: "#fef2f2",
    borderRadius: "4px",
    fontFamily: "monospace",
    whiteSpace: "pre-wrap" as const,
    wordBreak: "break-all" as const,
  } as React.CSSProperties,
  emptyState: {
    display: "flex",
    flexDirection: "column" as const,
    alignItems: "center",
    justifyContent: "center",
    padding: "48px 16px",
    color: "#9ca3af",
    fontSize: "14px",
    gap: "8px",
  } as React.CSSProperties,
  durationTimer: {
    fontSize: "12px",
    color: "#6b7280",
    fontVariantNumeric: "tabular-nums" as const,
    fontFamily: "monospace",
  } as React.CSSProperties,
} as const;

// ---------------------------------------------------------------------------
// Duration timer hook
// ---------------------------------------------------------------------------

function useDurationTimer(startedAt: string | null, completedAt: string | null): string {
  const [elapsed, setElapsed] = useState(0);

  useEffect(() => {
    if (!startedAt) {
      setElapsed(0);
      return;
    }

    const start = new Date(startedAt).getTime();

    if (completedAt) {
      setElapsed(new Date(completedAt).getTime() - start);
      return;
    }

    const tick = () => {
      setElapsed(Date.now() - start);
    };

    tick();
    const interval = setInterval(tick, 1000);
    return () => clearInterval(interval);
  }, [startedAt, completedAt]);

  const totalSeconds = Math.floor(elapsed / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;

  if (hours > 0) {
    return `${hours}h ${String(minutes).padStart(2, "0")}m ${String(seconds).padStart(2, "0")}s`;
  }
  if (minutes > 0) {
    return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
  }
  return `${seconds}s`;
}

// ---------------------------------------------------------------------------
// Animated counter hook
// ---------------------------------------------------------------------------

function useAnimatedCounter(target: number, durationMs: number = 500): number {
  const [display, setDisplay] = useState(target);
  const frameRef = useRef(0);

  useEffect(() => {
    const startValue = display;
    const diff = target - startValue;
    if (diff === 0) return;

    const startTime = performance.now();

    const animate = (now: number) => {
      const progress = Math.min((now - startTime) / durationMs, 1);
      const eased = 1 - Math.pow(1 - progress, 3);
      setDisplay(Math.round(startValue + diff * eased));

      if (progress < 1) {
        frameRef.current = requestAnimationFrame(animate);
      }
    };

    frameRef.current = requestAnimationFrame(animate);
    return () => cancelAnimationFrame(frameRef.current);
    // Only animate when target changes, not display
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target, durationMs]);

  return display;
}

// ---------------------------------------------------------------------------
// Phase progress helper
// ---------------------------------------------------------------------------

const PHASE_ORDER: SyncRunPhase[] = ["extract", "transform", "load"];

function getPhaseProgress(
  currentPhase: SyncRunPhase,
  extracted: number,
  loaded: number,
  total: number,
): Record<SyncRunPhase, number> {
  const idx = PHASE_ORDER.indexOf(currentPhase);
  const result: Record<SyncRunPhase, number> = { extract: 0, transform: 0, load: 0 };

  for (let i = 0; i < PHASE_ORDER.length; i++) {
    const phase = PHASE_ORDER[i];
    if (i < idx) {
      result[phase] = 100;
    } else if (i === idx) {
      if (total === 0) {
        result[phase] = 0;
      } else if (phase === "extract") {
        result[phase] = (extracted / total) * 100;
      } else if (phase === "load") {
        result[phase] = (loaded / Math.max(extracted, 1)) * 100;
      } else {
        result[phase] = (extracted / total) * 100;
      }
    }
  }

  return result;
}

function formatNumber(n: number): string {
  return n.toLocaleString();
}

// ---------------------------------------------------------------------------
// Single sync run card
// ---------------------------------------------------------------------------

interface SyncRunCardProps {
  run: SyncRun;
  liveProgress: SyncProgressEvent | null;
  onPause: (syncId: string) => void;
  onResume: (syncId: string) => void;
  onCancel: (syncId: string) => void;
}

const SyncRunCard: React.FC<SyncRunCardProps> = React.memo(
  ({ run, liveProgress, onPause, onResume, onCancel }) => {
    const [errorsExpanded, setErrorsExpanded] = useState(false);

    const status = liveProgress?.status ?? run.status;
    const phase = liveProgress?.phase ?? run.phase;
    const extracted = liveProgress?.records_extracted ?? run.records_extracted;
    const loaded = liveProgress?.records_loaded ?? run.records_loaded;
    const rejected = liveProgress?.records_rejected ?? run.records_rejected;
    const errorCount = liveProgress?.error_count ?? run.error_count;

    const estimatedTotal = extracted > 0 ? extracted * 1.1 : 100;
    const phaseProgress = useMemo(
      () => getPhaseProgress(phase, extracted, loaded, estimatedTotal),
      [phase, extracted, loaded, estimatedTotal],
    );

    const animatedExtracted = useAnimatedCounter(extracted);
    const animatedLoaded = useAnimatedCounter(loaded);
    const animatedRejected = useAnimatedCounter(rejected);

    const duration = useDurationTimer(run.started_at, run.completed_at);

    const isActive = status === "running" || status === "paused";

    const handlePause = useCallback(() => onPause(run.sync_id), [onPause, run.sync_id]);
    const handleResume = useCallback(() => onResume(run.sync_id), [onResume, run.sync_id]);
    const handleCancel = useCallback(() => onCancel(run.sync_id), [onCancel, run.sync_id]);
    const toggleErrors = useCallback(() => setErrorsExpanded((prev) => !prev), []);

    return (
      <div style={styles.card}>
        <div style={styles.cardHeader}>
          <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
            <span style={styles.syncName}>{run.sync_id}</span>
            <span style={styles.statusBadge(status)}>{status}</span>
          </div>
          <span style={styles.durationTimer}>{duration}</span>
        </div>

        <div style={styles.cardBody}>
          {/* Phase progress bars */}
          <div style={styles.phaseSection}>
            {PHASE_ORDER.map((p) => (
              <div key={p} style={styles.phaseRow}>
                <span style={styles.phaseLabel(p === phase && isActive)}>{p}</span>
                <div style={styles.progressBarOuter}>
                  <div
                    style={styles.progressBarInner(
                      phaseProgress[p],
                      p === phase && status === "running",
                    )}
                  />
                </div>
                <span style={{ fontSize: "11px", color: "#9ca3af", width: "36px", textAlign: "right" as const }}>
                  {Math.round(phaseProgress[p])}%
                </span>
              </div>
            ))}
          </div>

          {/* Stats */}
          <div style={styles.statsRow}>
            <div style={styles.stat}>
              <span style={styles.statLabel}>Extracted</span>
              <span style={styles.statValue}>{formatNumber(animatedExtracted)}</span>
            </div>
            <div style={styles.stat}>
              <span style={styles.statLabel}>Loaded</span>
              <span style={styles.statValue}>{formatNumber(animatedLoaded)}</span>
            </div>
            <div style={styles.stat}>
              <span style={styles.statLabel}>Rejected</span>
              <span style={errorCount > 0 ? styles.statValueError : styles.statValue}>
                {formatNumber(animatedRejected)}
              </span>
            </div>
            <div style={styles.stat}>
              <span style={styles.statLabel}>Errors</span>
              <span style={errorCount > 0 ? styles.statValueError : styles.statValue}>
                {formatNumber(errorCount)}
              </span>
            </div>
          </div>

          {/* Errors expandable */}
          {run.errors && run.errors.length > 0 && (
            <div style={styles.errorsSection}>
              <button style={styles.errorToggle} onClick={toggleErrors}>
                {errorsExpanded
                  ? `Hide errors (${run.errors.length})`
                  : `Show errors (${run.errors.length})`}
              </button>
              {errorsExpanded &&
                run.errors.map((err: SyncRunError, idx: number) => (
                  <div key={idx} style={styles.errorItem}>
                    [{err.error_code}] {err.message}
                    {err.timestamp && ` (${new Date(err.timestamp).toLocaleTimeString()})`}
                  </div>
                ))}
            </div>
          )}

          {/* Controls */}
          {isActive && (
            <div style={styles.controls}>
              {status === "running" && (
                <button style={styles.button("warning")} onClick={handlePause}>
                  Pause
                </button>
              )}
              {status === "paused" && (
                <button style={styles.button("primary")} onClick={handleResume}>
                  Resume
                </button>
              )}
              <button style={styles.button("danger")} onClick={handleCancel}>
                Cancel
              </button>
            </div>
          )}
        </div>
      </div>
    );
  },
);

// ---------------------------------------------------------------------------
// SyncMonitor Props
// ---------------------------------------------------------------------------

export interface SyncMonitorProps {
  syncId?: string;
  wsUrl?: string;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const SyncMonitorInner: React.FC<SyncMonitorProps> = ({ syncId, wsUrl }) => {
  const client = getClient();

  const wsEndpoint = useMemo(() => {
    if (wsUrl) return wsUrl;
    const protocol = typeof window !== "undefined" && window.location.protocol === "https:" ? "wss" : "ws";
    const host = typeof window !== "undefined" ? window.location.host : "localhost:8080";
    const path = syncId ? `/ws/syncs/${syncId}` : "/ws/syncs";
    return `${protocol}://${host}${path}`;
  }, [wsUrl, syncId]);

  const { connected, lastEvent } = useWebSocket(wsEndpoint);

  const [progressMap, setProgressMap] = useState<Map<string, SyncProgressEvent>>(new Map());

  useEffect(() => {
    if (!lastEvent) return;

    if (lastEvent.type === "sync_progress") {
      setProgressMap((prev) => {
        const next = new Map(prev);
        next.set(lastEvent.run_id, lastEvent as SyncProgressEvent);
        return next;
      });
    }
  }, [lastEvent]);

  const fetchRuns = useCallback(
    (signal: AbortSignal) => {
      if (syncId) {
        return client
          .listSyncRuns(syncId, { page_size: 10, status: "running" }, signal)
          .then((r) => r.data);
      }
      return client
        .listSyncs({ page_size: 50, status: "running" }, signal)
        .then(async (syncsResp) => {
          const runs: SyncRun[] = [];
          for (const sync of syncsResp.data) {
            try {
              const runsResp = await client.listSyncRuns(
                sync.id,
                { page_size: 1, status: "running" },
                signal,
              );
              runs.push(...runsResp.data);
            } catch {
              // Skip inaccessible syncs
            }
          }
          return runs;
        });
    },
    [client, syncId],
  );

  const { data: runs, loading, refetch } = useAPI<SyncRun[]>(fetchRuns);

  const handlePause = useCallback(
    async (id: string) => {
      await client.pauseSync(id);
      refetch();
    },
    [client, refetch],
  );

  const handleResume = useCallback(
    async (id: string) => {
      await client.resumeSync(id);
      refetch();
    },
    [client, refetch],
  );

  const handleCancel = useCallback(
    async (id: string) => {
      await client.cancelSync(id);
      refetch();
    },
    [client, refetch],
  );

  if (loading && !runs) {
    return (
      <div style={styles.container}>
        <div style={styles.header}>
          <h2 style={styles.title}>Sync Monitor</h2>
        </div>
        <div style={styles.emptyState}>Loading active syncs...</div>
      </div>
    );
  }

  const displayRuns = runs ?? [];

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h2 style={styles.title}>
          {syncId ? "Sync Monitor" : "Active Syncs"}
        </h2>
        <div style={styles.connectionStatus}>
          <span style={styles.connectionDot(connected)} />
          {connected ? "Live" : "Reconnecting..."}
        </div>
      </div>

      {displayRuns.length === 0 ? (
        <div style={styles.emptyState}>
          <span style={{ fontSize: "32px" }}>&#9203;</span>
          <span>No active sync runs</span>
        </div>
      ) : (
        displayRuns.map((run) => (
          <SyncRunCard
            key={run.id}
            run={run}
            liveProgress={progressMap.get(run.id) ?? null}
            onPause={handlePause}
            onResume={handleResume}
            onCancel={handleCancel}
          />
        ))
      )}
    </div>
  );
};

export const SyncMonitor = React.memo(SyncMonitorInner);
