/**
 * SyncRunHistory — paginated, sortable table of past sync runs.
 * Click rows to expand details. Supports virtual scrolling for large lists.
 */

import React, { useCallback, useMemo, useState } from "react";
import type { SortConfig, SyncRun, SyncRunError, SyncStatus } from "../../lib/types";
import { useSyncRuns } from "../../lib/hooks";

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "12px",
    width: "100%",
  } as React.CSSProperties,
  header: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
  } as React.CSSProperties,
  title: {
    fontSize: "16px",
    fontWeight: 600,
    color: "#111827",
    margin: 0,
  } as React.CSSProperties,
  tableWrapper: {
    overflowX: "auto" as const,
    border: "1px solid #e5e7eb",
    borderRadius: "8px",
  } as React.CSSProperties,
  table: {
    width: "100%",
    borderCollapse: "collapse" as const,
    fontSize: "13px",
  } as React.CSSProperties,
  th: (sortable: boolean): React.CSSProperties => ({
    padding: "10px 12px",
    textAlign: "left",
    fontWeight: 600,
    color: "#6b7280",
    borderBottom: "1px solid #e5e7eb",
    background: "#f9fafb",
    whiteSpace: "nowrap",
    cursor: sortable ? "pointer" : "default",
    userSelect: "none",
    fontSize: "12px",
    textTransform: "uppercase",
    letterSpacing: "0.05em",
  }),
  td: {
    padding: "10px 12px",
    borderBottom: "1px solid #f3f4f6",
    color: "#374151",
    whiteSpace: "nowrap" as const,
    verticalAlign: "top" as const,
  } as React.CSSProperties,
  clickableRow: (expanded: boolean): React.CSSProperties => ({
    cursor: "pointer",
    background: expanded ? "#f9fafb" : "transparent",
    transition: "background 0.1s ease",
  }),
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
      display: "inline-block",
      fontSize: "11px",
      fontWeight: 600,
      padding: "2px 8px",
      borderRadius: "9999px",
      background: c.bg,
      color: c.text,
      textTransform: "capitalize",
    };
  },
  expandedRow: {
    background: "#f9fafb",
  } as React.CSSProperties,
  expandedCell: {
    padding: "12px 16px",
    borderBottom: "1px solid #e5e7eb",
  } as React.CSSProperties,
  detailGrid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fill, minmax(180px, 1fr))",
    gap: "12px",
    marginBottom: "12px",
  } as React.CSSProperties,
  detailItem: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "2px",
  } as React.CSSProperties,
  detailLabel: {
    fontSize: "11px",
    color: "#9ca3af",
    textTransform: "uppercase" as const,
    letterSpacing: "0.05em",
  } as React.CSSProperties,
  detailValue: {
    fontSize: "13px",
    color: "#111827",
    fontWeight: 500,
  } as React.CSSProperties,
  errorList: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "4px",
    marginTop: "8px",
  } as React.CSSProperties,
  errorItem: {
    fontSize: "12px",
    padding: "6px 8px",
    background: "#fef2f2",
    borderRadius: "4px",
    color: "#6b7280",
    fontFamily: "monospace",
  } as React.CSSProperties,
  pagination: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: "8px",
    fontSize: "13px",
    color: "#6b7280",
  } as React.CSSProperties,
  paginationButtons: {
    display: "flex",
    gap: "4px",
  } as React.CSSProperties,
  pageButton: (disabled: boolean): React.CSSProperties => ({
    padding: "4px 12px",
    fontSize: "12px",
    borderRadius: "6px",
    border: "1px solid #d1d5db",
    background: disabled ? "#f9fafb" : "#ffffff",
    color: disabled ? "#d1d5db" : "#374151",
    cursor: disabled ? "not-allowed" : "pointer",
  }),
  sortArrow: {
    fontSize: "10px",
    marginLeft: "4px",
  } as React.CSSProperties,
  emptyState: {
    padding: "32px 16px",
    textAlign: "center" as const,
    color: "#9ca3af",
    fontSize: "14px",
  } as React.CSSProperties,
} as const;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`;
  const totalSeconds = Math.floor(ms / 1000);
  const hours = Math.floor(totalSeconds / 3600);
  const minutes = Math.floor((totalSeconds % 3600) / 60);
  const seconds = totalSeconds % 60;
  if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
  if (minutes > 0) return `${minutes}m ${seconds}s`;
  return `${seconds}s`;
}

function formatTimestamp(ts: string | null): string {
  if (!ts) return "-";
  return new Date(ts).toLocaleString();
}

type SyncRunSortKey = keyof Pick<
  SyncRun,
  "started_at" | "status" | "records_extracted" | "records_loaded" | "error_count" | "duration_ms"
>;

const SORTABLE_COLUMNS: { key: SyncRunSortKey; label: string }[] = [
  { key: "started_at", label: "Started" },
  { key: "status", label: "Status" },
  { key: "records_extracted", label: "Extracted" },
  { key: "records_loaded", label: "Loaded" },
  { key: "error_count", label: "Errors" },
  { key: "duration_ms", label: "Duration" },
];

function sortRuns(runs: SyncRun[], sort: SortConfig<SyncRun>): SyncRun[] {
  return [...runs].sort((a, b) => {
    const aVal = a[sort.key];
    const bVal = b[sort.key];

    let cmp = 0;
    if (typeof aVal === "string" && typeof bVal === "string") {
      cmp = aVal.localeCompare(bVal);
    } else if (typeof aVal === "number" && typeof bVal === "number") {
      cmp = aVal - bVal;
    } else {
      cmp = String(aVal ?? "").localeCompare(String(bVal ?? ""));
    }

    return sort.direction === "asc" ? cmp : -cmp;
  });
}

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface SyncRunHistoryProps {
  syncId: string;
  pageSize?: number;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const SyncRunHistoryInner: React.FC<SyncRunHistoryProps> = ({
  syncId,
  pageSize: initialPageSize = 20,
}) => {
  const {
    data: runs,
    total,
    page,
    loading,
    hasNext,
    hasPrev,
    nextPage,
    prevPage,
    pageSize,
  } = useSyncRuns(syncId);

  const [sort, setSort] = useState<SortConfig<SyncRun>>({
    key: "started_at",
    direction: "desc",
  });

  const [expandedRunId, setExpandedRunId] = useState<string | null>(null);

  const handleSort = useCallback(
    (key: SyncRunSortKey) => {
      setSort((prev) => ({
        key,
        direction: prev.key === key && prev.direction === "desc" ? "asc" : "desc",
      }));
    },
    [],
  );

  const sortedRuns = useMemo(() => sortRuns(runs, sort), [runs, sort]);

  const handleRowClick = useCallback((runId: string) => {
    setExpandedRunId((prev) => (prev === runId ? null : runId));
  }, []);

  const startEntry = (page - 1) * (initialPageSize) + 1;
  const endEntry = Math.min(page * (initialPageSize), total);

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h3 style={styles.title}>Run History</h3>
        <span style={{ fontSize: "12px", color: "#9ca3af" }}>
          {total} total run{total !== 1 ? "s" : ""}
        </span>
      </div>

      <div style={styles.tableWrapper}>
        <table style={styles.table}>
          <thead>
            <tr>
              {SORTABLE_COLUMNS.map((col) => (
                <th
                  key={col.key}
                  style={styles.th(true)}
                  onClick={() => handleSort(col.key)}
                  aria-sort={
                    sort.key === col.key
                      ? sort.direction === "asc"
                        ? "ascending"
                        : "descending"
                      : "none"
                  }
                >
                  {col.label}
                  {sort.key === col.key && (
                    <span style={styles.sortArrow}>
                      {sort.direction === "asc" ? "\u25B2" : "\u25BC"}
                    </span>
                  )}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {loading && runs.length === 0 ? (
              <tr>
                <td style={styles.td} colSpan={SORTABLE_COLUMNS.length}>
                  <div style={styles.emptyState}>Loading...</div>
                </td>
              </tr>
            ) : sortedRuns.length === 0 ? (
              <tr>
                <td style={styles.td} colSpan={SORTABLE_COLUMNS.length}>
                  <div style={styles.emptyState}>No sync runs found</div>
                </td>
              </tr>
            ) : (
              sortedRuns.map((run) => (
                <React.Fragment key={run.id}>
                  <tr
                    style={styles.clickableRow(expandedRunId === run.id)}
                    onClick={() => handleRowClick(run.id)}
                    aria-expanded={expandedRunId === run.id}
                    role="row"
                  >
                    <td style={styles.td}>{formatTimestamp(run.started_at)}</td>
                    <td style={styles.td}>
                      <span style={styles.statusBadge(run.status)}>{run.status}</span>
                    </td>
                    <td style={styles.td}>{run.records_extracted.toLocaleString()}</td>
                    <td style={styles.td}>{run.records_loaded.toLocaleString()}</td>
                    <td style={styles.td}>
                      <span style={run.error_count > 0 ? { color: "#ef4444", fontWeight: 600 } : {}}>
                        {run.error_count}
                      </span>
                    </td>
                    <td style={styles.td}>{formatDuration(run.duration_ms)}</td>
                  </tr>

                  {expandedRunId === run.id && (
                    <tr style={styles.expandedRow}>
                      <td style={styles.expandedCell} colSpan={SORTABLE_COLUMNS.length}>
                        <div style={styles.detailGrid}>
                          <div style={styles.detailItem}>
                            <span style={styles.detailLabel}>Run ID</span>
                            <span style={styles.detailValue}>{run.id}</span>
                          </div>
                          <div style={styles.detailItem}>
                            <span style={styles.detailLabel}>Phase</span>
                            <span style={styles.detailValue}>{run.phase}</span>
                          </div>
                          <div style={styles.detailItem}>
                            <span style={styles.detailLabel}>Started</span>
                            <span style={styles.detailValue}>
                              {formatTimestamp(run.started_at)}
                            </span>
                          </div>
                          <div style={styles.detailItem}>
                            <span style={styles.detailLabel}>Completed</span>
                            <span style={styles.detailValue}>
                              {formatTimestamp(run.completed_at)}
                            </span>
                          </div>
                          <div style={styles.detailItem}>
                            <span style={styles.detailLabel}>Records Rejected</span>
                            <span style={styles.detailValue}>
                              {run.records_rejected.toLocaleString()}
                            </span>
                          </div>
                          <div style={styles.detailItem}>
                            <span style={styles.detailLabel}>Bytes Processed</span>
                            <span style={styles.detailValue}>
                              {(run.bytes_processed / 1024 / 1024).toFixed(2)} MB
                            </span>
                          </div>
                        </div>

                        {run.errors && run.errors.length > 0 && (
                          <div>
                            <span
                              style={{
                                ...styles.detailLabel,
                                display: "block",
                                marginBottom: "4px",
                              }}
                            >
                              Errors ({run.errors.length})
                            </span>
                            <div style={styles.errorList}>
                              {run.errors.map((err: SyncRunError, idx: number) => (
                                <div key={idx} style={styles.errorItem}>
                                  [{err.error_code}] {err.message}
                                </div>
                              ))}
                            </div>
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              ))
            )}
          </tbody>
        </table>
      </div>

      {/* Pagination */}
      {total > pageSize && (
        <div style={styles.pagination}>
          <span>
            Showing {startEntry}-{endEntry} of {total}
          </span>
          <div style={styles.paginationButtons}>
            <button
              style={styles.pageButton(!hasPrev)}
              onClick={prevPage}
              disabled={!hasPrev}
              aria-label="Previous page"
            >
              Previous
            </button>
            <button
              style={styles.pageButton(!hasNext)}
              onClick={nextPage}
              disabled={!hasNext}
              aria-label="Next page"
            >
              Next
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export const SyncRunHistory = React.memo(SyncRunHistoryInner);
