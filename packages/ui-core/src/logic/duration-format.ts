/**
 * Duration formatting and number display utilities.
 * Used by SyncMonitor across all framework adapters.
 */

/**
 * Format elapsed milliseconds into a human-readable duration string.
 * Examples: "5s", "2m 05s", "1h 23m 05s"
 */
export function formatDuration(elapsedMs: number): string {
  const totalSeconds = Math.floor(elapsedMs / 1000);
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

/**
 * Calculate elapsed time between two timestamps.
 * If completedAt is null, uses current time.
 */
export function calculateElapsed(startedAt: string | null, completedAt: string | null): number {
  if (!startedAt) return 0;
  const start = new Date(startedAt).getTime();
  if (completedAt) {
    return new Date(completedAt).getTime() - start;
  }
  return Date.now() - start;
}

/**
 * Format a number with locale-specific grouping (e.g., 1,234,567).
 */
export function formatNumber(n: number): string {
  return n.toLocaleString();
}
