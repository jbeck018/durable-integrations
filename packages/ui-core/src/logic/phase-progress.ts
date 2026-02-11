/**
 * Sync phase progress calculation.
 * Used by SyncMonitor to compute progress bars per phase.
 */

import type { SyncRunPhase } from "../types/index.js";

export const PHASE_ORDER: SyncRunPhase[] = ["extract", "transform", "load"];

/**
 * Compute progress percentage for each sync phase.
 */
export function getPhaseProgress(
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
