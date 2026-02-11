/**
 * React hook for a live duration timer.
 * Uses ui-core's calculateElapsed and formatDuration.
 */

import { useEffect, useState } from "react";
import { calculateElapsed, formatDuration } from "@flowforge/ui-core";

export function useDurationTimer(startedAt: string | null, completedAt: string | null): string {
  const [elapsed, setElapsed] = useState(() => calculateElapsed(startedAt, completedAt));

  useEffect(() => {
    if (!startedAt) {
      setElapsed(0);
      return;
    }

    if (completedAt) {
      setElapsed(calculateElapsed(startedAt, completedAt));
      return;
    }

    const tick = () => setElapsed(calculateElapsed(startedAt, null));
    tick();
    const interval = setInterval(tick, 1000);
    return () => clearInterval(interval);
  }, [startedAt, completedAt]);

  return formatDuration(elapsed);
}
