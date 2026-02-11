/**
 * React hook for smoothly animating numeric counters.
 * Uses ui-core's animateCounter utility.
 */

import { useEffect, useRef, useState } from "react";
import { animateCounter } from "@flowforge/ui-core";

export function useAnimatedCounter(target: number, durationMs: number = 500): number {
  const [display, setDisplay] = useState(target);
  const prevTarget = useRef(target);

  useEffect(() => {
    const startValue = prevTarget.current;
    prevTarget.current = target;

    if (startValue === target) return;

    const cancel = animateCounter(startValue, target, durationMs, setDisplay);
    return cancel;
  }, [target, durationMs]);

  return display;
}
