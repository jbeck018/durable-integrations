/**
 * Counter animation utilities.
 * Framework-agnostic easing and animation frame management.
 */

/**
 * Cubic ease-out function: fast start, slow finish.
 */
export function easeOutCubic(progress: number): number {
  return 1 - Math.pow(1 - progress, 3);
}

/**
 * Animate a numeric value from start to target over durationMs.
 * Calls onUpdate with the current interpolated value each frame.
 * Returns a cancel function.
 */
export function animateCounter(
  startValue: number,
  targetValue: number,
  durationMs: number,
  onUpdate: (value: number) => void,
): () => void {
  const diff = targetValue - startValue;
  if (diff === 0) {
    onUpdate(targetValue);
    return () => {};
  }

  let frameId = 0;
  const startTime = performance.now();

  const animate = (now: number) => {
    const progress = Math.min((now - startTime) / durationMs, 1);
    const eased = easeOutCubic(progress);
    onUpdate(Math.round(startValue + diff * eased));

    if (progress < 1) {
      frameId = requestAnimationFrame(animate);
    }
  };

  frameId = requestAnimationFrame(animate);

  return () => cancelAnimationFrame(frameId);
}
