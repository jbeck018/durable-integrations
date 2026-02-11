/**
 * SVG bezier curve calculation for field mapper connecting lines.
 * Framework-agnostic — returns path data strings for any SVG renderer.
 */

/**
 * Generate an SVG cubic bezier path between two points.
 * The curve bulges horizontally (control points offset on X axis).
 */
export function bezierPath(
  sx: number,
  sy: number,
  dx: number,
  dy: number,
  bulge: number = 60,
): string {
  return `M ${sx} ${sy} C ${sx + bulge} ${sy}, ${dx - bulge} ${dy}, ${dx} ${dy}`;
}

/**
 * Calculate the midpoint of a bezier curve (approximate).
 * Useful for placing labels or delete buttons on mapping lines.
 */
export function bezierMidpoint(
  sx: number,
  sy: number,
  dx: number,
  dy: number,
): { x: number; y: number } {
  return {
    x: (sx + dx) / 2,
    y: (sy + dy) / 2,
  };
}

/**
 * Calculate line positions for all mappings relative to a container element.
 * Takes anchor element maps and returns position map keyed by mapping ID.
 */
export function calculateLinePositions(
  mappings: Array<{ id: string; sourceField: string; destinationField: string }>,
  sourceAnchors: Map<string, { left: number; top: number; width: number; height: number }>,
  destAnchors: Map<string, { left: number; top: number; width: number; height: number }>,
  containerOffset: { left: number; top: number },
): Map<string, { sx: number; sy: number; dx: number; dy: number }> {
  const positions = new Map<string, { sx: number; sy: number; dx: number; dy: number }>();

  for (const mapping of mappings) {
    const srcRect = sourceAnchors.get(mapping.sourceField);
    const dstRect = destAnchors.get(mapping.destinationField);

    if (srcRect && dstRect) {
      positions.set(mapping.id, {
        sx: srcRect.left + srcRect.width / 2 - containerOffset.left,
        sy: srcRect.top + srcRect.height / 2 - containerOffset.top,
        dx: dstRect.left + dstRect.width / 2 - containerOffset.left,
        dy: dstRect.top + dstRect.height / 2 - containerOffset.top,
      });
    }
  }

  return positions;
}
