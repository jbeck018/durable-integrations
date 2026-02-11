/**
 * MappingLine — SVG curved bezier path connecting mapped fields.
 * Color-coded by mapping status; clickable to select/delete.
 */

import React, { useCallback, useMemo, useState } from "react";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface MappingLineProps {
  id: string;
  sourceX: number;
  sourceY: number;
  destX: number;
  destY: number;
  status: "valid" | "error" | "warning" | "auto";
  isSelected: boolean;
  onClick: (id: string) => void;
  onDelete: (id: string) => void;
}

// ---------------------------------------------------------------------------
// Color mapping
// ---------------------------------------------------------------------------

const STATUS_COLORS: Record<string, { stroke: string; selectedStroke: string }> = {
  valid: { stroke: "#22c55e", selectedStroke: "#16a34a" },
  error: { stroke: "#ef4444", selectedStroke: "#dc2626" },
  warning: { stroke: "#f59e0b", selectedStroke: "#d97706" },
  auto: { stroke: "#8b5cf6", selectedStroke: "#7c3aed" },
};

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const MappingLineInner: React.FC<MappingLineProps> = ({
  id,
  sourceX,
  sourceY,
  destX,
  destY,
  status,
  isSelected,
  onClick,
  onDelete,
}) => {
  const [hovered, setHovered] = useState(false);

  const colors = STATUS_COLORS[status] ?? STATUS_COLORS.valid;
  const strokeColor = isSelected ? colors.selectedStroke : colors.stroke;
  const strokeWidth = isSelected || hovered ? 3 : 2;
  const opacity = isSelected || hovered ? 1 : 0.7;

  const pathData = useMemo(() => {
    const dx = destX - sourceX;
    const cpOffset = Math.max(Math.abs(dx) * 0.4, 60);

    const cp1x = sourceX + cpOffset;
    const cp1y = sourceY;
    const cp2x = destX - cpOffset;
    const cp2y = destY;

    return `M ${sourceX} ${sourceY} C ${cp1x} ${cp1y}, ${cp2x} ${cp2y}, ${destX} ${destY}`;
  }, [sourceX, sourceY, destX, destY]);

  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      onClick(id);
    },
    [onClick, id],
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent) => {
      if (e.key === "Delete" || e.key === "Backspace") {
        e.preventDefault();
        onDelete(id);
      }
      if (e.key === "Enter" || e.key === " ") {
        e.preventDefault();
        onClick(id);
      }
    },
    [onDelete, onClick, id],
  );

  const handleMouseEnter = useCallback(() => setHovered(true), []);
  const handleMouseLeave = useCallback(() => setHovered(false), []);

  const midX = (sourceX + destX) / 2;
  const midY = (sourceY + destY) / 2;

  return (
    <g
      role="button"
      tabIndex={0}
      aria-label={`Field mapping (${status})`}
      onClick={handleClick}
      onKeyDown={handleKeyDown}
      onMouseEnter={handleMouseEnter}
      onMouseLeave={handleMouseLeave}
      style={{ cursor: "pointer", outline: "none" }}
    >
      {/* Invisible wide path for easier mouse targeting */}
      <path
        d={pathData}
        fill="none"
        stroke="transparent"
        strokeWidth={12}
      />

      {/* Visible bezier line */}
      <path
        d={pathData}
        fill="none"
        stroke={strokeColor}
        strokeWidth={strokeWidth}
        strokeLinecap="round"
        opacity={opacity}
        style={{ transition: "stroke 0.15s ease, stroke-width 0.15s ease, opacity 0.15s ease" }}
      />

      {/* Animated dot on the line when selected */}
      {isSelected && (
        <circle r={4} fill={strokeColor}>
          <animateMotion dur="2s" repeatCount="indefinite" path={pathData} />
        </circle>
      )}

      {/* Source anchor dot */}
      <circle
        cx={sourceX}
        cy={sourceY}
        r={hovered || isSelected ? 5 : 4}
        fill={strokeColor}
        style={{ transition: "r 0.15s ease" }}
      />

      {/* Destination anchor dot */}
      <circle
        cx={destX}
        cy={destY}
        r={hovered || isSelected ? 5 : 4}
        fill={strokeColor}
        style={{ transition: "r 0.15s ease" }}
      />

      {/* Delete button shown on hover/select */}
      {(hovered || isSelected) && (
        <g
          onClick={(e) => {
            e.stopPropagation();
            onDelete(id);
          }}
          style={{ cursor: "pointer" }}
          role="button"
          aria-label="Delete mapping"
        >
          <circle cx={midX} cy={midY} r={10} fill="#ffffff" stroke={strokeColor} strokeWidth={1.5} />
          <line
            x1={midX - 4}
            y1={midY - 4}
            x2={midX + 4}
            y2={midY + 4}
            stroke="#ef4444"
            strokeWidth={2}
            strokeLinecap="round"
          />
          <line
            x1={midX + 4}
            y1={midY - 4}
            x2={midX - 4}
            y2={midY + 4}
            stroke="#ef4444"
            strokeWidth={2}
            strokeLinecap="round"
          />
        </g>
      )}
    </g>
  );
};

export const MappingLine = React.memo(MappingLineInner);
