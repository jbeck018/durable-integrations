/**
 * FieldNode — a single field in the field mapper tree.
 * Shows field name, type badge, nullable indicator, expand/collapse for nested objects,
 * and a connection anchor point for mapping lines.
 */

import React, { useCallback, useMemo, useRef, useState } from "react";
import type { SchemaField } from "../../lib/types";

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    userSelect: "none" as const,
  } as React.CSSProperties,
  row: (depth: number, isActive: boolean, isDragTarget: boolean): React.CSSProperties => ({
    display: "flex",
    alignItems: "center",
    gap: "6px",
    padding: `4px 8px 4px ${8 + depth * 16}px`,
    cursor: "pointer",
    borderRadius: "4px",
    background: isDragTarget ? "#eff6ff" : isActive ? "#f3f4f6" : "transparent",
    border: isDragTarget ? "1px dashed #3b82f6" : "1px solid transparent",
    transition: "background 0.1s ease",
    fontSize: "13px",
    lineHeight: "24px",
    minHeight: "32px",
  }),
  expandToggle: {
    width: "16px",
    height: "16px",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: "10px",
    color: "#9ca3af",
    cursor: "pointer",
    flexShrink: 0,
    borderRadius: "2px",
    border: "none",
    background: "transparent",
    padding: 0,
  } as React.CSSProperties,
  expandPlaceholder: {
    width: "16px",
    flexShrink: 0,
  } as React.CSSProperties,
  fieldName: {
    fontWeight: 500,
    color: "#111827",
    whiteSpace: "nowrap" as const,
    overflow: "hidden",
    textOverflow: "ellipsis",
    flex: 1,
    minWidth: 0,
  } as React.CSSProperties,
  requiredMarker: {
    color: "#ef4444",
    fontSize: "11px",
    fontWeight: 700,
    marginLeft: "1px",
  } as React.CSSProperties,
  typeBadge: (typeStr: string): React.CSSProperties => {
    const colorMap: Record<string, string> = {
      string: "#2563eb",
      number: "#16a34a",
      integer: "#16a34a",
      boolean: "#d97706",
      object: "#7c3aed",
      array: "#0891b2",
      null: "#9ca3af",
    };
    const color = colorMap[typeStr] ?? "#6b7280";
    return {
      fontSize: "10px",
      fontWeight: 600,
      padding: "0 5px",
      borderRadius: "4px",
      background: `${color}18`,
      color,
      whiteSpace: "nowrap",
      flexShrink: 0,
    };
  },
  nullableBadge: {
    fontSize: "10px",
    color: "#9ca3af",
    fontStyle: "italic" as const,
    flexShrink: 0,
  } as React.CSSProperties,
  anchor: (side: "left" | "right"): React.CSSProperties => ({
    width: "10px",
    height: "10px",
    borderRadius: "50%",
    border: "2px solid #3b82f6",
    background: "#ffffff",
    cursor: "crosshair",
    flexShrink: 0,
    transition: "background 0.1s ease, transform 0.1s ease",
    ...(side === "left" ? { marginRight: "4px" } : { marginLeft: "4px" }),
  }),
  anchorActive: {
    background: "#3b82f6",
    transform: "scale(1.3)",
  } as React.CSSProperties,
  children: {
    display: "flex",
    flexDirection: "column" as const,
  } as React.CSSProperties,
} as const;

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface FieldNodeProps {
  field: SchemaField;
  side: "source" | "destination";
  isMapped: boolean;
  isActive: boolean;
  isDragTarget: boolean;
  onAnchorMouseDown: (field: SchemaField, side: "source" | "destination", anchorEl: HTMLElement) => void;
  onAnchorMouseUp: (field: SchemaField, side: "source" | "destination") => void;
  onFieldClick: (field: SchemaField) => void;
  anchorRef?: (path: string, el: HTMLElement | null) => void;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const FieldNodeInner: React.FC<FieldNodeProps> = ({
  field,
  side,
  isMapped,
  isActive,
  isDragTarget,
  onAnchorMouseDown,
  onAnchorMouseUp,
  onFieldClick,
  anchorRef,
}) => {
  const [expanded, setExpanded] = useState(field.depth < 2);
  const anchorElRef = useRef<HTMLDivElement>(null);

  const hasChildren = field.children.length > 0;

  const handleToggle = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      setExpanded((prev) => !prev);
    },
    [],
  );

  const handleRowClick = useCallback(() => {
    onFieldClick(field);
  }, [onFieldClick, field]);

  const handleAnchorMouseDown = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      e.preventDefault();
      if (anchorElRef.current) {
        onAnchorMouseDown(field, side, anchorElRef.current);
      }
    },
    [onAnchorMouseDown, field, side],
  );

  const handleAnchorMouseUp = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      onAnchorMouseUp(field, side);
    },
    [onAnchorMouseUp, field, side],
  );

  const refCallback = useCallback(
    (el: HTMLDivElement | null) => {
      (anchorElRef as React.MutableRefObject<HTMLDivElement | null>).current = el;
      anchorRef?.(field.path, el);
    },
    [anchorRef, field.path],
  );

  const anchorStyle = useMemo(
    () => ({
      ...styles.anchor(side === "source" ? "right" : "left"),
      ...(isMapped || isActive ? styles.anchorActive : {}),
    }),
    [side, isMapped, isActive],
  );

  const displayType = Array.isArray(field.type) ? field.type.join(" | ") : field.type;

  return (
    <div style={styles.container}>
      <div
        style={styles.row(field.depth, isActive, isDragTarget)}
        onClick={handleRowClick}
        role="treeitem"
        aria-expanded={hasChildren ? expanded : undefined}
        aria-selected={isActive}
        aria-label={`${field.name}: ${displayType}${field.required ? " (required)" : ""}`}
      >
        {/* Connection anchor (left side for destination fields) */}
        {side === "destination" && (
          <div
            ref={refCallback}
            style={anchorStyle}
            onMouseDown={handleAnchorMouseDown}
            onMouseUp={handleAnchorMouseUp}
            role="button"
            aria-label={`Map to ${field.name}`}
          />
        )}

        {/* Expand/collapse toggle */}
        {hasChildren ? (
          <button
            style={styles.expandToggle}
            onClick={handleToggle}
            aria-label={expanded ? "Collapse" : "Expand"}
          >
            {expanded ? "\u25BC" : "\u25B6"}
          </button>
        ) : (
          <div style={styles.expandPlaceholder} />
        )}

        {/* Field name */}
        <span style={styles.fieldName}>
          {field.name}
          {field.required && <span style={styles.requiredMarker}>*</span>}
        </span>

        {/* Type badge */}
        <span style={styles.typeBadge(displayType)}>{displayType}</span>

        {/* Nullable indicator */}
        {field.nullable && <span style={styles.nullableBadge}>?</span>}

        {/* Connection anchor (right side for source fields) */}
        {side === "source" && (
          <div
            ref={refCallback}
            style={anchorStyle}
            onMouseDown={handleAnchorMouseDown}
            onMouseUp={handleAnchorMouseUp}
            role="button"
            aria-label={`Map from ${field.name}`}
          />
        )}
      </div>

      {/* Render children when expanded */}
      {hasChildren && expanded && (
        <div style={styles.children} role="group">
          {field.children.map((child) => (
            <FieldNodeInner
              key={child.path}
              field={child}
              side={side}
              isMapped={isMapped}
              isActive={isActive}
              isDragTarget={isDragTarget}
              onAnchorMouseDown={onAnchorMouseDown}
              onAnchorMouseUp={onAnchorMouseUp}
              onFieldClick={onFieldClick}
              anchorRef={anchorRef}
            />
          ))}
        </div>
      )}
    </div>
  );
};

export const FieldNode = React.memo(FieldNodeInner);
