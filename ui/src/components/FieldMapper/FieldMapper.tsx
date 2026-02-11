/**
 * FieldMapper — visual field mapping component.
 * Two-panel layout with source fields on the left, destination fields on the right,
 * connected by draggable SVG bezier lines. Supports auto-map and per-mapping transforms.
 */

import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import type {
  FieldTransform,
  JSONSchema,
  MappingPair,
  SchemaField,
} from "../../lib/types";
import { FieldNode } from "./FieldNode";
import { MappingLine } from "./MappingLine";

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "12px",
    width: "100%",
    position: "relative" as const,
  } as React.CSSProperties,
  toolbar: {
    display: "flex",
    gap: "8px",
    alignItems: "center",
    justifyContent: "space-between",
    flexWrap: "wrap" as const,
  } as React.CSSProperties,
  toolbarLeft: {
    display: "flex",
    gap: "8px",
    alignItems: "center",
  } as React.CSSProperties,
  button: (variant: "primary" | "secondary" | "danger"): React.CSSProperties => {
    const colors = {
      primary: { bg: "#3b82f6", text: "#ffffff", border: "#3b82f6" },
      secondary: { bg: "#ffffff", text: "#374151", border: "#d1d5db" },
      danger: { bg: "#ffffff", text: "#ef4444", border: "#fca5a5" },
    };
    const c = colors[variant];
    return {
      padding: "6px 14px",
      fontSize: "13px",
      fontWeight: 500,
      borderRadius: "6px",
      border: `1px solid ${c.border}`,
      background: c.bg,
      color: c.text,
      cursor: "pointer",
      transition: "opacity 0.15s ease",
      whiteSpace: "nowrap",
    };
  },
  mappingArea: {
    display: "flex",
    position: "relative" as const,
    border: "1px solid #e5e7eb",
    borderRadius: "8px",
    background: "#fafafa",
    minHeight: "400px",
    overflow: "hidden",
  } as React.CSSProperties,
  panel: {
    flex: 1,
    padding: "12px",
    overflow: "auto",
    maxHeight: "500px",
  } as React.CSSProperties,
  panelHeader: {
    fontSize: "12px",
    fontWeight: 600,
    color: "#6b7280",
    textTransform: "uppercase" as const,
    letterSpacing: "0.05em",
    marginBottom: "8px",
    padding: "0 8px",
  } as React.CSSProperties,
  svgOverlay: {
    position: "absolute" as const,
    top: 0,
    left: 0,
    width: "100%",
    height: "100%",
    pointerEvents: "none" as const,
    zIndex: 10,
  } as React.CSSProperties,
  svgInteractive: {
    pointerEvents: "auto" as const,
  } as React.CSSProperties,
  divider: {
    width: "1px",
    background: "#e5e7eb",
    flexShrink: 0,
  } as React.CSSProperties,
  mappingCount: {
    fontSize: "12px",
    color: "#9ca3af",
  } as React.CSSProperties,
  validationBar: (hasErrors: boolean): React.CSSProperties => ({
    padding: "8px 12px",
    borderRadius: "6px",
    fontSize: "12px",
    fontWeight: 500,
    background: hasErrors ? "#fef2f2" : "#f0fdf4",
    color: hasErrors ? "#dc2626" : "#16a34a",
    display: "flex",
    alignItems: "center",
    gap: "6px",
  }),
  transformConfig: {
    padding: "12px",
    border: "1px solid #e5e7eb",
    borderRadius: "8px",
    background: "#ffffff",
    display: "flex",
    flexDirection: "column" as const,
    gap: "8px",
  } as React.CSSProperties,
  transformRow: {
    display: "flex",
    gap: "8px",
    alignItems: "center",
  } as React.CSSProperties,
  transformInput: {
    flex: 1,
    padding: "6px 10px",
    fontSize: "13px",
    border: "1px solid #d1d5db",
    borderRadius: "6px",
    fontFamily: "monospace",
  } as React.CSSProperties,
  transformSelect: {
    padding: "6px 10px",
    fontSize: "13px",
    border: "1px solid #d1d5db",
    borderRadius: "6px",
    background: "#ffffff",
  } as React.CSSProperties,
  transformLabel: {
    fontSize: "12px",
    fontWeight: 500,
    color: "#374151",
  } as React.CSSProperties,
} as const;

// ---------------------------------------------------------------------------
// Schema -> SchemaField[] conversion
// ---------------------------------------------------------------------------

function flattenSchema(
  schema: JSONSchema,
  parentPath: string = "",
  depth: number = 0,
  parentRequired: string[] = [],
): SchemaField[] {
  const properties = schema.properties ?? {};
  const requiredSet = new Set(schema.required ?? parentRequired);

  return Object.entries(properties).map(([name, propSchema]) => {
    const path = parentPath ? `${parentPath}.${name}` : name;
    const fieldType =
      typeof propSchema.type === "string"
        ? propSchema.type
        : Array.isArray(propSchema.type)
          ? propSchema.type.filter((t) => t !== "null").join(" | ")
          : "any";

    const isNullable =
      propSchema.nullable === true ||
      (Array.isArray(propSchema.type) && propSchema.type.includes("null"));

    const children =
      fieldType === "object" || (propSchema.properties && Object.keys(propSchema.properties).length > 0)
        ? flattenSchema(propSchema, path, depth + 1, propSchema.required ?? [])
        : fieldType === "array" && propSchema.items?.properties
          ? flattenSchema(propSchema.items, path, depth + 1, propSchema.items.required ?? [])
          : [];

    return {
      path,
      name,
      type: fieldType,
      required: requiredSet.has(name),
      nullable: isNullable,
      description: propSchema.description ?? "",
      children,
      depth,
      schema: propSchema,
    };
  });
}

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface FieldMapperProps {
  sourceSchema: JSONSchema;
  destSchema: JSONSchema;
  mappings: MappingPair[];
  onChange: (mappings: MappingPair[]) => void;
  onAutoMap?: () => void;
  autoMapLoading?: boolean;
}

// ---------------------------------------------------------------------------
// Drag state
// ---------------------------------------------------------------------------

interface DragState {
  sourceField: SchemaField | null;
  side: "source" | "destination";
  startX: number;
  startY: number;
  currentX: number;
  currentY: number;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const FieldMapperInner: React.FC<FieldMapperProps> = ({
  sourceSchema,
  destSchema,
  mappings,
  onChange,
  onAutoMap,
  autoMapLoading = false,
}) => {
  const containerRef = useRef<HTMLDivElement>(null);
  const sourceAnchors = useRef<Map<string, HTMLElement>>(new Map());
  const destAnchors = useRef<Map<string, HTMLElement>>(new Map());

  const [selectedMappingId, setSelectedMappingId] = useState<string | null>(null);
  const [dragState, setDragState] = useState<DragState | null>(null);
  const [activeField, setActiveField] = useState<string | null>(null);
  const [linePositions, setLinePositions] = useState<
    Map<string, { sx: number; sy: number; dx: number; dy: number }>
  >(new Map());

  const sourceFields = useMemo(() => flattenSchema(sourceSchema), [sourceSchema]);
  const destFields = useMemo(() => flattenSchema(destSchema), [destSchema]);

  const mappedSourceFields = useMemo(
    () => new Set(mappings.map((m) => m.sourceField)),
    [mappings],
  );
  const mappedDestFields = useMemo(
    () => new Set(mappings.map((m) => m.destinationField)),
    [mappings],
  );

  const errorCount = useMemo(
    () => mappings.filter((m) => m.status === "error").length,
    [mappings],
  );

  // -----------------------------------------------------------------------
  // Anchor registration
  // -----------------------------------------------------------------------

  const registerSourceAnchor = useCallback((path: string, el: HTMLElement | null) => {
    if (el) {
      sourceAnchors.current.set(path, el);
    } else {
      sourceAnchors.current.delete(path);
    }
  }, []);

  const registerDestAnchor = useCallback((path: string, el: HTMLElement | null) => {
    if (el) {
      destAnchors.current.set(path, el);
    } else {
      destAnchors.current.delete(path);
    }
  }, []);

  // -----------------------------------------------------------------------
  // Recalculate line positions when mappings or layout changes
  // -----------------------------------------------------------------------

  const recalcLinePositions = useCallback(() => {
    const container = containerRef.current;
    if (!container) return;

    const containerRect = container.getBoundingClientRect();
    const newPositions = new Map<string, { sx: number; sy: number; dx: number; dy: number }>();

    for (const mapping of mappings) {
      const srcEl = sourceAnchors.current.get(mapping.sourceField);
      const dstEl = destAnchors.current.get(mapping.destinationField);

      if (srcEl && dstEl) {
        const srcRect = srcEl.getBoundingClientRect();
        const dstRect = dstEl.getBoundingClientRect();

        newPositions.set(mapping.id, {
          sx: srcRect.left + srcRect.width / 2 - containerRect.left,
          sy: srcRect.top + srcRect.height / 2 - containerRect.top,
          dx: dstRect.left + dstRect.width / 2 - containerRect.left,
          dy: dstRect.top + dstRect.height / 2 - containerRect.top,
        });
      }
    }

    setLinePositions(newPositions);
  }, [mappings]);

  useEffect(() => {
    recalcLinePositions();
    const timer = setTimeout(recalcLinePositions, 100);
    return () => clearTimeout(timer);
  }, [recalcLinePositions]);

  useEffect(() => {
    window.addEventListener("resize", recalcLinePositions);
    return () => window.removeEventListener("resize", recalcLinePositions);
  }, [recalcLinePositions]);

  // -----------------------------------------------------------------------
  // Drag-to-connect handlers
  // -----------------------------------------------------------------------

  const handleAnchorMouseDown = useCallback(
    (field: SchemaField, side: "source" | "destination", anchorEl: HTMLElement) => {
      const container = containerRef.current;
      if (!container) return;

      const containerRect = container.getBoundingClientRect();
      const anchorRect = anchorEl.getBoundingClientRect();

      const startX = anchorRect.left + anchorRect.width / 2 - containerRect.left;
      const startY = anchorRect.top + anchorRect.height / 2 - containerRect.top;

      setDragState({
        sourceField: field,
        side,
        startX,
        startY,
        currentX: startX,
        currentY: startY,
      });
    },
    [],
  );

  const handleAnchorMouseUp = useCallback(
    (field: SchemaField, side: "source" | "destination") => {
      if (!dragState || !dragState.sourceField) return;
      if (dragState.side === side) {
        setDragState(null);
        return;
      }

      const sourceField = dragState.side === "source" ? dragState.sourceField.path : field.path;
      const destField = dragState.side === "destination" ? dragState.sourceField.path : field.path;

      const alreadyMapped = mappings.some(
        (m) => m.sourceField === sourceField && m.destinationField === destField,
      );

      if (!alreadyMapped) {
        const newMapping: MappingPair = {
          id: `mapping_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
          sourceField,
          destinationField: destField,
          transform: null,
          isAutoMapped: false,
          status: "valid",
        };
        onChange([...mappings, newMapping]);
      }

      setDragState(null);
    },
    [dragState, mappings, onChange],
  );

  useEffect(() => {
    if (!dragState) return;

    const container = containerRef.current;
    if (!container) return;

    const containerRect = container.getBoundingClientRect();

    const handleMouseMove = (e: MouseEvent) => {
      setDragState((prev) =>
        prev
          ? {
              ...prev,
              currentX: e.clientX - containerRect.left,
              currentY: e.clientY - containerRect.top,
            }
          : null,
      );
    };

    const handleMouseUp = () => {
      setDragState(null);
    };

    window.addEventListener("mousemove", handleMouseMove);
    window.addEventListener("mouseup", handleMouseUp);
    return () => {
      window.removeEventListener("mousemove", handleMouseMove);
      window.removeEventListener("mouseup", handleMouseUp);
    };
  }, [dragState]);

  // -----------------------------------------------------------------------
  // Mapping actions
  // -----------------------------------------------------------------------

  const handleMappingClick = useCallback((id: string) => {
    setSelectedMappingId((prev) => (prev === id ? null : id));
  }, []);

  const handleMappingDelete = useCallback(
    (id: string) => {
      onChange(mappings.filter((m) => m.id !== id));
      if (selectedMappingId === id) {
        setSelectedMappingId(null);
      }
    },
    [mappings, onChange, selectedMappingId],
  );

  const handleClearAll = useCallback(() => {
    onChange([]);
    setSelectedMappingId(null);
  }, [onChange]);

  const handleFieldClick = useCallback((field: SchemaField) => {
    setActiveField((prev) => (prev === field.path ? null : field.path));
  }, []);

  // -----------------------------------------------------------------------
  // Transform editing for selected mapping
  // -----------------------------------------------------------------------

  const selectedMapping = useMemo(
    () => mappings.find((m) => m.id === selectedMappingId) ?? null,
    [mappings, selectedMappingId],
  );

  const handleTransformTypeChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => {
      if (!selectedMappingId) return;
      const newType = e.target.value;
      onChange(
        mappings.map((m) =>
          m.id === selectedMappingId
            ? {
                ...m,
                transform: newType
                  ? { type: newType, expression: m.transform?.expression ?? "", config: {} }
                  : null,
              }
            : m,
        ),
      );
    },
    [selectedMappingId, mappings, onChange],
  );

  const handleTransformExprChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => {
      if (!selectedMappingId) return;
      const expression = e.target.value;
      onChange(
        mappings.map((m) =>
          m.id === selectedMappingId && m.transform
            ? { ...m, transform: { ...m.transform, expression } }
            : m,
        ),
      );
    },
    [selectedMappingId, mappings, onChange],
  );

  // -----------------------------------------------------------------------
  // Render
  // -----------------------------------------------------------------------

  return (
    <div style={styles.container} ref={containerRef}>
      {/* Toolbar */}
      <div style={styles.toolbar}>
        <div style={styles.toolbarLeft}>
          {onAutoMap && (
            <button
              style={styles.button("primary")}
              onClick={onAutoMap}
              disabled={autoMapLoading}
            >
              {autoMapLoading ? "Mapping..." : "Auto-map fields"}
            </button>
          )}
          <button
            style={styles.button("danger")}
            onClick={handleClearAll}
            disabled={mappings.length === 0}
          >
            Clear all
          </button>
        </div>
        <span style={styles.mappingCount}>
          {mappings.length} mapping{mappings.length !== 1 ? "s" : ""}
        </span>
      </div>

      {/* Validation bar */}
      {mappings.length > 0 && (
        <div style={styles.validationBar(errorCount > 0)}>
          {errorCount > 0
            ? `${errorCount} mapping${errorCount !== 1 ? "s" : ""} with errors`
            : "All mappings valid"}
        </div>
      )}

      {/* Main mapping area */}
      <div style={styles.mappingArea}>
        {/* Source panel */}
        <div style={styles.panel}>
          <div style={styles.panelHeader}>Source fields</div>
          <div role="tree" aria-label="Source schema fields">
            {sourceFields.map((field) => (
              <FieldNode
                key={field.path}
                field={field}
                side="source"
                isMapped={mappedSourceFields.has(field.path)}
                isActive={activeField === field.path}
                isDragTarget={
                  dragState !== null && dragState.side === "destination"
                }
                onAnchorMouseDown={handleAnchorMouseDown}
                onAnchorMouseUp={handleAnchorMouseUp}
                onFieldClick={handleFieldClick}
                anchorRef={registerSourceAnchor}
              />
            ))}
          </div>
        </div>

        <div style={styles.divider} />

        {/* SVG overlay for mapping lines */}
        <svg style={styles.svgOverlay}>
          <g style={styles.svgInteractive}>
            {mappings.map((mapping) => {
              const pos = linePositions.get(mapping.id);
              if (!pos) return null;
              return (
                <MappingLine
                  key={mapping.id}
                  id={mapping.id}
                  sourceX={pos.sx}
                  sourceY={pos.sy}
                  destX={pos.dx}
                  destY={pos.dy}
                  status={mapping.isAutoMapped ? "auto" : mapping.status}
                  isSelected={mapping.id === selectedMappingId}
                  onClick={handleMappingClick}
                  onDelete={handleMappingDelete}
                />
              );
            })}

            {/* Drag preview line */}
            {dragState && (
              <path
                d={`M ${dragState.startX} ${dragState.startY} C ${dragState.startX + 60} ${dragState.startY}, ${dragState.currentX - 60} ${dragState.currentY}, ${dragState.currentX} ${dragState.currentY}`}
                fill="none"
                stroke="#93c5fd"
                strokeWidth={2}
                strokeDasharray="6 3"
                opacity={0.8}
              />
            )}
          </g>
        </svg>

        {/* Destination panel */}
        <div style={styles.panel}>
          <div style={styles.panelHeader}>Destination fields</div>
          <div role="tree" aria-label="Destination schema fields">
            {destFields.map((field) => (
              <FieldNode
                key={field.path}
                field={field}
                side="destination"
                isMapped={mappedDestFields.has(field.path)}
                isActive={activeField === field.path}
                isDragTarget={
                  dragState !== null && dragState.side === "source"
                }
                onAnchorMouseDown={handleAnchorMouseDown}
                onAnchorMouseUp={handleAnchorMouseUp}
                onFieldClick={handleFieldClick}
                anchorRef={registerDestAnchor}
              />
            ))}
          </div>
        </div>
      </div>

      {/* Transform configuration for selected mapping */}
      {selectedMapping && (
        <div style={styles.transformConfig}>
          <div style={styles.transformLabel}>
            Transform: {selectedMapping.sourceField} &#8594; {selectedMapping.destinationField}
          </div>
          <div style={styles.transformRow}>
            <select
              style={styles.transformSelect}
              value={selectedMapping.transform?.type ?? ""}
              onChange={handleTransformTypeChange}
              aria-label="Transform type"
            >
              <option value="">No transform</option>
              <option value="cast">Type cast</option>
              <option value="format">Format string</option>
              <option value="expression">Expression</option>
              <option value="lookup">Lookup table</option>
              <option value="hash">Hash</option>
              <option value="truncate">Truncate</option>
              <option value="default_value">Default value</option>
            </select>
            {selectedMapping.transform && (
              <input
                style={styles.transformInput}
                type="text"
                placeholder="Expression or configuration..."
                value={selectedMapping.transform.expression}
                onChange={handleTransformExprChange}
                aria-label="Transform expression"
              />
            )}
            <button
              style={styles.button("danger")}
              onClick={() => handleMappingDelete(selectedMapping.id)}
            >
              Delete
            </button>
          </div>
        </div>
      )}
    </div>
  );
};

export const FieldMapper = React.memo(FieldMapperInner);
