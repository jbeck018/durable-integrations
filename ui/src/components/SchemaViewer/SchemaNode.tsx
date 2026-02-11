/**
 * SchemaNode — renders a single node in the schema tree.
 * Shows field name, type, description, required/optional markers.
 * Supports expand/collapse for objects and arrays.
 * Diff highlighting: added (green), removed (red), changed (yellow), unchanged (default).
 */

import React, { useCallback, useState } from "react";
import type { DiffStatus, JSONSchema } from "../../lib/types";

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const DIFF_COLORS: Record<DiffStatus, { bg: string; border: string; text: string }> = {
  added: { bg: "#f0fdf4", border: "#86efac", text: "#166534" },
  removed: { bg: "#fef2f2", border: "#fca5a5", text: "#dc2626" },
  changed: { bg: "#fefce8", border: "#fde047", text: "#854d0e" },
  unchanged: { bg: "transparent", border: "transparent", text: "#111827" },
};

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
  } as React.CSSProperties,
  row: (depth: number, diff: DiffStatus): React.CSSProperties => {
    const dc = DIFF_COLORS[diff];
    return {
      display: "flex",
      alignItems: "flex-start",
      gap: "6px",
      padding: `3px 8px 3px ${8 + depth * 20}px`,
      borderLeft: diff !== "unchanged" ? `3px solid ${dc.border}` : "3px solid transparent",
      background: dc.bg,
      transition: "background 0.1s ease",
      minHeight: "28px",
      lineHeight: "22px",
      fontSize: "13px",
    };
  },
  expandToggle: {
    width: "16px",
    height: "22px",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    fontSize: "10px",
    color: "#9ca3af",
    cursor: "pointer",
    flexShrink: 0,
    border: "none",
    background: "transparent",
    padding: 0,
  } as React.CSSProperties,
  expandPlaceholder: {
    width: "16px",
    flexShrink: 0,
  } as React.CSSProperties,
  fieldName: (diff: DiffStatus): React.CSSProperties => ({
    fontWeight: 600,
    color: DIFF_COLORS[diff].text,
    whiteSpace: "nowrap" as const,
    flexShrink: 0,
  }),
  requiredMarker: {
    color: "#ef4444",
    fontSize: "11px",
    fontWeight: 700,
    marginLeft: "1px",
  } as React.CSSProperties,
  colon: {
    color: "#9ca3af",
    flexShrink: 0,
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
      fontSize: "11px",
      fontWeight: 600,
      padding: "0 5px",
      borderRadius: "4px",
      background: `${color}15`,
      color,
      whiteSpace: "nowrap",
      flexShrink: 0,
    };
  },
  description: {
    fontSize: "12px",
    color: "#9ca3af",
    overflow: "hidden",
    textOverflow: "ellipsis",
    whiteSpace: "nowrap" as const,
    minWidth: 0,
    flex: 1,
  } as React.CSSProperties,
  diffBadge: (diff: DiffStatus): React.CSSProperties => {
    const dc = DIFF_COLORS[diff];
    return {
      fontSize: "10px",
      fontWeight: 600,
      padding: "0 5px",
      borderRadius: "4px",
      background: dc.border,
      color: dc.text,
      textTransform: "uppercase",
      flexShrink: 0,
    };
  },
  formatBadge: {
    fontSize: "10px",
    color: "#6b7280",
    fontStyle: "italic" as const,
    flexShrink: 0,
  } as React.CSSProperties,
  children: {
    display: "flex",
    flexDirection: "column" as const,
  } as React.CSSProperties,
  enumValues: {
    fontSize: "11px",
    color: "#6b7280",
    fontFamily: "monospace",
    flexShrink: 0,
  } as React.CSSProperties,
} as const;

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface SchemaNodeProps {
  name: string;
  schema: JSONSchema;
  depth: number;
  required: boolean;
  diff: DiffStatus;
  matchesSearch: boolean;
  compareSchema?: JSONSchema;
  searchQuery: string;
  defaultExpanded?: boolean;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function resolveType(schema: JSONSchema): string {
  if (typeof schema.type === "string") return schema.type;
  if (Array.isArray(schema.type)) return schema.type.filter((t) => t !== "null").join(" | ");
  if (schema.oneOf || schema.anyOf) return "union";
  if (schema.allOf) return "intersection";
  if (schema.properties) return "object";
  if (schema.items) return "array";
  return "any";
}

function isExpandable(schema: JSONSchema): boolean {
  const type = resolveType(schema);
  if (type === "object" && schema.properties && Object.keys(schema.properties).length > 0) return true;
  if (type === "array" && schema.items?.properties && Object.keys(schema.items.properties).length > 0) return true;
  return false;
}

function getChildProperties(schema: JSONSchema): Record<string, JSONSchema> {
  const type = resolveType(schema);
  if (type === "object" && schema.properties) return schema.properties;
  if (type === "array" && schema.items?.properties) return schema.items.properties;
  return {};
}

function computeFieldDiff(
  name: string,
  currentSchema: JSONSchema | undefined,
  compareSchema: JSONSchema | undefined,
): DiffStatus {
  if (!compareSchema) return "unchanged";
  if (!currentSchema) return "removed";

  const currentProps = currentSchema.properties ?? {};
  const compareProps = compareSchema.properties ?? {};

  if (name in currentProps && !(name in compareProps)) return "added";
  if (!(name in currentProps) && name in compareProps) return "removed";

  const curr = currentProps[name];
  const comp = compareProps[name];
  if (curr && comp) {
    const currType = resolveType(curr);
    const compType = resolveType(comp);
    if (currType !== compType) return "changed";
  }

  return "unchanged";
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const SchemaNodeInner: React.FC<SchemaNodeProps> = ({
  name,
  schema,
  depth,
  required,
  diff,
  matchesSearch,
  compareSchema,
  searchQuery,
  defaultExpanded,
}) => {
  const [expanded, setExpanded] = useState(defaultExpanded ?? depth < 2);

  const type = resolveType(schema);
  const expandable = isExpandable(schema);
  const childProps = getChildProperties(schema);
  const childNames = Object.keys(childProps);
  const requiredChildren = new Set(schema.required ?? []);

  const handleToggle = useCallback(
    (e: React.MouseEvent) => {
      e.stopPropagation();
      setExpanded((prev) => !prev);
    },
    [],
  );

  const isNullable =
    schema.nullable === true ||
    (Array.isArray(schema.type) && schema.type.includes("null"));

  const hasEnum = schema.enum && schema.enum.length > 0;

  const displayStyle = !matchesSearch && searchQuery ? { opacity: 0.3 } : {};

  return (
    <div style={{ ...styles.container, ...displayStyle }}>
      <div style={styles.row(depth, diff)} role="treeitem" aria-expanded={expandable ? expanded : undefined}>
        {expandable ? (
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

        <span style={styles.fieldName(diff)}>
          {name}
          {required && <span style={styles.requiredMarker}>*</span>}
        </span>

        <span style={styles.colon}>:</span>

        <span style={styles.typeBadge(type)}>{type}</span>

        {isNullable && <span style={styles.formatBadge}>nullable</span>}

        {schema.format && <span style={styles.formatBadge}>{schema.format}</span>}

        {hasEnum && (
          <span style={styles.enumValues}>
            [{schema.enum!.slice(0, 4).map(String).join(", ")}
            {schema.enum!.length > 4 ? `, +${schema.enum!.length - 4}` : ""}]
          </span>
        )}

        {diff !== "unchanged" && <span style={styles.diffBadge(diff)}>{diff}</span>}

        {schema.description && (
          <span style={styles.description} title={schema.description}>
            {schema.description}
          </span>
        )}
      </div>

      {expandable && expanded && (
        <div style={styles.children} role="group">
          {childNames.map((childName) => {
            const childSchema = childProps[childName];
            const childDiff = compareSchema
              ? computeFieldDiff(childName, schema, compareSchema)
              : ("unchanged" as DiffStatus);

            const childMatchesSearch =
              !searchQuery ||
              childName.toLowerCase().includes(searchQuery.toLowerCase()) ||
              (childSchema.description ?? "").toLowerCase().includes(searchQuery.toLowerCase());

            return (
              <SchemaNodeInner
                key={childName}
                name={childName}
                schema={childSchema}
                depth={depth + 1}
                required={requiredChildren.has(childName)}
                diff={childDiff}
                matchesSearch={childMatchesSearch}
                compareSchema={compareSchema?.properties?.[childName]}
                searchQuery={searchQuery}
              />
            );
          })}

          {/* Show removed fields from compare schema */}
          {compareSchema?.properties &&
            Object.keys(compareSchema.properties)
              .filter((k) => !(k in childProps))
              .map((removedName) => {
                const removedSchema = compareSchema.properties![removedName];
                const removedMatchesSearch =
                  !searchQuery ||
                  removedName.toLowerCase().includes(searchQuery.toLowerCase());

                return (
                  <SchemaNodeInner
                    key={`removed-${removedName}`}
                    name={removedName}
                    schema={removedSchema}
                    depth={depth + 1}
                    required={false}
                    diff="removed"
                    matchesSearch={removedMatchesSearch}
                    searchQuery={searchQuery}
                  />
                );
              })}
        </div>
      )}
    </div>
  );
};

export const SchemaNode = React.memo(SchemaNodeInner);
