/**
 * SchemaViewer — JSON Schema tree viewer with search, type indicators,
 * required markers, and optional side-by-side diff view.
 */

import React, { useCallback, useMemo, useState } from "react";
import type { DiffStatus, JSONSchema } from "../../lib/types";
import { useDebounce } from "../../lib/hooks";
import { SchemaNode } from "./SchemaNode";

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "12px",
    width: "100%",
  } as React.CSSProperties,
  toolbar: {
    display: "flex",
    gap: "8px",
    alignItems: "center",
    flexWrap: "wrap" as const,
  } as React.CSSProperties,
  searchInput: {
    flex: "1 1 200px",
    minWidth: "200px",
    padding: "8px 12px",
    border: "1px solid #d1d5db",
    borderRadius: "8px",
    fontSize: "13px",
    outline: "none",
  } as React.CSSProperties,
  toggleButton: (active: boolean): React.CSSProperties => ({
    padding: "6px 14px",
    fontSize: "12px",
    fontWeight: 500,
    borderRadius: "6px",
    border: `1px solid ${active ? "#3b82f6" : "#d1d5db"}`,
    background: active ? "#eff6ff" : "#ffffff",
    color: active ? "#1d4ed8" : "#374151",
    cursor: "pointer",
  }),
  fieldCount: {
    fontSize: "12px",
    color: "#9ca3af",
    whiteSpace: "nowrap" as const,
  } as React.CSSProperties,
  treeContainer: {
    border: "1px solid #e5e7eb",
    borderRadius: "8px",
    overflow: "auto",
    maxHeight: "600px",
    background: "#ffffff",
  } as React.CSSProperties,
  diffContainer: {
    display: "grid",
    gridTemplateColumns: "1fr 1fr",
    gap: "0",
    border: "1px solid #e5e7eb",
    borderRadius: "8px",
    overflow: "hidden",
  } as React.CSSProperties,
  diffPanel: {
    overflow: "auto",
    maxHeight: "600px",
    background: "#ffffff",
  } as React.CSSProperties,
  diffPanelHeader: {
    position: "sticky" as const,
    top: 0,
    zIndex: 1,
    padding: "8px 12px",
    fontSize: "12px",
    fontWeight: 600,
    color: "#6b7280",
    background: "#f9fafb",
    borderBottom: "1px solid #e5e7eb",
    textTransform: "uppercase" as const,
    letterSpacing: "0.05em",
  } as React.CSSProperties,
  diffDivider: {
    width: "1px",
    background: "#e5e7eb",
  } as React.CSSProperties,
  legend: {
    display: "flex",
    gap: "12px",
    fontSize: "11px",
    color: "#6b7280",
    flexWrap: "wrap" as const,
  } as React.CSSProperties,
  legendItem: {
    display: "flex",
    alignItems: "center",
    gap: "4px",
  } as React.CSSProperties,
  legendDot: (color: string): React.CSSProperties => ({
    width: "8px",
    height: "8px",
    borderRadius: "2px",
    background: color,
    flexShrink: 0,
  }),
  emptyState: {
    padding: "32px 16px",
    textAlign: "center" as const,
    color: "#9ca3af",
    fontSize: "14px",
  } as React.CSSProperties,
  rootTypeLabel: {
    padding: "8px 12px",
    fontSize: "12px",
    color: "#6b7280",
    borderBottom: "1px solid #f3f4f6",
    background: "#fafafa",
    fontFamily: "monospace",
  } as React.CSSProperties,
} as const;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function countFields(schema: JSONSchema): number {
  let count = 0;
  const props = schema.properties ?? {};
  for (const key of Object.keys(props)) {
    count += 1;
    const child = props[key];
    if (child.properties) {
      count += countFields(child);
    } else if (child.items?.properties) {
      count += countFields(child.items);
    }
  }
  return count;
}

function fieldMatchesSearch(name: string, schema: JSONSchema, query: string): boolean {
  const lq = query.toLowerCase();
  if (name.toLowerCase().includes(lq)) return true;
  if ((schema.description ?? "").toLowerCase().includes(lq)) return true;

  const props = schema.properties ?? {};
  for (const [childName, childSchema] of Object.entries(props)) {
    if (fieldMatchesSearch(childName, childSchema, query)) return true;
  }
  if (schema.items?.properties) {
    for (const [childName, childSchema] of Object.entries(schema.items.properties)) {
      if (fieldMatchesSearch(childName, childSchema, query)) return true;
    }
  }

  return false;
}

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface SchemaViewerProps {
  schema: JSONSchema;
  compareSchema?: JSONSchema;
  title?: string;
  compareTitle?: string;
  defaultExpanded?: boolean;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const SchemaViewerInner: React.FC<SchemaViewerProps> = ({
  schema,
  compareSchema,
  title = "Schema",
  compareTitle = "Previous Schema",
  defaultExpanded,
}) => {
  const [searchText, setSearchText] = useState("");
  const [diffMode, setDiffMode] = useState(!!compareSchema);
  const debouncedSearch = useDebounce(searchText, 200);

  const fieldCount = useMemo(() => countFields(schema), [schema]);
  const compareFieldCount = useMemo(
    () => (compareSchema ? countFields(compareSchema) : 0),
    [compareSchema],
  );

  const handleSearchChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => setSearchText(e.target.value),
    [],
  );

  const handleToggleDiff = useCallback(() => setDiffMode((prev) => !prev), []);

  const properties = schema.properties ?? {};
  const propertyNames = Object.keys(properties);
  const requiredSet = new Set(schema.required ?? []);

  const renderTree = useCallback(
    (
      schemaObj: JSONSchema,
      compareObj: JSONSchema | undefined,
      showRemoved: boolean,
    ) => {
      const props = schemaObj.properties ?? {};
      const propNames = Object.keys(props);
      const compProps = compareObj?.properties ?? {};
      const reqSet = new Set(schemaObj.required ?? []);

      return (
        <div role="tree" aria-label="Schema tree">
          {propNames.map((name) => {
            const fieldSchema = props[name];
            const isDiffMode = showRemoved && compareObj;
            const fieldDiff: DiffStatus = isDiffMode
              ? name in compProps
                ? JSON.stringify(fieldSchema) !== JSON.stringify(compProps[name])
                  ? "changed"
                  : "unchanged"
                : "added"
              : "unchanged";

            const matches =
              !debouncedSearch || fieldMatchesSearch(name, fieldSchema, debouncedSearch);

            return (
              <SchemaNode
                key={name}
                name={name}
                schema={fieldSchema}
                depth={0}
                required={reqSet.has(name)}
                diff={fieldDiff}
                matchesSearch={matches}
                compareSchema={isDiffMode ? compProps[name] : undefined}
                searchQuery={debouncedSearch}
                defaultExpanded={defaultExpanded}
              />
            );
          })}

          {/* Show fields that exist in compare but not in current (removed) */}
          {showRemoved &&
            compareObj?.properties &&
            Object.keys(compareObj.properties)
              .filter((k) => !(k in props))
              .map((removedName) => {
                const removedSchema = compareObj.properties![removedName];
                const matches =
                  !debouncedSearch ||
                  fieldMatchesSearch(removedName, removedSchema, debouncedSearch);

                return (
                  <SchemaNode
                    key={`removed-${removedName}`}
                    name={removedName}
                    schema={removedSchema}
                    depth={0}
                    required={false}
                    diff="removed"
                    matchesSearch={matches}
                    searchQuery={debouncedSearch}
                  />
                );
              })}
        </div>
      );
    },
    [debouncedSearch, defaultExpanded],
  );

  const hasDiff = !!compareSchema;

  return (
    <div style={styles.container}>
      {/* Toolbar */}
      <div style={styles.toolbar}>
        <input
          style={styles.searchInput}
          type="search"
          placeholder="Search fields..."
          value={searchText}
          onChange={handleSearchChange}
          aria-label="Search schema fields"
        />
        {hasDiff && (
          <button
            style={styles.toggleButton(diffMode)}
            onClick={handleToggleDiff}
          >
            {diffMode ? "Hide diff" : "Show diff"}
          </button>
        )}
        <span style={styles.fieldCount}>
          {fieldCount} field{fieldCount !== 1 ? "s" : ""}
          {hasDiff && diffMode && ` (was ${compareFieldCount})`}
        </span>
      </div>

      {/* Diff legend */}
      {diffMode && hasDiff && (
        <div style={styles.legend}>
          <div style={styles.legendItem}>
            <div style={styles.legendDot("#86efac")} />
            <span>Added</span>
          </div>
          <div style={styles.legendItem}>
            <div style={styles.legendDot("#fca5a5")} />
            <span>Removed</span>
          </div>
          <div style={styles.legendItem}>
            <div style={styles.legendDot("#fde047")} />
            <span>Changed</span>
          </div>
        </div>
      )}

      {/* Content */}
      {propertyNames.length === 0 && !compareSchema ? (
        <div style={styles.emptyState}>No fields defined in this schema</div>
      ) : diffMode && compareSchema ? (
        /* Side-by-side diff view */
        <div style={styles.diffContainer}>
          <div style={styles.diffPanel}>
            <div style={styles.diffPanelHeader}>{title} (current)</div>
            <div style={styles.rootTypeLabel}>
              type: {schema.type ?? "object"} | {fieldCount} fields
            </div>
            {renderTree(schema, compareSchema, true)}
          </div>
          <div style={styles.diffPanel}>
            <div style={styles.diffPanelHeader}>{compareTitle} (previous)</div>
            <div style={styles.rootTypeLabel}>
              type: {compareSchema.type ?? "object"} | {compareFieldCount} fields
            </div>
            {renderTree(compareSchema, schema, true)}
          </div>
        </div>
      ) : (
        /* Single schema tree view */
        <div style={styles.treeContainer}>
          <div style={styles.rootTypeLabel}>
            type: {schema.type ?? "object"} | {fieldCount} fields
          </div>
          {renderTree(schema, undefined, false)}
        </div>
      )}
    </div>
  );
};

export const SchemaViewer = React.memo(SchemaViewerInner);
