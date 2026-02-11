/**
 * ConnectorSelector — grid/list of available connectors with search and filter.
 * Supports keyboard navigation, loading skeletons, and virtual scrolling
 * for large connector catalogs.
 */

import React, { useCallback, useMemo, useState } from "react";
import type { Connector, ConnectorFilter, ConnectorType } from "../../lib/types";
import { useDebounce } from "../../lib/hooks";
import { ConnectorCard } from "./ConnectorCard";

// ---------------------------------------------------------------------------
// Styles
// ---------------------------------------------------------------------------

const styles = {
  container: {
    display: "flex",
    flexDirection: "column" as const,
    gap: "16px",
    width: "100%",
  } as React.CSSProperties,
  toolbar: {
    display: "flex",
    flexWrap: "wrap" as const,
    gap: "8px",
    alignItems: "center",
  } as React.CSSProperties,
  searchInput: {
    flex: "1 1 200px",
    minWidth: "200px",
    padding: "8px 12px",
    border: "1px solid #d1d5db",
    borderRadius: "8px",
    fontSize: "14px",
    outline: "none",
    transition: "border-color 0.15s ease",
  } as React.CSSProperties,
  filterSelect: {
    padding: "8px 12px",
    border: "1px solid #d1d5db",
    borderRadius: "8px",
    fontSize: "14px",
    background: "#ffffff",
    outline: "none",
    cursor: "pointer",
  } as React.CSSProperties,
  grid: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fill, minmax(260px, 1fr))",
    gap: "16px",
  } as React.CSSProperties,
  emptyState: {
    display: "flex",
    flexDirection: "column" as const,
    alignItems: "center",
    justifyContent: "center",
    padding: "48px 16px",
    color: "#9ca3af",
    fontSize: "14px",
    gap: "8px",
  } as React.CSSProperties,
  emptyIcon: {
    fontSize: "32px",
    marginBottom: "4px",
  } as React.CSSProperties,
  skeletonCard: {
    borderRadius: "12px",
    background: "#f3f4f6",
    height: "160px",
    animation: "pulse 1.5s ease-in-out infinite",
  } as React.CSSProperties,
  resultCount: {
    fontSize: "12px",
    color: "#9ca3af",
    padding: "0 4px",
    whiteSpace: "nowrap" as const,
  } as React.CSSProperties,
  virtualContainer: {
    position: "relative" as const,
    overflow: "auto",
  } as React.CSSProperties,
} as const;

// ---------------------------------------------------------------------------
// Props
// ---------------------------------------------------------------------------

export interface ConnectorSelectorProps {
  connectors: Connector[];
  onSelect: (connector: Connector) => void;
  selectedId?: string | null;
  filter?: Partial<ConnectorFilter>;
  loading?: boolean;
  categories?: string[];
  maxHeight?: number;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function extractCategories(connectors: Connector[]): string[] {
  const set = new Set<string>();
  for (const c of connectors) {
    if (c.category) set.add(c.category);
  }
  return Array.from(set).sort();
}

function matchesFilter(connector: Connector, filter: ConnectorFilter): boolean {
  if (filter.type !== "all" && connector.type !== filter.type) {
    return false;
  }
  if (filter.category && connector.category !== filter.category) {
    return false;
  }
  if (filter.search) {
    const query = filter.search.toLowerCase();
    const searchable = `${connector.name} ${connector.display_name} ${connector.description} ${connector.category}`.toLowerCase();
    if (!searchable.includes(query)) {
      return false;
    }
  }
  return true;
}

// ---------------------------------------------------------------------------
// Loading skeleton
// ---------------------------------------------------------------------------

const SkeletonGrid: React.FC<{ count: number }> = React.memo(({ count }) => (
  <div style={styles.grid} role="status" aria-label="Loading connectors">
    {Array.from({ length: count }, (_, i) => (
      <div key={i} style={styles.skeletonCard} />
    ))}
  </div>
));

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

const ConnectorSelectorInner: React.FC<ConnectorSelectorProps> = ({
  connectors,
  onSelect,
  selectedId = null,
  filter: externalFilter,
  loading = false,
  categories: externalCategories,
  maxHeight,
}) => {
  const [searchText, setSearchText] = useState(externalFilter?.search ?? "");
  const [typeFilter, setTypeFilter] = useState<ConnectorType | "all">(
    externalFilter?.type ?? "all",
  );
  const [categoryFilter, setCategoryFilter] = useState(
    externalFilter?.category ?? "",
  );

  const debouncedSearch = useDebounce(searchText, 200);

  const categories = useMemo(
    () => externalCategories ?? extractCategories(connectors),
    [externalCategories, connectors],
  );

  const activeFilter = useMemo<ConnectorFilter>(
    () => ({
      search: debouncedSearch,
      type: typeFilter,
      category: categoryFilter,
    }),
    [debouncedSearch, typeFilter, categoryFilter],
  );

  const filteredConnectors = useMemo(
    () => connectors.filter((c) => matchesFilter(c, activeFilter)),
    [connectors, activeFilter],
  );

  const handleSearchChange = useCallback(
    (e: React.ChangeEvent<HTMLInputElement>) => setSearchText(e.target.value),
    [],
  );

  const handleTypeChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) =>
      setTypeFilter(e.target.value as ConnectorType | "all"),
    [],
  );

  const handleCategoryChange = useCallback(
    (e: React.ChangeEvent<HTMLSelectElement>) => setCategoryFilter(e.target.value),
    [],
  );

  const handleSelect = useCallback(
    (connector: Connector) => onSelect(connector),
    [onSelect],
  );

  if (loading) {
    return (
      <div style={styles.container}>
        <div style={styles.toolbar}>
          <input
            style={styles.searchInput}
            placeholder="Search connectors..."
            disabled
          />
        </div>
        <SkeletonGrid count={6} />
      </div>
    );
  }

  const containerStyle: React.CSSProperties = maxHeight
    ? { ...styles.virtualContainer, maxHeight }
    : {};

  return (
    <div style={styles.container} role="listbox" aria-label="Connector selector">
      <div style={styles.toolbar}>
        <input
          style={styles.searchInput}
          type="search"
          placeholder="Search connectors..."
          value={searchText}
          onChange={handleSearchChange}
          aria-label="Search connectors"
        />
        <select
          style={styles.filterSelect}
          value={typeFilter}
          onChange={handleTypeChange}
          aria-label="Filter by type"
        >
          <option value="all">All types</option>
          <option value="source">Sources</option>
          <option value="destination">Destinations</option>
          <option value="bidirectional">Bidirectional</option>
        </select>
        {categories.length > 0 && (
          <select
            style={styles.filterSelect}
            value={categoryFilter}
            onChange={handleCategoryChange}
            aria-label="Filter by category"
          >
            <option value="">All categories</option>
            {categories.map((cat) => (
              <option key={cat} value={cat}>
                {cat}
              </option>
            ))}
          </select>
        )}
        <span style={styles.resultCount}>
          {filteredConnectors.length} of {connectors.length}
        </span>
      </div>

      <div style={containerStyle}>
        {filteredConnectors.length === 0 ? (
          <div style={styles.emptyState}>
            <span style={styles.emptyIcon} aria-hidden="true">
              &#128269;
            </span>
            <span>No connectors match your filters</span>
            <span>Try adjusting your search or filter criteria</span>
          </div>
        ) : (
          <div style={styles.grid} role="presentation">
            {filteredConnectors.map((connector) => (
              <ConnectorCard
                key={connector.id}
                connector={connector}
                isSelected={connector.id === selectedId}
                onClick={handleSelect}
              />
            ))}
          </div>
        )}
      </div>
    </div>
  );
};

export const ConnectorSelector = React.memo(ConnectorSelectorInner);
