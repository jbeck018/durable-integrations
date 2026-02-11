/**
 * Connector filtering and schema field search logic.
 * Used by ConnectorSelector and SchemaViewer across all framework adapters.
 */

import type { Connector, ConnectorFilter, JSONSchema } from "../types/index.js";

/**
 * Extract unique sorted categories from a list of connectors.
 */
export function extractCategories(connectors: Connector[]): string[] {
  const set = new Set<string>();
  for (const c of connectors) {
    if (c.category) set.add(c.category);
  }
  return Array.from(set).sort();
}

/**
 * Check if a connector matches the given filter criteria.
 */
export function matchesFilter(connector: Connector, filter: ConnectorFilter): boolean {
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

/**
 * Check if a schema field (or any of its descendants) matches a search query.
 */
export function fieldMatchesSearch(name: string, schema: JSONSchema, query: string): boolean {
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
