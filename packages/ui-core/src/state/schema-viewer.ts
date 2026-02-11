/**
 * Schema viewer state — manages search, diff mode, and expanded state.
 * Framework-agnostic.
 */

import type { DiffStatus, JSONSchema } from "../types/index.js";
import { countFields } from "../logic/schema-flatten.js";
import { fieldMatchesSearch } from "../logic/field-matching.js";
import { Store } from "./store.js";

export interface SchemaViewerState {
  schema: JSONSchema;
  compareSchema: JSONSchema | null;
  searchText: string;
  diffMode: boolean;
  expandedPaths: Set<string>;
}

export function createSchemaViewerStore(
  schema: JSONSchema,
  compareSchema: JSONSchema | null = null,
) {
  const store = new Store<SchemaViewerState>({
    schema,
    compareSchema,
    searchText: "",
    diffMode: !!compareSchema,
    expandedPaths: new Set(),
  });

  return {
    store,

    setSearchText(searchText: string) {
      store.setState((s) => ({ ...s, searchText }));
    },

    toggleDiffMode() {
      store.setState((s) => ({ ...s, diffMode: !s.diffMode }));
    },

    toggleExpanded(path: string) {
      store.setState((s) => {
        const next = new Set(s.expandedPaths);
        if (next.has(path)) {
          next.delete(path);
        } else {
          next.add(path);
        }
        return { ...s, expandedPaths: next };
      });
    },

    getFieldCount(): number {
      return countFields(store.getState().schema);
    },

    getCompareFieldCount(): number {
      const state = store.getState();
      return state.compareSchema ? countFields(state.compareSchema) : 0;
    },

    doesFieldMatchSearch(name: string, fieldSchema: JSONSchema, debouncedSearch: string): boolean {
      if (!debouncedSearch) return true;
      return fieldMatchesSearch(name, fieldSchema, debouncedSearch);
    },

    getFieldDiffStatus(
      name: string,
      fieldSchema: JSONSchema,
      compareProps: Record<string, JSONSchema> | undefined,
    ): DiffStatus {
      if (!compareProps) return "unchanged";
      if (!(name in compareProps)) return "added";
      if (JSON.stringify(fieldSchema) !== JSON.stringify(compareProps[name])) return "changed";
      return "unchanged";
    },
  };
}

export type SchemaViewerStore = ReturnType<typeof createSchemaViewerStore>;
