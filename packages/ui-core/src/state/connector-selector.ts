/**
 * Connector selector state — manages filter, search, and selection state
 * for the connector picker UI. Framework-agnostic.
 */

import type { Connector, ConnectorFilter, ConnectorType } from "../types/index.js";
import { extractCategories, matchesFilter } from "../logic/field-matching.js";
import { Store } from "./store.js";

export interface ConnectorSelectorState {
  connectors: Connector[];
  searchText: string;
  typeFilter: ConnectorType | "all";
  categoryFilter: string;
  selectedId: string | null;
  loading: boolean;
}

export function createConnectorSelectorStore(
  initialConnectors: Connector[] = [],
) {
  const store = new Store<ConnectorSelectorState>({
    connectors: initialConnectors,
    searchText: "",
    typeFilter: "all",
    categoryFilter: "",
    selectedId: null,
    loading: false,
  });

  return {
    store,

    setConnectors(connectors: Connector[]) {
      store.setState((s) => ({ ...s, connectors }));
    },

    setSearchText(searchText: string) {
      store.setState((s) => ({ ...s, searchText }));
    },

    setTypeFilter(typeFilter: ConnectorType | "all") {
      store.setState((s) => ({ ...s, typeFilter }));
    },

    setCategoryFilter(categoryFilter: string) {
      store.setState((s) => ({ ...s, categoryFilter }));
    },

    setSelectedId(selectedId: string | null) {
      store.setState((s) => ({ ...s, selectedId }));
    },

    setLoading(loading: boolean) {
      store.setState((s) => ({ ...s, loading }));
    },

    getFilter(debouncedSearch: string): ConnectorFilter {
      const state = store.getState();
      return {
        search: debouncedSearch,
        type: state.typeFilter,
        category: state.categoryFilter,
      };
    },

    getFilteredConnectors(debouncedSearch: string): Connector[] {
      const state = store.getState();
      const filter = this.getFilter(debouncedSearch);
      return state.connectors.filter((c) => matchesFilter(c, filter));
    },

    getCategories(): string[] {
      return extractCategories(store.getState().connectors);
    },
  };
}

export type ConnectorSelectorStore = ReturnType<typeof createConnectorSelectorStore>;
