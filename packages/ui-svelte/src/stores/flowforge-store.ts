/**
 * Svelte writable stores wrapping ui-core observable stores.
 * Bridges the core Store to Svelte's reactive store contract.
 */

import { readable, type Readable } from "svelte/store";
import { Store, type Selector } from "@flowforge/ui-core";

/**
 * Convert a ui-core Store into a Svelte readable store.
 * Svelte components can use this with $store syntax.
 */
export function storeToSvelteReadable<T>(store: Store<T>): Readable<T> {
  return readable(store.getState(), (set) => {
    return store.subscribe((state) => set(state));
  });
}

/**
 * Convert a selected slice of a ui-core Store into a Svelte readable store.
 */
export function storeSelectToSvelteReadable<T, S>(
  store: Store<T>,
  selector: Selector<T, S>,
): Readable<S> {
  return readable(selector(store.getState()), (set) => {
    return store.select(selector, (value) => set(value));
  });
}
