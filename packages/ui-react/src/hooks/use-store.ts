/**
 * React hook that bridges the ui-core observable Store to React state.
 * Triggers re-renders when the selected slice of state changes.
 */

import { useSyncExternalStore, useCallback, useRef } from "react";
import type { Store, Selector } from "@flowforge/ui-core";

/**
 * Subscribe to the full store state.
 */
export function useStore<T>(store: Store<T>): T {
  return useSyncExternalStore(
    (cb) => store.subscribe(cb),
    () => store.getState(),
    () => store.getState(),
  );
}

/**
 * Subscribe to a derived slice of the store state.
 * Only triggers re-render when the selected value changes.
 */
export function useStoreSelector<T, S>(store: Store<T>, selector: Selector<T, S>): S {
  const selectorRef = useRef(selector);
  selectorRef.current = selector;

  const getSnapshot = useCallback(() => {
    return selectorRef.current(store.getState());
  }, [store]);

  return useSyncExternalStore(
    (cb) => store.subscribe(cb),
    getSnapshot,
    getSnapshot,
  );
}
