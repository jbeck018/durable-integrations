/**
 * Observable store — framework-agnostic reactive state management.
 * Inspired by TanStack Store. Each framework adapter bridges this
 * to its own reactivity system (React useState, Angular signals, Svelte stores).
 */

export type Listener<T> = (state: T, prevState: T) => void;
export type Selector<T, S> = (state: T) => S;
export type Updater<T> = T | ((prev: T) => T);

export class Store<T> {
  private state: T;
  private listeners = new Set<Listener<T>>();

  constructor(initialState: T) {
    this.state = initialState;
  }

  getState(): T {
    return this.state;
  }

  setState(updater: Updater<T>): void {
    const prevState = this.state;
    this.state = typeof updater === "function"
      ? (updater as (prev: T) => T)(prevState)
      : updater;

    if (this.state !== prevState) {
      for (const listener of this.listeners) {
        listener(this.state, prevState);
      }
    }
  }

  subscribe(listener: Listener<T>): () => void {
    this.listeners.add(listener);
    return () => {
      this.listeners.delete(listener);
    };
  }

  /**
   * Subscribe to a derived value. Listener only fires when the selected
   * value changes (by reference equality).
   */
  select<S>(selector: Selector<T, S>, listener: (value: S, prevValue: S) => void): () => void {
    let prevSelected = selector(this.state);
    return this.subscribe((state) => {
      const selected = selector(state);
      if (selected !== prevSelected) {
        const prev = prevSelected;
        prevSelected = selected;
        listener(selected, prev);
      }
    });
  }
}
