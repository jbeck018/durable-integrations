/**
 * Angular service that bridges ui-core Store to Angular signals/observables.
 */

import { Injectable, signal, type WritableSignal, type Signal } from "@angular/core";
import { Observable, Subject } from "rxjs";
import { Store, type Selector } from "@flowforge/ui-core";

/**
 * Bridge a ui-core Store to an Angular signal.
 * Returns a readonly signal that updates when the store changes.
 */
export function storeToSignal<T>(store: Store<T>): Signal<T> {
  const sig: WritableSignal<T> = signal(store.getState());
  store.subscribe((state) => sig.set(state));
  return sig.asReadonly();
}

/**
 * Bridge a ui-core Store to an RxJS Observable.
 */
export function storeToObservable<T>(store: Store<T>): Observable<T> {
  return new Observable((subscriber) => {
    subscriber.next(store.getState());
    const unsub = store.subscribe((state) => subscriber.next(state));
    return unsub;
  });
}

/**
 * Bridge a selected slice of a ui-core Store to an Angular signal.
 */
export function storeSelectToSignal<T, S>(store: Store<T>, selector: Selector<T, S>): Signal<S> {
  const sig: WritableSignal<S> = signal(selector(store.getState()));
  store.select(selector, (value) => sig.set(value));
  return sig.asReadonly();
}
