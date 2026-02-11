/**
 * Svelte store wrapping the ui-core OAuthManager.
 */

import { writable, type Readable } from "svelte/store";
import {
  OAuthManager,
  type OAuthManagerConfig,
  type OAuthCompleteMessage,
} from "@flowforge/ui-core";

export interface OAuthStoreValue {
  loading: boolean;
  error: Error | null;
}

export interface OAuthStore extends Readable<OAuthStoreValue> {
  authorize: (connectorId: string, tenantId: string) => Promise<OAuthCompleteMessage>;
  reauthorize: (connectionId: string, tenantId: string) => Promise<OAuthCompleteMessage>;
}

export function createOAuthStore(config: OAuthManagerConfig): OAuthStore {
  const manager = new OAuthManager(config);
  const { subscribe, set, update } = writable<OAuthStoreValue>({
    loading: false,
    error: null,
  });

  return {
    subscribe,

    async authorize(connectorId: string, tenantId: string) {
      update((s) => ({ ...s, loading: true, error: null }));
      try {
        const result = await manager.authorize(connectorId, tenantId);
        update((s) => ({ ...s, loading: false }));
        return result;
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        update((s) => ({ ...s, loading: false, error: e }));
        throw e;
      }
    },

    async reauthorize(connectionId: string, tenantId: string) {
      update((s) => ({ ...s, loading: true, error: null }));
      try {
        const result = await manager.reauthorize(connectionId, tenantId);
        update((s) => ({ ...s, loading: false }));
        return result;
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        update((s) => ({ ...s, loading: false, error: e }));
        throw e;
      }
    },
  };
}
