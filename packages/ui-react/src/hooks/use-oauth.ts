/**
 * React hook wrapping the ui-core OAuthManager.
 */

import { useCallback, useMemo, useRef, useState } from "react";
import { OAuthManager, type OAuthManagerConfig, type OAuthCompleteMessage } from "@flowforge/ui-core";

interface UseOAuthResult {
  authorize: (connectorId: string, tenantId: string) => Promise<OAuthCompleteMessage>;
  reauthorize: (connectionId: string, tenantId: string) => Promise<OAuthCompleteMessage>;
  loading: boolean;
  error: Error | null;
}

export function useOAuth(config: OAuthManagerConfig): UseOAuthResult {
  const managerRef = useRef<OAuthManager | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<Error | null>(null);

  if (!managerRef.current) {
    managerRef.current = new OAuthManager(config);
  }

  const authorize = useCallback(
    async (connectorId: string, tenantId: string) => {
      setLoading(true);
      setError(null);
      try {
        const result = await managerRef.current!.authorize(connectorId, tenantId);
        return result;
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        setError(e);
        throw e;
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  const reauthorize = useCallback(
    async (connectionId: string, tenantId: string) => {
      setLoading(true);
      setError(null);
      try {
        const result = await managerRef.current!.reauthorize(connectionId, tenantId);
        return result;
      } catch (err) {
        const e = err instanceof Error ? err : new Error(String(err));
        setError(e);
        throw e;
      } finally {
        setLoading(false);
      }
    },
    [],
  );

  return useMemo(
    () => ({ authorize, reauthorize, loading, error }),
    [authorize, reauthorize, loading, error],
  );
}
