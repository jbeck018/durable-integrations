/**
 * FlowForge React context provider.
 * Provides configuration, auth, and Connect transport to all FlowForge components.
 */

import React, { createContext, useContext, useMemo } from "react";
import type { OAuthManagerConfig } from "@flowforge/ui-core";

export interface FlowForgeConfig {
  apiBaseUrl: string;
  wsBaseUrl?: string;
  oauth?: OAuthManagerConfig;
}

const FlowForgeContext = createContext<FlowForgeConfig | null>(null);

export interface FlowForgeProviderProps {
  config: FlowForgeConfig;
  children: React.ReactNode;
}

export const FlowForgeProvider: React.FC<FlowForgeProviderProps> = ({ config, children }) => {
  const memoized = useMemo(() => config, [config.apiBaseUrl, config.wsBaseUrl]);
  return (
    <FlowForgeContext.Provider value={memoized}>
      {children}
    </FlowForgeContext.Provider>
  );
};

export function useFlowForgeConfig(): FlowForgeConfig {
  const ctx = useContext(FlowForgeContext);
  if (!ctx) {
    throw new Error("useFlowForgeConfig must be used within a <FlowForgeProvider>");
  }
  return ctx;
}
