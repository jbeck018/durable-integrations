/**
 * OAuth2 popup flow manager — framework-agnostic.
 * Coordinates the BFF OAuth flow:
 * 1. Client calls POST /oauth/initiate to get authorize_url + session_id
 * 2. Opens popup to authorize_url
 * 3. IdP redirects to /oauth/callback (server-side)
 * 4. Callback page posts message to opener
 * 5. This manager receives the message and resolves
 */

import { TokenStore, type OAuthSession } from "./token-store.js";

export interface OAuthInitiateResponse {
  session_id: string;
  authorize_url: string;
}

export interface OAuthCompleteMessage {
  type: "oauth_complete";
  sessionId: string;
  connectionId: string;
  success: boolean;
  error?: string;
}

export interface OAuthManagerConfig {
  apiBaseUrl: string;
  popupWidth?: number;
  popupHeight?: number;
  timeoutMs?: number;
}

export class OAuthManager {
  private readonly config: OAuthManagerConfig;
  private readonly tokenStore: TokenStore;
  private popup: Window | null = null;

  constructor(config: OAuthManagerConfig) {
    this.config = {
      popupWidth: 600,
      popupHeight: 700,
      timeoutMs: 5 * 60 * 1000, // 5 minutes
      ...config,
    };
    this.tokenStore = new TokenStore();
  }

  /**
   * Start an OAuth flow for a connector. Returns a promise that resolves
   * when the user completes authentication.
   */
  async authorize(connectorId: string, tenantId: string): Promise<OAuthCompleteMessage> {
    // 1. Initiate OAuth session via BFF
    const response = await fetch(`${this.config.apiBaseUrl}/oauth/initiate`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ connector_id: connectorId, tenant_id: tenantId }),
    });

    if (!response.ok) {
      throw new Error(`OAuth initiation failed: ${response.statusText}`);
    }

    const { session_id, authorize_url } = (await response.json()) as OAuthInitiateResponse;

    // 2. Store session reference
    this.tokenStore.saveSession({
      sessionId: session_id,
      connectorId,
      status: "pending",
      createdAt: Date.now(),
    });

    // 3. Open popup
    return this.openPopup(authorize_url, session_id);
  }

  private openPopup(url: string, sessionId: string): Promise<OAuthCompleteMessage> {
    const { popupWidth, popupHeight, timeoutMs } = this.config;
    const left = Math.round((screen.width - popupWidth!) / 2);
    const top = Math.round((screen.height - popupHeight!) / 2);

    this.popup = window.open(
      url,
      "flowforge_oauth",
      `width=${popupWidth},height=${popupHeight},left=${left},top=${top},toolbar=no,menubar=no`,
    );

    return new Promise<OAuthCompleteMessage>((resolve, reject) => {
      const cleanup = () => {
        window.removeEventListener("message", handleMessage);
        clearInterval(pollTimer);
        clearTimeout(timeoutTimer);
      };

      const handleMessage = (event: MessageEvent) => {
        const data = event.data as OAuthCompleteMessage;
        if (data?.type !== "oauth_complete") return;
        if (data.sessionId !== sessionId) return;

        cleanup();
        this.popup?.close();
        this.popup = null;

        if (data.success) {
          this.tokenStore.updateSessionStatus(sessionId, "completed", data.connectionId);
          resolve(data);
        } else {
          this.tokenStore.updateSessionStatus(sessionId, "error");
          reject(new Error(data.error ?? "OAuth failed"));
        }
      };

      // Poll for popup closure (user closes popup manually)
      const pollTimer = setInterval(() => {
        if (this.popup?.closed) {
          cleanup();
          this.tokenStore.updateSessionStatus(sessionId, "error");
          reject(new Error("OAuth popup was closed"));
        }
      }, 500);

      // Timeout
      const timeoutTimer = setTimeout(() => {
        cleanup();
        this.popup?.close();
        this.popup = null;
        this.tokenStore.updateSessionStatus(sessionId, "error");
        reject(new Error("OAuth flow timed out"));
      }, timeoutMs!);

      window.addEventListener("message", handleMessage);
    });
  }

  /**
   * Check if a connection's OAuth token is still valid.
   */
  async checkStatus(connectionId: string): Promise<{ valid: boolean }> {
    const response = await fetch(`${this.config.apiBaseUrl}/oauth/status`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ connection_id: connectionId }),
    });
    return response.json();
  }

  /**
   * Trigger re-authorization for an existing connection.
   */
  async reauthorize(connectionId: string, tenantId: string): Promise<OAuthCompleteMessage> {
    const response = await fetch(`${this.config.apiBaseUrl}/oauth/reauthorize`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ connection_id: connectionId, tenant_id: tenantId }),
    });

    if (!response.ok) {
      throw new Error(`OAuth re-authorization failed: ${response.statusText}`);
    }

    const { session_id, authorize_url } = (await response.json()) as OAuthInitiateResponse;

    this.tokenStore.saveSession({
      sessionId: session_id,
      connectorId: connectionId,
      connectionId,
      status: "pending",
      createdAt: Date.now(),
    });

    return this.openPopup(authorize_url, session_id);
  }

  /**
   * Clean up expired sessions.
   */
  cleanup(): void {
    this.tokenStore.clearExpired();
  }
}
