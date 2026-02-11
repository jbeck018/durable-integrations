/**
 * Angular injectable wrapping the ui-core OAuthManager.
 */

import { Injectable, signal } from "@angular/core";
import {
  OAuthManager,
  type OAuthManagerConfig,
  type OAuthCompleteMessage,
} from "@flowforge/ui-core";

@Injectable({ providedIn: "root" })
export class FlowForgeOAuthService {
  private manager: OAuthManager | null = null;

  readonly loading = signal(false);
  readonly error = signal<Error | null>(null);

  configure(config: OAuthManagerConfig): void {
    this.manager = new OAuthManager(config);
  }

  async authorize(connectorId: string, tenantId: string): Promise<OAuthCompleteMessage> {
    if (!this.manager) throw new Error("OAuthManager not configured. Call configure() first.");

    this.loading.set(true);
    this.error.set(null);

    try {
      const result = await this.manager.authorize(connectorId, tenantId);
      return result;
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      this.error.set(e);
      throw e;
    } finally {
      this.loading.set(false);
    }
  }

  async reauthorize(connectionId: string, tenantId: string): Promise<OAuthCompleteMessage> {
    if (!this.manager) throw new Error("OAuthManager not configured. Call configure() first.");

    this.loading.set(true);
    this.error.set(null);

    try {
      const result = await this.manager.reauthorize(connectionId, tenantId);
      return result;
    } catch (err) {
      const e = err instanceof Error ? err : new Error(String(err));
      this.error.set(e);
      throw e;
    } finally {
      this.loading.set(false);
    }
  }
}
