/**
 * Token store — manages OAuth session references client-side.
 * Actual tokens are stored server-side (Vault). The client only
 * stores session_id references for the BFF pattern.
 */

export interface OAuthSession {
  sessionId: string;
  connectorId: string;
  connectionId?: string;
  status: "pending" | "completed" | "error";
  createdAt: number;
}

export class TokenStore {
  private readonly storageKey: string;

  constructor(storageKey: string = "flowforge_oauth_sessions") {
    this.storageKey = storageKey;
  }

  getSessions(): OAuthSession[] {
    try {
      const raw = sessionStorage.getItem(this.storageKey);
      return raw ? JSON.parse(raw) : [];
    } catch {
      return [];
    }
  }

  getSession(sessionId: string): OAuthSession | null {
    return this.getSessions().find((s) => s.sessionId === sessionId) ?? null;
  }

  saveSession(session: OAuthSession): void {
    const sessions = this.getSessions().filter((s) => s.sessionId !== session.sessionId);
    sessions.push(session);
    sessionStorage.setItem(this.storageKey, JSON.stringify(sessions));
  }

  updateSessionStatus(sessionId: string, status: OAuthSession["status"], connectionId?: string): void {
    const session = this.getSession(sessionId);
    if (session) {
      session.status = status;
      if (connectionId) session.connectionId = connectionId;
      this.saveSession(session);
    }
  }

  removeSession(sessionId: string): void {
    const sessions = this.getSessions().filter((s) => s.sessionId !== sessionId);
    sessionStorage.setItem(this.storageKey, JSON.stringify(sessions));
  }

  clearExpired(maxAgeMs: number = 10 * 60 * 1000): void {
    const now = Date.now();
    const sessions = this.getSessions().filter((s) => now - s.createdAt < maxAgeMs);
    sessionStorage.setItem(this.storageKey, JSON.stringify(sessions));
  }
}
