/**
 * FlowForge API client.
 * Type-safe HTTP client for all FlowForge REST endpoints.
 */

import type {
  APIError,
  APIResponse,
  Catalog,
  CheckStatus,
  Connection,
  Connector,
  FieldMapping,
  MCPServer,
  PaginatedResponse,
  Sync,
  SyncRun,
  Stream,
  Tenant,
} from "./types";

// ---------------------------------------------------------------------------
// Request / response helper types
// ---------------------------------------------------------------------------

interface RequestOptions {
  method: string;
  path: string;
  body?: unknown;
  query?: Record<string, string | number | boolean | undefined>;
  signal?: AbortSignal;
}

interface PaginationParams {
  page?: number;
  page_size?: number;
}

interface CreateConnectionRequest {
  connector_id: string;
  tenant_id: string;
  name: string;
  config: Record<string, unknown>;
}

interface UpdateConnectionRequest {
  name?: string;
  config?: Record<string, unknown>;
}

interface CreateSyncRequest {
  name: string;
  source_connection_id: string;
  destination_connection_id: string;
  tenant_id: string;
  schedule: string;
  catalog: {
    streams: Array<{
      stream_name: string;
      sync_mode: string;
      destination_sync_mode: string;
      cursor_field?: string[];
      primary_key?: string[][];
    }>;
  };
}

interface UpdateSyncRequest {
  name?: string;
  schedule?: string;
  catalog?: CreateSyncRequest["catalog"];
}

interface CreateFieldMappingRequest {
  source_stream: string;
  source_field: string;
  destination_stream: string;
  destination_field: string;
  transform?: {
    type: string;
    expression: string;
    config?: Record<string, unknown>;
  } | null;
}

interface AutoMapFieldsRequest {
  source_stream: string;
  destination_stream: string;
  strategy: "name_match" | "type_match" | "ai_suggest";
}

interface CreateMCPServerRequest {
  name: string;
  url: string;
  tenant_id: string;
}

interface HealthResponse {
  status: "healthy" | "degraded" | "unhealthy";
  version: string;
  uptime_seconds: number;
  components: Record<
    string,
    { status: string; message?: string; latency_ms?: number }
  >;
}

// ---------------------------------------------------------------------------
// FlowForgeClient
// ---------------------------------------------------------------------------

export class FlowForgeClient {
  private readonly baseURL: string;
  private readonly headers: Record<string, string>;

  constructor(baseURL: string = "/api/v1", authToken?: string) {
    this.baseURL = baseURL.replace(/\/+$/, "");
    this.headers = {
      "Content-Type": "application/json",
      Accept: "application/json",
    };
    if (authToken) {
      this.headers["Authorization"] = `Bearer ${authToken}`;
    }
  }

  /**
   * Set or update the auth token for subsequent requests.
   */
  setAuthToken(token: string): void {
    this.headers["Authorization"] = `Bearer ${token}`;
  }

  /**
   * Clear the auth token.
   */
  clearAuthToken(): void {
    delete this.headers["Authorization"];
  }

  // -------------------------------------------------------------------------
  // Internal HTTP helpers
  // -------------------------------------------------------------------------

  private buildURL(path: string, query?: Record<string, string | number | boolean | undefined>): string {
    const url = new URL(`${this.baseURL}${path}`, window.location.origin);
    if (query) {
      for (const [key, value] of Object.entries(query)) {
        if (value !== undefined) {
          url.searchParams.set(key, String(value));
        }
      }
    }
    return url.toString();
  }

  private async request<T>(opts: RequestOptions): Promise<T> {
    const url = this.buildURL(opts.path, opts.query);

    const init: RequestInit = {
      method: opts.method,
      headers: { ...this.headers },
      signal: opts.signal,
    };

    if (opts.body !== undefined) {
      init.body = JSON.stringify(opts.body);
    }

    let response: Response;
    try {
      response = await fetch(url, init);
    } catch (err) {
      const apiError: APIError = {
        status: 0,
        code: "NETWORK_ERROR",
        message: err instanceof Error ? err.message : "Network error",
      };
      throw apiError;
    }

    if (!response.ok) {
      let apiError: APIError;
      try {
        const errorBody = await response.json();
        apiError = {
          status: response.status,
          code: errorBody.code ?? `HTTP_${response.status}`,
          message: errorBody.message ?? response.statusText,
          details: errorBody.details,
          request_id: errorBody.request_id,
        };
      } catch {
        apiError = {
          status: response.status,
          code: `HTTP_${response.status}`,
          message: response.statusText,
        };
      }
      throw apiError;
    }

    if (response.status === 204) {
      return undefined as unknown as T;
    }

    return response.json() as Promise<T>;
  }

  private get<T>(path: string, query?: Record<string, string | number | boolean | undefined>, signal?: AbortSignal): Promise<T> {
    return this.request<T>({ method: "GET", path, query, signal });
  }

  private post<T>(path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    return this.request<T>({ method: "POST", path, body, signal });
  }

  private put<T>(path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    return this.request<T>({ method: "PUT", path, body, signal });
  }

  private patch<T>(path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
    return this.request<T>({ method: "PATCH", path, body, signal });
  }

  private del<T>(path: string, signal?: AbortSignal): Promise<T> {
    return this.request<T>({ method: "DELETE", path, signal });
  }

  // -------------------------------------------------------------------------
  // Health
  // -------------------------------------------------------------------------

  async health(signal?: AbortSignal): Promise<HealthResponse> {
    return this.get<HealthResponse>("/health", undefined, signal);
  }

  // -------------------------------------------------------------------------
  // Connectors
  // -------------------------------------------------------------------------

  async listConnectors(
    params?: PaginationParams & { type?: string; category?: string; search?: string },
    signal?: AbortSignal,
  ): Promise<PaginatedResponse<Connector>> {
    return this.get<PaginatedResponse<Connector>>("/connectors", params as Record<string, string | number>, signal);
  }

  async getConnector(id: string, signal?: AbortSignal): Promise<APIResponse<Connector>> {
    return this.get<APIResponse<Connector>>(`/connectors/${encodeURIComponent(id)}`, undefined, signal);
  }

  // -------------------------------------------------------------------------
  // Connections
  // -------------------------------------------------------------------------

  async listConnections(
    params?: PaginationParams & { tenant_id?: string; connector_id?: string },
    signal?: AbortSignal,
  ): Promise<PaginatedResponse<Connection>> {
    return this.get<PaginatedResponse<Connection>>("/connections", params as Record<string, string | number>, signal);
  }

  async getConnection(id: string, signal?: AbortSignal): Promise<APIResponse<Connection>> {
    return this.get<APIResponse<Connection>>(`/connections/${encodeURIComponent(id)}`, undefined, signal);
  }

  async createConnection(data: CreateConnectionRequest, signal?: AbortSignal): Promise<APIResponse<Connection>> {
    return this.post<APIResponse<Connection>>("/connections", data, signal);
  }

  async updateConnection(id: string, data: UpdateConnectionRequest, signal?: AbortSignal): Promise<APIResponse<Connection>> {
    return this.patch<APIResponse<Connection>>(`/connections/${encodeURIComponent(id)}`, data, signal);
  }

  async deleteConnection(id: string, signal?: AbortSignal): Promise<void> {
    return this.del<void>(`/connections/${encodeURIComponent(id)}`, signal);
  }

  async checkConnection(id: string, signal?: AbortSignal): Promise<APIResponse<{ status: CheckStatus; message: string }>> {
    return this.post<APIResponse<{ status: CheckStatus; message: string }>>(`/connections/${encodeURIComponent(id)}/check`, undefined, signal);
  }

  // -------------------------------------------------------------------------
  // Streams (discovery via connection)
  // -------------------------------------------------------------------------

  async discoverStreams(connectionId: string, signal?: AbortSignal): Promise<APIResponse<Catalog>> {
    return this.get<APIResponse<Catalog>>(`/connections/${encodeURIComponent(connectionId)}/streams`, undefined, signal);
  }

  async listStreams(
    params?: PaginationParams & { connection_id?: string },
    signal?: AbortSignal,
  ): Promise<PaginatedResponse<Stream>> {
    return this.get<PaginatedResponse<Stream>>("/streams", params as Record<string, string | number>, signal);
  }

  // -------------------------------------------------------------------------
  // Syncs
  // -------------------------------------------------------------------------

  async listSyncs(
    params?: PaginationParams & { tenant_id?: string; status?: string },
    signal?: AbortSignal,
  ): Promise<PaginatedResponse<Sync>> {
    return this.get<PaginatedResponse<Sync>>("/syncs", params as Record<string, string | number>, signal);
  }

  async getSync(id: string, signal?: AbortSignal): Promise<APIResponse<Sync>> {
    return this.get<APIResponse<Sync>>(`/syncs/${encodeURIComponent(id)}`, undefined, signal);
  }

  async createSync(data: CreateSyncRequest, signal?: AbortSignal): Promise<APIResponse<Sync>> {
    return this.post<APIResponse<Sync>>("/syncs", data, signal);
  }

  async updateSync(id: string, data: UpdateSyncRequest, signal?: AbortSignal): Promise<APIResponse<Sync>> {
    return this.patch<APIResponse<Sync>>(`/syncs/${encodeURIComponent(id)}`, data, signal);
  }

  async deleteSync(id: string, signal?: AbortSignal): Promise<void> {
    return this.del<void>(`/syncs/${encodeURIComponent(id)}`, signal);
  }

  async triggerSync(id: string, signal?: AbortSignal): Promise<APIResponse<SyncRun>> {
    return this.post<APIResponse<SyncRun>>(`/syncs/${encodeURIComponent(id)}/trigger`, undefined, signal);
  }

  async pauseSync(id: string, signal?: AbortSignal): Promise<void> {
    return this.post<void>(`/syncs/${encodeURIComponent(id)}/pause`, undefined, signal);
  }

  async resumeSync(id: string, signal?: AbortSignal): Promise<void> {
    return this.post<void>(`/syncs/${encodeURIComponent(id)}/resume`, undefined, signal);
  }

  async cancelSync(id: string, signal?: AbortSignal): Promise<void> {
    return this.post<void>(`/syncs/${encodeURIComponent(id)}/cancel`, undefined, signal);
  }

  // -------------------------------------------------------------------------
  // Sync Runs
  // -------------------------------------------------------------------------

  async listSyncRuns(
    syncId: string,
    params?: PaginationParams & { status?: string },
    signal?: AbortSignal,
  ): Promise<PaginatedResponse<SyncRun>> {
    return this.get<PaginatedResponse<SyncRun>>(
      `/syncs/${encodeURIComponent(syncId)}/runs`,
      params as Record<string, string | number>,
      signal,
    );
  }

  async getSyncRun(syncId: string, runId: string, signal?: AbortSignal): Promise<APIResponse<SyncRun>> {
    return this.get<APIResponse<SyncRun>>(
      `/syncs/${encodeURIComponent(syncId)}/runs/${encodeURIComponent(runId)}`,
      undefined,
      signal,
    );
  }

  // -------------------------------------------------------------------------
  // Field Mappings
  // -------------------------------------------------------------------------

  async listFieldMappings(
    syncId: string,
    signal?: AbortSignal,
  ): Promise<APIResponse<FieldMapping[]>> {
    return this.get<APIResponse<FieldMapping[]>>(
      `/syncs/${encodeURIComponent(syncId)}/field-mappings`,
      undefined,
      signal,
    );
  }

  async createFieldMapping(
    syncId: string,
    data: CreateFieldMappingRequest,
    signal?: AbortSignal,
  ): Promise<APIResponse<FieldMapping>> {
    return this.post<APIResponse<FieldMapping>>(
      `/syncs/${encodeURIComponent(syncId)}/field-mappings`,
      data,
      signal,
    );
  }

  async updateFieldMapping(
    syncId: string,
    mappingId: string,
    data: Partial<CreateFieldMappingRequest>,
    signal?: AbortSignal,
  ): Promise<APIResponse<FieldMapping>> {
    return this.patch<APIResponse<FieldMapping>>(
      `/syncs/${encodeURIComponent(syncId)}/field-mappings/${encodeURIComponent(mappingId)}`,
      data,
      signal,
    );
  }

  async deleteFieldMapping(syncId: string, mappingId: string, signal?: AbortSignal): Promise<void> {
    return this.del<void>(
      `/syncs/${encodeURIComponent(syncId)}/field-mappings/${encodeURIComponent(mappingId)}`,
      signal,
    );
  }

  async autoMapFields(
    syncId: string,
    data: AutoMapFieldsRequest,
    signal?: AbortSignal,
  ): Promise<APIResponse<FieldMapping[]>> {
    return this.post<APIResponse<FieldMapping[]>>(
      `/syncs/${encodeURIComponent(syncId)}/field-mappings/auto-map`,
      data,
      signal,
    );
  }

  // -------------------------------------------------------------------------
  // Tenants
  // -------------------------------------------------------------------------

  async listTenants(
    params?: PaginationParams,
    signal?: AbortSignal,
  ): Promise<PaginatedResponse<Tenant>> {
    return this.get<PaginatedResponse<Tenant>>("/tenants", params as Record<string, string | number>, signal);
  }

  async getTenant(id: string, signal?: AbortSignal): Promise<APIResponse<Tenant>> {
    return this.get<APIResponse<Tenant>>(`/tenants/${encodeURIComponent(id)}`, undefined, signal);
  }

  // -------------------------------------------------------------------------
  // MCP Servers
  // -------------------------------------------------------------------------

  async listMCPServers(
    params?: PaginationParams & { tenant_id?: string },
    signal?: AbortSignal,
  ): Promise<PaginatedResponse<MCPServer>> {
    return this.get<PaginatedResponse<MCPServer>>("/mcp-servers", params as Record<string, string | number>, signal);
  }

  async getMCPServer(id: string, signal?: AbortSignal): Promise<APIResponse<MCPServer>> {
    return this.get<APIResponse<MCPServer>>(`/mcp-servers/${encodeURIComponent(id)}`, undefined, signal);
  }

  async createMCPServer(data: CreateMCPServerRequest, signal?: AbortSignal): Promise<APIResponse<MCPServer>> {
    return this.post<APIResponse<MCPServer>>("/mcp-servers", data, signal);
  }

  async deleteMCPServer(id: string, signal?: AbortSignal): Promise<void> {
    return this.del<void>(`/mcp-servers/${encodeURIComponent(id)}`, signal);
  }
}

// ---------------------------------------------------------------------------
// Singleton / default instance
// ---------------------------------------------------------------------------

let defaultClient: FlowForgeClient | null = null;

export function getClient(): FlowForgeClient {
  if (!defaultClient) {
    defaultClient = new FlowForgeClient();
  }
  return defaultClient;
}

export function initClient(baseURL: string, authToken?: string): FlowForgeClient {
  defaultClient = new FlowForgeClient(baseURL, authToken);
  return defaultClient;
}
