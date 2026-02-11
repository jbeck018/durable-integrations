-- FlowForge initial database schema
-- Migration: 001_initial
-- Description: Creates all core tables, enums, indexes, and constraints.

BEGIN;

-- ============================================================================
-- ENUM TYPES
-- ============================================================================

CREATE TYPE connector_status AS ENUM ('active', 'inactive', 'deprecated', 'error');
CREATE TYPE connection_status AS ENUM ('active', 'inactive', 'error', 'pending');
CREATE TYPE auth_type AS ENUM ('oauth2', 'api_key', 'basic', 'service_account', 'none');
CREATE TYPE sync_status AS ENUM ('active', 'paused', 'disabled', 'error');
CREATE TYPE sync_run_status AS ENUM ('pending', 'running', 'completed', 'failed', 'cancelled');
CREATE TYPE mcp_server_status AS ENUM ('active', 'inactive', 'error');

-- ============================================================================
-- EXTENSION: uuid-ossp for UUID generation
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ============================================================================
-- TABLE: tenants
-- ============================================================================

CREATE TABLE tenants (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name        TEXT NOT NULL,
    namespace   TEXT NOT NULL UNIQUE,
    config      JSONB NOT NULL DEFAULT '{}'::jsonb,
    resource_quotas JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_tenants_namespace ON tenants (namespace);
CREATE INDEX idx_tenants_created_at ON tenants (created_at);

-- ============================================================================
-- TABLE: connectors
-- ============================================================================

CREATE TABLE connectors (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id        UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name             TEXT NOT NULL,
    connector_type   TEXT NOT NULL,
    config_encrypted BYTEA,
    status           connector_status NOT NULL DEFAULT 'active',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE INDEX idx_connectors_tenant_id ON connectors (tenant_id);
CREATE INDEX idx_connectors_type ON connectors (connector_type);
CREATE INDEX idx_connectors_status ON connectors (status);

-- ============================================================================
-- TABLE: connections
-- ============================================================================

CREATE TABLE connections (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    connector_id    UUID NOT NULL REFERENCES connectors(id) ON DELETE CASCADE,
    auth_type       auth_type NOT NULL DEFAULT 'none',
    credentials_ref TEXT,
    status          connection_status NOT NULL DEFAULT 'pending',
    last_checked_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_connections_tenant_id ON connections (tenant_id);
CREATE INDEX idx_connections_connector_id ON connections (connector_id);
CREATE INDEX idx_connections_status ON connections (status);

-- ============================================================================
-- TABLE: syncs
-- ============================================================================

CREATE TABLE syncs (
    id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id             UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    source_connection_id  UUID NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
    dest_connection_id    UUID NOT NULL REFERENCES connections(id) ON DELETE RESTRICT,
    config                JSONB NOT NULL DEFAULT '{}'::jsonb,
    schedule              JSONB NOT NULL DEFAULT '{}'::jsonb,
    status                sync_status NOT NULL DEFAULT 'active',
    created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_syncs_tenant_id ON syncs (tenant_id);
CREATE INDEX idx_syncs_source_connection ON syncs (source_connection_id);
CREATE INDEX idx_syncs_dest_connection ON syncs (dest_connection_id);
CREATE INDEX idx_syncs_status ON syncs (status);

-- ============================================================================
-- TABLE: sync_runs
-- ============================================================================

CREATE TABLE sync_runs (
    id                    UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sync_id               UUID NOT NULL REFERENCES syncs(id) ON DELETE CASCADE,
    tenant_id             UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    status                sync_run_status NOT NULL DEFAULT 'pending',
    records_extracted     BIGINT NOT NULL DEFAULT 0,
    records_transformed   BIGINT NOT NULL DEFAULT 0,
    records_loaded        BIGINT NOT NULL DEFAULT 0,
    errors                JSONB NOT NULL DEFAULT '[]'::jsonb,
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ
);

CREATE INDEX idx_sync_runs_sync_id ON sync_runs (sync_id);
CREATE INDEX idx_sync_runs_tenant_id ON sync_runs (tenant_id);
CREATE INDEX idx_sync_runs_status ON sync_runs (status);
CREATE INDEX idx_sync_runs_started_at ON sync_runs (started_at DESC);
CREATE INDEX idx_sync_runs_sync_started ON sync_runs (sync_id, started_at DESC);

-- ============================================================================
-- TABLE: sync_state
-- ============================================================================

CREATE TABLE sync_state (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sync_id     UUID NOT NULL REFERENCES syncs(id) ON DELETE CASCADE,
    stream_name TEXT NOT NULL,
    state_data  JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sync_id, stream_name)
);

CREATE INDEX idx_sync_state_sync_id ON sync_state (sync_id);

-- ============================================================================
-- TABLE: streams
-- ============================================================================

CREATE TABLE streams (
    id                   UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    connection_id        UUID NOT NULL REFERENCES connections(id) ON DELETE CASCADE,
    name                 TEXT NOT NULL,
    namespace            TEXT,
    schema               JSONB NOT NULL DEFAULT '{}'::jsonb,
    supported_sync_modes TEXT[] NOT NULL DEFAULT ARRAY['full_refresh'],
    discovered_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (connection_id, name, namespace)
);

CREATE INDEX idx_streams_connection_id ON streams (connection_id);
CREATE INDEX idx_streams_name ON streams (name);

-- ============================================================================
-- TABLE: schema_versions
-- ============================================================================

CREATE TABLE schema_versions (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    stream_id  UUID NOT NULL REFERENCES streams(id) ON DELETE CASCADE,
    version    INT NOT NULL,
    schema     JSONB NOT NULL,
    diff       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (stream_id, version)
);

CREATE INDEX idx_schema_versions_stream_id ON schema_versions (stream_id);
CREATE INDEX idx_schema_versions_stream_version ON schema_versions (stream_id, version DESC);

-- ============================================================================
-- TABLE: field_mappings
-- ============================================================================

CREATE TABLE field_mappings (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    sync_id       UUID NOT NULL REFERENCES syncs(id) ON DELETE CASCADE,
    source_stream TEXT NOT NULL,
    mappings      JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (sync_id, source_stream)
);

CREATE INDEX idx_field_mappings_sync_id ON field_mappings (sync_id);

-- ============================================================================
-- TABLE: audit_log
-- ============================================================================

CREATE TABLE audit_log (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id     UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    user_id       TEXT NOT NULL,
    action        TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id   TEXT NOT NULL,
    details       JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_audit_log_tenant_id ON audit_log (tenant_id);
CREATE INDEX idx_audit_log_user_id ON audit_log (user_id);
CREATE INDEX idx_audit_log_action ON audit_log (action);
CREATE INDEX idx_audit_log_resource ON audit_log (resource_type, resource_id);
CREATE INDEX idx_audit_log_created_at ON audit_log (created_at DESC);
CREATE INDEX idx_audit_log_tenant_created ON audit_log (tenant_id, created_at DESC);

-- ============================================================================
-- TABLE: mcp_servers
-- ============================================================================

CREATE TABLE mcp_servers (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    name       TEXT NOT NULL,
    config     JSONB NOT NULL DEFAULT '{}'::jsonb,
    endpoint   TEXT NOT NULL,
    status     mcp_server_status NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, name)
);

CREATE INDEX idx_mcp_servers_tenant_id ON mcp_servers (tenant_id);
CREATE INDEX idx_mcp_servers_status ON mcp_servers (status);

-- ============================================================================
-- TABLE: api_keys
-- ============================================================================

CREATE TABLE api_keys (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    tenant_id  UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    key_hash   TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    scopes     TEXT[] NOT NULL DEFAULT '{}',
    rate_limit INT NOT NULL DEFAULT 1000,
    expires_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_api_keys_tenant_id ON api_keys (tenant_id);
CREATE INDEX idx_api_keys_key_hash ON api_keys (key_hash);
CREATE INDEX idx_api_keys_expires_at ON api_keys (expires_at) WHERE expires_at IS NOT NULL;

-- ============================================================================
-- TRIGGER FUNCTION: auto-update updated_at
-- ============================================================================

CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply auto-update triggers to all tables with updated_at
CREATE TRIGGER trg_tenants_updated_at
    BEFORE UPDATE ON tenants
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_connectors_updated_at
    BEFORE UPDATE ON connectors
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_connections_updated_at
    BEFORE UPDATE ON connections
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_syncs_updated_at
    BEFORE UPDATE ON syncs
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_sync_state_updated_at
    BEFORE UPDATE ON sync_state
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_streams_updated_at
    BEFORE UPDATE ON streams
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_field_mappings_updated_at
    BEFORE UPDATE ON field_mappings
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_mcp_servers_updated_at
    BEFORE UPDATE ON mcp_servers
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

COMMIT;
