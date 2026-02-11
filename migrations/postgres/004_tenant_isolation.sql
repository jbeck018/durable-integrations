-- FlowForge tenant isolation schema extensions
-- Migration: 004_tenant_isolation
-- Description: Adds Neon project-per-tenant columns to the tenants table
--              for database isolation via Neon serverless Postgres.

BEGIN;

-- Add Neon project tracking columns to tenants table.
ALTER TABLE tenants ADD COLUMN neon_project_id TEXT;
ALTER TABLE tenants ADD COLUMN connection_uri_encrypted TEXT;
ALTER TABLE tenants ADD COLUMN region TEXT DEFAULT 'aws-us-east-1';
ALTER TABLE tenants ADD COLUMN db_schema_version INTEGER DEFAULT 0;
ALTER TABLE tenants ADD COLUMN db_provisioned_at TIMESTAMPTZ;

-- Index for looking up tenants by Neon project (used by deprovisioning).
CREATE INDEX idx_tenants_neon_project_id ON tenants (neon_project_id)
    WHERE neon_project_id IS NOT NULL;

-- Index for finding tenants that need schema migrations.
CREATE INDEX idx_tenants_db_schema_version ON tenants (db_schema_version);

COMMIT;
