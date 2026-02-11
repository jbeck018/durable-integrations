-- =============================================================================
-- FlowForge Database Initialization
-- Executed automatically by PostgreSQL on first startup.
-- =============================================================================

-- Create the flowforge database if it does not exist.
-- In the Docker setup, POSTGRES_DB already creates it, but this guards against
-- re-initialization when the init script is applied to an existing cluster.
SELECT 'CREATE DATABASE flowforge'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'flowforge')\gexec

-- Connect to the flowforge database and enable required extensions.
\connect flowforge;

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- Create the Temporal database used by temporalio/auto-setup.
SELECT 'CREATE DATABASE temporal'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'temporal')\gexec

SELECT 'CREATE DATABASE temporal_visibility'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'temporal_visibility')\gexec
