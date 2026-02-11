#!/usr/bin/env bash
# =============================================================================
# FlowForge Database Migration Runner
#
# Applies SQL migration files from migrations/postgres/ in alphabetical order.
# Tracks applied migrations in a schema_migrations table to ensure idempotency.
#
# Usage:
#   ./scripts/migrate.sh                    # Uses defaults or env vars
#   POSTGRES_HOST=db ./scripts/migrate.sh   # Override host
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
MIGRATIONS_DIR="${PROJECT_ROOT}/migrations/postgres"

DB_HOST="${POSTGRES_HOST:-localhost}"
DB_PORT="${POSTGRES_PORT:-5432}"
DB_USER="${POSTGRES_USER:-flowforge}"
DB_PASSWORD="${POSTGRES_PASSWORD:-flowforge_dev_password}"
DB_NAME="${POSTGRES_DB:-flowforge}"
DB_SSL_MODE="${POSTGRES_SSL_MODE:-disable}"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

info()  { printf "${BLUE}[INFO]${NC}  %s\n" "$1"; }
ok()    { printf "${GREEN}[OK]${NC}    %s\n" "$1"; }
warn()  { printf "${YELLOW}[WARN]${NC}  %s\n" "$1"; }
fail()  { printf "${RED}[FAIL]${NC}  %s\n" "$1"; exit 1; }

# ---------------------------------------------------------------------------
# Build psql connection string
# ---------------------------------------------------------------------------
psql_cmd() {
    PGPASSWORD="${DB_PASSWORD}" psql \
        -h "${DB_HOST}" \
        -p "${DB_PORT}" \
        -U "${DB_USER}" \
        -d "${DB_NAME}" \
        -v ON_ERROR_STOP=1 \
        --no-psqlrc \
        "$@"
}

# ---------------------------------------------------------------------------
# Check that psql is available
# ---------------------------------------------------------------------------
check_psql() {
    if ! command -v psql &>/dev/null; then
        fail "psql is not installed. Install PostgreSQL client tools."
    fi
}

# ---------------------------------------------------------------------------
# Wait for database connectivity
# ---------------------------------------------------------------------------
wait_for_database() {
    local max_wait=60
    local elapsed=0
    local interval=2

    info "Waiting for PostgreSQL at ${DB_HOST}:${DB_PORT}..."

    while [ "$elapsed" -lt "$max_wait" ]; do
        if PGPASSWORD="${DB_PASSWORD}" pg_isready -h "${DB_HOST}" -p "${DB_PORT}" -U "${DB_USER}" -d "${DB_NAME}" &>/dev/null; then
            ok "PostgreSQL is accepting connections."
            return 0
        fi
        sleep "$interval"
        elapsed=$((elapsed + interval))
    done

    fail "PostgreSQL did not become available within ${max_wait}s."
}

# ---------------------------------------------------------------------------
# Ensure the schema_migrations tracking table exists
# ---------------------------------------------------------------------------
ensure_migrations_table() {
    info "Ensuring schema_migrations table exists..."

    psql_cmd -q <<'EOSQL'
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT PRIMARY KEY,
    filename    TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    checksum    TEXT NOT NULL
);
EOSQL

    ok "schema_migrations table is ready."
}

# ---------------------------------------------------------------------------
# Check if a migration has already been applied
# ---------------------------------------------------------------------------
is_applied() {
    local version="$1"
    local count
    count=$(psql_cmd -tAq -c "SELECT COUNT(*) FROM schema_migrations WHERE version = '${version}';")
    [ "$count" -gt 0 ]
}

# ---------------------------------------------------------------------------
# Compute a checksum for a migration file
# ---------------------------------------------------------------------------
file_checksum() {
    local filepath="$1"
    if command -v sha256sum &>/dev/null; then
        sha256sum "$filepath" | awk '{print $1}'
    elif command -v shasum &>/dev/null; then
        shasum -a 256 "$filepath" | awk '{print $1}'
    else
        md5sum "$filepath" | awk '{print $1}'
    fi
}

# ---------------------------------------------------------------------------
# Apply all pending migrations
# ---------------------------------------------------------------------------
apply_migrations() {
    local applied=0
    local skipped=0

    if [ ! -d "${MIGRATIONS_DIR}" ]; then
        fail "Migrations directory not found: ${MIGRATIONS_DIR}"
    fi

    local migration_files
    migration_files=$(find "${MIGRATIONS_DIR}" -name '*.sql' -type f | sort)

    if [ -z "${migration_files}" ]; then
        warn "No migration files found in ${MIGRATIONS_DIR}."
        return 0
    fi

    for filepath in ${migration_files}; do
        local filename
        filename=$(basename "$filepath")
        local version
        version=$(echo "$filename" | sed 's/_.*//')

        if is_applied "$version"; then
            skipped=$((skipped + 1))
            continue
        fi

        info "Applying migration: ${filename}..."

        local checksum
        checksum=$(file_checksum "$filepath")

        psql_cmd -q < "$filepath"

        psql_cmd -q -c "INSERT INTO schema_migrations (version, filename, checksum) VALUES ('${version}', '${filename}', '${checksum}');"

        ok "Applied: ${filename}"
        applied=$((applied + 1))
    done

    info "Migration summary: ${applied} applied, ${skipped} skipped (already applied)."
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    printf "\n${GREEN}FlowForge Database Migration Runner${NC}\n\n"

    check_psql
    wait_for_database
    ensure_migrations_table
    apply_migrations

    ok "All migrations complete."
}

main "$@"
