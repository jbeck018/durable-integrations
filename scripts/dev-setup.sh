#!/usr/bin/env bash
# =============================================================================
# FlowForge Development Environment Setup
#
# Checks prerequisites, starts infrastructure via docker-compose, waits for
# healthy services, runs database migrations, and prints connection info.
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMPOSE_DIR="${PROJECT_ROOT}/deploy/docker-compose"
COMPOSE_FILE="${COMPOSE_DIR}/docker-compose.yml"
ENV_FILE="${COMPOSE_DIR}/.env"
ENV_EXAMPLE="${COMPOSE_DIR}/.env.example"
MIGRATE_SCRIPT="${SCRIPT_DIR}/migrate.sh"

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
# Prerequisite checks
# ---------------------------------------------------------------------------
check_prerequisites() {
    info "Checking prerequisites..."

    local missing=0

    if ! command -v go &>/dev/null; then
        warn "go is not installed. Install Go 1.22+ from https://go.dev/dl/"
        missing=1
    else
        ok "go $(go version | awk '{print $3}' | sed 's/go//')"
    fi

    if ! command -v docker &>/dev/null; then
        warn "docker is not installed. Install Docker from https://docs.docker.com/get-docker/"
        missing=1
    else
        ok "docker $(docker --version | awk '{print $3}' | tr -d ',')"
    fi

    if docker compose version &>/dev/null; then
        ok "docker compose $(docker compose version --short 2>/dev/null || echo 'available')"
    elif command -v docker-compose &>/dev/null; then
        ok "docker-compose $(docker-compose --version | awk '{print $NF}')"
    else
        warn "docker compose is not available. Install Docker Compose v2."
        missing=1
    fi

    if [ "$missing" -ne 0 ]; then
        fail "Missing prerequisites. Please install the tools listed above."
    fi

    ok "All prerequisites satisfied."
}

# ---------------------------------------------------------------------------
# Determine docker compose command
# ---------------------------------------------------------------------------
compose_cmd() {
    if docker compose version &>/dev/null; then
        echo "docker compose"
    else
        echo "docker-compose"
    fi
}

# ---------------------------------------------------------------------------
# Copy .env.example if .env does not exist
# ---------------------------------------------------------------------------
ensure_env_file() {
    if [ ! -f "${ENV_FILE}" ]; then
        info "Creating .env from .env.example..."
        cp "${ENV_EXAMPLE}" "${ENV_FILE}"
        ok ".env file created at ${ENV_FILE}"
    else
        ok ".env file already exists."
    fi
}

# ---------------------------------------------------------------------------
# Start docker-compose services
# ---------------------------------------------------------------------------
start_services() {
    info "Starting infrastructure services..."
    local cmd
    cmd="$(compose_cmd)"
    $cmd -f "${COMPOSE_FILE}" up -d
    ok "Docker compose services started."
}

# ---------------------------------------------------------------------------
# Wait for a service to be healthy
# ---------------------------------------------------------------------------
wait_for_service() {
    local service_name="$1"
    local max_wait="${2:-120}"
    local elapsed=0
    local interval=3

    info "Waiting for ${service_name} to become healthy (timeout: ${max_wait}s)..."

    while [ "$elapsed" -lt "$max_wait" ]; do
        local health
        health=$(docker inspect --format='{{.State.Health.Status}}' "flowforge-${service_name}" 2>/dev/null || echo "not_found")

        if [ "$health" = "healthy" ]; then
            ok "${service_name} is healthy."
            return 0
        fi

        sleep "$interval"
        elapsed=$((elapsed + interval))
    done

    fail "${service_name} did not become healthy within ${max_wait}s."
}

# ---------------------------------------------------------------------------
# Wait for all infrastructure to be ready
# ---------------------------------------------------------------------------
wait_for_infrastructure() {
    info "Waiting for infrastructure services to become healthy..."
    wait_for_service "postgres" 60
    wait_for_service "redis" 30
    wait_for_service "temporal" 120
    wait_for_service "minio" 60
    wait_for_service "vault" 30
    ok "All infrastructure services are healthy."
}

# ---------------------------------------------------------------------------
# Run database migrations
# ---------------------------------------------------------------------------
run_migrations() {
    info "Running database migrations..."
    if [ -x "${MIGRATE_SCRIPT}" ]; then
        bash "${MIGRATE_SCRIPT}"
        ok "Database migrations completed."
    else
        warn "Migration script not found or not executable at ${MIGRATE_SCRIPT}. Skipping."
    fi
}

# ---------------------------------------------------------------------------
# Print connection information
# ---------------------------------------------------------------------------
print_connection_info() {
    printf "\n"
    printf "${GREEN}============================================================${NC}\n"
    printf "${GREEN}  FlowForge Development Environment Ready${NC}\n"
    printf "${GREEN}============================================================${NC}\n"
    printf "\n"
    printf "  ${BLUE}API Server:${NC}        http://localhost:8080\n"
    printf "  ${BLUE}gRPC Server:${NC}       localhost:9090\n"
    printf "  ${BLUE}MCP Gateway:${NC}       http://localhost:8090\n"
    printf "\n"
    printf "  ${BLUE}PostgreSQL:${NC}        localhost:5432  (user: flowforge)\n"
    printf "  ${BLUE}Redis:${NC}             localhost:6379\n"
    printf "  ${BLUE}Temporal:${NC}          localhost:7233\n"
    printf "  ${BLUE}Temporal UI:${NC}       http://localhost:8088\n"
    printf "  ${BLUE}MinIO Console:${NC}     http://localhost:9001  (user: flowforge)\n"
    printf "  ${BLUE}Vault:${NC}             http://localhost:8200  (token: flowforge-dev-token)\n"
    printf "\n"
    printf "  ${BLUE}Prometheus:${NC}        http://localhost:9091\n"
    printf "  ${BLUE}Grafana:${NC}           http://localhost:3000  (admin/admin)\n"
    printf "\n"
    printf "  Run ${YELLOW}make build${NC} to compile binaries.\n"
    printf "  Run ${YELLOW}make test${NC} to execute tests.\n"
    printf "  Run ${YELLOW}make docker-down${NC} to stop services.\n"
    printf "\n"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
    printf "\n${GREEN}FlowForge Development Setup${NC}\n\n"

    check_prerequisites
    ensure_env_file
    start_services
    wait_for_infrastructure
    run_migrations
    print_connection_info
}

main "$@"
