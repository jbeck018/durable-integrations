# =============================================================================
# FlowForge Multi-Stage Dockerfile
# Builds: flowforge-api, flowforge-worker, flowforge-mcp-gateway
# Usage:
#   docker build --target api -t flowforge-api:latest .
#   docker build --target worker -t flowforge-worker:latest .
#   docker build --target mcp-gateway -t flowforge-mcp-gateway:latest .
# =============================================================================

# ---------------------------------------------------------------------------
# Stage: builder
# Compiles all Go binaries with static linking for scratch/distroless targets.
# ---------------------------------------------------------------------------
FROM golang:1.24-alpine AS builder

RUN apk add --no-cache ca-certificates git tzdata

WORKDIR /build

COPY go.mod go.sum ./
RUN go mod download && go mod verify

COPY . .

ARG VERSION=dev
ARG BUILD_TIME=""

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" \
    -o /out/flowforge-api ./cmd/flowforge-api

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" \
    -o /out/flowforge-worker ./cmd/flowforge-worker

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -trimpath \
    -ldflags="-s -w -X main.version=${VERSION} -X main.buildTime=${BUILD_TIME}" \
    -o /out/flowforge-mcp-gateway ./cmd/flowforge-mcp-gateway

# ---------------------------------------------------------------------------
# Stage: api
# Minimal image for the FlowForge REST/gRPC API server.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS api

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/flowforge-api /usr/local/bin/flowforge-api
COPY migrations /migrations

ENV TZ=UTC
EXPOSE 8080 9090
USER nonroot:nonroot

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/flowforge-api", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/flowforge-api"]

# ---------------------------------------------------------------------------
# Stage: worker
# Minimal image for the FlowForge Temporal worker.
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS worker

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/flowforge-worker /usr/local/bin/flowforge-worker

ENV TZ=UTC
EXPOSE 9091
USER nonroot:nonroot

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/flowforge-worker", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/flowforge-worker"]

# ---------------------------------------------------------------------------
# Stage: mcp-gateway
# Minimal image for the FlowForge MCP Gateway (SSE + WebSocket).
# ---------------------------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS mcp-gateway

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /out/flowforge-mcp-gateway /usr/local/bin/flowforge-mcp-gateway

ENV TZ=UTC
EXPOSE 8090
USER nonroot:nonroot

HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD ["/usr/local/bin/flowforge-mcp-gateway", "healthcheck"]

ENTRYPOINT ["/usr/local/bin/flowforge-mcp-gateway"]
