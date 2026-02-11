package rest

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/flowforge/flowforge/internal/api/middleware"
	"github.com/flowforge/flowforge/internal/common"
)

// Server is the FlowForge REST API HTTP server with graceful shutdown.
type Server struct {
	router     chi.Router
	httpServer *http.Server
	config     *common.Config
	logger     *slog.Logger

	// Handler dependencies — wired by the caller before calling Start.
	ConnectorHandlers    *ConnectorHandlers
	ConnectionHandlers   *ConnectionHandlers
	SyncHandlers         *SyncHandlers
	SyncRunHandlers      *SyncRunHandlers
	StreamHandlers       *StreamHandlers
	TenantHandlers       *TenantHandlers
	FieldMappingHandlers *FieldMappingHandlers
	SchemaHandlers       *SchemaHandlers
	MCPHandlers          *MCPHandlers
	OAuthHandlers        *OAuthHandlers
	HealthHandler        *HealthHandler

	// Optional auth middleware — set before calling RegisterRoutes.
	AuthMiddleware func(http.Handler) http.Handler
}

// NewServer creates a new REST API server with chi router and the given config.
func NewServer(cfg *common.Config) *Server {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: parseSlogLevel(cfg.LogLevel),
	}))

	r := chi.NewRouter()

	s := &Server{
		router: r,
		config: cfg,
		logger: logger,
		// Defaults for handler groups when no repos are injected.
		ConnectorHandlers: &ConnectorHandlers{},
		HealthHandler:     &HealthHandler{},
	}

	addr := fmt.Sprintf("%s:%d", cfg.APIHost, cfg.APIPort)
	s.httpServer = &http.Server{
		Addr:              addr,
		Handler:           r,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       cfg.RequestTimeout,
		WriteTimeout:      cfg.RequestTimeout + 5*time.Second,
		IdleTimeout:       120 * time.Second,
		MaxHeaderBytes:    1 << 20, // 1 MB
	}

	return s
}

// RegisterRoutes mounts all route groups and middleware on the chi router.
// Must be called after handler dependencies are wired.
func (s *Server) RegisterRoutes() {
	r := s.router

	// Global middleware stack — order matters.
	r.Use(middleware.Recovery(s.logger))
	r.Use(middleware.CORSConfig())
	r.Use(middleware.RequestLogger(s.logger))
	r.Use(middleware.RateLimit(s.config.WorkerConcurrency*10))
	r.Use(chimiddleware.RealIP)
	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.Compress(5))
	r.Use(chimiddleware.Timeout(s.config.RequestTimeout))

	// Health check — no auth required.
	r.Get("/api/v1/health", s.HealthHandler.Health)

	// OpenAPI spec documentation endpoint.
	r.Get("/api/v1/docs", s.handleDocs)

	// Authenticated API routes.
	r.Route("/api/v1", func(api chi.Router) {
		// Apply auth middleware if configured.
		if s.AuthMiddleware != nil {
			api.Use(s.AuthMiddleware)
		}
		api.Use(middleware.TenantIsolation())

		// Connectors (read-only, uses CDK registry).
		api.Route("/connectors", func(r chi.Router) {
			if s.ConnectorHandlers != nil {
				s.ConnectorHandlers.Routes(r)
			}
		})

		// Connections CRUD.
		api.Route("/connections", func(r chi.Router) {
			if s.ConnectionHandlers != nil {
				s.ConnectionHandlers.Routes(r)
			}
		})

		// Syncs CRUD + orchestration.
		api.Route("/syncs", func(r chi.Router) {
			if s.SyncHandlers != nil {
				s.SyncHandlers.Routes(r)
			}
		})

		// Sync runs.
		api.Route("/sync-runs", func(r chi.Router) {
			if s.SyncRunHandlers != nil {
				s.SyncRunHandlers.Routes(r)
			}
		})

		// Streams discovery.
		api.Route("/streams", func(r chi.Router) {
			if s.StreamHandlers != nil {
				s.StreamHandlers.Routes(r)
			}
		})

		// Tenants.
		api.Route("/tenants", func(r chi.Router) {
			if s.TenantHandlers != nil {
				s.TenantHandlers.Routes(r)
			}
		})

		// Schemas.
		api.Route("/schemas", func(r chi.Router) {
			if s.SchemaHandlers != nil {
				s.SchemaHandlers.Routes(r)
			}
		})

		// Field mappings.
		api.Route("/field-mappings", func(r chi.Router) {
			if s.FieldMappingHandlers != nil {
				s.FieldMappingHandlers.Routes(r)
			}
		})

		// MCP servers.
		api.Route("/mcp", func(r chi.Router) {
			if s.MCPHandlers != nil {
				s.MCPHandlers.Routes(r)
			}
		})

		// OAuth BFF endpoints for popup-based OAuth2 flows.
		api.Route("/oauth", func(r chi.Router) {
			if s.OAuthHandlers != nil {
				s.OAuthHandlers.Routes(r)
			}
		})
	})
}

// Start begins listening for HTTP connections and blocks until the server
// shuts down gracefully in response to SIGINT or SIGTERM.
func (s *Server) Start() error {
	s.RegisterRoutes()

	// Channel for OS signals.
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	// Channel for server errors.
	errCh := make(chan error, 1)

	go func() {
		s.logger.Info("starting FlowForge API server",
			slog.String("addr", s.httpServer.Addr),
			slog.String("env", s.config.Environment),
		)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			errCh <- err
		}
	}()

	// Wait for signal or error.
	select {
	case sig := <-sigCh:
		s.logger.Info("received shutdown signal", slog.String("signal", sig.String()))
	case err := <-errCh:
		s.logger.Error("server error", slog.String("error", err.Error()))
		return err
	}

	// Graceful shutdown with configured grace period.
	ctx, cancel := context.WithTimeout(context.Background(), s.config.ShutdownGracePeriod)
	defer cancel()

	s.logger.Info("shutting down gracefully",
		slog.Duration("grace_period", s.config.ShutdownGracePeriod),
	)

	if err := s.httpServer.Shutdown(ctx); err != nil {
		s.logger.Error("graceful shutdown failed", slog.String("error", err.Error()))
		return err
	}

	s.logger.Info("server stopped")
	return nil
}

// Router returns the underlying chi.Router for testing or custom route injection.
func (s *Server) Router() chi.Router {
	return s.router
}

// handleDocs serves a minimal OpenAPI spec page.
func (s *Server) handleDocs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(openAPISpec))
}

// parseSlogLevel converts a string log level to slog.Level.
func parseSlogLevel(level string) slog.Level {
	switch level {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// openAPISpec is the embedded OpenAPI 3.0 specification for the FlowForge API.
const openAPISpec = `{
  "openapi": "3.0.3",
  "info": {
    "title": "FlowForge API",
    "description": "FlowForge integration platform REST API for managing connectors, connections, syncs, and data streams.",
    "version": "1.0.0",
    "contact": {
      "name": "FlowForge Engineering",
      "url": "https://flowforge.io"
    }
  },
  "servers": [
    {
      "url": "/api/v1",
      "description": "FlowForge API v1"
    }
  ],
  "paths": {
    "/health": {
      "get": {
        "summary": "Health check",
        "operationId": "healthCheck",
        "tags": ["Health"],
        "responses": {
          "200": {
            "description": "Service is healthy",
            "content": {
              "application/json": {
                "schema": {
                  "type": "object",
                  "properties": {
                    "status": { "type": "string" },
                    "service": { "type": "string" },
                    "time": { "type": "string", "format": "date-time" }
                  }
                }
              }
            }
          }
        }
      }
    },
    "/connectors": {
      "get": {
        "summary": "List registered connectors",
        "operationId": "listConnectors",
        "tags": ["Connectors"],
        "responses": {
          "200": { "description": "List of connectors" }
        }
      }
    },
    "/connectors/{connectorName}": {
      "get": {
        "summary": "Get connector details",
        "operationId": "getConnector",
        "tags": ["Connectors"],
        "parameters": [
          { "name": "connectorName", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": {
          "200": { "description": "Connector details" },
          "404": { "description": "Connector not found" }
        }
      }
    },
    "/connectors/test": {
      "post": {
        "summary": "Test connector connection",
        "operationId": "testConnection",
        "tags": ["Connectors"],
        "requestBody": {
          "required": true,
          "content": {
            "application/json": {
              "schema": {
                "type": "object",
                "required": ["connector_name", "config"],
                "properties": {
                  "connector_name": { "type": "string" },
                  "config": { "type": "object" }
                }
              }
            }
          }
        },
        "responses": {
          "200": { "description": "Connection test result" }
        }
      }
    },
    "/connections": {
      "get": {
        "summary": "List connections",
        "operationId": "listConnections",
        "tags": ["Connections"],
        "parameters": [
          { "name": "page", "in": "query", "schema": { "type": "integer", "default": 1 } },
          { "name": "page_size", "in": "query", "schema": { "type": "integer", "default": 20 } }
        ],
        "responses": { "200": { "description": "Paginated connections" } }
      },
      "post": {
        "summary": "Create connection",
        "operationId": "createConnection",
        "tags": ["Connections"],
        "responses": { "201": { "description": "Connection created" } }
      }
    },
    "/connections/{connectionID}": {
      "get": {
        "summary": "Get connection",
        "operationId": "getConnection",
        "tags": ["Connections"],
        "parameters": [
          { "name": "connectionID", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Connection details" } }
      },
      "put": {
        "summary": "Update connection",
        "operationId": "updateConnection",
        "tags": ["Connections"],
        "parameters": [
          { "name": "connectionID", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Connection updated" } }
      },
      "delete": {
        "summary": "Delete connection",
        "operationId": "deleteConnection",
        "tags": ["Connections"],
        "parameters": [
          { "name": "connectionID", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "204": { "description": "Connection deleted" } }
      }
    },
    "/syncs": {
      "get": {
        "summary": "List syncs",
        "operationId": "listSyncs",
        "tags": ["Syncs"],
        "responses": { "200": { "description": "Paginated syncs" } }
      },
      "post": {
        "summary": "Create sync",
        "operationId": "createSync",
        "tags": ["Syncs"],
        "responses": { "201": { "description": "Sync created" } }
      }
    },
    "/syncs/{syncID}": {
      "get": {
        "summary": "Get sync",
        "operationId": "getSync",
        "tags": ["Syncs"],
        "parameters": [
          { "name": "syncID", "in": "path", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Sync details" } }
      },
      "put": {
        "summary": "Update sync",
        "operationId": "updateSync",
        "tags": ["Syncs"],
        "responses": { "200": { "description": "Sync updated" } }
      },
      "delete": {
        "summary": "Delete sync",
        "operationId": "deleteSync",
        "tags": ["Syncs"],
        "responses": { "204": { "description": "Sync deleted" } }
      }
    },
    "/syncs/{syncID}/trigger": {
      "post": {
        "summary": "Trigger sync execution",
        "operationId": "triggerSync",
        "tags": ["Syncs"],
        "responses": { "201": { "description": "Sync run started" } }
      }
    },
    "/syncs/{syncID}/pause": {
      "post": {
        "summary": "Pause sync",
        "operationId": "pauseSync",
        "tags": ["Syncs"],
        "responses": { "200": { "description": "Sync paused" } }
      }
    },
    "/syncs/{syncID}/resume": {
      "post": {
        "summary": "Resume sync",
        "operationId": "resumeSync",
        "tags": ["Syncs"],
        "responses": { "200": { "description": "Sync resumed" } }
      }
    },
    "/sync-runs": {
      "get": {
        "summary": "List sync runs",
        "operationId": "listSyncRuns",
        "tags": ["SyncRuns"],
        "parameters": [
          { "name": "sync_id", "in": "query", "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Paginated sync runs" } }
      }
    },
    "/sync-runs/{syncRunID}": {
      "get": {
        "summary": "Get sync run",
        "operationId": "getSyncRun",
        "tags": ["SyncRuns"],
        "responses": { "200": { "description": "Sync run details" } }
      }
    },
    "/sync-runs/{syncRunID}/logs": {
      "get": {
        "summary": "Get sync run logs",
        "operationId": "getSyncRunLogs",
        "tags": ["SyncRuns"],
        "responses": { "200": { "description": "Paginated sync run logs" } }
      }
    },
    "/streams": {
      "get": {
        "summary": "List streams for a connection",
        "operationId": "listStreams",
        "tags": ["Streams"],
        "parameters": [
          { "name": "connection_id", "in": "query", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "List of streams" } }
      }
    },
    "/streams/discover": {
      "post": {
        "summary": "Discover streams from a connection",
        "operationId": "discoverStreams",
        "tags": ["Streams"],
        "responses": { "200": { "description": "Discovered catalog" } }
      }
    },
    "/streams/{streamName}/schema": {
      "get": {
        "summary": "Get stream schema",
        "operationId": "getStreamSchema",
        "tags": ["Streams"],
        "responses": { "200": { "description": "Stream JSON schema" } }
      }
    },
    "/tenants": {
      "get": {
        "summary": "List tenants",
        "operationId": "listTenants",
        "tags": ["Tenants"],
        "responses": { "200": { "description": "Paginated tenants" } }
      },
      "post": {
        "summary": "Create tenant",
        "operationId": "createTenant",
        "tags": ["Tenants"],
        "responses": { "201": { "description": "Tenant created" } }
      }
    },
    "/tenants/{tenantID}": {
      "get": {
        "summary": "Get tenant",
        "operationId": "getTenant",
        "tags": ["Tenants"],
        "responses": { "200": { "description": "Tenant details" } }
      },
      "put": {
        "summary": "Update tenant",
        "operationId": "updateTenant",
        "tags": ["Tenants"],
        "responses": { "200": { "description": "Tenant updated" } }
      }
    },
    "/schemas/versions": {
      "get": {
        "summary": "Get schema versions for a stream",
        "operationId": "getSchemaVersions",
        "tags": ["Schemas"],
        "parameters": [
          { "name": "stream", "in": "query", "required": true, "schema": { "type": "string" } }
        ],
        "responses": { "200": { "description": "Schema versions" } }
      }
    },
    "/schemas/compare": {
      "get": {
        "summary": "Compare two schema versions",
        "operationId": "compareSchemas",
        "tags": ["Schemas"],
        "parameters": [
          { "name": "stream", "in": "query", "required": true, "schema": { "type": "string" } },
          { "name": "v1", "in": "query", "required": true, "schema": { "type": "integer" } },
          { "name": "v2", "in": "query", "required": true, "schema": { "type": "integer" } }
        ],
        "responses": { "200": { "description": "Schema comparison result" } }
      }
    },
    "/field-mappings": {
      "post": {
        "summary": "Create field mapping",
        "operationId": "createMapping",
        "tags": ["FieldMappings"],
        "responses": { "201": { "description": "Mapping created" } }
      }
    },
    "/field-mappings/auto": {
      "post": {
        "summary": "Auto-map fields",
        "operationId": "autoMap",
        "tags": ["FieldMappings"],
        "responses": { "200": { "description": "Auto-generated mappings" } }
      }
    },
    "/field-mappings/{mappingID}": {
      "get": {
        "summary": "Get field mapping",
        "operationId": "getMapping",
        "tags": ["FieldMappings"],
        "responses": { "200": { "description": "Mapping details" } }
      },
      "put": {
        "summary": "Update field mapping",
        "operationId": "updateMapping",
        "tags": ["FieldMappings"],
        "responses": { "200": { "description": "Mapping updated" } }
      }
    },
    "/mcp": {
      "get": {
        "summary": "List MCP servers",
        "operationId": "listMCPServers",
        "tags": ["MCP"],
        "responses": { "200": { "description": "Paginated MCP servers" } }
      },
      "post": {
        "summary": "Register MCP server",
        "operationId": "registerMCPServer",
        "tags": ["MCP"],
        "responses": { "201": { "description": "MCP server registered" } }
      }
    },
    "/mcp/{mcpServerID}": {
      "get": {
        "summary": "Get MCP server",
        "operationId": "getMCPServer",
        "tags": ["MCP"],
        "responses": { "200": { "description": "MCP server details" } }
      },
      "put": {
        "summary": "Update MCP server",
        "operationId": "updateMCPServer",
        "tags": ["MCP"],
        "responses": { "200": { "description": "MCP server updated" } }
      },
      "delete": {
        "summary": "Delete MCP server",
        "operationId": "deleteMCPServer",
        "tags": ["MCP"],
        "responses": { "204": { "description": "MCP server deleted" } }
      }
    }
  },
  "components": {
    "securitySchemes": {
      "bearerAuth": {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "API Key or JWT"
      }
    }
  },
  "security": [
    { "bearerAuth": [] }
  ]
}`
