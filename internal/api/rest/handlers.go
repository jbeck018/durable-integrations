package rest

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// ---------------------------------------------------------------------------
// Domain models for API layer
// ---------------------------------------------------------------------------

// Connection represents a configured connector instance.
type Connection struct {
	ID            string          `json:"id"`
	TenantID      string          `json:"tenant_id"`
	Name          string          `json:"name"`
	ConnectorName string          `json:"connector_name"`
	ConnectorType string          `json:"connector_type"`
	Config        json.RawMessage `json:"config"`
	Status        string          `json:"status"`
	CreatedAt     time.Time       `json:"created_at"`
	UpdatedAt     time.Time       `json:"updated_at"`
}

// Sync represents a data synchronization job.
type Sync struct {
	ID              string                    `json:"id"`
	TenantID        string                    `json:"tenant_id"`
	Name            string                    `json:"name"`
	SourceID        string                    `json:"source_id"`
	DestinationID   string                    `json:"destination_id"`
	Schedule        string                    `json:"schedule,omitempty"`
	Status          string                    `json:"status"`
	Catalog         *protocol.ConfiguredCatalog `json:"catalog,omitempty"`
	FieldMappings   []FieldMapping            `json:"field_mappings,omitempty"`
	CreatedAt       time.Time                 `json:"created_at"`
	UpdatedAt       time.Time                 `json:"updated_at"`
}

// SyncRun represents a single execution of a sync job.
type SyncRun struct {
	ID             string    `json:"id"`
	SyncID         string    `json:"sync_id"`
	TenantID       string    `json:"tenant_id"`
	Status         string    `json:"status"`
	RecordsRead    int64     `json:"records_read"`
	RecordsWritten int64     `json:"records_written"`
	BytesRead      int64     `json:"bytes_read"`
	BytesWritten   int64     `json:"bytes_written"`
	ErrorMessage   string    `json:"error_message,omitempty"`
	StartedAt      time.Time `json:"started_at"`
	FinishedAt     time.Time `json:"finished_at,omitempty"`
}

// SyncRunLog is a log entry for a sync run.
type SyncRunLog struct {
	ID        string    `json:"id"`
	SyncRunID string    `json:"sync_run_id"`
	Level     string    `json:"level"`
	Message   string    `json:"message"`
	Timestamp time.Time `json:"timestamp"`
}

// Tenant represents a FlowForge tenant.
type Tenant struct {
	ID        string                 `json:"id"`
	Name      string                 `json:"name"`
	Plan      string                 `json:"plan"`
	Settings  map[string]interface{} `json:"settings,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
}

// FieldMapping describes a field-level mapping between source and destination.
type FieldMapping struct {
	ID          string `json:"id"`
	SyncID      string `json:"sync_id,omitempty"`
	TenantID    string `json:"tenant_id"`
	SourceField string `json:"source_field"`
	DestField   string `json:"dest_field"`
	Transform   string `json:"transform,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// SchemaVersion tracks schema changes over time.
type SchemaVersion struct {
	ID        string          `json:"id"`
	Stream    string          `json:"stream"`
	Version   int             `json:"version"`
	Schema    json.RawMessage `json:"schema"`
	CreatedAt time.Time       `json:"created_at"`
}

// SchemaComparison holds a diff between two schema versions.
type SchemaComparison struct {
	Stream   string                `json:"stream"`
	Version1 int                   `json:"version1"`
	Version2 int                   `json:"version2"`
	Changes  []SchemaFieldChange   `json:"changes"`
}

// SchemaFieldChange describes a single schema field change.
type SchemaFieldChange struct {
	Field     string `json:"field"`
	Change    string `json:"change"`
	OldType   string `json:"old_type,omitempty"`
	NewType   string `json:"new_type,omitempty"`
}

// MCPServer represents a registered MCP server.
type MCPServer struct {
	ID          string            `json:"id"`
	TenantID    string            `json:"tenant_id"`
	Name        string            `json:"name"`
	URL         string            `json:"url"`
	Transport   string            `json:"transport"`
	Tools       []string          `json:"tools,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	Status      string            `json:"status"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// ---------------------------------------------------------------------------
// Request payload types
// ---------------------------------------------------------------------------

// CreateConnectionRequest is the payload for creating a new connection.
type CreateConnectionRequest struct {
	Name          string          `json:"name"`
	ConnectorName string          `json:"connector_name"`
	Config        json.RawMessage `json:"config"`
}

// UpdateConnectionRequest is the payload for updating a connection.
type UpdateConnectionRequest struct {
	Name   string          `json:"name,omitempty"`
	Config json.RawMessage `json:"config,omitempty"`
}

// TestConnectionRequest is the payload for testing a connector connection.
type TestConnectionRequest struct {
	ConnectorName string          `json:"connector_name"`
	Config        json.RawMessage `json:"config"`
}

// CreateSyncRequest is the payload for creating a new sync.
type CreateSyncRequest struct {
	Name          string                     `json:"name"`
	SourceID      string                     `json:"source_id"`
	DestinationID string                     `json:"destination_id"`
	Schedule      string                     `json:"schedule,omitempty"`
	Catalog       *protocol.ConfiguredCatalog `json:"catalog,omitempty"`
	FieldMappings []FieldMapping             `json:"field_mappings,omitempty"`
}

// UpdateSyncRequest is the payload for updating a sync.
type UpdateSyncRequest struct {
	Name          string                     `json:"name,omitempty"`
	Schedule      string                     `json:"schedule,omitempty"`
	Catalog       *protocol.ConfiguredCatalog `json:"catalog,omitempty"`
	FieldMappings []FieldMapping             `json:"field_mappings,omitempty"`
}

// CreateTenantRequest is the payload for creating a tenant.
type CreateTenantRequest struct {
	Name     string                 `json:"name"`
	Plan     string                 `json:"plan"`
	Settings map[string]interface{} `json:"settings,omitempty"`
}

// UpdateTenantRequest is the payload for updating a tenant.
type UpdateTenantRequest struct {
	Name     string                 `json:"name,omitempty"`
	Plan     string                 `json:"plan,omitempty"`
	Settings map[string]interface{} `json:"settings,omitempty"`
}

// CreateMappingRequest is the payload for creating a field mapping.
type CreateMappingRequest struct {
	SyncID      string `json:"sync_id"`
	SourceField string `json:"source_field"`
	DestField   string `json:"dest_field"`
	Transform   string `json:"transform,omitempty"`
}

// UpdateMappingRequest is the payload for updating a field mapping.
type UpdateMappingRequest struct {
	SourceField string `json:"source_field,omitempty"`
	DestField   string `json:"dest_field,omitempty"`
	Transform   string `json:"transform,omitempty"`
}

// AutoMapRequest is the payload for auto-mapping fields.
type AutoMapRequest struct {
	SyncID   string `json:"sync_id"`
	StreamName string `json:"stream_name"`
}

// DiscoverStreamsRequest is the payload for discovering streams from a connection.
type DiscoverStreamsRequest struct {
	ConnectionID string `json:"connection_id"`
}

// RegisterMCPServerRequest is the payload for registering an MCP server.
type RegisterMCPServerRequest struct {
	Name      string            `json:"name"`
	URL       string            `json:"url"`
	Transport string            `json:"transport"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// UpdateMCPServerRequest is the payload for updating an MCP server.
type UpdateMCPServerRequest struct {
	Name      string            `json:"name,omitempty"`
	URL       string            `json:"url,omitempty"`
	Transport string            `json:"transport,omitempty"`
	Metadata  map[string]string `json:"metadata,omitempty"`
}

// ---------------------------------------------------------------------------
// Repository interfaces — dependency injection for testability
// ---------------------------------------------------------------------------

// ConnectionRepository defines persistence operations for connections.
type ConnectionRepository interface {
	Create(ctx context.Context, conn *Connection) error
	Get(ctx context.Context, tenantID, id string) (*Connection, error)
	List(ctx context.Context, tenantID string, page, pageSize int) ([]*Connection, int, error)
	Update(ctx context.Context, conn *Connection) error
	Delete(ctx context.Context, tenantID, id string) error
}

// SyncRepository defines persistence operations for syncs.
type SyncRepository interface {
	Create(ctx context.Context, s *Sync) error
	Get(ctx context.Context, tenantID, id string) (*Sync, error)
	List(ctx context.Context, tenantID string, page, pageSize int) ([]*Sync, int, error)
	Update(ctx context.Context, s *Sync) error
	Delete(ctx context.Context, tenantID, id string) error
}

// SyncRunRepository defines persistence operations for sync runs.
type SyncRunRepository interface {
	Get(ctx context.Context, tenantID, id string) (*SyncRun, error)
	List(ctx context.Context, tenantID, syncID string, page, pageSize int) ([]*SyncRun, int, error)
	GetLogs(ctx context.Context, tenantID, syncRunID string, page, pageSize int) ([]*SyncRunLog, int, error)
}

// TenantRepository defines persistence operations for tenants.
type TenantRepository interface {
	Create(ctx context.Context, t *Tenant) error
	Get(ctx context.Context, id string) (*Tenant, error)
	List(ctx context.Context, page, pageSize int) ([]*Tenant, int, error)
	Update(ctx context.Context, t *Tenant) error
}

// FieldMappingRepository defines persistence operations for field mappings.
type FieldMappingRepository interface {
	Create(ctx context.Context, m *FieldMapping) error
	Get(ctx context.Context, tenantID, id string) (*FieldMapping, error)
	Update(ctx context.Context, m *FieldMapping) error
	AutoMap(ctx context.Context, tenantID, syncID, streamName string) ([]FieldMapping, error)
}

// SchemaRepository defines persistence operations for schema versions.
type SchemaRepository interface {
	GetVersions(ctx context.Context, tenantID, stream string) ([]SchemaVersion, error)
	Compare(ctx context.Context, tenantID, stream string, v1, v2 int) (*SchemaComparison, error)
}

// MCPServerRepository defines persistence operations for MCP servers.
type MCPServerRepository interface {
	Create(ctx context.Context, s *MCPServer) error
	Get(ctx context.Context, tenantID, id string) (*MCPServer, error)
	List(ctx context.Context, tenantID string, page, pageSize int) ([]*MCPServer, int, error)
	Update(ctx context.Context, s *MCPServer) error
	Delete(ctx context.Context, tenantID, id string) error
}

// StreamService provides stream discovery operations.
type StreamService interface {
	Discover(ctx context.Context, tenantID, connectionID string) (*protocol.Catalog, error)
	List(ctx context.Context, tenantID, connectionID string) ([]protocol.Stream, error)
	GetSchema(ctx context.Context, tenantID, connectionID, streamName string) (json.RawMessage, error)
}

// SyncService provides sync orchestration operations.
type SyncService interface {
	Trigger(ctx context.Context, tenantID, syncID string) (*SyncRun, error)
	Pause(ctx context.Context, tenantID, syncID string) error
	Resume(ctx context.Context, tenantID, syncID string) error
}

// ---------------------------------------------------------------------------
// Connector Handlers
// ---------------------------------------------------------------------------

// ConnectorHandlers serves connector-related API endpoints.
type ConnectorHandlers struct{}

// Routes registers all connector routes on the given chi.Router.
func (h *ConnectorHandlers) Routes(r chi.Router) {
	r.Get("/", h.ListConnectors)
	r.Get("/{connectorName}", h.GetConnector)
	r.Post("/test", h.TestConnection)
}

// ListConnectors returns all registered connectors.
func (h *ConnectorHandlers) ListConnectors(w http.ResponseWriter, r *http.Request) {
	connectors := cdk.ListConnectors()
	JSON(w, http.StatusOK, connectors)
}

// GetConnector returns metadata and spec for a single connector.
func (h *ConnectorHandlers) GetConnector(w http.ResponseWriter, r *http.Request) {
	name := chi.URLParam(r, "connectorName")
	meta, err := cdk.GetMeta(name)
	if err != nil {
		Error(w, http.StatusNotFound, "connector not found: "+name, "CONNECTOR_NOT_FOUND")
		return
	}

	// Retrieve spec if available.
	type connectorDetail struct {
		cdk.ConnectorMeta
		Spec *protocol.Spec `json:"spec,omitempty"`
	}

	detail := connectorDetail{ConnectorMeta: meta}
	switch meta.Type {
	case "source", "bidirectional":
		if src, serr := cdk.GetSource(name); serr == nil {
			if spec, specErr := src.Spec(); specErr == nil {
				detail.Spec = spec
			}
		}
	case "destination":
		if dst, derr := cdk.GetDestination(name); derr == nil {
			if spec, specErr := dst.Spec(); specErr == nil {
				detail.Spec = spec
			}
		}
	}

	JSON(w, http.StatusOK, detail)
}

// TestConnection tests connectivity for a connector with given config.
func (h *ConnectorHandlers) TestConnection(w http.ResponseWriter, r *http.Request) {
	var req TestConnectionRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.ConnectorName == "" {
		Error(w, http.StatusBadRequest, "connector_name is required", "VALIDATION_ERROR")
		return
	}

	// Try source first, then destination.
	var result *protocol.CheckResult
	if src, err := cdk.GetSource(req.ConnectorName); err == nil {
		res, checkErr := src.Check(r.Context(), req.Config)
		if checkErr != nil {
			Error(w, http.StatusBadGateway, "connection check failed: "+checkErr.Error(), "CHECK_FAILED")
			return
		}
		result = res
	} else if dst, err := cdk.GetDestination(req.ConnectorName); err == nil {
		res, checkErr := dst.Check(r.Context(), req.Config)
		if checkErr != nil {
			Error(w, http.StatusBadGateway, "connection check failed: "+checkErr.Error(), "CHECK_FAILED")
			return
		}
		result = res
	} else {
		Error(w, http.StatusNotFound, "connector not found: "+req.ConnectorName, "CONNECTOR_NOT_FOUND")
		return
	}

	JSON(w, http.StatusOK, result)
}

// ---------------------------------------------------------------------------
// Connection Handlers
// ---------------------------------------------------------------------------

// ConnectionHandlers serves connection CRUD endpoints.
type ConnectionHandlers struct {
	Repo ConnectionRepository
}

// Routes registers all connection routes on the given chi.Router.
func (h *ConnectionHandlers) Routes(r chi.Router) {
	r.Post("/", h.CreateConnection)
	r.Get("/", h.ListConnections)
	r.Get("/{connectionID}", h.GetConnection)
	r.Put("/{connectionID}", h.UpdateConnection)
	r.Delete("/{connectionID}", h.DeleteConnection)
}

// CreateConnection creates a new connection.
func (h *ConnectionHandlers) CreateConnection(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())

	var req CreateConnectionRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.Name == "" || req.ConnectorName == "" {
		Error(w, http.StatusBadRequest, "name and connector_name are required", "VALIDATION_ERROR")
		return
	}

	meta, err := cdk.GetMeta(req.ConnectorName)
	if err != nil {
		Error(w, http.StatusBadRequest, "unknown connector: "+req.ConnectorName, "CONNECTOR_NOT_FOUND")
		return
	}

	now := time.Now().UTC()
	conn := &Connection{
		ID:            generateID(),
		TenantID:      tenantID,
		Name:          req.Name,
		ConnectorName: req.ConnectorName,
		ConnectorType: meta.Type,
		Config:        req.Config,
		Status:        "active",
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := h.Repo.Create(r.Context(), conn); err != nil {
		handleRepoError(w, err)
		return
	}

	Created(w, conn, "/api/v1/connections/"+conn.ID)
}

// GetConnection returns a single connection by ID.
func (h *ConnectionHandlers) GetConnection(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	connID := chi.URLParam(r, "connectionID")

	conn, err := h.Repo.Get(r.Context(), tenantID, connID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, conn)
}

// ListConnections returns paginated connections for the tenant.
func (h *ConnectionHandlers) ListConnections(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	page, pageSize := PaginationFromRequest(r)

	conns, total, err := h.Repo.List(r.Context(), tenantID, page, pageSize)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	Paginated(w, conns, total, page, pageSize)
}

// UpdateConnection updates an existing connection.
func (h *ConnectionHandlers) UpdateConnection(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	connID := chi.URLParam(r, "connectionID")

	existing, err := h.Repo.Get(r.Context(), tenantID, connID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	var req UpdateConnectionRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.Name != "" {
		existing.Name = req.Name
	}
	if req.Config != nil {
		existing.Config = req.Config
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := h.Repo.Update(r.Context(), existing); err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, existing)
}

// DeleteConnection removes a connection.
func (h *ConnectionHandlers) DeleteConnection(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	connID := chi.URLParam(r, "connectionID")

	if err := h.Repo.Delete(r.Context(), tenantID, connID); err != nil {
		handleRepoError(w, err)
		return
	}

	NoContent(w)
}

// ---------------------------------------------------------------------------
// Sync Handlers
// ---------------------------------------------------------------------------

// SyncHandlers serves sync CRUD and orchestration endpoints.
type SyncHandlers struct {
	Repo    SyncRepository
	Service SyncService
}

// Routes registers all sync routes on the given chi.Router.
func (h *SyncHandlers) Routes(r chi.Router) {
	r.Post("/", h.CreateSync)
	r.Get("/", h.ListSyncs)
	r.Get("/{syncID}", h.GetSync)
	r.Put("/{syncID}", h.UpdateSync)
	r.Delete("/{syncID}", h.DeleteSync)
	r.Post("/{syncID}/trigger", h.TriggerSync)
	r.Post("/{syncID}/pause", h.PauseSync)
	r.Post("/{syncID}/resume", h.ResumeSync)
}

// CreateSync creates a new sync job.
func (h *SyncHandlers) CreateSync(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())

	var req CreateSyncRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.Name == "" || req.SourceID == "" || req.DestinationID == "" {
		Error(w, http.StatusBadRequest, "name, source_id, and destination_id are required", "VALIDATION_ERROR")
		return
	}

	now := time.Now().UTC()
	s := &Sync{
		ID:            generateID(),
		TenantID:      tenantID,
		Name:          req.Name,
		SourceID:      req.SourceID,
		DestinationID: req.DestinationID,
		Schedule:      req.Schedule,
		Status:        "active",
		Catalog:       req.Catalog,
		FieldMappings: req.FieldMappings,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := h.Repo.Create(r.Context(), s); err != nil {
		handleRepoError(w, err)
		return
	}

	Created(w, s, "/api/v1/syncs/"+s.ID)
}

// GetSync returns a single sync by ID.
func (h *SyncHandlers) GetSync(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	syncID := chi.URLParam(r, "syncID")

	s, err := h.Repo.Get(r.Context(), tenantID, syncID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, s)
}

// ListSyncs returns paginated syncs for the tenant.
func (h *SyncHandlers) ListSyncs(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	page, pageSize := PaginationFromRequest(r)

	syncs, total, err := h.Repo.List(r.Context(), tenantID, page, pageSize)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	Paginated(w, syncs, total, page, pageSize)
}

// UpdateSync updates an existing sync.
func (h *SyncHandlers) UpdateSync(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	syncID := chi.URLParam(r, "syncID")

	existing, err := h.Repo.Get(r.Context(), tenantID, syncID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	var req UpdateSyncRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.Name != "" {
		existing.Name = req.Name
	}
	if req.Schedule != "" {
		existing.Schedule = req.Schedule
	}
	if req.Catalog != nil {
		existing.Catalog = req.Catalog
	}
	if req.FieldMappings != nil {
		existing.FieldMappings = req.FieldMappings
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := h.Repo.Update(r.Context(), existing); err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, existing)
}

// DeleteSync removes a sync.
func (h *SyncHandlers) DeleteSync(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	syncID := chi.URLParam(r, "syncID")

	if err := h.Repo.Delete(r.Context(), tenantID, syncID); err != nil {
		handleRepoError(w, err)
		return
	}

	NoContent(w)
}

// TriggerSync manually triggers a sync execution.
func (h *SyncHandlers) TriggerSync(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	syncID := chi.URLParam(r, "syncID")

	run, err := h.Service.Trigger(r.Context(), tenantID, syncID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	Created(w, run, "/api/v1/sync-runs/"+run.ID)
}

// PauseSync pauses an active sync.
func (h *SyncHandlers) PauseSync(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	syncID := chi.URLParam(r, "syncID")

	if err := h.Service.Pause(r.Context(), tenantID, syncID); err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, map[string]string{"status": "paused", "sync_id": syncID})
}

// ResumeSync resumes a paused sync.
func (h *SyncHandlers) ResumeSync(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	syncID := chi.URLParam(r, "syncID")

	if err := h.Service.Resume(r.Context(), tenantID, syncID); err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, map[string]string{"status": "active", "sync_id": syncID})
}

// ---------------------------------------------------------------------------
// SyncRun Handlers
// ---------------------------------------------------------------------------

// SyncRunHandlers serves sync run read-only endpoints.
type SyncRunHandlers struct {
	Repo SyncRunRepository
}

// Routes registers all sync run routes on the given chi.Router.
func (h *SyncRunHandlers) Routes(r chi.Router) {
	r.Get("/", h.ListSyncRuns)
	r.Get("/{syncRunID}", h.GetSyncRun)
	r.Get("/{syncRunID}/logs", h.GetSyncRunLogs)
}

// GetSyncRun returns a single sync run by ID.
func (h *SyncRunHandlers) GetSyncRun(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	runID := chi.URLParam(r, "syncRunID")

	run, err := h.Repo.Get(r.Context(), tenantID, runID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, run)
}

// ListSyncRuns returns paginated sync runs, optionally filtered by sync_id.
func (h *SyncRunHandlers) ListSyncRuns(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	syncID := r.URL.Query().Get("sync_id")
	page, pageSize := PaginationFromRequest(r)

	runs, total, err := h.Repo.List(r.Context(), tenantID, syncID, page, pageSize)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	Paginated(w, runs, total, page, pageSize)
}

// GetSyncRunLogs returns paginated logs for a sync run.
func (h *SyncRunHandlers) GetSyncRunLogs(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	runID := chi.URLParam(r, "syncRunID")
	page, pageSize := PaginationFromRequest(r)

	logs, total, err := h.Repo.GetLogs(r.Context(), tenantID, runID, page, pageSize)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	Paginated(w, logs, total, page, pageSize)
}

// ---------------------------------------------------------------------------
// Stream Handlers
// ---------------------------------------------------------------------------

// StreamHandlers serves stream discovery endpoints.
type StreamHandlers struct {
	Service StreamService
}

// Routes registers all stream routes on the given chi.Router.
func (h *StreamHandlers) Routes(r chi.Router) {
	r.Post("/discover", h.DiscoverStreams)
	r.Get("/", h.ListStreams)
	r.Get("/{streamName}/schema", h.GetStreamSchema)
}

// DiscoverStreams discovers available streams from a connection.
func (h *StreamHandlers) DiscoverStreams(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())

	var req DiscoverStreamsRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.ConnectionID == "" {
		Error(w, http.StatusBadRequest, "connection_id is required", "VALIDATION_ERROR")
		return
	}

	catalog, err := h.Service.Discover(r.Context(), tenantID, req.ConnectionID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, catalog)
}

// ListStreams returns streams for a connection.
func (h *StreamHandlers) ListStreams(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	connectionID := r.URL.Query().Get("connection_id")
	if connectionID == "" {
		Error(w, http.StatusBadRequest, "connection_id query parameter is required", "VALIDATION_ERROR")
		return
	}

	streams, err := h.Service.List(r.Context(), tenantID, connectionID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, streams)
}

// GetStreamSchema returns the JSON schema for a specific stream.
func (h *StreamHandlers) GetStreamSchema(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	streamName := chi.URLParam(r, "streamName")
	connectionID := r.URL.Query().Get("connection_id")
	if connectionID == "" {
		Error(w, http.StatusBadRequest, "connection_id query parameter is required", "VALIDATION_ERROR")
		return
	}

	schema, err := h.Service.GetSchema(r.Context(), tenantID, connectionID, streamName)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, json.RawMessage(schema))
}

// ---------------------------------------------------------------------------
// Tenant Handlers
// ---------------------------------------------------------------------------

// TenantHandlers serves tenant CRUD endpoints.
type TenantHandlers struct {
	Repo TenantRepository
}

// Routes registers all tenant routes on the given chi.Router.
func (h *TenantHandlers) Routes(r chi.Router) {
	r.Post("/", h.CreateTenant)
	r.Get("/", h.ListTenants)
	r.Get("/{tenantID}", h.GetTenant)
	r.Put("/{tenantID}", h.UpdateTenant)
}

// CreateTenant creates a new tenant.
func (h *TenantHandlers) CreateTenant(w http.ResponseWriter, r *http.Request) {
	var req CreateTenantRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.Name == "" {
		Error(w, http.StatusBadRequest, "name is required", "VALIDATION_ERROR")
		return
	}

	now := time.Now().UTC()
	t := &Tenant{
		ID:        generateID(),
		Name:      req.Name,
		Plan:      req.Plan,
		Settings:  req.Settings,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if t.Plan == "" {
		t.Plan = "free"
	}

	if err := h.Repo.Create(r.Context(), t); err != nil {
		handleRepoError(w, err)
		return
	}

	Created(w, t, "/api/v1/tenants/"+t.ID)
}

// GetTenant returns a single tenant by ID.
func (h *TenantHandlers) GetTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	t, err := h.Repo.Get(r.Context(), tenantID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, t)
}

// UpdateTenant updates an existing tenant.
func (h *TenantHandlers) UpdateTenant(w http.ResponseWriter, r *http.Request) {
	tenantID := chi.URLParam(r, "tenantID")

	existing, err := h.Repo.Get(r.Context(), tenantID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	var req UpdateTenantRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.Name != "" {
		existing.Name = req.Name
	}
	if req.Plan != "" {
		existing.Plan = req.Plan
	}
	if req.Settings != nil {
		existing.Settings = req.Settings
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := h.Repo.Update(r.Context(), existing); err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, existing)
}

// ListTenants returns paginated tenants.
func (h *TenantHandlers) ListTenants(w http.ResponseWriter, r *http.Request) {
	page, pageSize := PaginationFromRequest(r)

	tenants, total, err := h.Repo.List(r.Context(), page, pageSize)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	Paginated(w, tenants, total, page, pageSize)
}

// ---------------------------------------------------------------------------
// Field Mapping Handlers
// ---------------------------------------------------------------------------

// FieldMappingHandlers serves field mapping endpoints.
type FieldMappingHandlers struct {
	Repo FieldMappingRepository
}

// Routes registers all field mapping routes on the given chi.Router.
func (h *FieldMappingHandlers) Routes(r chi.Router) {
	r.Post("/", h.CreateMapping)
	r.Get("/{mappingID}", h.GetMapping)
	r.Put("/{mappingID}", h.UpdateMapping)
	r.Post("/auto", h.AutoMap)
}

// CreateMapping creates a new field mapping.
func (h *FieldMappingHandlers) CreateMapping(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())

	var req CreateMappingRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.SourceField == "" || req.DestField == "" || req.SyncID == "" {
		Error(w, http.StatusBadRequest, "sync_id, source_field, and dest_field are required", "VALIDATION_ERROR")
		return
	}

	now := time.Now().UTC()
	m := &FieldMapping{
		ID:          generateID(),
		SyncID:      req.SyncID,
		TenantID:    tenantID,
		SourceField: req.SourceField,
		DestField:   req.DestField,
		Transform:   req.Transform,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if err := h.Repo.Create(r.Context(), m); err != nil {
		handleRepoError(w, err)
		return
	}

	Created(w, m, "/api/v1/field-mappings/"+m.ID)
}

// GetMapping returns a single field mapping by ID.
func (h *FieldMappingHandlers) GetMapping(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	mappingID := chi.URLParam(r, "mappingID")

	m, err := h.Repo.Get(r.Context(), tenantID, mappingID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, m)
}

// UpdateMapping updates an existing field mapping.
func (h *FieldMappingHandlers) UpdateMapping(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	mappingID := chi.URLParam(r, "mappingID")

	existing, err := h.Repo.Get(r.Context(), tenantID, mappingID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	var req UpdateMappingRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.SourceField != "" {
		existing.SourceField = req.SourceField
	}
	if req.DestField != "" {
		existing.DestField = req.DestField
	}
	if req.Transform != "" {
		existing.Transform = req.Transform
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := h.Repo.Update(r.Context(), existing); err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, existing)
}

// AutoMap automatically generates field mappings based on name matching.
func (h *FieldMappingHandlers) AutoMap(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())

	var req AutoMapRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.SyncID == "" || req.StreamName == "" {
		Error(w, http.StatusBadRequest, "sync_id and stream_name are required", "VALIDATION_ERROR")
		return
	}

	mappings, err := h.Repo.AutoMap(r.Context(), tenantID, req.SyncID, req.StreamName)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, mappings)
}

// ---------------------------------------------------------------------------
// Schema Handlers
// ---------------------------------------------------------------------------

// SchemaHandlers serves schema version endpoints.
type SchemaHandlers struct {
	Repo SchemaRepository
}

// Routes registers all schema routes on the given chi.Router.
func (h *SchemaHandlers) Routes(r chi.Router) {
	r.Get("/versions", h.GetSchemaVersions)
	r.Get("/compare", h.CompareSchemas)
}

// GetSchemaVersions returns all schema versions for a stream.
func (h *SchemaHandlers) GetSchemaVersions(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	stream := r.URL.Query().Get("stream")
	if stream == "" {
		Error(w, http.StatusBadRequest, "stream query parameter is required", "VALIDATION_ERROR")
		return
	}

	versions, err := h.Repo.GetVersions(r.Context(), tenantID, stream)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, versions)
}

// CompareSchemas compares two schema versions.
func (h *SchemaHandlers) CompareSchemas(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	stream := r.URL.Query().Get("stream")
	v1 := queryInt(r, "v1", 0)
	v2 := queryInt(r, "v2", 0)

	if stream == "" || v1 == 0 || v2 == 0 {
		Error(w, http.StatusBadRequest, "stream, v1, and v2 query parameters are required", "VALIDATION_ERROR")
		return
	}

	comparison, err := h.Repo.Compare(r.Context(), tenantID, stream, v1, v2)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, comparison)
}

// ---------------------------------------------------------------------------
// MCP Handlers
// ---------------------------------------------------------------------------

// MCPHandlers serves MCP server management endpoints.
type MCPHandlers struct {
	Repo MCPServerRepository
}

// Routes registers all MCP routes on the given chi.Router.
func (h *MCPHandlers) Routes(r chi.Router) {
	r.Get("/", h.ListMCPServers)
	r.Post("/", h.RegisterMCPServer)
	r.Get("/{mcpServerID}", h.GetMCPServer)
	r.Put("/{mcpServerID}", h.UpdateMCPServer)
	r.Delete("/{mcpServerID}", h.DeleteMCPServer)
}

// ListMCPServers returns paginated MCP servers for the tenant.
func (h *MCPHandlers) ListMCPServers(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	page, pageSize := PaginationFromRequest(r)

	servers, total, err := h.Repo.List(r.Context(), tenantID, page, pageSize)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	Paginated(w, servers, total, page, pageSize)
}

// GetMCPServer returns a single MCP server by ID.
func (h *MCPHandlers) GetMCPServer(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	serverID := chi.URLParam(r, "mcpServerID")

	server, err := h.Repo.Get(r.Context(), tenantID, serverID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, server)
}

// RegisterMCPServer registers a new MCP server.
func (h *MCPHandlers) RegisterMCPServer(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())

	var req RegisterMCPServerRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.Name == "" || req.URL == "" || req.Transport == "" {
		Error(w, http.StatusBadRequest, "name, url, and transport are required", "VALIDATION_ERROR")
		return
	}

	now := time.Now().UTC()
	server := &MCPServer{
		ID:        generateID(),
		TenantID:  tenantID,
		Name:      req.Name,
		URL:       req.URL,
		Transport: req.Transport,
		Metadata:  req.Metadata,
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := h.Repo.Create(r.Context(), server); err != nil {
		handleRepoError(w, err)
		return
	}

	Created(w, server, "/api/v1/mcp/"+server.ID)
}

// UpdateMCPServer updates an existing MCP server.
func (h *MCPHandlers) UpdateMCPServer(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	serverID := chi.URLParam(r, "mcpServerID")

	existing, err := h.Repo.Get(r.Context(), tenantID, serverID)
	if err != nil {
		handleRepoError(w, err)
		return
	}

	var req UpdateMCPServerRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_REQUEST")
		return
	}

	if req.Name != "" {
		existing.Name = req.Name
	}
	if req.URL != "" {
		existing.URL = req.URL
	}
	if req.Transport != "" {
		existing.Transport = req.Transport
	}
	if req.Metadata != nil {
		existing.Metadata = req.Metadata
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := h.Repo.Update(r.Context(), existing); err != nil {
		handleRepoError(w, err)
		return
	}

	JSON(w, http.StatusOK, existing)
}

// DeleteMCPServer removes an MCP server.
func (h *MCPHandlers) DeleteMCPServer(w http.ResponseWriter, r *http.Request) {
	tenantID := common.TenantIDFrom(r.Context())
	serverID := chi.URLParam(r, "mcpServerID")

	if err := h.Repo.Delete(r.Context(), tenantID, serverID); err != nil {
		handleRepoError(w, err)
		return
	}

	NoContent(w)
}

// ---------------------------------------------------------------------------
// Health Handler
// ---------------------------------------------------------------------------

// HealthHandler serves the health check endpoint.
type HealthHandler struct{}

// Health returns the service health status.
func (h *HealthHandler) Health(w http.ResponseWriter, r *http.Request) {
	JSON(w, http.StatusOK, map[string]interface{}{
		"status":  "healthy",
		"service": "flowforge-api",
		"time":    time.Now().UTC().Format(time.RFC3339),
	})
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// generateID generates a unique identifier using google/uuid.
func generateID() string {
	return generateUUID()
}

// handleRepoError maps repository errors to appropriate HTTP responses.
func handleRepoError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, common.ErrNotFound):
		Error(w, http.StatusNotFound, err.Error(), "NOT_FOUND")
	case errors.Is(err, common.ErrAlreadyExists):
		Error(w, http.StatusConflict, err.Error(), "ALREADY_EXISTS")
	case errors.Is(err, common.ErrUnauthorized):
		Error(w, http.StatusUnauthorized, err.Error(), "UNAUTHORIZED")
	case errors.Is(err, common.ErrForbidden):
		Error(w, http.StatusForbidden, err.Error(), "FORBIDDEN")
	case errors.Is(err, common.ErrRateLimited):
		Error(w, http.StatusTooManyRequests, err.Error(), "RATE_LIMITED")
	case errors.Is(err, common.ErrInvalidConfig):
		Error(w, http.StatusBadRequest, err.Error(), "INVALID_CONFIG")
	case errors.Is(err, common.ErrSchemaValidation):
		Error(w, http.StatusUnprocessableEntity, err.Error(), "SCHEMA_VALIDATION")
	case errors.Is(err, common.ErrTimeout):
		Error(w, http.StatusGatewayTimeout, err.Error(), "TIMEOUT")
	case errors.Is(err, common.ErrCancelled):
		Error(w, http.StatusServiceUnavailable, "operation cancelled", "CANCELLED")
	default:
		var ve common.ValidationErrors
		if errors.As(err, &ve) {
			ErrorWithDetails(w, http.StatusUnprocessableEntity, "validation failed", "VALIDATION_ERROR", ve)
			return
		}
		Error(w, http.StatusInternalServerError, "internal server error", "INTERNAL_ERROR")
	}
}
