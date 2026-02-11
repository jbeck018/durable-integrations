package graphql

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/api/rest"
	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// ---------------------------------------------------------------------------
// GraphQL-specific response types
// ---------------------------------------------------------------------------

// PageInfo provides pagination metadata for GraphQL connections.
type PageInfo struct {
	Total           int  `json:"total"`
	Page            int  `json:"page"`
	PageSize        int  `json:"pageSize"`
	Pages           int  `json:"pages"`
	HasNextPage     bool `json:"hasNextPage"`
	HasPreviousPage bool `json:"hasPreviousPage"`
}

// newPageInfo creates a PageInfo from pagination params.
func newPageInfo(total, page, pageSize int) PageInfo {
	pages := 0
	if pageSize > 0 {
		pages = (total + pageSize - 1) / pageSize
	}
	return PageInfo{
		Total:           total,
		Page:            page,
		PageSize:        pageSize,
		Pages:           pages,
		HasNextPage:     page < pages,
		HasPreviousPage: page > 1,
	}
}

// ConnectionConnection wraps a paginated list of connections.
type ConnectionConnection struct {
	Nodes    []*rest.Connection `json:"nodes"`
	PageInfo PageInfo           `json:"pageInfo"`
}

// SyncConnection wraps a paginated list of syncs.
type SyncConnection struct {
	Nodes    []*rest.Sync `json:"nodes"`
	PageInfo PageInfo     `json:"pageInfo"`
}

// SyncRunConnection wraps a paginated list of sync runs.
type SyncRunConnection struct {
	Nodes    []*rest.SyncRun `json:"nodes"`
	PageInfo PageInfo        `json:"pageInfo"`
}

// SyncRunLogConnection wraps a paginated list of sync run logs.
type SyncRunLogConnection struct {
	Nodes    []*rest.SyncRunLog `json:"nodes"`
	PageInfo PageInfo           `json:"pageInfo"`
}

// MCPServerConnection wraps a paginated list of MCP servers.
type MCPServerConnection struct {
	Nodes    []*rest.MCPServer `json:"nodes"`
	PageInfo PageInfo          `json:"pageInfo"`
}

// ConnectorNode is a GraphQL-friendly connector representation.
type ConnectorNode struct {
	Name        string         `json:"name"`
	DisplayName string         `json:"displayName"`
	Version     string         `json:"version"`
	Type        string         `json:"type"`
	Category    string         `json:"category"`
	Icon        string         `json:"icon,omitempty"`
	Spec        *protocol.Spec `json:"spec,omitempty"`
}

// CheckResultNode is a GraphQL-friendly check result.
type CheckResultNode struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

// CatalogNode is a GraphQL-friendly catalog representation.
type CatalogNode struct {
	Streams []protocol.Stream `json:"streams"`
}

// SyncStatusEvent represents a real-time sync status change.
type SyncStatusEvent struct {
	SyncID         string    `json:"syncId"`
	PreviousStatus string    `json:"previousStatus"`
	NewStatus      string    `json:"newStatus"`
	Timestamp      time.Time `json:"timestamp"`
}

// SyncRunProgressEvent represents a real-time sync run progress update.
type SyncRunProgressEvent struct {
	SyncRunID      string    `json:"syncRunId"`
	Status         string    `json:"status"`
	RecordsRead    int64     `json:"recordsRead"`
	RecordsWritten int64     `json:"recordsWritten"`
	BytesRead      int64     `json:"bytesRead"`
	BytesWritten   int64     `json:"bytesWritten"`
	Timestamp      time.Time `json:"timestamp"`
}

// ---------------------------------------------------------------------------
// DataLoader for N+1 prevention
// ---------------------------------------------------------------------------

// dataLoaderKey is a context key for DataLoaders.
type dataLoaderKey struct{}

// DataLoaders aggregates per-request data loaders to prevent N+1 queries.
type DataLoaders struct {
	connectionLoader *batchLoader[string, *rest.Connection]
	syncLoader       *batchLoader[string, *rest.Sync]
	connectorLoader  *batchLoader[string, *ConnectorNode]
}

// batchLoader implements a generic DataLoader with batching and caching.
type batchLoader[K comparable, V any] struct {
	mu      sync.Mutex
	cache   map[K]V
	batchFn func(ctx context.Context, keys []K) (map[K]V, error)
	pending []K
}

// newBatchLoader creates a new batch loader with the given batch function.
func newBatchLoader[K comparable, V any](fn func(ctx context.Context, keys []K) (map[K]V, error)) *batchLoader[K, V] {
	return &batchLoader[K, V]{
		cache:   make(map[K]V),
		batchFn: fn,
	}
}

// Load retrieves a value by key, batching and caching as needed.
func (bl *batchLoader[K, V]) Load(ctx context.Context, key K) (V, error) {
	bl.mu.Lock()
	if v, ok := bl.cache[key]; ok {
		bl.mu.Unlock()
		return v, nil
	}
	bl.pending = append(bl.pending, key)
	pendingKeys := make([]K, len(bl.pending))
	copy(pendingKeys, bl.pending)
	bl.pending = bl.pending[:0]
	bl.mu.Unlock()

	results, err := bl.batchFn(ctx, pendingKeys)
	if err != nil {
		var zero V
		return zero, err
	}

	bl.mu.Lock()
	for k, v := range results {
		bl.cache[k] = v
	}
	v := bl.cache[key]
	bl.mu.Unlock()

	return v, nil
}

// WithDataLoaders attaches DataLoaders to the context for the given request.
func WithDataLoaders(ctx context.Context, r *Resolver) context.Context {
	loaders := &DataLoaders{
		connectionLoader: newBatchLoader(func(ctx context.Context, keys []string) (map[string]*rest.Connection, error) {
			tenantID := common.TenantIDFrom(ctx)
			result := make(map[string]*rest.Connection, len(keys))
			for _, key := range keys {
				conn, err := r.ConnectionRepo.Get(ctx, tenantID, key)
				if err == nil {
					result[key] = conn
				}
			}
			return result, nil
		}),
		syncLoader: newBatchLoader(func(ctx context.Context, keys []string) (map[string]*rest.Sync, error) {
			tenantID := common.TenantIDFrom(ctx)
			result := make(map[string]*rest.Sync, len(keys))
			for _, key := range keys {
				s, err := r.SyncRepo.Get(ctx, tenantID, key)
				if err == nil {
					result[key] = s
				}
			}
			return result, nil
		}),
		connectorLoader: newBatchLoader(func(ctx context.Context, keys []string) (map[string]*ConnectorNode, error) {
			result := make(map[string]*ConnectorNode, len(keys))
			for _, key := range keys {
				meta, err := cdk.GetMeta(key)
				if err == nil {
					node := &ConnectorNode{
						Name:        meta.Name,
						DisplayName: meta.DisplayName,
						Version:     meta.Version,
						Type:        meta.Type,
						Category:    meta.Category,
						Icon:        meta.Icon,
					}
					result[key] = node
				}
			}
			return result, nil
		}),
	}
	return context.WithValue(ctx, dataLoaderKey{}, loaders)
}

// loadersFrom extracts DataLoaders from context.
func loadersFrom(ctx context.Context) *DataLoaders {
	v, _ := ctx.Value(dataLoaderKey{}).(*DataLoaders)
	return v
}

// ---------------------------------------------------------------------------
// Resolver
// ---------------------------------------------------------------------------

// Resolver implements all GraphQL query, mutation, and subscription resolvers.
// Dependencies are injected as interfaces for testability.
type Resolver struct {
	ConnectionRepo   rest.ConnectionRepository
	SyncRepo         rest.SyncRepository
	SyncRunRepo      rest.SyncRunRepository
	TenantRepo       rest.TenantRepository
	FieldMappingRepo rest.FieldMappingRepository
	SchemaRepo       rest.SchemaRepository
	MCPServerRepo    rest.MCPServerRepository
	StreamService    rest.StreamService
	SyncService      rest.SyncService

	// Subscriptions hub.
	subMu           sync.RWMutex
	syncStatusSubs  map[string][]chan SyncStatusEvent
	runProgressSubs map[string][]chan SyncRunProgressEvent
}

// NewResolver creates a new Resolver with initialized subscription maps.
func NewResolver(
	connRepo rest.ConnectionRepository,
	syncRepo rest.SyncRepository,
	syncRunRepo rest.SyncRunRepository,
	tenantRepo rest.TenantRepository,
	fieldMappingRepo rest.FieldMappingRepository,
	schemaRepo rest.SchemaRepository,
	mcpRepo rest.MCPServerRepository,
	streamSvc rest.StreamService,
	syncSvc rest.SyncService,
) *Resolver {
	return &Resolver{
		ConnectionRepo:   connRepo,
		SyncRepo:         syncRepo,
		SyncRunRepo:      syncRunRepo,
		TenantRepo:       tenantRepo,
		FieldMappingRepo: fieldMappingRepo,
		SchemaRepo:       schemaRepo,
		MCPServerRepo:    mcpRepo,
		StreamService:    streamSvc,
		SyncService:      syncSvc,
		syncStatusSubs:   make(map[string][]chan SyncStatusEvent),
		runProgressSubs:  make(map[string][]chan SyncRunProgressEvent),
	}
}

// ---------------------------------------------------------------------------
// Query Resolvers
// ---------------------------------------------------------------------------

// QueryConnectors returns all registered connectors.
func (r *Resolver) QueryConnectors(ctx context.Context) ([]ConnectorNode, error) {
	metas := cdk.ListConnectors()
	nodes := make([]ConnectorNode, 0, len(metas))
	for _, m := range metas {
		node := ConnectorNode{
			Name:        m.Name,
			DisplayName: m.DisplayName,
			Version:     m.Version,
			Type:        m.Type,
			Category:    m.Category,
			Icon:        m.Icon,
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

// QueryConnector returns a single connector by name.
func (r *Resolver) QueryConnector(ctx context.Context, name string) (*ConnectorNode, error) {
	meta, err := cdk.GetMeta(name)
	if err != nil {
		return nil, err
	}
	node := &ConnectorNode{
		Name:        meta.Name,
		DisplayName: meta.DisplayName,
		Version:     meta.Version,
		Type:        meta.Type,
		Category:    meta.Category,
		Icon:        meta.Icon,
	}

	switch meta.Type {
	case "source", "bidirectional":
		if src, serr := cdk.GetSource(name); serr == nil {
			if spec, specErr := src.Spec(); specErr == nil {
				node.Spec = spec
			}
		}
	case "destination":
		if dst, derr := cdk.GetDestination(name); derr == nil {
			if spec, specErr := dst.Spec(); specErr == nil {
				node.Spec = spec
			}
		}
	}

	return node, nil
}

// QueryConnection returns a single connection by ID.
func (r *Resolver) QueryConnection(ctx context.Context, id string) (*rest.Connection, error) {
	tenantID := common.TenantIDFrom(ctx)
	if loaders := loadersFrom(ctx); loaders != nil {
		return loaders.connectionLoader.Load(ctx, id)
	}
	return r.ConnectionRepo.Get(ctx, tenantID, id)
}

// QueryConnections returns paginated connections.
func (r *Resolver) QueryConnections(ctx context.Context, page, pageSize *int) (*ConnectionConnection, error) {
	tenantID := common.TenantIDFrom(ctx)
	p, ps := defaultPagination(page, pageSize)

	conns, total, err := r.ConnectionRepo.List(ctx, tenantID, p, ps)
	if err != nil {
		return nil, err
	}

	return &ConnectionConnection{
		Nodes:    conns,
		PageInfo: newPageInfo(total, p, ps),
	}, nil
}

// QuerySync returns a single sync by ID.
func (r *Resolver) QuerySync(ctx context.Context, id string) (*rest.Sync, error) {
	tenantID := common.TenantIDFrom(ctx)
	if loaders := loadersFrom(ctx); loaders != nil {
		return loaders.syncLoader.Load(ctx, id)
	}
	return r.SyncRepo.Get(ctx, tenantID, id)
}

// QuerySyncs returns paginated syncs.
func (r *Resolver) QuerySyncs(ctx context.Context, page, pageSize *int) (*SyncConnection, error) {
	tenantID := common.TenantIDFrom(ctx)
	p, ps := defaultPagination(page, pageSize)

	syncs, total, err := r.SyncRepo.List(ctx, tenantID, p, ps)
	if err != nil {
		return nil, err
	}

	return &SyncConnection{
		Nodes:    syncs,
		PageInfo: newPageInfo(total, p, ps),
	}, nil
}

// QuerySyncRun returns a single sync run by ID.
func (r *Resolver) QuerySyncRun(ctx context.Context, id string) (*rest.SyncRun, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.SyncRunRepo.Get(ctx, tenantID, id)
}

// QuerySyncRuns returns paginated sync runs, optionally filtered by sync ID.
func (r *Resolver) QuerySyncRuns(ctx context.Context, syncID *string, page, pageSize *int) (*SyncRunConnection, error) {
	tenantID := common.TenantIDFrom(ctx)
	p, ps := defaultPagination(page, pageSize)

	sid := ""
	if syncID != nil {
		sid = *syncID
	}

	runs, total, err := r.SyncRunRepo.List(ctx, tenantID, sid, p, ps)
	if err != nil {
		return nil, err
	}

	return &SyncRunConnection{
		Nodes:    runs,
		PageInfo: newPageInfo(total, p, ps),
	}, nil
}

// QueryStreams returns streams for a connection.
func (r *Resolver) QueryStreams(ctx context.Context, connectionID string) ([]protocol.Stream, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.StreamService.List(ctx, tenantID, connectionID)
}

// QueryStreamSchema returns the schema for a specific stream.
func (r *Resolver) QueryStreamSchema(ctx context.Context, connectionID, streamName string) (json.RawMessage, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.StreamService.GetSchema(ctx, tenantID, connectionID, streamName)
}

// QueryTenant returns a tenant by ID.
func (r *Resolver) QueryTenant(ctx context.Context, id string) (*rest.Tenant, error) {
	return r.TenantRepo.Get(ctx, id)
}

// QueryTenants returns paginated tenants.
func (r *Resolver) QueryTenants(ctx context.Context, page, pageSize *int) ([]*rest.Tenant, error) {
	p, ps := defaultPagination(page, pageSize)
	tenants, _, err := r.TenantRepo.List(ctx, p, ps)
	return tenants, err
}

// QueryFieldMapping returns a field mapping by ID.
func (r *Resolver) QueryFieldMapping(ctx context.Context, id string) (*rest.FieldMapping, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.FieldMappingRepo.Get(ctx, tenantID, id)
}

// QuerySchemaVersions returns all schema versions for a stream.
func (r *Resolver) QuerySchemaVersions(ctx context.Context, stream string) ([]rest.SchemaVersion, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.SchemaRepo.GetVersions(ctx, tenantID, stream)
}

// QuerySchemaComparison compares two schema versions.
func (r *Resolver) QuerySchemaComparison(ctx context.Context, stream string, v1, v2 int) (*rest.SchemaComparison, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.SchemaRepo.Compare(ctx, tenantID, stream, v1, v2)
}

// QueryMCPServers returns paginated MCP servers.
func (r *Resolver) QueryMCPServers(ctx context.Context, page, pageSize *int) (*MCPServerConnection, error) {
	tenantID := common.TenantIDFrom(ctx)
	p, ps := defaultPagination(page, pageSize)

	servers, total, err := r.MCPServerRepo.List(ctx, tenantID, p, ps)
	if err != nil {
		return nil, err
	}

	return &MCPServerConnection{
		Nodes:    servers,
		PageInfo: newPageInfo(total, p, ps),
	}, nil
}

// QueryMCPServer returns a single MCP server by ID.
func (r *Resolver) QueryMCPServer(ctx context.Context, id string) (*rest.MCPServer, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.MCPServerRepo.Get(ctx, tenantID, id)
}

// ---------------------------------------------------------------------------
// Mutation Resolvers
// ---------------------------------------------------------------------------

// MutationCreateConnection creates a new connection.
func (r *Resolver) MutationCreateConnection(ctx context.Context, name, connectorName string, config json.RawMessage) (*rest.Connection, error) {
	tenantID := common.TenantIDFrom(ctx)

	meta, err := cdk.GetMeta(connectorName)
	if err != nil {
		return nil, fmt.Errorf("unknown connector: %s", connectorName)
	}

	now := time.Now().UTC()
	conn := &rest.Connection{
		ID:            rest.NewID(),
		TenantID:      tenantID,
		Name:          name,
		ConnectorName: connectorName,
		ConnectorType: meta.Type,
		Config:        config,
		Status:        "active",
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := r.ConnectionRepo.Create(ctx, conn); err != nil {
		return nil, err
	}

	return conn, nil
}

// MutationUpdateConnection updates an existing connection.
func (r *Resolver) MutationUpdateConnection(ctx context.Context, id string, name *string, config json.RawMessage) (*rest.Connection, error) {
	tenantID := common.TenantIDFrom(ctx)

	existing, err := r.ConnectionRepo.Get(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}

	if name != nil {
		existing.Name = *name
	}
	if config != nil {
		existing.Config = config
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := r.ConnectionRepo.Update(ctx, existing); err != nil {
		return nil, err
	}

	return existing, nil
}

// MutationDeleteConnection deletes a connection.
func (r *Resolver) MutationDeleteConnection(ctx context.Context, id string) (bool, error) {
	tenantID := common.TenantIDFrom(ctx)
	if err := r.ConnectionRepo.Delete(ctx, tenantID, id); err != nil {
		return false, err
	}
	return true, nil
}

// MutationTestConnection tests connectivity for a connector with given config.
func (r *Resolver) MutationTestConnection(ctx context.Context, connectorName string, config json.RawMessage) (*CheckResultNode, error) {
	if src, err := cdk.GetSource(connectorName); err == nil {
		result, checkErr := src.Check(ctx, config)
		if checkErr != nil {
			return nil, fmt.Errorf("connection check failed: %w", checkErr)
		}
		return &CheckResultNode{Status: string(result.Status), Message: result.Message}, nil
	}

	if dst, err := cdk.GetDestination(connectorName); err == nil {
		result, checkErr := dst.Check(ctx, config)
		if checkErr != nil {
			return nil, fmt.Errorf("connection check failed: %w", checkErr)
		}
		return &CheckResultNode{Status: string(result.Status), Message: result.Message}, nil
	}

	return nil, fmt.Errorf("connector not found: %s", connectorName)
}

// MutationCreateSync creates a new sync.
func (r *Resolver) MutationCreateSync(ctx context.Context, name, sourceID, destID string, schedule *string) (*rest.Sync, error) {
	tenantID := common.TenantIDFrom(ctx)

	now := time.Now().UTC()
	s := &rest.Sync{
		ID:            rest.NewID(),
		TenantID:      tenantID,
		Name:          name,
		SourceID:      sourceID,
		DestinationID: destID,
		Status:        "active",
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if schedule != nil {
		s.Schedule = *schedule
	}

	if err := r.SyncRepo.Create(ctx, s); err != nil {
		return nil, err
	}

	return s, nil
}

// MutationUpdateSync updates an existing sync.
func (r *Resolver) MutationUpdateSync(ctx context.Context, id string, name, schedule *string) (*rest.Sync, error) {
	tenantID := common.TenantIDFrom(ctx)

	existing, err := r.SyncRepo.Get(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}

	if name != nil {
		existing.Name = *name
	}
	if schedule != nil {
		existing.Schedule = *schedule
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := r.SyncRepo.Update(ctx, existing); err != nil {
		return nil, err
	}

	return existing, nil
}

// MutationDeleteSync deletes a sync.
func (r *Resolver) MutationDeleteSync(ctx context.Context, id string) (bool, error) {
	tenantID := common.TenantIDFrom(ctx)
	if err := r.SyncRepo.Delete(ctx, tenantID, id); err != nil {
		return false, err
	}
	return true, nil
}

// MutationTriggerSync manually triggers a sync execution.
func (r *Resolver) MutationTriggerSync(ctx context.Context, id string) (*rest.SyncRun, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.SyncService.Trigger(ctx, tenantID, id)
}

// MutationPauseSync pauses an active sync.
func (r *Resolver) MutationPauseSync(ctx context.Context, id string) (*rest.Sync, error) {
	tenantID := common.TenantIDFrom(ctx)
	if err := r.SyncService.Pause(ctx, tenantID, id); err != nil {
		return nil, err
	}
	return r.SyncRepo.Get(ctx, tenantID, id)
}

// MutationResumeSync resumes a paused sync.
func (r *Resolver) MutationResumeSync(ctx context.Context, id string) (*rest.Sync, error) {
	tenantID := common.TenantIDFrom(ctx)
	if err := r.SyncService.Resume(ctx, tenantID, id); err != nil {
		return nil, err
	}
	return r.SyncRepo.Get(ctx, tenantID, id)
}

// MutationCreateTenant creates a new tenant.
func (r *Resolver) MutationCreateTenant(ctx context.Context, name string, plan *string, settings map[string]interface{}) (*rest.Tenant, error) {
	now := time.Now().UTC()
	t := &rest.Tenant{
		ID:        rest.NewID(),
		Name:      name,
		Plan:      "free",
		Settings:  settings,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if plan != nil {
		t.Plan = *plan
	}

	if err := r.TenantRepo.Create(ctx, t); err != nil {
		return nil, err
	}

	return t, nil
}

// MutationUpdateTenant updates a tenant.
func (r *Resolver) MutationUpdateTenant(ctx context.Context, id string, name, plan *string, settings map[string]interface{}) (*rest.Tenant, error) {
	existing, err := r.TenantRepo.Get(ctx, id)
	if err != nil {
		return nil, err
	}

	if name != nil {
		existing.Name = *name
	}
	if plan != nil {
		existing.Plan = *plan
	}
	if settings != nil {
		existing.Settings = settings
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := r.TenantRepo.Update(ctx, existing); err != nil {
		return nil, err
	}

	return existing, nil
}

// MutationCreateFieldMapping creates a new field mapping.
func (r *Resolver) MutationCreateFieldMapping(ctx context.Context, syncID, sourceField, destField string, transform *string) (*rest.FieldMapping, error) {
	tenantID := common.TenantIDFrom(ctx)

	now := time.Now().UTC()
	m := &rest.FieldMapping{
		ID:          rest.NewID(),
		SyncID:      syncID,
		TenantID:    tenantID,
		SourceField: sourceField,
		DestField:   destField,
		CreatedAt:   now,
		UpdatedAt:   now,
	}

	if transform != nil {
		m.Transform = *transform
	}

	if err := r.FieldMappingRepo.Create(ctx, m); err != nil {
		return nil, err
	}

	return m, nil
}

// MutationUpdateFieldMapping updates a field mapping.
func (r *Resolver) MutationUpdateFieldMapping(ctx context.Context, id string, sourceField, destField, transform *string) (*rest.FieldMapping, error) {
	tenantID := common.TenantIDFrom(ctx)

	existing, err := r.FieldMappingRepo.Get(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}

	if sourceField != nil {
		existing.SourceField = *sourceField
	}
	if destField != nil {
		existing.DestField = *destField
	}
	if transform != nil {
		existing.Transform = *transform
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := r.FieldMappingRepo.Update(ctx, existing); err != nil {
		return nil, err
	}

	return existing, nil
}

// MutationAutoMapFields auto-generates field mappings based on name matching.
func (r *Resolver) MutationAutoMapFields(ctx context.Context, syncID, streamName string) ([]rest.FieldMapping, error) {
	tenantID := common.TenantIDFrom(ctx)
	return r.FieldMappingRepo.AutoMap(ctx, tenantID, syncID, streamName)
}

// MutationRegisterMCPServer registers a new MCP server.
func (r *Resolver) MutationRegisterMCPServer(ctx context.Context, name, url, transport string, metadata map[string]string) (*rest.MCPServer, error) {
	tenantID := common.TenantIDFrom(ctx)

	now := time.Now().UTC()
	server := &rest.MCPServer{
		ID:        rest.NewID(),
		TenantID:  tenantID,
		Name:      name,
		URL:       url,
		Transport: transport,
		Metadata:  metadata,
		Status:    "active",
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := r.MCPServerRepo.Create(ctx, server); err != nil {
		return nil, err
	}

	return server, nil
}

// MutationUpdateMCPServer updates an MCP server.
func (r *Resolver) MutationUpdateMCPServer(ctx context.Context, id string, name, url, transport *string, metadata map[string]string) (*rest.MCPServer, error) {
	tenantID := common.TenantIDFrom(ctx)

	existing, err := r.MCPServerRepo.Get(ctx, tenantID, id)
	if err != nil {
		return nil, err
	}

	if name != nil {
		existing.Name = *name
	}
	if url != nil {
		existing.URL = *url
	}
	if transport != nil {
		existing.Transport = *transport
	}
	if metadata != nil {
		existing.Metadata = metadata
	}
	existing.UpdatedAt = time.Now().UTC()

	if err := r.MCPServerRepo.Update(ctx, existing); err != nil {
		return nil, err
	}

	return existing, nil
}

// MutationDeleteMCPServer deletes an MCP server.
func (r *Resolver) MutationDeleteMCPServer(ctx context.Context, id string) (bool, error) {
	tenantID := common.TenantIDFrom(ctx)
	if err := r.MCPServerRepo.Delete(ctx, tenantID, id); err != nil {
		return false, err
	}
	return true, nil
}

// MutationDiscoverStreams discovers streams from a connection.
func (r *Resolver) MutationDiscoverStreams(ctx context.Context, connectionID string) (*CatalogNode, error) {
	tenantID := common.TenantIDFrom(ctx)
	catalog, err := r.StreamService.Discover(ctx, tenantID, connectionID)
	if err != nil {
		return nil, err
	}
	return &CatalogNode{Streams: catalog.Streams}, nil
}

// ---------------------------------------------------------------------------
// Subscription Support
// ---------------------------------------------------------------------------

// SubscribeSyncStatus registers a listener for sync status changes.
func (r *Resolver) SubscribeSyncStatus(ctx context.Context, syncID *string) <-chan SyncStatusEvent {
	ch := make(chan SyncStatusEvent, 16)
	key := "*"
	if syncID != nil {
		key = *syncID
	}

	r.subMu.Lock()
	r.syncStatusSubs[key] = append(r.syncStatusSubs[key], ch)
	r.subMu.Unlock()

	go func() {
		<-ctx.Done()
		r.subMu.Lock()
		subs := r.syncStatusSubs[key]
		for i, sub := range subs {
			if sub == ch {
				r.syncStatusSubs[key] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		r.subMu.Unlock()
		close(ch)
	}()

	return ch
}

// SubscribeSyncRunProgress registers a listener for sync run progress.
func (r *Resolver) SubscribeSyncRunProgress(ctx context.Context, syncRunID string) <-chan SyncRunProgressEvent {
	ch := make(chan SyncRunProgressEvent, 16)

	r.subMu.Lock()
	r.runProgressSubs[syncRunID] = append(r.runProgressSubs[syncRunID], ch)
	r.subMu.Unlock()

	go func() {
		<-ctx.Done()
		r.subMu.Lock()
		subs := r.runProgressSubs[syncRunID]
		for i, sub := range subs {
			if sub == ch {
				r.runProgressSubs[syncRunID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
		r.subMu.Unlock()
		close(ch)
	}()

	return ch
}

// PublishSyncStatus publishes a sync status change event to all subscribers.
func (r *Resolver) PublishSyncStatus(event SyncStatusEvent) {
	r.subMu.RLock()
	defer r.subMu.RUnlock()

	for _, ch := range r.syncStatusSubs[event.SyncID] {
		select {
		case ch <- event:
		default:
			// Drop if subscriber is slow.
		}
	}
	for _, ch := range r.syncStatusSubs["*"] {
		select {
		case ch <- event:
		default:
		}
	}
}

// PublishSyncRunProgress publishes a sync run progress event.
func (r *Resolver) PublishSyncRunProgress(event SyncRunProgressEvent) {
	r.subMu.RLock()
	defer r.subMu.RUnlock()

	for _, ch := range r.runProgressSubs[event.SyncRunID] {
		select {
		case ch <- event:
		default:
		}
	}
}

// ---------------------------------------------------------------------------
// GraphQL HTTP Handler
// ---------------------------------------------------------------------------

// graphQLRequest represents an incoming GraphQL HTTP request.
type graphQLRequest struct {
	Query         string                 `json:"query"`
	OperationName string                 `json:"operationName,omitempty"`
	Variables     map[string]interface{} `json:"variables,omitempty"`
}

// graphQLResponse is the standard GraphQL response envelope.
type graphQLResponse struct {
	Data   interface{}    `json:"data,omitempty"`
	Errors []graphQLError `json:"errors,omitempty"`
}

// graphQLError is a single GraphQL error entry.
type graphQLError struct {
	Message string `json:"message"`
}

// Handler returns an http.Handler that serves GraphQL requests.
// It provides a simple execution engine for the FlowForge schema.
func (r *Resolver) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusMethodNotAllowed)
			_ = json.NewEncoder(w).Encode(graphQLResponse{
				Errors: []graphQLError{{Message: "only POST method is supported"}},
			})
			return
		}

		var gqlReq graphQLRequest
		if err := json.NewDecoder(req.Body).Decode(&gqlReq); err != nil {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_ = json.NewEncoder(w).Encode(graphQLResponse{
				Errors: []graphQLError{{Message: "invalid request body: " + err.Error()}},
			})
			return
		}

		ctx := WithDataLoaders(req.Context(), r)

		data, errs := r.execute(ctx, gqlReq)

		resp := graphQLResponse{Data: data}
		for _, err := range errs {
			resp.Errors = append(resp.Errors, graphQLError{Message: err.Error()})
		}

		w.Header().Set("Content-Type", "application/json")
		if len(errs) > 0 && data == nil {
			w.WriteHeader(http.StatusBadRequest)
		} else {
			w.WriteHeader(http.StatusOK)
		}
		enc := json.NewEncoder(w)
		enc.SetEscapeHTML(false)
		_ = enc.Encode(resp)
	})
}

// execute performs a basic field-level dispatch against the resolver methods.
// This is a lightweight execution engine that maps top-level query/mutation
// fields to the corresponding resolver methods without a full parser.
func (r *Resolver) execute(ctx context.Context, req graphQLRequest) (interface{}, []error) {
	if req.Query == "" {
		return nil, []error{fmt.Errorf("query is required")}
	}

	// Detect operation type from the query string prefix.
	opType := detectOperation(req.Query)

	switch opType {
	case "query":
		return r.executeQuery(ctx, req.Variables)
	case "mutation":
		return r.executeMutation(ctx, req.Variables)
	default:
		return nil, []error{fmt.Errorf("unsupported operation: %s", opType)}
	}
}

// detectOperation identifies whether the query is a query, mutation, or subscription.
func detectOperation(query string) string {
	// Strip leading whitespace and look for the operation keyword.
	trimmed := query
	for i := 0; i < len(trimmed); i++ {
		if trimmed[i] == ' ' || trimmed[i] == '\t' || trimmed[i] == '\n' || trimmed[i] == '\r' {
			continue
		}
		trimmed = trimmed[i:]
		break
	}

	if len(trimmed) >= 8 && trimmed[:8] == "mutation" {
		return "mutation"
	}
	if len(trimmed) >= 12 && trimmed[:12] == "subscription" {
		return "subscription"
	}
	// Default to query (covers both "query {" and shorthand "{ ... }").
	return "query"
}

// executeQuery dispatches top-level query field resolvers based on variables.
// This is a simplified dispatcher; in production gqlgen codegen would handle
// full field resolution. Here we expose the resolver methods via a variables-based
// dispatch matching common query patterns.
func (r *Resolver) executeQuery(ctx context.Context, vars map[string]interface{}) (interface{}, []error) {
	results := make(map[string]interface{})
	var errs []error

	field := stringVar(vars, "field")
	switch field {
	case "connectors":
		data, err := r.QueryConnectors(ctx)
		if err != nil {
			errs = append(errs, err)
		}
		results["connectors"] = data

	case "connector":
		name := stringVar(vars, "name")
		data, err := r.QueryConnector(ctx, name)
		if err != nil {
			errs = append(errs, err)
		}
		results["connector"] = data

	case "connections":
		p, ps := intPtrVar(vars, "page"), intPtrVar(vars, "pageSize")
		data, err := r.QueryConnections(ctx, p, ps)
		if err != nil {
			errs = append(errs, err)
		}
		results["connections"] = data

	case "connection":
		id := stringVar(vars, "id")
		data, err := r.QueryConnection(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["connection"] = data

	case "syncs":
		p, ps := intPtrVar(vars, "page"), intPtrVar(vars, "pageSize")
		data, err := r.QuerySyncs(ctx, p, ps)
		if err != nil {
			errs = append(errs, err)
		}
		results["syncs"] = data

	case "sync":
		id := stringVar(vars, "id")
		data, err := r.QuerySync(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["sync"] = data

	case "syncRuns":
		sid := stringPtrVar(vars, "syncId")
		p, ps := intPtrVar(vars, "page"), intPtrVar(vars, "pageSize")
		data, err := r.QuerySyncRuns(ctx, sid, p, ps)
		if err != nil {
			errs = append(errs, err)
		}
		results["syncRuns"] = data

	case "syncRun":
		id := stringVar(vars, "id")
		data, err := r.QuerySyncRun(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["syncRun"] = data

	case "streams":
		connID := stringVar(vars, "connectionId")
		data, err := r.QueryStreams(ctx, connID)
		if err != nil {
			errs = append(errs, err)
		}
		results["streams"] = data

	case "streamSchema":
		connID := stringVar(vars, "connectionId")
		name := stringVar(vars, "streamName")
		data, err := r.QueryStreamSchema(ctx, connID, name)
		if err != nil {
			errs = append(errs, err)
		}
		results["streamSchema"] = data

	case "tenant":
		id := stringVar(vars, "id")
		data, err := r.QueryTenant(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["tenant"] = data

	case "tenants":
		p, ps := intPtrVar(vars, "page"), intPtrVar(vars, "pageSize")
		data, err := r.QueryTenants(ctx, p, ps)
		if err != nil {
			errs = append(errs, err)
		}
		results["tenants"] = data

	case "fieldMapping":
		id := stringVar(vars, "id")
		data, err := r.QueryFieldMapping(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["fieldMapping"] = data

	case "schemaVersions":
		stream := stringVar(vars, "stream")
		data, err := r.QuerySchemaVersions(ctx, stream)
		if err != nil {
			errs = append(errs, err)
		}
		results["schemaVersions"] = data

	case "schemaComparison":
		stream := stringVar(vars, "stream")
		v1 := intVar(vars, "v1")
		v2 := intVar(vars, "v2")
		data, err := r.QuerySchemaComparison(ctx, stream, v1, v2)
		if err != nil {
			errs = append(errs, err)
		}
		results["schemaComparison"] = data

	case "mcpServers":
		p, ps := intPtrVar(vars, "page"), intPtrVar(vars, "pageSize")
		data, err := r.QueryMCPServers(ctx, p, ps)
		if err != nil {
			errs = append(errs, err)
		}
		results["mcpServers"] = data

	case "mcpServer":
		id := stringVar(vars, "id")
		data, err := r.QueryMCPServer(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["mcpServer"] = data

	default:
		// If no specific field is requested, return all connectors as a default.
		data, err := r.QueryConnectors(ctx)
		if err != nil {
			errs = append(errs, err)
		}
		results["connectors"] = data
	}

	return results, errs
}

// executeMutation dispatches top-level mutation field resolvers.
func (r *Resolver) executeMutation(ctx context.Context, vars map[string]interface{}) (interface{}, []error) {
	results := make(map[string]interface{})
	var errs []error

	field := stringVar(vars, "field")
	switch field {
	case "createConnection":
		name := stringVar(vars, "name")
		connectorName := stringVar(vars, "connectorName")
		config := jsonVar(vars, "config")
		data, err := r.MutationCreateConnection(ctx, name, connectorName, config)
		if err != nil {
			errs = append(errs, err)
		}
		results["createConnection"] = data

	case "updateConnection":
		id := stringVar(vars, "id")
		name := stringPtrVar(vars, "name")
		config := jsonVar(vars, "config")
		data, err := r.MutationUpdateConnection(ctx, id, name, config)
		if err != nil {
			errs = append(errs, err)
		}
		results["updateConnection"] = data

	case "deleteConnection":
		id := stringVar(vars, "id")
		data, err := r.MutationDeleteConnection(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["deleteConnection"] = data

	case "testConnection":
		connectorName := stringVar(vars, "connectorName")
		config := jsonVar(vars, "config")
		data, err := r.MutationTestConnection(ctx, connectorName, config)
		if err != nil {
			errs = append(errs, err)
		}
		results["testConnection"] = data

	case "createSync":
		name := stringVar(vars, "name")
		sourceID := stringVar(vars, "sourceId")
		destID := stringVar(vars, "destinationId")
		schedule := stringPtrVar(vars, "schedule")
		data, err := r.MutationCreateSync(ctx, name, sourceID, destID, schedule)
		if err != nil {
			errs = append(errs, err)
		}
		results["createSync"] = data

	case "updateSync":
		id := stringVar(vars, "id")
		name := stringPtrVar(vars, "name")
		schedule := stringPtrVar(vars, "schedule")
		data, err := r.MutationUpdateSync(ctx, id, name, schedule)
		if err != nil {
			errs = append(errs, err)
		}
		results["updateSync"] = data

	case "deleteSync":
		id := stringVar(vars, "id")
		data, err := r.MutationDeleteSync(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["deleteSync"] = data

	case "triggerSync":
		id := stringVar(vars, "id")
		data, err := r.MutationTriggerSync(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["triggerSync"] = data

	case "pauseSync":
		id := stringVar(vars, "id")
		data, err := r.MutationPauseSync(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["pauseSync"] = data

	case "resumeSync":
		id := stringVar(vars, "id")
		data, err := r.MutationResumeSync(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["resumeSync"] = data

	case "createTenant":
		name := stringVar(vars, "name")
		plan := stringPtrVar(vars, "plan")
		settings := mapVar(vars, "settings")
		data, err := r.MutationCreateTenant(ctx, name, plan, settings)
		if err != nil {
			errs = append(errs, err)
		}
		results["createTenant"] = data

	case "updateTenant":
		id := stringVar(vars, "id")
		name := stringPtrVar(vars, "name")
		plan := stringPtrVar(vars, "plan")
		settings := mapVar(vars, "settings")
		data, err := r.MutationUpdateTenant(ctx, id, name, plan, settings)
		if err != nil {
			errs = append(errs, err)
		}
		results["updateTenant"] = data

	case "createFieldMapping":
		syncID := stringVar(vars, "syncId")
		sourceField := stringVar(vars, "sourceField")
		destField := stringVar(vars, "destField")
		transform := stringPtrVar(vars, "transform")
		data, err := r.MutationCreateFieldMapping(ctx, syncID, sourceField, destField, transform)
		if err != nil {
			errs = append(errs, err)
		}
		results["createFieldMapping"] = data

	case "updateFieldMapping":
		id := stringVar(vars, "id")
		sourceField := stringPtrVar(vars, "sourceField")
		destField := stringPtrVar(vars, "destField")
		transform := stringPtrVar(vars, "transform")
		data, err := r.MutationUpdateFieldMapping(ctx, id, sourceField, destField, transform)
		if err != nil {
			errs = append(errs, err)
		}
		results["updateFieldMapping"] = data

	case "autoMapFields":
		syncID := stringVar(vars, "syncId")
		streamName := stringVar(vars, "streamName")
		data, err := r.MutationAutoMapFields(ctx, syncID, streamName)
		if err != nil {
			errs = append(errs, err)
		}
		results["autoMapFields"] = data

	case "registerMCPServer":
		name := stringVar(vars, "name")
		url := stringVar(vars, "url")
		transport := stringVar(vars, "transport")
		metadata := stringMapVar(vars, "metadata")
		data, err := r.MutationRegisterMCPServer(ctx, name, url, transport, metadata)
		if err != nil {
			errs = append(errs, err)
		}
		results["registerMCPServer"] = data

	case "updateMCPServer":
		id := stringVar(vars, "id")
		name := stringPtrVar(vars, "name")
		url := stringPtrVar(vars, "url")
		transport := stringPtrVar(vars, "transport")
		metadata := stringMapVar(vars, "metadata")
		data, err := r.MutationUpdateMCPServer(ctx, id, name, url, transport, metadata)
		if err != nil {
			errs = append(errs, err)
		}
		results["updateMCPServer"] = data

	case "deleteMCPServer":
		id := stringVar(vars, "id")
		data, err := r.MutationDeleteMCPServer(ctx, id)
		if err != nil {
			errs = append(errs, err)
		}
		results["deleteMCPServer"] = data

	case "discoverStreams":
		connID := stringVar(vars, "connectionId")
		data, err := r.MutationDiscoverStreams(ctx, connID)
		if err != nil {
			errs = append(errs, err)
		}
		results["discoverStreams"] = data

	default:
		errs = append(errs, fmt.Errorf("unknown mutation field: %s", field))
	}

	return results, errs
}

// ---------------------------------------------------------------------------
// Variable extraction helpers
// ---------------------------------------------------------------------------

func stringVar(vars map[string]interface{}, key string) string {
	if v, ok := vars[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func stringPtrVar(vars map[string]interface{}, key string) *string {
	if v, ok := vars[key]; ok {
		if s, ok := v.(string); ok {
			return &s
		}
	}
	return nil
}

func intVar(vars map[string]interface{}, key string) int {
	if v, ok := vars[key]; ok {
		switch n := v.(type) {
		case float64:
			return int(n)
		case int:
			return n
		case json.Number:
			i, _ := n.Int64()
			return int(i)
		}
	}
	return 0
}

func intPtrVar(vars map[string]interface{}, key string) *int {
	if v, ok := vars[key]; ok {
		switch n := v.(type) {
		case float64:
			i := int(n)
			return &i
		case int:
			return &n
		case json.Number:
			i, _ := n.Int64()
			ii := int(i)
			return &ii
		}
	}
	return nil
}

func jsonVar(vars map[string]interface{}, key string) json.RawMessage {
	if v, ok := vars[key]; ok {
		b, err := json.Marshal(v)
		if err == nil {
			return b
		}
	}
	return nil
}

func mapVar(vars map[string]interface{}, key string) map[string]interface{} {
	if v, ok := vars[key]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			return m
		}
	}
	return nil
}

func stringMapVar(vars map[string]interface{}, key string) map[string]string {
	if v, ok := vars[key]; ok {
		if m, ok := v.(map[string]interface{}); ok {
			result := make(map[string]string, len(m))
			for k, v := range m {
				if s, ok := v.(string); ok {
					result[k] = s
				}
			}
			return result
		}
	}
	return nil
}

// defaultPagination provides default pagination values.
func defaultPagination(page, pageSize *int) (int, int) {
	p := 1
	ps := 20
	if page != nil && *page > 0 {
		p = *page
	}
	if pageSize != nil && *pageSize > 0 {
		ps = *pageSize
		if ps > 100 {
			ps = 100
		}
	}
	return p, ps
}
