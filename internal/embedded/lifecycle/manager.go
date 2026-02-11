// Package lifecycle manages the lifecycle of embedded integrations within the
// iPaaS layer: creation, enabling, disabling, deletion, and status tracking.
package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"github.com/flowforge/flowforge/internal/mapping/auto"
)

// IntegrationStatus represents the current state of an integration.
type IntegrationStatus string

const (
	StatusCreated  IntegrationStatus = "created"
	StatusEnabled  IntegrationStatus = "enabled"
	StatusDisabled IntegrationStatus = "disabled"
	StatusError    IntegrationStatus = "error"
	StatusDeleted  IntegrationStatus = "deleted"
)

// SyncSchedule defines when an integration should sync.
type SyncSchedule struct {
	CronExpression  string `json:"cron_expression,omitempty"`
	IntervalMinutes int    `json:"interval_minutes,omitempty"`
	Timezone        string `json:"timezone,omitempty"`
}

// IntegrationConfig holds the user-provided configuration for creating an
// embedded integration.
type IntegrationConfig struct {
	ConnectorType   string              `json:"connector_type"`
	AuthConfig      json.RawMessage     `json:"auth_config"`
	StreamSelection []string            `json:"stream_selection"`
	SyncSchedule    SyncSchedule        `json:"sync_schedule"`
	FieldMappings   []auto.FieldMapping `json:"field_mappings,omitempty"`
}

// Integration represents a fully configured embedded integration instance.
type Integration struct {
	ID            string            `json:"id"`
	TenantID      string            `json:"tenant_id"`
	ConnectorType string            `json:"connector_type"`
	Config        IntegrationConfig `json:"config"`
	Status        IntegrationStatus `json:"status"`
	ErrorMessage  string            `json:"error_message,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	UpdatedAt     time.Time         `json:"updated_at"`
	EnabledAt     *time.Time        `json:"enabled_at,omitempty"`
	LastSyncAt    *time.Time        `json:"last_sync_at,omitempty"`
	SyncCount     int64             `json:"sync_count"`
}

// IntegrationStatusDetail provides extended status information.
type IntegrationStatusDetail struct {
	Integration  *Integration `json:"integration"`
	Health       string       `json:"health"`
	NextSyncAt   *time.Time   `json:"next_sync_at,omitempty"`
	RecordsTotal int64        `json:"records_total"`
}

// IntegrationRepository provides persistence for integration records.
// Implementations may be in-memory (for development) or database-backed.
type IntegrationRepository interface {
	Create(ctx context.Context, integration *Integration) error
	GetByID(ctx context.Context, id string) (*Integration, error)
	Update(ctx context.Context, integration *Integration) error
	ListByTenant(ctx context.Context, tenantID string) ([]*Integration, error)
	Delete(ctx context.Context, id string) error
}

// LifecycleManager manages the full lifecycle of embedded integrations.
// It delegates persistence to an IntegrationRepository for database-backed
// storage in production, or in-memory storage for development.
type LifecycleManager struct {
	repo IntegrationRepository
}

// NewLifecycleManager creates a LifecycleManager with the given repository.
// Pass NewInMemoryRepository() for development or a database-backed
// implementation for production.
func NewLifecycleManager(repo IntegrationRepository) *LifecycleManager {
	return &LifecycleManager{repo: repo}
}

// InMemoryRepository is a development-only in-memory implementation of
// IntegrationRepository. Data is lost on restart.
type InMemoryRepository struct {
	mu           sync.RWMutex
	integrations map[string]*Integration
	tenantIndex  map[string][]string
}

// NewInMemoryRepository creates an InMemoryRepository for development use.
func NewInMemoryRepository() *InMemoryRepository {
	return &InMemoryRepository{
		integrations: make(map[string]*Integration),
		tenantIndex:  make(map[string][]string),
	}
}

func (r *InMemoryRepository) Create(_ context.Context, integration *Integration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.integrations[integration.ID] = integration
	r.tenantIndex[integration.TenantID] = append(r.tenantIndex[integration.TenantID], integration.ID)
	return nil
}

func (r *InMemoryRepository) GetByID(_ context.Context, id string) (*Integration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	i, ok := r.integrations[id]
	if !ok {
		return nil, fmt.Errorf("integration %q not found", id)
	}
	cp := *i
	return &cp, nil
}

func (r *InMemoryRepository) Update(_ context.Context, integration *Integration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.integrations[integration.ID] = integration
	return nil
}

func (r *InMemoryRepository) ListByTenant(_ context.Context, tenantID string) ([]*Integration, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := r.tenantIndex[tenantID]
	result := make([]*Integration, 0, len(ids))
	for _, id := range ids {
		if i := r.integrations[id]; i != nil && i.Status != StatusDeleted {
			cp := *i
			result = append(result, &cp)
		}
	}
	return result, nil
}

func (r *InMemoryRepository) Delete(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	i, ok := r.integrations[id]
	if !ok {
		return fmt.Errorf("integration %q not found", id)
	}
	i.Status = StatusDeleted
	i.UpdatedAt = time.Now().UTC()
	// Remove from tenant index
	ids := r.tenantIndex[i.TenantID]
	for j, tid := range ids {
		if tid == id {
			r.tenantIndex[i.TenantID] = append(ids[:j], ids[j+1:]...)
			break
		}
	}
	return nil
}

// CreateIntegration creates a new integration for the given tenant. The
// integration starts in the "created" state and must be explicitly enabled.
func (lm *LifecycleManager) CreateIntegration(ctx context.Context, tenantID string, config IntegrationConfig) (*Integration, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenant ID is required")
	}
	if config.ConnectorType == "" {
		return nil, fmt.Errorf("connector type is required")
	}

	now := time.Now().UTC()
	integration := &Integration{
		ID:            uuid.New().String(),
		TenantID:      tenantID,
		ConnectorType: config.ConnectorType,
		Config:        config,
		Status:        StatusCreated,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := lm.repo.Create(ctx, integration); err != nil {
		return nil, fmt.Errorf("persist integration: %w", err)
	}

	return integration, nil
}

// EnableIntegration transitions an integration to the "enabled" state,
// making it eligible for scheduled syncs.
func (lm *LifecycleManager) EnableIntegration(ctx context.Context, integrationID string) error {
	integration, err := lm.repo.GetByID(ctx, integrationID)
	if err != nil {
		return err
	}

	if integration.Status == StatusDeleted {
		return fmt.Errorf("cannot enable deleted integration %q", integrationID)
	}

	now := time.Now().UTC()
	integration.Status = StatusEnabled
	integration.UpdatedAt = now
	integration.EnabledAt = &now
	integration.ErrorMessage = ""

	return lm.repo.Update(ctx, integration)
}

// DisableIntegration transitions an integration to the "disabled" state,
// pausing any scheduled syncs.
func (lm *LifecycleManager) DisableIntegration(ctx context.Context, integrationID string) error {
	integration, err := lm.repo.GetByID(ctx, integrationID)
	if err != nil {
		return err
	}

	if integration.Status == StatusDeleted {
		return fmt.Errorf("cannot disable deleted integration %q", integrationID)
	}

	integration.Status = StatusDisabled
	integration.UpdatedAt = time.Now().UTC()

	return lm.repo.Update(ctx, integration)
}

// DeleteIntegration marks an integration as deleted. This is a soft delete;
// the integration record is retained for audit purposes.
func (lm *LifecycleManager) DeleteIntegration(ctx context.Context, integrationID string) error {
	return lm.repo.Delete(ctx, integrationID)
}

// GetIntegration returns an integration by ID.
func (lm *LifecycleManager) GetIntegration(ctx context.Context, integrationID string) (*Integration, error) {
	return lm.repo.GetByID(ctx, integrationID)
}

// GetStatus returns detailed status for an integration.
func (lm *LifecycleManager) GetStatus(ctx context.Context, integrationID string) (*IntegrationStatusDetail, error) {
	integration, err := lm.repo.GetByID(ctx, integrationID)
	if err != nil {
		return nil, err
	}

	health := "unknown"
	switch integration.Status {
	case StatusEnabled:
		health = "healthy"
		if integration.ErrorMessage != "" {
			health = "degraded"
		}
	case StatusDisabled:
		health = "paused"
	case StatusError:
		health = "unhealthy"
	case StatusCreated:
		health = "pending"
	case StatusDeleted:
		health = "deleted"
	}

	detail := &IntegrationStatusDetail{
		Integration: integration,
		Health:      health,
	}

	// Compute next sync time if enabled and schedule is configured.
	if integration.Status == StatusEnabled && integration.Config.SyncSchedule.IntervalMinutes > 0 {
		var nextSync time.Time
		if integration.LastSyncAt != nil {
			nextSync = integration.LastSyncAt.Add(time.Duration(integration.Config.SyncSchedule.IntervalMinutes) * time.Minute)
		} else if integration.EnabledAt != nil {
			nextSync = integration.EnabledAt.Add(time.Duration(integration.Config.SyncSchedule.IntervalMinutes) * time.Minute)
		}
		detail.NextSyncAt = &nextSync
	}

	return detail, nil
}

// ListByTenant returns all non-deleted integrations for a tenant.
func (lm *LifecycleManager) ListByTenant(ctx context.Context, tenantID string) ([]*Integration, error) {
	return lm.repo.ListByTenant(ctx, tenantID)
}

// RecordSync updates the sync metadata after a successful sync.
func (lm *LifecycleManager) RecordSync(ctx context.Context, integrationID string, recordsProcessed int64) error {
	integration, err := lm.repo.GetByID(ctx, integrationID)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	integration.LastSyncAt = &now
	integration.SyncCount++
	integration.UpdatedAt = now

	return lm.repo.Update(ctx, integration)
}

// SetError transitions an integration to the error state with a message.
func (lm *LifecycleManager) SetError(ctx context.Context, integrationID string, errMsg string) error {
	integration, err := lm.repo.GetByID(ctx, integrationID)
	if err != nil {
		return err
	}

	integration.Status = StatusError
	integration.ErrorMessage = errMsg
	integration.UpdatedAt = time.Now().UTC()

	return lm.repo.Update(ctx, integration)
}

// UpdateConfig updates the configuration for an existing integration.
func (lm *LifecycleManager) UpdateConfig(ctx context.Context, integrationID string, config IntegrationConfig) error {
	integration, err := lm.repo.GetByID(ctx, integrationID)
	if err != nil {
		return err
	}

	if integration.Status == StatusDeleted {
		return fmt.Errorf("cannot update deleted integration %q", integrationID)
	}

	integration.Config = config
	integration.UpdatedAt = time.Now().UTC()

	return lm.repo.Update(ctx, integration)
}
