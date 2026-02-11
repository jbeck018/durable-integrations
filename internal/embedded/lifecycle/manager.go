// Package lifecycle manages the lifecycle of embedded integrations within the
// iPaaS layer: creation, enabling, disabling, deletion, and status tracking.
package lifecycle

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/mapping/auto"
	"github.com/google/uuid"
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
	CronExpression string `json:"cron_expression,omitempty"`
	IntervalMinutes int   `json:"interval_minutes,omitempty"`
	Timezone       string `json:"timezone,omitempty"`
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

// LifecycleManager manages the full lifecycle of embedded integrations.
// In production this would be backed by a database; here we use an in-memory
// store keyed by tenant and integration ID.
type LifecycleManager struct {
	mu           sync.RWMutex
	integrations map[string]*Integration // keyed by integration ID
	tenantIndex  map[string][]string     // tenant ID -> list of integration IDs
}

// NewLifecycleManager creates a new LifecycleManager.
func NewLifecycleManager() *LifecycleManager {
	return &LifecycleManager{
		integrations: make(map[string]*Integration),
		tenantIndex:  make(map[string][]string),
	}
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

	lm.mu.Lock()
	defer lm.mu.Unlock()

	lm.integrations[integration.ID] = integration
	lm.tenantIndex[tenantID] = append(lm.tenantIndex[tenantID], integration.ID)

	return integration, nil
}

// EnableIntegration transitions an integration to the "enabled" state,
// making it eligible for scheduled syncs.
func (lm *LifecycleManager) EnableIntegration(ctx context.Context, integrationID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	integration, ok := lm.integrations[integrationID]
	if !ok {
		return fmt.Errorf("integration %q not found", integrationID)
	}

	if integration.Status == StatusDeleted {
		return fmt.Errorf("cannot enable deleted integration %q", integrationID)
	}

	now := time.Now().UTC()
	integration.Status = StatusEnabled
	integration.UpdatedAt = now
	integration.EnabledAt = &now
	integration.ErrorMessage = ""

	return nil
}

// DisableIntegration transitions an integration to the "disabled" state,
// pausing any scheduled syncs.
func (lm *LifecycleManager) DisableIntegration(ctx context.Context, integrationID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	integration, ok := lm.integrations[integrationID]
	if !ok {
		return fmt.Errorf("integration %q not found", integrationID)
	}

	if integration.Status == StatusDeleted {
		return fmt.Errorf("cannot disable deleted integration %q", integrationID)
	}

	integration.Status = StatusDisabled
	integration.UpdatedAt = time.Now().UTC()

	return nil
}

// DeleteIntegration marks an integration as deleted. This is a soft delete;
// the integration record is retained for audit purposes.
func (lm *LifecycleManager) DeleteIntegration(ctx context.Context, integrationID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	integration, ok := lm.integrations[integrationID]
	if !ok {
		return fmt.Errorf("integration %q not found", integrationID)
	}

	integration.Status = StatusDeleted
	integration.UpdatedAt = time.Now().UTC()

	// Remove from tenant index.
	ids := lm.tenantIndex[integration.TenantID]
	for i, id := range ids {
		if id == integrationID {
			lm.tenantIndex[integration.TenantID] = append(ids[:i], ids[i+1:]...)
			break
		}
	}

	return nil
}

// GetIntegration returns an integration by ID.
func (lm *LifecycleManager) GetIntegration(ctx context.Context, integrationID string) (*Integration, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	integration, ok := lm.integrations[integrationID]
	if !ok {
		return nil, fmt.Errorf("integration %q not found", integrationID)
	}

	// Return a copy to prevent mutation.
	cp := *integration
	return &cp, nil
}

// GetStatus returns detailed status for an integration.
func (lm *LifecycleManager) GetStatus(ctx context.Context, integrationID string) (*IntegrationStatusDetail, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	integration, ok := lm.integrations[integrationID]
	if !ok {
		return nil, fmt.Errorf("integration %q not found", integrationID)
	}

	cp := *integration
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
		Integration: &cp,
		Health:      health,
	}

	// Compute next sync time if enabled and schedule is configured.
	if integration.Status == StatusEnabled && integration.Config.SyncSchedule.IntervalMinutes > 0 {
		var nextSync time.Time
		if integration.LastSyncAt != nil {
			nextSync = integration.LastSyncAt.Add(time.Duration(integration.Config.SyncSchedule.IntervalMinutes) * time.Minute)
		} else {
			nextSync = integration.EnabledAt.Add(time.Duration(integration.Config.SyncSchedule.IntervalMinutes) * time.Minute)
		}
		detail.NextSyncAt = &nextSync
	}

	return detail, nil
}

// ListByTenant returns all non-deleted integrations for a tenant.
func (lm *LifecycleManager) ListByTenant(ctx context.Context, tenantID string) ([]*Integration, error) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()

	ids := lm.tenantIndex[tenantID]
	result := make([]*Integration, 0, len(ids))
	for _, id := range ids {
		integration := lm.integrations[id]
		if integration != nil && integration.Status != StatusDeleted {
			cp := *integration
			result = append(result, &cp)
		}
	}
	return result, nil
}

// RecordSync updates the sync metadata after a successful sync.
func (lm *LifecycleManager) RecordSync(ctx context.Context, integrationID string, recordsProcessed int64) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	integration, ok := lm.integrations[integrationID]
	if !ok {
		return fmt.Errorf("integration %q not found", integrationID)
	}

	now := time.Now().UTC()
	integration.LastSyncAt = &now
	integration.SyncCount++
	integration.UpdatedAt = now

	return nil
}

// SetError transitions an integration to the error state with a message.
func (lm *LifecycleManager) SetError(ctx context.Context, integrationID string, errMsg string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	integration, ok := lm.integrations[integrationID]
	if !ok {
		return fmt.Errorf("integration %q not found", integrationID)
	}

	integration.Status = StatusError
	integration.ErrorMessage = errMsg
	integration.UpdatedAt = time.Now().UTC()

	return nil
}

// UpdateConfig updates the configuration for an existing integration.
func (lm *LifecycleManager) UpdateConfig(ctx context.Context, integrationID string, config IntegrationConfig) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	integration, ok := lm.integrations[integrationID]
	if !ok {
		return fmt.Errorf("integration %q not found", integrationID)
	}

	if integration.Status == StatusDeleted {
		return fmt.Errorf("cannot update deleted integration %q", integrationID)
	}

	integration.Config = config
	integration.UpdatedAt = time.Now().UTC()

	return nil
}
