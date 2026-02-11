// Package tenant provides multi-tenant management for FlowForge.
// It handles tenant lifecycle, resource quota enforcement, and
// namespace isolation across the platform.
package tenant

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/internal/observability/logging"
)

// TenantStatus represents the lifecycle state of a tenant.
type TenantStatus string

const (
	StatusActive    TenantStatus = "active"
	StatusSuspended TenantStatus = "suspended"
	StatusDeleted   TenantStatus = "deleted"
)

// ResourceQuotas defines the resource limits for a tenant.
type ResourceQuotas struct {
	MaxConnectors      int   `json:"max_connectors"`
	MaxSyncs           int   `json:"max_syncs"`
	MaxRecordsPerMonth int64 `json:"max_records_per_month"`
	MaxStorageBytes    int64 `json:"max_storage_bytes"`
}

// Tenant represents a FlowForge tenant with quotas and metadata.
type Tenant struct {
	ID              string         `json:"id"`
	Name            string         `json:"name"`
	Namespace       string         `json:"namespace"`
	Quotas          ResourceQuotas `json:"quotas"`
	Status          TenantStatus   `json:"status"`
	NeonProjectID   string         `json:"neon_project_id,omitempty"`
	Region          string         `json:"region,omitempty"`
	DBSchemaVersion int            `json:"db_schema_version,omitempty"`
	CreatedAt       time.Time      `json:"created_at"`
	UpdatedAt       time.Time      `json:"updated_at"`
}

// TenantRepository provides persistence operations for tenants.
type TenantRepository interface {
	Create(ctx context.Context, t *TenantRecord) (*TenantRecord, error)
	GetByID(ctx context.Context, id string) (*TenantRecord, error)
	Update(ctx context.Context, t *TenantRecord) (*TenantRecord, error)
	List(ctx context.Context, limit, offset int) ([]*TenantRecord, error)
	CountConnectors(ctx context.Context, tenantID string) (int, error)
	CountSyncs(ctx context.Context, tenantID string) (int, error)
	GetMonthlyRecordCount(ctx context.Context, tenantID string) (int64, error)
	GetStorageBytes(ctx context.Context, tenantID string) (int64, error)
}

// TenantRecord is the database representation of a tenant.
type TenantRecord struct {
	ID                     string          `json:"id"`
	Name                   string          `json:"name"`
	Namespace              string          `json:"namespace"`
	Config                 json.RawMessage `json:"config"`
	ResourceQuotas         json.RawMessage `json:"resource_quotas"`
	NeonProjectID          string          `json:"neon_project_id,omitempty"`
	ConnectionURIEncrypted string          `json:"connection_uri_encrypted,omitempty"`
	Region                 string          `json:"region,omitempty"`
	DBSchemaVersion        int             `json:"db_schema_version"`
	CreatedAt              time.Time       `json:"created_at"`
	UpdatedAt              time.Time       `json:"updated_at"`
}

// TenantCache provides caching for tenant data.
type TenantCache interface {
	Get(ctx context.Context, tenantID string) (*Tenant, bool, error)
	Set(ctx context.Context, tenantID string, t *Tenant) error
	Invalidate(ctx context.Context, tenantID string) error
}

// DatabaseProvisioner provisions and deprovisions isolated databases for tenants.
type DatabaseProvisioner interface {
	ProvisionDatabase(ctx context.Context, tenantID, tenantName string) (*TenantDatabase, error)
	DeprovisionDatabase(ctx context.Context, projectID string) error
	GetConnectionURI(ctx context.Context, projectID string) (string, error)
}

// TenantManager manages the full lifecycle of tenants including
// creation, quota enforcement, database isolation, and status management.
type TenantManager struct {
	repo        TenantRepository
	cache       TenantCache
	provisioner DatabaseProvisioner
	logger      *logging.Logger
}

// NewTenantManager creates a TenantManager. The cache and provisioner
// parameters may be nil if caching or database isolation is not desired.
func NewTenantManager(repo TenantRepository, cache TenantCache, provisioner DatabaseProvisioner) *TenantManager {
	return &TenantManager{
		repo:        repo,
		cache:       cache,
		provisioner: provisioner,
		logger:      logging.Global().WithField("component", "tenant_manager"),
	}
}

// CreateTenant provisions a new tenant with the given name, namespace, and quotas.
// It validates inputs, applies defaults for zero quotas, persists the tenant,
// and returns the created tenant.
func (tm *TenantManager) CreateTenant(ctx context.Context, name, namespace string, quotas ResourceQuotas) (*Tenant, error) {
	// Validate inputs.
	var errs common.ValidationErrors
	if name == "" {
		errs = append(errs, common.ValidationError{Field: "name", Message: "required"})
	}
	if namespace == "" {
		errs = append(errs, common.ValidationError{Field: "namespace", Message: "required"})
	}
	if len(errs) > 0 {
		return nil, errs
	}

	// Apply default quotas for unset values.
	if quotas.MaxConnectors <= 0 {
		quotas.MaxConnectors = 10
	}
	if quotas.MaxSyncs <= 0 {
		quotas.MaxSyncs = 20
	}
	if quotas.MaxRecordsPerMonth <= 0 {
		quotas.MaxRecordsPerMonth = 10_000_000
	}
	if quotas.MaxStorageBytes <= 0 {
		quotas.MaxStorageBytes = 10 * 1024 * 1024 * 1024 // 10 GB
	}

	quotasJSON, err := json.Marshal(quotas)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal quotas: %w", err)
	}

	record := &TenantRecord{
		Name:           name,
		Namespace:      namespace,
		Config:         json.RawMessage("{}"),
		ResourceQuotas: quotasJSON,
	}

	created, err := tm.repo.Create(ctx, record)
	if err != nil {
		return nil, fmt.Errorf("failed to create tenant: %w", err)
	}

	tenant := recordToTenant(created)

	// Provision an isolated Neon database if a provisioner is configured.
	if tm.provisioner != nil {
		dbInfo, provErr := tm.provisioner.ProvisionDatabase(ctx, tenant.ID, tenant.Name)
		if provErr != nil {
			tm.logger.WithContext(ctx).Error("failed to provision tenant database",
				"tenant_id", tenant.ID,
				"error", provErr,
			)
			// Store tenant record with a note that provisioning failed.
			// The caller can retry provisioning later.
		} else {
			tenant.NeonProjectID = dbInfo.ProjectID
			tenant.Region = dbInfo.Region

			// Update the tenant record with Neon project details.
			// In production, encrypt the connection URI before storing.
			if updateErr := tm.updateDatabaseInfo(ctx, tenant.ID, dbInfo); updateErr != nil {
				tm.logger.WithContext(ctx).Error("failed to store tenant database info",
					"tenant_id", tenant.ID,
					"error", updateErr,
				)
			}
		}
	}

	tm.logger.WithContext(ctx).Info("tenant created",
		"tenant_id", tenant.ID,
		"namespace", tenant.Namespace,
		"neon_project_id", tenant.NeonProjectID,
	)

	return tenant, nil
}

// GetTenant retrieves a tenant by ID, checking the cache first.
func (tm *TenantManager) GetTenant(ctx context.Context, tenantID string) (*Tenant, error) {
	if tenantID == "" {
		return nil, fmt.Errorf("tenantID is required: %w", common.ErrInvalidConfig)
	}

	// Check cache.
	if tm.cache != nil {
		cached, found, err := tm.cache.Get(ctx, tenantID)
		if err != nil {
			tm.logger.WithContext(ctx).Warn("tenant cache read failed",
				"tenant_id", tenantID,
				"error", err,
			)
		} else if found {
			return cached, nil
		}
	}

	record, err := tm.repo.GetByID(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("failed to get tenant %s: %w", tenantID, err)
	}

	tenant := recordToTenant(record)

	// Populate cache.
	if tm.cache != nil {
		if cacheErr := tm.cache.Set(ctx, tenantID, tenant); cacheErr != nil {
			tm.logger.WithContext(ctx).Warn("tenant cache write failed",
				"tenant_id", tenantID,
				"error", cacheErr,
			)
		}
	}

	return tenant, nil
}

// UpdateQuotas updates the resource quotas for a tenant. It merges the new
// quotas with the existing tenant record and persists the change.
func (tm *TenantManager) UpdateQuotas(ctx context.Context, tenantID string, quotas ResourceQuotas) error {
	if tenantID == "" {
		return fmt.Errorf("tenantID is required: %w", common.ErrInvalidConfig)
	}

	// Fetch the existing record.
	record, err := tm.repo.GetByID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("failed to get tenant %s for quota update: %w", tenantID, err)
	}

	quotasJSON, err := json.Marshal(quotas)
	if err != nil {
		return fmt.Errorf("failed to marshal quotas: %w", err)
	}

	record.ResourceQuotas = quotasJSON

	if _, err := tm.repo.Update(ctx, record); err != nil {
		return fmt.Errorf("failed to update tenant %s quotas: %w", tenantID, err)
	}

	// Invalidate cache.
	if tm.cache != nil {
		if cacheErr := tm.cache.Invalidate(ctx, tenantID); cacheErr != nil {
			tm.logger.WithContext(ctx).Warn("tenant cache invalidation failed",
				"tenant_id", tenantID,
				"error", cacheErr,
			)
		}
	}

	tm.logger.WithContext(ctx).Info("tenant quotas updated",
		"tenant_id", tenantID,
	)

	return nil
}

// CheckQuota verifies that a tenant has not exceeded the specified resource
// limit. Supported resource names: "connectors", "syncs", "records", "storage".
// Returns nil if the tenant is within limits, or an error describing the violation.
func (tm *TenantManager) CheckQuota(ctx context.Context, tenantID, resource string) error {
	if tenantID == "" {
		return fmt.Errorf("tenantID is required: %w", common.ErrInvalidConfig)
	}

	tenant, err := tm.GetTenant(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("failed to check quota: %w", err)
	}

	if tenant.Status != StatusActive {
		return fmt.Errorf("tenant %s is %s: %w", tenantID, tenant.Status, common.ErrForbidden)
	}

	switch resource {
	case "connectors":
		count, err := tm.repo.CountConnectors(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("failed to count connectors for tenant %s: %w", tenantID, err)
		}
		if count >= tenant.Quotas.MaxConnectors {
			return fmt.Errorf("tenant %s has reached the maximum number of connectors (%d): %w",
				tenantID, tenant.Quotas.MaxConnectors, common.ErrRateLimited)
		}

	case "syncs":
		count, err := tm.repo.CountSyncs(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("failed to count syncs for tenant %s: %w", tenantID, err)
		}
		if count >= tenant.Quotas.MaxSyncs {
			return fmt.Errorf("tenant %s has reached the maximum number of syncs (%d): %w",
				tenantID, tenant.Quotas.MaxSyncs, common.ErrRateLimited)
		}

	case "records":
		monthlyCount, err := tm.repo.GetMonthlyRecordCount(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("failed to get monthly record count for tenant %s: %w", tenantID, err)
		}
		if monthlyCount >= tenant.Quotas.MaxRecordsPerMonth {
			return fmt.Errorf("tenant %s has reached the monthly record limit (%d): %w",
				tenantID, tenant.Quotas.MaxRecordsPerMonth, common.ErrRateLimited)
		}

	case "storage":
		storageBytes, err := tm.repo.GetStorageBytes(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("failed to get storage usage for tenant %s: %w", tenantID, err)
		}
		if storageBytes >= tenant.Quotas.MaxStorageBytes {
			return fmt.Errorf("tenant %s has reached the storage limit (%d bytes): %w",
				tenantID, tenant.Quotas.MaxStorageBytes, common.ErrRateLimited)
		}

	default:
		return fmt.Errorf("unknown resource type %q", resource)
	}

	return nil
}

// TemporalNamespace returns the Temporal namespace for a tenant.
// Each tenant gets its own namespace to prevent workflow interference.
func TemporalNamespace(tenantID string) string {
	return fmt.Sprintf("tenant-%s", tenantID)
}

// S3Prefix returns the S3 key prefix for a tenant's blob storage.
// All tenant objects are stored under: tenants/{tenant_id}/
func S3Prefix(tenantID string) string {
	return fmt.Sprintf("tenants/%s/", tenantID)
}

// RedisKeyPrefix returns the Redis key prefix for a tenant.
// Format: flowforge:{tenant_id}:{purpose}
func RedisKeyPrefix(tenantID, purpose string) string {
	return fmt.Sprintf("flowforge:%s:%s:", tenantID, purpose)
}

// updateDatabaseInfo stores the Neon project details on the tenant record.
func (tm *TenantManager) updateDatabaseInfo(ctx context.Context, tenantID string, dbInfo *TenantDatabase) error {
	record, err := tm.repo.GetByID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("get tenant for db info update: %w", err)
	}

	record.NeonProjectID = dbInfo.ProjectID
	record.ConnectionURIEncrypted = dbInfo.ConnectionURI
	record.Region = dbInfo.Region

	if _, err := tm.repo.Update(ctx, record); err != nil {
		return fmt.Errorf("update tenant db info: %w", err)
	}

	// Invalidate cache since the tenant record changed.
	if tm.cache != nil {
		_ = tm.cache.Invalidate(ctx, tenantID)
	}

	return nil
}

// recordToTenant converts a TenantRecord from the database into the
// domain Tenant type.
func recordToTenant(r *TenantRecord) *Tenant {
	var quotas ResourceQuotas
	if len(r.ResourceQuotas) > 0 {
		_ = json.Unmarshal(r.ResourceQuotas, &quotas)
	}

	return &Tenant{
		ID:              r.ID,
		Name:            r.Name,
		Namespace:       r.Namespace,
		Quotas:          quotas,
		Status:          StatusActive,
		NeonProjectID:   r.NeonProjectID,
		Region:          r.Region,
		DBSchemaVersion: r.DBSchemaVersion,
		CreatedAt:       r.CreatedAt,
		UpdatedAt:       r.UpdatedAt,
	}
}
