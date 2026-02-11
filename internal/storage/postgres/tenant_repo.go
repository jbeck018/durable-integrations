package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/lib/pq"
)

// Tenant represents a FlowForge tenant in the database.
type Tenant struct {
	ID             string          `json:"id"`
	Name           string          `json:"name"`
	Namespace      string          `json:"namespace"`
	Config         json.RawMessage `json:"config"`
	ResourceQuotas json.RawMessage `json:"resource_quotas"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

// TenantRepo provides CRUD operations for the tenants table.
type TenantRepo struct {
	db *DB
}

// NewTenantRepo creates a TenantRepo backed by the given connection pool.
func NewTenantRepo(db *DB) *TenantRepo {
	return &TenantRepo{db: db}
}

// scanTenant scans a single tenant row into a Tenant struct.
func scanTenant(row interface{ Scan(dest ...interface{}) error }) (*Tenant, error) {
	t := &Tenant{}
	err := row.Scan(
		&t.ID,
		&t.Name,
		&t.Namespace,
		&t.Config,
		&t.ResourceQuotas,
		&t.CreatedAt,
		&t.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return t, nil
}

const tenantColumns = `id, name, namespace, config, resource_quotas, created_at, updated_at`

// Create inserts a new tenant and returns it with server-generated fields populated.
func (r *TenantRepo) Create(ctx context.Context, t *Tenant) (*Tenant, error) {
	if t.ID == "" {
		t.ID = newID()
	}
	if t.Config == nil {
		t.Config = json.RawMessage("{}")
	}
	if t.ResourceQuotas == nil {
		t.ResourceQuotas = json.RawMessage("{}")
	}

	result := &Tenant{}
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO tenants (id, name, namespace, config, resource_quotas)
		 VALUES ($1, $2, $3, $4, $5)
		 RETURNING `+tenantColumns,
		[]interface{}{t.ID, t.Name, t.Namespace, t.Config, t.ResourceQuotas},
		func(row *sql.Row) error {
			var err error
			result, err = scanTenant(row)
			return err
		},
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, fmt.Errorf("tenant namespace %q: %w", t.Namespace, common.ErrAlreadyExists)
		}
		return nil, err
	}
	return result, nil
}

// GetByID retrieves a tenant by its UUID.
func (r *TenantRepo) GetByID(ctx context.Context, id string) (*Tenant, error) {
	var result *Tenant
	err := queryRow(ctx, r.db.pool,
		`SELECT `+tenantColumns+` FROM tenants WHERE id = $1`,
		[]interface{}{id},
		func(row *sql.Row) error {
			var err error
			result, err = scanTenant(row)
			return err
		},
	)
	return result, err
}

// GetByNamespace retrieves a tenant by its unique namespace.
func (r *TenantRepo) GetByNamespace(ctx context.Context, namespace string) (*Tenant, error) {
	var result *Tenant
	err := queryRow(ctx, r.db.pool,
		`SELECT `+tenantColumns+` FROM tenants WHERE namespace = $1`,
		[]interface{}{namespace},
		func(row *sql.Row) error {
			var err error
			result, err = scanTenant(row)
			return err
		},
	)
	return result, err
}

// List returns all tenants, ordered by creation time descending.
// limit and offset provide pagination; use limit=0 for no limit.
func (r *TenantRepo) List(ctx context.Context, limit, offset int) ([]*Tenant, error) {
	query := `SELECT ` + tenantColumns + ` FROM tenants ORDER BY created_at DESC`
	args := make([]interface{}, 0, 2)
	argIdx := 1

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, limit)
		argIdx++
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, offset)
	}

	var tenants []*Tenant
	err := queryRows(ctx, r.db.pool, query, args, func(rows *sql.Rows) error {
		t, err := scanTenant(rows)
		if err != nil {
			return err
		}
		tenants = append(tenants, t)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return tenants, nil
}

// Update modifies an existing tenant. Only non-zero fields are updated.
func (r *TenantRepo) Update(ctx context.Context, t *Tenant) (*Tenant, error) {
	var result *Tenant
	err := queryRow(ctx, r.db.pool,
		`UPDATE tenants
		 SET name = $2, namespace = $3, config = $4, resource_quotas = $5
		 WHERE id = $1
		 RETURNING `+tenantColumns,
		[]interface{}{t.ID, t.Name, t.Namespace, t.Config, t.ResourceQuotas},
		func(row *sql.Row) error {
			var err error
			result, err = scanTenant(row)
			return err
		},
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, fmt.Errorf("tenant namespace %q: %w", t.Namespace, common.ErrAlreadyExists)
		}
		return nil, err
	}
	return result, err
}

// Delete removes a tenant by ID. Returns ErrNotFound if no such tenant exists.
func (r *TenantRepo) Delete(ctx context.Context, id string) error {
	return execExpectOne(ctx, r.db.pool,
		`DELETE FROM tenants WHERE id = $1`, id,
	)
}
