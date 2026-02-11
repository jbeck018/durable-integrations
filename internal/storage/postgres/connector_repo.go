package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/lib/pq"
)

// Connector represents a connector registration in the database.
type Connector struct {
	ID              string    `json:"id"`
	TenantID        string    `json:"tenant_id"`
	Name            string    `json:"name"`
	ConnectorType   string    `json:"connector_type"`
	ConfigEncrypted []byte    `json:"config_encrypted,omitempty"`
	Status          string    `json:"status"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// ConnectorRepo provides CRUD operations for the connectors table.
type ConnectorRepo struct {
	db *DB
}

// NewConnectorRepo creates a ConnectorRepo backed by the given connection pool.
func NewConnectorRepo(db *DB) *ConnectorRepo {
	return &ConnectorRepo{db: db}
}

const connectorColumns = `id, tenant_id, name, connector_type, config_encrypted, status, created_at, updated_at`

// scanConnector scans a row into a Connector struct.
func scanConnector(row interface{ Scan(dest ...interface{}) error }) (*Connector, error) {
	c := &Connector{}
	err := row.Scan(
		&c.ID,
		&c.TenantID,
		&c.Name,
		&c.ConnectorType,
		&c.ConfigEncrypted,
		&c.Status,
		&c.CreatedAt,
		&c.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return c, nil
}

// Create inserts a new connector and returns it with server-generated fields.
func (r *ConnectorRepo) Create(ctx context.Context, c *Connector) (*Connector, error) {
	if c.ID == "" {
		c.ID = newID()
	}
	if c.Status == "" {
		c.Status = "active"
	}

	var result *Connector
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO connectors (id, tenant_id, name, connector_type, config_encrypted, status)
		 VALUES ($1, $2, $3, $4, $5, $6::connector_status)
		 RETURNING `+connectorColumns,
		[]interface{}{c.ID, c.TenantID, c.Name, c.ConnectorType, c.ConfigEncrypted, c.Status},
		func(row *sql.Row) error {
			var err error
			result, err = scanConnector(row)
			return err
		},
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, fmt.Errorf("connector %q for tenant %s: %w", c.Name, c.TenantID, common.ErrAlreadyExists)
		}
		return nil, err
	}
	return result, nil
}

// GetByID retrieves a connector by its UUID.
func (r *ConnectorRepo) GetByID(ctx context.Context, id string) (*Connector, error) {
	var result *Connector
	err := queryRow(ctx, r.db.pool,
		`SELECT `+connectorColumns+` FROM connectors WHERE id = $1`,
		[]interface{}{id},
		func(row *sql.Row) error {
			var err error
			result, err = scanConnector(row)
			return err
		},
	)
	return result, err
}

// ListByTenant returns all connectors for a given tenant.
func (r *ConnectorRepo) ListByTenant(ctx context.Context, tenantID string, limit, offset int) ([]*Connector, error) {
	query := `SELECT ` + connectorColumns + ` FROM connectors WHERE tenant_id = $1 ORDER BY created_at DESC`
	args := []interface{}{tenantID}
	argIdx := 2

	if limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, limit)
		argIdx++
	}
	if offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, offset)
	}

	var connectors []*Connector
	err := queryRows(ctx, r.db.pool, query, args, func(rows *sql.Rows) error {
		c, err := scanConnector(rows)
		if err != nil {
			return err
		}
		connectors = append(connectors, c)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connectors, nil
}

// Update modifies an existing connector.
func (r *ConnectorRepo) Update(ctx context.Context, c *Connector) (*Connector, error) {
	var result *Connector
	err := queryRow(ctx, r.db.pool,
		`UPDATE connectors
		 SET name = $2, connector_type = $3, config_encrypted = $4, status = $5::connector_status
		 WHERE id = $1
		 RETURNING `+connectorColumns,
		[]interface{}{c.ID, c.Name, c.ConnectorType, c.ConfigEncrypted, c.Status},
		func(row *sql.Row) error {
			var err error
			result, err = scanConnector(row)
			return err
		},
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, fmt.Errorf("connector %q: %w", c.Name, common.ErrAlreadyExists)
		}
		return nil, err
	}
	return result, err
}

// Delete removes a connector by ID.
func (r *ConnectorRepo) Delete(ctx context.Context, id string) error {
	return execExpectOne(ctx, r.db.pool,
		`DELETE FROM connectors WHERE id = $1`, id,
	)
}
