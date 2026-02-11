package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Connection represents a configured connection to an external system.
type Connection struct {
	ID             string     `json:"id"`
	TenantID       string     `json:"tenant_id"`
	ConnectorID    string     `json:"connector_id"`
	AuthType       string     `json:"auth_type"`
	CredentialsRef string     `json:"credentials_ref,omitempty"`
	Status         string     `json:"status"`
	LastCheckedAt  *time.Time `json:"last_checked_at,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
}

// ConnectionRepo provides CRUD operations for the connections table.
type ConnectionRepo struct {
	db *DB
}

// NewConnectionRepo creates a ConnectionRepo backed by the given connection pool.
func NewConnectionRepo(db *DB) *ConnectionRepo {
	return &ConnectionRepo{db: db}
}

const connectionColumns = `id, tenant_id, connector_id, auth_type, credentials_ref, status, last_checked_at, created_at, updated_at`

// scanConnection scans a row into a Connection struct.
func scanConnection(row interface {
	Scan(dest ...interface{}) error
}) (*Connection, error) {
	c := &Connection{}
	var credRef sql.NullString
	var lastChecked sql.NullTime
	err := row.Scan(
		&c.ID,
		&c.TenantID,
		&c.ConnectorID,
		&c.AuthType,
		&credRef,
		&c.Status,
		&lastChecked,
		&c.CreatedAt,
		&c.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	c.CredentialsRef = stringFromNull(credRef)
	c.LastCheckedAt = timePtr(lastChecked)
	return c, nil
}

// Create inserts a new connection and returns it with server-generated fields.
func (r *ConnectionRepo) Create(ctx context.Context, c *Connection) (*Connection, error) {
	if c.ID == "" {
		c.ID = newID()
	}
	if c.Status == "" {
		c.Status = "pending"
	}
	if c.AuthType == "" {
		c.AuthType = "none"
	}

	var result *Connection
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO connections (id, tenant_id, connector_id, auth_type, credentials_ref, status)
		 VALUES ($1, $2, $3, $4::auth_type, $5, $6::connection_status)
		 RETURNING `+connectionColumns,
		[]interface{}{c.ID, c.TenantID, c.ConnectorID, c.AuthType, nullString(c.CredentialsRef), c.Status},
		func(row *sql.Row) error {
			var err error
			result, err = scanConnection(row)
			return err
		},
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23503" {
			return nil, fmt.Errorf("connection references invalid connector or tenant: %w", err)
		}
		return nil, err
	}
	return result, nil
}

// GetByID retrieves a connection by its UUID.
func (r *ConnectionRepo) GetByID(ctx context.Context, id string) (*Connection, error) {
	var result *Connection
	err := queryRow(ctx, r.db.pool,
		`SELECT `+connectionColumns+` FROM connections WHERE id = $1`,
		[]interface{}{id},
		func(row *sql.Row) error {
			var err error
			result, err = scanConnection(row)
			return err
		},
	)
	return result, err
}

// ListByTenant returns all connections for a given tenant.
func (r *ConnectionRepo) ListByTenant(ctx context.Context, tenantID string, limit, offset int) ([]*Connection, error) {
	query := `SELECT ` + connectionColumns + ` FROM connections WHERE tenant_id = $1 ORDER BY created_at DESC`
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

	var connections []*Connection
	err := queryRows(ctx, r.db.pool, query, args, func(rows *sql.Rows) error {
		c, err := scanConnection(rows)
		if err != nil {
			return err
		}
		connections = append(connections, c)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return connections, nil
}

// ListByConnector returns all connections for a given connector.
func (r *ConnectionRepo) ListByConnector(ctx context.Context, connectorID string) ([]*Connection, error) {
	var connections []*Connection
	err := queryRows(ctx, r.db.pool,
		`SELECT `+connectionColumns+` FROM connections WHERE connector_id = $1 ORDER BY created_at DESC`,
		[]interface{}{connectorID},
		func(rows *sql.Rows) error {
			c, err := scanConnection(rows)
			if err != nil {
				return err
			}
			connections = append(connections, c)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return connections, nil
}

// Update modifies an existing connection.
func (r *ConnectionRepo) Update(ctx context.Context, c *Connection) (*Connection, error) {
	var result *Connection
	err := queryRow(ctx, r.db.pool,
		`UPDATE connections
		 SET connector_id = $2, auth_type = $3::auth_type, credentials_ref = $4,
		     status = $5::connection_status, last_checked_at = $6
		 WHERE id = $1
		 RETURNING `+connectionColumns,
		[]interface{}{c.ID, c.ConnectorID, c.AuthType, nullString(c.CredentialsRef), c.Status, nullTimePtr(c.LastCheckedAt)},
		func(row *sql.Row) error {
			var err error
			result, err = scanConnection(row)
			return err
		},
	)
	return result, err
}

// UpdateStatus updates only the status and last_checked_at fields.
func (r *ConnectionRepo) UpdateStatus(ctx context.Context, id, status string) error {
	return execExpectOne(ctx, r.db.pool,
		`UPDATE connections SET status = $2::connection_status, last_checked_at = now() WHERE id = $1`,
		id, status,
	)
}

// Delete removes a connection by ID.
func (r *ConnectionRepo) Delete(ctx context.Context, id string) error {
	return execExpectOne(ctx, r.db.pool,
		`DELETE FROM connections WHERE id = $1`, id,
	)
}
