package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// AuditEntry represents a single entry in the append-only audit log.
type AuditEntry struct {
	ID           string          `json:"id"`
	TenantID     string          `json:"tenant_id"`
	UserID       string          `json:"user_id"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id"`
	Details      json.RawMessage `json:"details"`
	CreatedAt    time.Time       `json:"created_at"`
}

// AuditRepo provides append and query operations for the audit_log table.
// The audit log is append-only: entries cannot be updated or deleted.
type AuditRepo struct {
	db *DB
}

// NewAuditRepo creates an AuditRepo backed by the given connection pool.
func NewAuditRepo(db *DB) *AuditRepo {
	return &AuditRepo{db: db}
}

const auditColumns = `id, tenant_id, user_id, action, resource_type, resource_id, details, created_at`

// scanAuditEntry scans a row into an AuditEntry struct.
func scanAuditEntry(row interface {
	Scan(dest ...interface{}) error
}) (*AuditEntry, error) {
	e := &AuditEntry{}
	err := row.Scan(
		&e.ID,
		&e.TenantID,
		&e.UserID,
		&e.Action,
		&e.ResourceType,
		&e.ResourceID,
		&e.Details,
		&e.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return e, nil
}

// Append inserts a new audit log entry and returns it with server-generated fields.
func (r *AuditRepo) Append(ctx context.Context, entry *AuditEntry) (*AuditEntry, error) {
	if entry.ID == "" {
		entry.ID = newID()
	}
	if entry.Details == nil {
		entry.Details = json.RawMessage("{}")
	}

	var result *AuditEntry
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO audit_log (id, tenant_id, user_id, action, resource_type, resource_id, details)
		 VALUES ($1, $2, $3, $4, $5, $6, $7)
		 RETURNING `+auditColumns,
		[]interface{}{entry.ID, entry.TenantID, entry.UserID, entry.Action, entry.ResourceType, entry.ResourceID, entry.Details},
		func(row *sql.Row) error {
			var err error
			result, err = scanAuditEntry(row)
			return err
		},
	)
	return result, err
}

// AppendBatch inserts multiple audit entries in a single transaction.
func (r *AuditRepo) AppendBatch(ctx context.Context, entries []*AuditEntry) ([]*AuditEntry, error) {
	var results []*AuditEntry
	err := r.db.WithTx(ctx, func(tx *Tx) error {
		for _, entry := range entries {
			if entry.ID == "" {
				entry.ID = newID()
			}
			if entry.Details == nil {
				entry.Details = json.RawMessage("{}")
			}

			var result *AuditEntry
			err := queryRow(ctx, tx.tx,
				`INSERT INTO audit_log (id, tenant_id, user_id, action, resource_type, resource_id, details)
				 VALUES ($1, $2, $3, $4, $5, $6, $7)
				 RETURNING `+auditColumns,
				[]interface{}{entry.ID, entry.TenantID, entry.UserID, entry.Action, entry.ResourceType, entry.ResourceID, entry.Details},
				func(row *sql.Row) error {
					var err error
					result, err = scanAuditEntry(row)
					return err
				},
			)
			if err != nil {
				return err
			}
			results = append(results, result)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return results, nil
}

// AuditFilter specifies criteria for querying audit log entries.
type AuditFilter struct {
	TenantID     string
	UserID       string
	Action       string
	ResourceType string
	ResourceID   string
	Since        *time.Time
	Until        *time.Time
	Limit        int
	Offset       int
}

// List queries audit log entries matching the given filter.
// Results are returned in reverse chronological order.
func (r *AuditRepo) List(ctx context.Context, filter AuditFilter) ([]*AuditEntry, error) {
	query := `SELECT ` + auditColumns + ` FROM audit_log WHERE 1=1`
	args := make([]interface{}, 0, 8)
	argIdx := 1

	if filter.TenantID != "" {
		query += fmt.Sprintf(" AND tenant_id = $%d", argIdx)
		args = append(args, filter.TenantID)
		argIdx++
	}
	if filter.UserID != "" {
		query += fmt.Sprintf(" AND user_id = $%d", argIdx)
		args = append(args, filter.UserID)
		argIdx++
	}
	if filter.Action != "" {
		query += fmt.Sprintf(" AND action = $%d", argIdx)
		args = append(args, filter.Action)
		argIdx++
	}
	if filter.ResourceType != "" {
		query += fmt.Sprintf(" AND resource_type = $%d", argIdx)
		args = append(args, filter.ResourceType)
		argIdx++
	}
	if filter.ResourceID != "" {
		query += fmt.Sprintf(" AND resource_id = $%d", argIdx)
		args = append(args, filter.ResourceID)
		argIdx++
	}
	if filter.Since != nil {
		query += fmt.Sprintf(" AND created_at >= $%d", argIdx)
		args = append(args, *filter.Since)
		argIdx++
	}
	if filter.Until != nil {
		query += fmt.Sprintf(" AND created_at <= $%d", argIdx)
		args = append(args, *filter.Until)
		argIdx++
	}

	query += " ORDER BY created_at DESC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIdx)
		args = append(args, filter.Limit)
		argIdx++
	}
	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, filter.Offset)
	}

	var entries []*AuditEntry
	err := queryRows(ctx, r.db.pool, query, args, func(rows *sql.Rows) error {
		e, err := scanAuditEntry(rows)
		if err != nil {
			return err
		}
		entries = append(entries, e)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return entries, nil
}

// GetByID retrieves a single audit entry by its UUID.
func (r *AuditRepo) GetByID(ctx context.Context, id string) (*AuditEntry, error) {
	var result *AuditEntry
	err := queryRow(ctx, r.db.pool,
		`SELECT `+auditColumns+` FROM audit_log WHERE id = $1`,
		[]interface{}{id},
		func(row *sql.Row) error {
			var err error
			result, err = scanAuditEntry(row)
			return err
		},
	)
	return result, err
}

// CountByTenant returns the total number of audit entries for a tenant.
func (r *AuditRepo) CountByTenant(ctx context.Context, tenantID string) (int64, error) {
	var count int64
	err := queryRow(ctx, r.db.pool,
		`SELECT COUNT(*) FROM audit_log WHERE tenant_id = $1`,
		[]interface{}{tenantID},
		func(row *sql.Row) error {
			return row.Scan(&count)
		},
	)
	return count, err
}
