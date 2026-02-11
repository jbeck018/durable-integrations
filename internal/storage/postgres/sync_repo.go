package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// Sync represents a data synchronization configuration.
type Sync struct {
	ID                 string          `json:"id"`
	TenantID           string          `json:"tenant_id"`
	SourceConnectionID string          `json:"source_connection_id"`
	DestConnectionID   string          `json:"dest_connection_id"`
	Config             json.RawMessage `json:"config"`
	Schedule           json.RawMessage `json:"schedule"`
	Status             string          `json:"status"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

// SyncRun represents a single execution of a sync.
type SyncRun struct {
	ID                  string          `json:"id"`
	SyncID              string          `json:"sync_id"`
	TenantID            string          `json:"tenant_id"`
	Status              string          `json:"status"`
	RecordsExtracted    int64           `json:"records_extracted"`
	RecordsTransformed  int64           `json:"records_transformed"`
	RecordsLoaded       int64           `json:"records_loaded"`
	Errors              json.RawMessage `json:"errors"`
	StartedAt           *time.Time      `json:"started_at,omitempty"`
	CompletedAt         *time.Time      `json:"completed_at,omitempty"`
}

// SyncState represents checkpoint state for incremental sync resumption.
type SyncState struct {
	ID         string          `json:"id"`
	SyncID     string          `json:"sync_id"`
	StreamName string          `json:"stream_name"`
	StateData  json.RawMessage `json:"state_data"`
	UpdatedAt  time.Time       `json:"updated_at"`
}

// SyncRepo provides CRUD operations for syncs, sync_runs, and sync_state tables.
type SyncRepo struct {
	db *DB
}

// NewSyncRepo creates a SyncRepo backed by the given connection pool.
func NewSyncRepo(db *DB) *SyncRepo {
	return &SyncRepo{db: db}
}

const syncColumns = `id, tenant_id, source_connection_id, dest_connection_id, config, schedule, status, created_at, updated_at`

// scanSync scans a row into a Sync struct.
func scanSync(row interface{ Scan(dest ...interface{}) error }) (*Sync, error) {
	s := &Sync{}
	err := row.Scan(
		&s.ID,
		&s.TenantID,
		&s.SourceConnectionID,
		&s.DestConnectionID,
		&s.Config,
		&s.Schedule,
		&s.Status,
		&s.CreatedAt,
		&s.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return s, nil
}

const syncRunColumns = `id, sync_id, tenant_id, status, records_extracted, records_transformed, records_loaded, errors, started_at, completed_at`

// scanSyncRun scans a row into a SyncRun struct.
func scanSyncRun(row interface{ Scan(dest ...interface{}) error }) (*SyncRun, error) {
	r := &SyncRun{}
	var startedAt, completedAt sql.NullTime
	err := row.Scan(
		&r.ID,
		&r.SyncID,
		&r.TenantID,
		&r.Status,
		&r.RecordsExtracted,
		&r.RecordsTransformed,
		&r.RecordsLoaded,
		&r.Errors,
		&startedAt,
		&completedAt,
	)
	if err != nil {
		return nil, err
	}
	r.StartedAt = timePtr(startedAt)
	r.CompletedAt = timePtr(completedAt)
	return r, nil
}

const syncStateColumns = `id, sync_id, stream_name, state_data, updated_at`

// scanSyncState scans a row into a SyncState struct.
func scanSyncState(row interface{ Scan(dest ...interface{}) error }) (*SyncState, error) {
	st := &SyncState{}
	err := row.Scan(
		&st.ID,
		&st.SyncID,
		&st.StreamName,
		&st.StateData,
		&st.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	return st, nil
}

// -------------------------------------------------------------------------
// Sync CRUD
// -------------------------------------------------------------------------

// Create inserts a new sync and returns it with server-generated fields.
func (r *SyncRepo) Create(ctx context.Context, s *Sync) (*Sync, error) {
	if s.ID == "" {
		s.ID = newID()
	}
	if s.Status == "" {
		s.Status = "active"
	}
	if s.Config == nil {
		s.Config = json.RawMessage("{}")
	}
	if s.Schedule == nil {
		s.Schedule = json.RawMessage("{}")
	}

	var result *Sync
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO syncs (id, tenant_id, source_connection_id, dest_connection_id, config, schedule, status)
		 VALUES ($1, $2, $3, $4, $5, $6, $7::sync_status)
		 RETURNING `+syncColumns,
		[]interface{}{s.ID, s.TenantID, s.SourceConnectionID, s.DestConnectionID, s.Config, s.Schedule, s.Status},
		func(row *sql.Row) error {
			var err error
			result, err = scanSync(row)
			return err
		},
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23503" {
			return nil, fmt.Errorf("sync references invalid connection or tenant: %w", pqErr)
		}
		return nil, err
	}
	return result, nil
}

// GetByID retrieves a sync by its UUID.
func (r *SyncRepo) GetByID(ctx context.Context, id string) (*Sync, error) {
	var result *Sync
	err := queryRow(ctx, r.db.pool,
		`SELECT `+syncColumns+` FROM syncs WHERE id = $1`,
		[]interface{}{id},
		func(row *sql.Row) error {
			var err error
			result, err = scanSync(row)
			return err
		},
	)
	return result, err
}

// ListByTenant returns all syncs for a given tenant.
func (r *SyncRepo) ListByTenant(ctx context.Context, tenantID string, limit, offset int) ([]*Sync, error) {
	query := `SELECT ` + syncColumns + ` FROM syncs WHERE tenant_id = $1 ORDER BY created_at DESC`
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

	var syncs []*Sync
	err := queryRows(ctx, r.db.pool, query, args, func(rows *sql.Rows) error {
		s, err := scanSync(rows)
		if err != nil {
			return err
		}
		syncs = append(syncs, s)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return syncs, nil
}

// Update modifies an existing sync.
func (r *SyncRepo) Update(ctx context.Context, s *Sync) (*Sync, error) {
	var result *Sync
	err := queryRow(ctx, r.db.pool,
		`UPDATE syncs
		 SET source_connection_id = $2, dest_connection_id = $3,
		     config = $4, schedule = $5, status = $6::sync_status
		 WHERE id = $1
		 RETURNING `+syncColumns,
		[]interface{}{s.ID, s.SourceConnectionID, s.DestConnectionID, s.Config, s.Schedule, s.Status},
		func(row *sql.Row) error {
			var err error
			result, err = scanSync(row)
			return err
		},
	)
	return result, err
}

// Delete removes a sync by ID.
func (r *SyncRepo) Delete(ctx context.Context, id string) error {
	return execExpectOne(ctx, r.db.pool,
		`DELETE FROM syncs WHERE id = $1`, id,
	)
}

// -------------------------------------------------------------------------
// SyncRun operations
// -------------------------------------------------------------------------

// CreateRun inserts a new sync run record.
func (r *SyncRepo) CreateRun(ctx context.Context, run *SyncRun) (*SyncRun, error) {
	if run.ID == "" {
		run.ID = newID()
	}
	if run.Status == "" {
		run.Status = "pending"
	}
	if run.Errors == nil {
		run.Errors = json.RawMessage("[]")
	}

	var result *SyncRun
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO sync_runs (id, sync_id, tenant_id, status, records_extracted, records_transformed, records_loaded, errors, started_at, completed_at)
		 VALUES ($1, $2, $3, $4::sync_run_status, $5, $6, $7, $8, $9, $10)
		 RETURNING `+syncRunColumns,
		[]interface{}{
			run.ID, run.SyncID, run.TenantID, run.Status,
			run.RecordsExtracted, run.RecordsTransformed, run.RecordsLoaded,
			run.Errors, nullTimePtr(run.StartedAt), nullTimePtr(run.CompletedAt),
		},
		func(row *sql.Row) error {
			var err error
			result, err = scanSyncRun(row)
			return err
		},
	)
	return result, err
}

// UpdateRun updates a sync run record (typically to update counts or status).
func (r *SyncRepo) UpdateRun(ctx context.Context, run *SyncRun) (*SyncRun, error) {
	var result *SyncRun
	err := queryRow(ctx, r.db.pool,
		`UPDATE sync_runs
		 SET status = $2::sync_run_status, records_extracted = $3,
		     records_transformed = $4, records_loaded = $5,
		     errors = $6, started_at = $7, completed_at = $8
		 WHERE id = $1
		 RETURNING `+syncRunColumns,
		[]interface{}{
			run.ID, run.Status,
			run.RecordsExtracted, run.RecordsTransformed, run.RecordsLoaded,
			run.Errors, nullTimePtr(run.StartedAt), nullTimePtr(run.CompletedAt),
		},
		func(row *sql.Row) error {
			var err error
			result, err = scanSyncRun(row)
			return err
		},
	)
	return result, err
}

// GetLatestRun returns the most recent sync run for a given sync.
func (r *SyncRepo) GetLatestRun(ctx context.Context, syncID string) (*SyncRun, error) {
	var result *SyncRun
	err := queryRow(ctx, r.db.pool,
		`SELECT `+syncRunColumns+` FROM sync_runs
		 WHERE sync_id = $1
		 ORDER BY started_at DESC NULLS LAST
		 LIMIT 1`,
		[]interface{}{syncID},
		func(row *sql.Row) error {
			var err error
			result, err = scanSyncRun(row)
			return err
		},
	)
	return result, err
}

// ListRuns returns sync runs for a given sync ID, ordered by start time descending.
func (r *SyncRepo) ListRuns(ctx context.Context, syncID string, limit, offset int) ([]*SyncRun, error) {
	query := `SELECT ` + syncRunColumns + ` FROM sync_runs WHERE sync_id = $1 ORDER BY started_at DESC NULLS LAST`
	args := []interface{}{syncID}
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

	var runs []*SyncRun
	err := queryRows(ctx, r.db.pool, query, args, func(rows *sql.Rows) error {
		run, err := scanSyncRun(rows)
		if err != nil {
			return err
		}
		runs = append(runs, run)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return runs, nil
}

// -------------------------------------------------------------------------
// SyncState operations (checkpoint / incremental state)
// -------------------------------------------------------------------------

// SaveState upserts the checkpoint state for a sync+stream combination.
// Uses ON CONFLICT to atomically insert or update.
func (r *SyncRepo) SaveState(ctx context.Context, st *SyncState) (*SyncState, error) {
	if st.ID == "" {
		st.ID = newID()
	}
	if st.StateData == nil {
		st.StateData = json.RawMessage("{}")
	}

	var result *SyncState
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO sync_state (id, sync_id, stream_name, state_data)
		 VALUES ($1, $2, $3, $4)
		 ON CONFLICT (sync_id, stream_name)
		 DO UPDATE SET state_data = EXCLUDED.state_data, updated_at = now()
		 RETURNING `+syncStateColumns,
		[]interface{}{st.ID, st.SyncID, st.StreamName, st.StateData},
		func(row *sql.Row) error {
			var err error
			result, err = scanSyncState(row)
			return err
		},
	)
	return result, err
}

// GetState retrieves the checkpoint state for a sync+stream combination.
func (r *SyncRepo) GetState(ctx context.Context, syncID, streamName string) (*SyncState, error) {
	var result *SyncState
	err := queryRow(ctx, r.db.pool,
		`SELECT `+syncStateColumns+` FROM sync_state
		 WHERE sync_id = $1 AND stream_name = $2`,
		[]interface{}{syncID, streamName},
		func(row *sql.Row) error {
			var err error
			result, err = scanSyncState(row)
			return err
		},
	)
	return result, err
}
