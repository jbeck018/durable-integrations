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

// Stream represents a discovered data stream for a connection.
type Stream struct {
	ID                 string          `json:"id"`
	ConnectionID       string          `json:"connection_id"`
	Name               string          `json:"name"`
	Namespace          string          `json:"namespace,omitempty"`
	Schema             json.RawMessage `json:"schema"`
	SupportedSyncModes []string        `json:"supported_sync_modes"`
	DiscoveredAt       time.Time       `json:"discovered_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

// SchemaVersion represents a versioned snapshot of a stream's schema.
type SchemaVersion struct {
	ID        string          `json:"id"`
	StreamID  string          `json:"stream_id"`
	Version   int             `json:"version"`
	Schema    json.RawMessage `json:"schema"`
	Diff      json.RawMessage `json:"diff"`
	CreatedAt time.Time       `json:"created_at"`
}

// StreamRepo provides CRUD operations for streams and schema_versions tables.
type StreamRepo struct {
	db *DB
}

// NewStreamRepo creates a StreamRepo backed by the given connection pool.
func NewStreamRepo(db *DB) *StreamRepo {
	return &StreamRepo{db: db}
}

const streamColumns = `id, connection_id, name, namespace, schema, supported_sync_modes, discovered_at, updated_at`

// scanStream scans a row into a Stream struct.
func scanStream(row interface{ Scan(dest ...interface{}) error }) (*Stream, error) {
	s := &Stream{}
	var ns sql.NullString
	err := row.Scan(
		&s.ID,
		&s.ConnectionID,
		&s.Name,
		&ns,
		&s.Schema,
		pq.Array(&s.SupportedSyncModes),
		&s.DiscoveredAt,
		&s.UpdatedAt,
	)
	if err != nil {
		return nil, err
	}
	s.Namespace = stringFromNull(ns)
	return s, nil
}

const schemaVersionColumns = `id, stream_id, version, schema, diff, created_at`

// scanSchemaVersion scans a row into a SchemaVersion struct.
func scanSchemaVersion(row interface{ Scan(dest ...interface{}) error }) (*SchemaVersion, error) {
	sv := &SchemaVersion{}
	err := row.Scan(
		&sv.ID,
		&sv.StreamID,
		&sv.Version,
		&sv.Schema,
		&sv.Diff,
		&sv.CreatedAt,
	)
	if err != nil {
		return nil, err
	}
	return sv, nil
}

// -------------------------------------------------------------------------
// Stream CRUD
// -------------------------------------------------------------------------

// Create inserts a new stream. If a stream with the same connection_id+name+namespace
// already exists, it returns ErrAlreadyExists.
func (r *StreamRepo) Create(ctx context.Context, s *Stream) (*Stream, error) {
	if s.ID == "" {
		s.ID = newID()
	}
	if s.Schema == nil {
		s.Schema = json.RawMessage("{}")
	}
	if s.SupportedSyncModes == nil {
		s.SupportedSyncModes = []string{"full_refresh"}
	}

	var result *Stream
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO streams (id, connection_id, name, namespace, schema, supported_sync_modes)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING `+streamColumns,
		[]interface{}{s.ID, s.ConnectionID, s.Name, nullString(s.Namespace), s.Schema, pq.Array(s.SupportedSyncModes)},
		func(row *sql.Row) error {
			var err error
			result, err = scanStream(row)
			return err
		},
	)
	if err != nil {
		if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
			return nil, fmt.Errorf("stream %q in connection %s: %w", s.Name, s.ConnectionID, common.ErrAlreadyExists)
		}
		return nil, err
	}
	return result, nil
}

// GetByID retrieves a stream by its UUID.
func (r *StreamRepo) GetByID(ctx context.Context, id string) (*Stream, error) {
	var result *Stream
	err := queryRow(ctx, r.db.pool,
		`SELECT `+streamColumns+` FROM streams WHERE id = $1`,
		[]interface{}{id},
		func(row *sql.Row) error {
			var err error
			result, err = scanStream(row)
			return err
		},
	)
	return result, err
}

// ListByConnection returns all streams for a given connection.
func (r *StreamRepo) ListByConnection(ctx context.Context, connectionID string) ([]*Stream, error) {
	var streams []*Stream
	err := queryRows(ctx, r.db.pool,
		`SELECT `+streamColumns+` FROM streams WHERE connection_id = $1 ORDER BY name`,
		[]interface{}{connectionID},
		func(rows *sql.Rows) error {
			s, err := scanStream(rows)
			if err != nil {
				return err
			}
			streams = append(streams, s)
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	return streams, nil
}

// Update modifies a stream's schema and supported sync modes.
func (r *StreamRepo) Update(ctx context.Context, s *Stream) (*Stream, error) {
	var result *Stream
	err := queryRow(ctx, r.db.pool,
		`UPDATE streams
		 SET name = $2, namespace = $3, schema = $4, supported_sync_modes = $5
		 WHERE id = $1
		 RETURNING `+streamColumns,
		[]interface{}{s.ID, s.Name, nullString(s.Namespace), s.Schema, pq.Array(s.SupportedSyncModes)},
		func(row *sql.Row) error {
			var err error
			result, err = scanStream(row)
			return err
		},
	)
	return result, err
}

// Delete removes a stream by ID.
func (r *StreamRepo) Delete(ctx context.Context, id string) error {
	return execExpectOne(ctx, r.db.pool,
		`DELETE FROM streams WHERE id = $1`, id,
	)
}

// UpsertBatch inserts or updates multiple streams in a single transaction.
// This is used during catalog discovery to efficiently persist all discovered streams.
func (r *StreamRepo) UpsertBatch(ctx context.Context, streams []*Stream) ([]*Stream, error) {
	var results []*Stream
	err := r.db.WithTx(ctx, func(tx *Tx) error {
		for _, s := range streams {
			if s.ID == "" {
				s.ID = newID()
			}
			if s.Schema == nil {
				s.Schema = json.RawMessage("{}")
			}
			if s.SupportedSyncModes == nil {
				s.SupportedSyncModes = []string{"full_refresh"}
			}

			var result *Stream
			err := queryRow(ctx, tx.tx,
				`INSERT INTO streams (id, connection_id, name, namespace, schema, supported_sync_modes)
				 VALUES ($1, $2, $3, $4, $5, $6)
				 ON CONFLICT (connection_id, name, namespace)
				 DO UPDATE SET schema = EXCLUDED.schema,
				              supported_sync_modes = EXCLUDED.supported_sync_modes
				 RETURNING `+streamColumns,
				[]interface{}{s.ID, s.ConnectionID, s.Name, nullString(s.Namespace), s.Schema, pq.Array(s.SupportedSyncModes)},
				func(row *sql.Row) error {
					var err error
					result, err = scanStream(row)
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

// -------------------------------------------------------------------------
// SchemaVersion operations
// -------------------------------------------------------------------------

// CreateVersion inserts a new schema version for a stream.
// The version number is auto-incremented based on the highest existing version.
func (r *StreamRepo) CreateVersion(ctx context.Context, sv *SchemaVersion) (*SchemaVersion, error) {
	if sv.ID == "" {
		sv.ID = newID()
	}
	if sv.Diff == nil {
		sv.Diff = json.RawMessage("{}")
	}

	var result *SchemaVersion
	err := queryRow(ctx, r.db.pool,
		`INSERT INTO schema_versions (id, stream_id, version, schema, diff)
		 VALUES ($1, $2,
		   COALESCE((SELECT MAX(version) FROM schema_versions WHERE stream_id = $2), 0) + 1,
		   $3, $4)
		 RETURNING `+schemaVersionColumns,
		[]interface{}{sv.ID, sv.StreamID, sv.Schema, sv.Diff},
		func(row *sql.Row) error {
			var err error
			result, err = scanSchemaVersion(row)
			return err
		},
	)
	return result, err
}

// GetLatestVersion returns the most recent schema version for a stream.
func (r *StreamRepo) GetLatestVersion(ctx context.Context, streamID string) (*SchemaVersion, error) {
	var result *SchemaVersion
	err := queryRow(ctx, r.db.pool,
		`SELECT `+schemaVersionColumns+` FROM schema_versions
		 WHERE stream_id = $1
		 ORDER BY version DESC
		 LIMIT 1`,
		[]interface{}{streamID},
		func(row *sql.Row) error {
			var err error
			result, err = scanSchemaVersion(row)
			return err
		},
	)
	return result, err
}

// ListVersions returns all schema versions for a stream, newest first.
func (r *StreamRepo) ListVersions(ctx context.Context, streamID string, limit, offset int) ([]*SchemaVersion, error) {
	query := `SELECT ` + schemaVersionColumns + ` FROM schema_versions WHERE stream_id = $1 ORDER BY version DESC`
	args := []interface{}{streamID}
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

	var versions []*SchemaVersion
	err := queryRows(ctx, r.db.pool, query, args, func(rows *sql.Rows) error {
		sv, err := scanSchemaVersion(rows)
		if err != nil {
			return err
		}
		versions = append(versions, sv)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return versions, nil
}

// GetVersionByNumber retrieves a specific schema version by stream ID and version number.
func (r *StreamRepo) GetVersionByNumber(ctx context.Context, streamID string, version int) (*SchemaVersion, error) {
	var result *SchemaVersion
	err := queryRow(ctx, r.db.pool,
		`SELECT `+schemaVersionColumns+` FROM schema_versions
		 WHERE stream_id = $1 AND version = $2`,
		[]interface{}{streamID, version},
		func(row *sql.Row) error {
			var err error
			result, err = scanSchemaVersion(row)
			return err
		},
	)
	return result, err
}
