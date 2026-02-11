// Package postgres provides PostgreSQL storage for FlowForge.
// It wraps database/sql with connection pooling, transaction helpers,
// and shared query utilities used by all repository implementations.
package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// Ensure pq driver and uuid are linked.
var (
	_ = pq.ErrNotSupported
	_ = uuid.Nil
)

// DB wraps *sql.DB with connection pool configuration and helpers.
type DB struct {
	pool *sql.DB
}

// NewDB opens a PostgreSQL connection pool with the given DSN and max connections.
// It configures idle connection settings and validates connectivity before returning.
func NewDB(dsn string, maxConns int) (*DB, error) {
	pool, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("postgres open: %w", err)
	}

	pool.SetMaxOpenConns(maxConns)
	pool.SetMaxIdleConns(maxConns / 2)
	pool.SetConnMaxLifetime(30 * time.Minute)
	pool.SetConnMaxIdleTime(5 * time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := pool.PingContext(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("postgres ping: %w", err)
	}

	return &DB{pool: pool}, nil
}

// Close shuts down the connection pool.
func (db *DB) Close() error {
	return db.pool.Close()
}

// Ping verifies the connection is alive.
func (db *DB) Ping(ctx context.Context) error {
	return db.pool.PingContext(ctx)
}

// Pool returns the underlying *sql.DB for advanced use cases.
func (db *DB) Pool() *sql.DB {
	return db.pool
}

// Tx is a transaction handle that implements the same query interface as DB.
type Tx struct {
	tx *sql.Tx
}

// WithTx executes fn inside a database transaction.
// If fn returns an error the transaction is rolled back; otherwise it is committed.
func (db *DB) WithTx(ctx context.Context, fn func(tx *Tx) error) error {
	sqlTx, err := db.pool.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}

	t := &Tx{tx: sqlTx}
	if err := fn(t); err != nil {
		if rbErr := sqlTx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed: %v (original: %w)", rbErr, err)
		}
		return err
	}

	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("commit tx: %w", err)
	}
	return nil
}

// querier abstracts *sql.DB and *sql.Tx so repos can run queries
// against either a connection pool or an active transaction.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// q returns the querier for a given optional transaction.
// If tx is nil the pool is used directly.
func (db *DB) q(tx *Tx) querier {
	if tx != nil {
		return tx.tx
	}
	return db.pool
}

// queryRow executes a query expected to return at most one row.
// The scanner function receives the *sql.Row to scan into struct fields.
func queryRow(ctx context.Context, q querier, query string, args []interface{}, scanner func(*sql.Row) error) error {
	row := q.QueryRowContext(ctx, query, args...)
	err := scanner(row)
	if err == sql.ErrNoRows {
		return common.ErrNotFound
	}
	return err
}

// queryRows executes a query expected to return zero or more rows.
// The scanner function is called once per row and should scan the row into a value
// and append it to a result slice.
func queryRows(ctx context.Context, q querier, query string, args []interface{}, scanner func(*sql.Rows) error) error {
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		if err := scanner(rows); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
	}
	return rows.Err()
}

// exec executes a non-SELECT statement and returns the number of rows affected.
func exec(ctx context.Context, q querier, query string, args ...interface{}) (int64, error) {
	result, err := q.ExecContext(ctx, query, args...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// execExpectOne executes a non-SELECT statement and returns ErrNotFound
// if no rows were affected.
func execExpectOne(ctx context.Context, q querier, query string, args ...interface{}) error {
	n, err := exec(ctx, q, query, args...)
	if err != nil {
		return err
	}
	if n == 0 {
		return common.ErrNotFound
	}
	return nil
}

// jsonBytes marshals v to JSON bytes for storage in a JSONB column.
// If v is nil, it returns the JSON null representation.
func jsonBytes(v interface{}) ([]byte, error) {
	if v == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(v)
}

// newID generates a new UUID string for use as a primary key.
func newID() string {
	return uuid.New().String()
}

// nullTimePtr converts a *time.Time to sql.NullTime for nullable timestamp columns.
func nullTimePtr(t *time.Time) sql.NullTime {
	if t == nil {
		return sql.NullTime{}
	}
	return sql.NullTime{Time: *t, Valid: true}
}

// timePtr converts sql.NullTime back to *time.Time.
func timePtr(nt sql.NullTime) *time.Time {
	if !nt.Valid {
		return nil
	}
	t := nt.Time
	return &t
}

// nullString converts a string to sql.NullString, treating "" as NULL.
func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

// stringFromNull converts sql.NullString back to string.
func stringFromNull(ns sql.NullString) string {
	if !ns.Valid {
		return ""
	}
	return ns.String
}
