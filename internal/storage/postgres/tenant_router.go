package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

// TenantRouter manages per-tenant database connection pools.
// It maintains a connection pool cache keyed by tenant ID, with lazy
// initialization on first access and automatic cleanup of idle pools.
type TenantRouter struct {
	catalog *DB // Control plane (shared) database

	mu    sync.RWMutex
	pools map[string]*tenantPool

	resolver    TenantURIResolver
	maxConns    int
	idleTimeout time.Duration
	stopCleanup chan struct{}
}

// tenantPool tracks a per-tenant connection pool and its last access time.
type tenantPool struct {
	db         *DB
	lastAccess time.Time
}

// TenantURIResolver resolves a tenant ID to its database connection URI.
// This is typically backed by the catalog database's tenants table.
type TenantURIResolver interface {
	ResolveURI(ctx context.Context, tenantID string) (string, error)
}

// TenantRouterConfig configures the TenantRouter.
type TenantRouterConfig struct {
	Catalog     *DB
	Resolver    TenantURIResolver
	MaxConns    int           // Max connections per tenant pool (default: 5)
	IdleTimeout time.Duration // Close pools idle longer than this (default: 5 min)
}

// NewTenantRouter creates a TenantRouter and starts a background goroutine
// to clean up idle tenant pools.
func NewTenantRouter(cfg TenantRouterConfig) *TenantRouter {
	if cfg.MaxConns <= 0 {
		cfg.MaxConns = 5
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Minute
	}

	tr := &TenantRouter{
		catalog:     cfg.Catalog,
		pools:       make(map[string]*tenantPool),
		resolver:    cfg.Resolver,
		maxConns:    cfg.MaxConns,
		idleTimeout: cfg.IdleTimeout,
		stopCleanup: make(chan struct{}),
	}

	go tr.cleanupLoop()

	return tr
}

// Catalog returns the shared control plane database.
func (tr *TenantRouter) Catalog() *DB {
	return tr.catalog
}

// ForTenant returns a database connection pool for the given tenant.
// Pools are lazily created on first access and cached for reuse.
func (tr *TenantRouter) ForTenant(ctx context.Context, tenantID string) (*DB, error) {
	// Fast path: check if pool exists.
	tr.mu.RLock()
	tp, ok := tr.pools[tenantID]
	if ok {
		tp.lastAccess = time.Now()
		tr.mu.RUnlock()
		return tp.db, nil
	}
	tr.mu.RUnlock()

	// Slow path: resolve URI and create pool.
	uri, err := tr.resolver.ResolveURI(ctx, tenantID)
	if err != nil {
		return nil, fmt.Errorf("resolve tenant %s URI: %w", tenantID, err)
	}

	db, err := NewDB(uri, tr.maxConns)
	if err != nil {
		return nil, fmt.Errorf("connect to tenant %s database: %w", tenantID, err)
	}

	tr.mu.Lock()
	// Double-check: another goroutine may have created the pool while we waited.
	if existing, ok := tr.pools[tenantID]; ok {
		tr.mu.Unlock()
		db.Close()
		existing.lastAccess = time.Now()
		return existing.db, nil
	}
	tr.pools[tenantID] = &tenantPool{
		db:         db,
		lastAccess: time.Now(),
	}
	tr.mu.Unlock()

	return db, nil
}

// EvictTenant removes a tenant's pool from the cache and closes it.
// This should be called when a tenant is deprovisioned.
func (tr *TenantRouter) EvictTenant(tenantID string) error {
	tr.mu.Lock()
	tp, ok := tr.pools[tenantID]
	if ok {
		delete(tr.pools, tenantID)
	}
	tr.mu.Unlock()

	if ok {
		return tp.db.Close()
	}
	return nil
}

// Close stops the cleanup goroutine and closes all tenant pools.
func (tr *TenantRouter) Close() error {
	close(tr.stopCleanup)

	tr.mu.Lock()
	defer tr.mu.Unlock()

	var firstErr error
	for id, tp := range tr.pools {
		if err := tp.db.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("close tenant %s pool: %w", id, err)
		}
		delete(tr.pools, id)
	}
	return firstErr
}

// PoolCount returns the number of active tenant pools.
func (tr *TenantRouter) PoolCount() int {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return len(tr.pools)
}

// cleanupLoop periodically closes idle tenant pools.
// Neon's scale-to-zero handles compute suspension on the server side,
// but we still clean up client-side pools to free file descriptors and memory.
func (tr *TenantRouter) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-tr.stopCleanup:
			return
		case <-ticker.C:
			tr.evictIdle()
		}
	}
}

// evictIdle closes tenant pools that have been idle longer than idleTimeout.
func (tr *TenantRouter) evictIdle() {
	now := time.Now()
	var toEvict []string

	tr.mu.RLock()
	for id, tp := range tr.pools {
		if now.Sub(tp.lastAccess) > tr.idleTimeout {
			toEvict = append(toEvict, id)
		}
	}
	tr.mu.RUnlock()

	for _, id := range toEvict {
		tr.mu.Lock()
		tp, ok := tr.pools[id]
		if ok && now.Sub(tp.lastAccess) > tr.idleTimeout {
			delete(tr.pools, id)
			tp.db.Close()
		}
		tr.mu.Unlock()
	}
}

// URIEncryptor is the interface for encrypting/decrypting connection URIs.
type URIEncryptor interface {
	Encrypt(plaintext []byte) ([]byte, error)
	Decrypt(ciphertext []byte) ([]byte, error)
}

// CatalogTenantURIResolver resolves tenant URIs from the catalog database.
type CatalogTenantURIResolver struct {
	catalog   *DB
	encryptor URIEncryptor
}

// NewCatalogTenantURIResolver creates a resolver that queries the catalog database.
// The encryptor parameter may be nil for development (plaintext URIs).
func NewCatalogTenantURIResolver(catalog *DB, encryptor URIEncryptor) *CatalogTenantURIResolver {
	return &CatalogTenantURIResolver{catalog: catalog, encryptor: encryptor}
}

// ResolveURI looks up the decrypted connection URI for a tenant from the catalog.
func (r *CatalogTenantURIResolver) ResolveURI(ctx context.Context, tenantID string) (string, error) {
	var uri sql.NullString
	err := queryRow(ctx, r.catalog.pool, `
		SELECT connection_uri_encrypted
		FROM tenants
		WHERE id = $1 AND neon_project_id IS NOT NULL
	`, []interface{}{tenantID}, func(row *sql.Row) error {
		return row.Scan(&uri)
	})
	if err != nil {
		return "", fmt.Errorf("query tenant URI: %w", err)
	}
	if !uri.Valid || uri.String == "" {
		return "", fmt.Errorf("tenant %s has no provisioned database", tenantID)
	}

	// Decrypt the URI if an encryptor is configured.
	if r.encryptor != nil {
		decrypted, err := r.encryptor.Decrypt([]byte(uri.String))
		if err != nil {
			return "", fmt.Errorf("decrypt tenant %s URI: %w", tenantID, err)
		}
		return string(decrypted), nil
	}

	return uri.String, nil
}
