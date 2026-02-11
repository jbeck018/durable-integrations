package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const (
	configCachePrefix = "flowforge:config:"
	schemaCachePrefix = "flowforge:schema:"
	defaultCacheTTL   = 15 * time.Minute
)

// TenantConfigCachePrefix returns a tenant-scoped config cache key prefix.
// Format: flowforge:{tenantID}:config:
func TenantConfigCachePrefix(tenantID string) string {
	return fmt.Sprintf("flowforge:%s:config:", tenantID)
}

// TenantSchemaCachePrefix returns a tenant-scoped schema cache key prefix.
// Format: flowforge:{tenantID}:schema:
func TenantSchemaCachePrefix(tenantID string) string {
	return fmt.Sprintf("flowforge:%s:schema:", tenantID)
}

// ConfigCache caches connector configurations by connector ID.
// Configs are stored as JSON with a TTL to ensure fresh data.
type ConfigCache struct {
	client *Client
	ttl    time.Duration
	prefix string // Overridable key prefix for tenant isolation
}

// NewConfigCache creates a ConfigCache with the given TTL.
// If ttl is zero, the default 15-minute TTL is used.
func NewConfigCache(client *Client, ttl time.Duration) *ConfigCache {
	if ttl == 0 {
		ttl = defaultCacheTTL
	}
	return &ConfigCache{client: client, ttl: ttl, prefix: configCachePrefix}
}

// NewTenantConfigCache creates a ConfigCache scoped to a specific tenant.
func NewTenantConfigCache(client *Client, tenantID string, ttl time.Duration) *ConfigCache {
	if ttl == 0 {
		ttl = defaultCacheTTL
	}
	return &ConfigCache{client: client, ttl: ttl, prefix: TenantConfigCachePrefix(tenantID)}
}

// configKey builds the Redis key for a connector config entry.
func configKey(connectorID string) string {
	return configCachePrefix + connectorID
}

// Get retrieves a cached connector config by connector ID.
// Returns the raw JSON config and true if found, nil and false if not cached.
func (c *ConfigCache) Get(ctx context.Context, connectorID string) (json.RawMessage, bool, error) {
	data, err := c.client.GetRaw(ctx, c.prefix+connectorID)
	if err != nil {
		if isNilError(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("config cache get: %w", err)
	}
	return json.RawMessage(data), true, nil
}

// Set caches a connector config with the configured TTL.
func (c *ConfigCache) Set(ctx context.Context, connectorID string, config json.RawMessage) error {
	err := c.client.SetRaw(ctx, c.prefix+connectorID, []byte(config), c.ttl)
	if err != nil {
		return fmt.Errorf("config cache set: %w", err)
	}
	return nil
}

// Invalidate removes a connector config from the cache.
func (c *ConfigCache) Invalidate(ctx context.Context, connectorID string) error {
	return c.client.Delete(ctx, c.prefix+connectorID)
}

// InvalidateAll removes all connector configs matching the prefix.
func (c *ConfigCache) InvalidateAll(ctx context.Context) error {
	return deleteByPattern(ctx, c.client, c.prefix+"*")
}

// SchemaCache caches stream schemas by stream ID.
type SchemaCache struct {
	client *Client
	ttl    time.Duration
	prefix string // Overridable key prefix for tenant isolation
}

// NewSchemaCache creates a SchemaCache with the given TTL.
// If ttl is zero, the default 15-minute TTL is used.
func NewSchemaCache(client *Client, ttl time.Duration) *SchemaCache {
	if ttl == 0 {
		ttl = defaultCacheTTL
	}
	return &SchemaCache{client: client, ttl: ttl, prefix: schemaCachePrefix}
}

// NewTenantSchemaCache creates a SchemaCache scoped to a specific tenant.
func NewTenantSchemaCache(client *Client, tenantID string, ttl time.Duration) *SchemaCache {
	if ttl == 0 {
		ttl = defaultCacheTTL
	}
	return &SchemaCache{client: client, ttl: ttl, prefix: TenantSchemaCachePrefix(tenantID)}
}

// schemaKey builds the Redis key for a stream schema entry.
func schemaKey(streamID string) string {
	return schemaCachePrefix + streamID
}

// Get retrieves a cached stream schema by stream ID.
// Returns the raw JSON schema and true if found, nil and false if not cached.
func (c *SchemaCache) Get(ctx context.Context, streamID string) (json.RawMessage, bool, error) {
	data, err := c.client.GetRaw(ctx, c.prefix+streamID)
	if err != nil {
		if isNilError(err) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("schema cache get: %w", err)
	}
	return json.RawMessage(data), true, nil
}

// Set caches a stream schema with the configured TTL.
func (c *SchemaCache) Set(ctx context.Context, streamID string, schema json.RawMessage) error {
	err := c.client.SetRaw(ctx, c.prefix+streamID, []byte(schema), c.ttl)
	if err != nil {
		return fmt.Errorf("schema cache set: %w", err)
	}
	return nil
}

// Invalidate removes a stream schema from the cache.
func (c *SchemaCache) Invalidate(ctx context.Context, streamID string) error {
	return c.client.Delete(ctx, c.prefix+streamID)
}

// InvalidateAll removes all stream schemas matching the prefix.
func (c *SchemaCache) InvalidateAll(ctx context.Context) error {
	return deleteByPattern(ctx, c.client, c.prefix+"*")
}

// isNilError checks if the error is a redis.Nil (key not found).
func isNilError(err error) bool {
	return err != nil && err.Error() == "redis: nil"
}

// deleteByPattern removes all keys matching the given glob pattern using SCAN
// to avoid blocking the server with a KEYS command on large datasets.
func deleteByPattern(ctx context.Context, client *Client, pattern string) error {
	var cursor uint64
	for {
		keys, nextCursor, err := client.rdb.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return fmt.Errorf("scan pattern %q: %w", pattern, err)
		}
		if len(keys) > 0 {
			if err := client.rdb.Del(ctx, keys...).Err(); err != nil {
				return fmt.Errorf("delete keys: %w", err)
			}
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return nil
}
