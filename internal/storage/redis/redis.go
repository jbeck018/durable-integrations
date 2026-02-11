// Package redis provides a Redis client wrapper for FlowForge.
// It wraps the go-redis client with connection pooling, JSON serialization,
// and TTL-aware operations used by caching, rate limiting, and pub/sub.
package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// Client wraps a go-redis client with connection pooling and serialization helpers.
type Client struct {
	rdb *goredis.Client
}

// NewClient creates a new Redis client with connection pooling.
// addr is "host:port", password may be empty, db is the Redis database number.
func NewClient(addr, password string, db int) (*Client, error) {
	rdb := goredis.NewClient(&goredis.Options{
		Addr:            addr,
		Password:        password,
		DB:              db,
		PoolSize:        20,
		MinIdleConns:    5,
		MaxRetries:      3,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     3 * time.Second,
		WriteTimeout:    3 * time.Second,
		PoolTimeout:     4 * time.Second,
		ConnMaxIdleTime: 5 * time.Minute,
		ConnMaxLifetime: 30 * time.Minute,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := rdb.Ping(ctx).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &Client{rdb: rdb}, nil
}

// Close shuts down the Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

// Ping checks that the Redis connection is alive.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Underlying returns the raw go-redis client for advanced operations.
func (c *Client) Underlying() *goredis.Client {
	return c.rdb
}

// Get retrieves a value by key and JSON-unmarshals it into dest.
// Returns ErrNil (from go-redis) if the key does not exist.
func (c *Client) Get(ctx context.Context, key string, dest interface{}) error {
	data, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil {
		return err
	}
	return json.Unmarshal(data, dest)
}

// Set stores a value by key, JSON-marshaling it first. No expiration is set.
func (c *Client) Set(ctx context.Context, key string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return c.rdb.Set(ctx, key, data, 0).Err()
}

// SetWithTTL stores a value by key with an explicit time-to-live.
func (c *Client) SetWithTTL(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	return c.rdb.Set(ctx, key, data, ttl).Err()
}

// Delete removes one or more keys.
func (c *Client) Delete(ctx context.Context, keys ...string) error {
	return c.rdb.Del(ctx, keys...).Err()
}

// Exists returns true if the key exists.
func (c *Client) Exists(ctx context.Context, key string) (bool, error) {
	n, err := c.rdb.Exists(ctx, key).Result()
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

// Expire sets a TTL on an existing key.
func (c *Client) Expire(ctx context.Context, key string, ttl time.Duration) error {
	return c.rdb.Expire(ctx, key, ttl).Err()
}

// GetRaw retrieves the raw bytes for a key without deserialization.
func (c *Client) GetRaw(ctx context.Context, key string) ([]byte, error) {
	return c.rdb.Get(ctx, key).Bytes()
}

// SetRaw stores raw bytes for a key with an optional TTL.
func (c *Client) SetRaw(ctx context.Context, key string, data []byte, ttl time.Duration) error {
	return c.rdb.Set(ctx, key, data, ttl).Err()
}

// EvalSha executes a preloaded Lua script by SHA.
func (c *Client) EvalSha(ctx context.Context, sha string, keys []string, args ...interface{}) *goredis.Cmd {
	return c.rdb.EvalSha(ctx, sha, keys, args...)
}

// ScriptLoad loads a Lua script into the Redis script cache and returns its SHA.
func (c *Client) ScriptLoad(ctx context.Context, script string) (string, error) {
	return c.rdb.ScriptLoad(ctx, script).Result()
}
