// Package ratelimit provides distributed rate limiting adapters for the
// FlowForge connector runtime. The DistributedAdapter wraps the Redis-backed
// rate limiter from internal/storage/redis and implements the
// connectors/common.DistributedRateLimiter interface so that multiple workers
// sharing a Redis instance can enforce a single global rate limit per
// connector/tenant combination.
package ratelimit

import (
	"context"
	"fmt"
	"time"

	"github.com/flowforge/flowforge/internal/storage/redis"
)

// DistributedAdapter implements common.DistributedRateLimiter by polling the
// Redis-backed RateLimiter until a request is allowed.
type DistributedAdapter struct {
	limiter  *redis.RateLimiter
	key      string
	limit    int
	window   time.Duration
	pollWait time.Duration
}

// NewDistributedAdapter creates a DistributedAdapter.
//
// Parameters:
//   - limiter: the Redis-backed rate limiter
//   - key: the rate limit key (e.g. "bigquery:tenant-123")
//   - limit: maximum requests allowed in the window
//   - window: the time window for the limit
func NewDistributedAdapter(limiter *redis.RateLimiter, key string, limit int, window time.Duration) *DistributedAdapter {
	return &DistributedAdapter{
		limiter:  limiter,
		key:      key,
		limit:    limit,
		window:   window,
		pollWait: 100 * time.Millisecond,
	}
}

// Wait blocks until the rate limit allows the request or ctx is cancelled.
// It implements common.DistributedRateLimiter.
func (d *DistributedAdapter) Wait(ctx context.Context) error {
	for {
		allowed, err := d.limiter.Allow(ctx, d.key, d.limit, d.window)
		if err != nil {
			return fmt.Errorf("distributed rate limit check: %w", err)
		}
		if allowed {
			return nil
		}

		// Wait before retrying.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d.pollWait):
		}
	}
}

// BuildKey creates a standardized rate limit key for a connector and tenant.
// Format: {tenant_id}:{connector_type}:{window}
// This follows the formalized Redis key isolation pattern:
// flowforge:{tenant_id}:ratelimit:{connector_type}:{window}
// The "flowforge:ratelimit:" prefix is added by the RateLimiter itself.
func BuildKey(connectorType, tenantID string) string {
	return tenantID + ":" + connectorType
}

// BuildKeyWithWindow creates a rate limit key including the window identifier.
// Format: {tenant_id}:{connector_type}:{window}
func BuildKeyWithWindow(connectorType, tenantID, window string) string {
	return tenantID + ":" + connectorType + ":" + window
}
