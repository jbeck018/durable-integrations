package redis

import (
	"context"
	"fmt"
	"time"

	goredis "github.com/redis/go-redis/v9"
)

// rateLimitScript is a Lua script implementing a sliding window token bucket.
// It atomically checks and decrements the counter, setting expiration on first use.
//
// KEYS[1] = rate limit key
// ARGV[1] = max tokens (limit)
// ARGV[2] = window duration in seconds
//
// Returns 1 if allowed, 0 if rate limited.
const rateLimitScript = `
local key = KEYS[1]
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

local current = tonumber(redis.call("GET", key) or "0")

if current >= limit then
    return 0
end

current = redis.call("INCR", key)

if current == 1 then
    redis.call("EXPIRE", key, window)
end

if current > limit then
    return 0
end

return 1
`

// RateLimiter implements a token bucket rate limiter using Redis.
// It uses a Lua script for atomic check-and-increment to prevent race conditions.
type RateLimiter struct {
	client    *Client
	scriptSHA string
	prefix    string
}

// NewRateLimiter creates a RateLimiter and preloads the Lua script into Redis.
func NewRateLimiter(client *Client, prefix string) (*RateLimiter, error) {
	if prefix == "" {
		prefix = "flowforge:ratelimit:"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sha, err := client.ScriptLoad(ctx, rateLimitScript)
	if err != nil {
		return nil, fmt.Errorf("load rate limit script: %w", err)
	}

	return &RateLimiter{
		client:    client,
		scriptSHA: sha,
		prefix:    prefix,
	}, nil
}

// Allow checks whether a request identified by key is allowed under the given
// rate limit. limit is the maximum number of requests allowed in the window.
// Returns true if the request is allowed, false if rate limited.
func (rl *RateLimiter) Allow(ctx context.Context, key string, limit int, window time.Duration) (bool, error) {
	fullKey := rl.prefix + key
	windowSecs := int(window.Seconds())
	if windowSecs < 1 {
		windowSecs = 1
	}

	result, err := rl.client.EvalSha(ctx, rl.scriptSHA, []string{fullKey}, limit, windowSecs).Int64()
	if err != nil {
		// If NOSCRIPT error, reload the script and retry once.
		if isNoScriptError(err) {
			sha, loadErr := rl.client.ScriptLoad(ctx, rateLimitScript)
			if loadErr != nil {
				return false, fmt.Errorf("reload rate limit script: %w", loadErr)
			}
			rl.scriptSHA = sha
			result, err = rl.client.EvalSha(ctx, rl.scriptSHA, []string{fullKey}, limit, windowSecs).Int64()
			if err != nil {
				return false, fmt.Errorf("rate limit eval after reload: %w", err)
			}
		} else {
			return false, fmt.Errorf("rate limit eval: %w", err)
		}
	}

	return result == 1, nil
}

// Remaining returns the number of remaining requests for a key in the current window.
func (rl *RateLimiter) Remaining(ctx context.Context, key string, limit int) (int, error) {
	fullKey := rl.prefix + key
	val, err := rl.client.rdb.Get(ctx, fullKey).Int64()
	if err != nil {
		if isNilError(err) {
			return limit, nil
		}
		return 0, fmt.Errorf("get remaining: %w", err)
	}
	remaining := limit - int(val)
	if remaining < 0 {
		remaining = 0
	}
	return remaining, nil
}

// Reset removes the rate limit counter for a key, effectively resetting the window.
func (rl *RateLimiter) Reset(ctx context.Context, key string) error {
	return rl.client.Delete(ctx, rl.prefix+key)
}

// isNoScriptError checks if the error is a Redis NOSCRIPT error,
// indicating the Lua script needs to be reloaded.
func isNoScriptError(err error) bool {
	if err == nil {
		return false
	}
	redisErr, ok := err.(goredis.Error)
	if !ok {
		return false
	}
	return len(redisErr.Error()) >= 8 && redisErr.Error()[:8] == "NOSCRIPT"
}
