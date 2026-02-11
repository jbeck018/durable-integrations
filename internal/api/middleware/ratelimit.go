package middleware

import (
	"encoding/json"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/common"
)

// tokenBucket implements a classic token bucket rate limiter.
type tokenBucket struct {
	mu         sync.Mutex
	tokens     float64
	maxTokens  float64
	refillRate float64 // tokens per second
	lastRefill time.Time
}

// newTokenBucket creates a token bucket configured for the given requests per minute.
func newTokenBucket(requestsPerMinute int) *tokenBucket {
	rate := float64(requestsPerMinute) / 60.0
	return &tokenBucket{
		tokens:     float64(requestsPerMinute),
		maxTokens:  float64(requestsPerMinute),
		refillRate: rate,
		lastRefill: time.Now(),
	}
}

// allow attempts to consume one token. Returns whether the request is allowed
// and the number of seconds until the next token is available.
func (tb *tokenBucket) allow() (bool, float64) {
	tb.mu.Lock()
	defer tb.mu.Unlock()

	now := time.Now()
	elapsed := now.Sub(tb.lastRefill).Seconds()
	tb.tokens = math.Min(tb.maxTokens, tb.tokens+elapsed*tb.refillRate)
	tb.lastRefill = now

	if tb.tokens >= 1.0 {
		tb.tokens--
		return true, 0
	}

	retryAfter := (1.0 - tb.tokens) / tb.refillRate
	return false, retryAfter
}

// rateLimiterStore maintains per-tenant token buckets with periodic cleanup.
type rateLimiterStore struct {
	buckets           sync.Map
	requestsPerMinute int
}

// newRateLimiterStore creates a store and starts a background cleanup goroutine.
func newRateLimiterStore(requestsPerMinute int) *rateLimiterStore {
	s := &rateLimiterStore{
		requestsPerMinute: requestsPerMinute,
	}
	go s.cleanup()
	return s
}

// getBucket returns or creates a token bucket for the given tenant.
func (s *rateLimiterStore) getBucket(tenantID string) *tokenBucket {
	if v, ok := s.buckets.Load(tenantID); ok {
		tb, _ := v.(*tokenBucket)
		return tb
	}
	bucket := newTokenBucket(s.requestsPerMinute)
	actual, _ := s.buckets.LoadOrStore(tenantID, bucket)
	tb, _ := actual.(*tokenBucket)
	return tb
}

// cleanup removes stale buckets every 5 minutes to prevent unbounded memory growth.
func (s *rateLimiterStore) cleanup() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		s.buckets.Range(func(key, value interface{}) bool {
			bucket, _ := value.(*tokenBucket)
			bucket.mu.Lock()
			idle := time.Since(bucket.lastRefill)
			bucket.mu.Unlock()
			if idle > 10*time.Minute {
				s.buckets.Delete(key)
			}
			return true
		})
	}
}

// RateLimit returns middleware that enforces per-tenant rate limiting using a
// token bucket algorithm. Tenants are identified via context (set by auth middleware).
// Unauthenticated requests use the client IP as the bucket key.
func RateLimit(requestsPerMinute int) func(http.Handler) http.Handler {
	store := newRateLimiterStore(requestsPerMinute)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := common.TenantIDFrom(r.Context())
			if key == "" {
				key = realIP(r)
			}

			bucket := store.getBucket(key)
			allowed, retryAfter := bucket.allow()

			// Always set rate limit headers.
			w.Header().Set("X-RateLimit-Limit", strconv.Itoa(requestsPerMinute))
			remaining := int(math.Max(0, bucket.tokens))
			w.Header().Set("X-RateLimit-Remaining", strconv.Itoa(remaining))

			if !allowed {
				retrySeconds := int(math.Ceil(retryAfter))
				if retrySeconds < 1 {
					retrySeconds = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retrySeconds))
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusTooManyRequests)
				enc := json.NewEncoder(w)
				enc.SetEscapeHTML(false)
				_ = enc.Encode(map[string]interface{}{
					"error": map[string]interface{}{
						"code":    "RATE_LIMITED",
						"message": "rate limit exceeded, retry after " + strconv.Itoa(retrySeconds) + " seconds",
					},
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// realIP extracts the client IP from X-Forwarded-For, X-Real-IP, or RemoteAddr.
func realIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Take the first IP in the chain.
		for i := 0; i < len(xff); i++ {
			if xff[i] == ',' {
				return xff[:i]
			}
		}
		return xff
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return xri
	}
	return r.RemoteAddr
}
