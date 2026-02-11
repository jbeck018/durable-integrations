package common

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
)

// DistributedRateLimiter allows connectors to use a shared (e.g. Redis-backed)
// rate limiter for multi-worker deployments.
type DistributedRateLimiter interface {
	// Wait blocks until the rate limit allows the request or ctx is cancelled.
	Wait(ctx context.Context) error
}

// RESTClient is a shared HTTP client that wraps http.Client with automatic
// retry, rate limiting, authentication, and logging. All REST API connectors
// should construct one via NewRESTClient and use its Get/Post/Put/Patch/Delete
// helpers instead of raw http calls.
type RESTClient struct {
	client          *http.Client
	baseURL         string
	maxRetries      int
	backoff         time.Duration
	rateLimit       float64 // requests per second; 0 = unlimited
	apiKey          string
	apiKeyHdr       string
	tokenSrc        oauth2.TokenSource
	logger          *log.Logger
	maxResponseSize int64 // max response body size in bytes; 0 = default (50MB)
	distLimiter     DistributedRateLimiter

	// adaptive rate limiter state
	adaptiveRate    float64 // current rate for AIMD; 0 = not adaptive
	adaptiveInitial float64

	// rate limiter state
	mu      sync.Mutex
	lastReq time.Time
}

// ClientOption configures a RESTClient.
type ClientOption func(*RESTClient)

// WithOAuth2 sets an OAuth2 token source for bearer-token authentication.
func WithOAuth2(ts oauth2.TokenSource) ClientOption {
	return func(c *RESTClient) {
		c.tokenSrc = ts
	}
}

// WithAPIKey sets a static API key that is sent via the specified header on
// every request. Common headers include "Authorization", "X-Api-Key", etc.
func WithAPIKey(key, header string) ClientOption {
	return func(c *RESTClient) {
		c.apiKey = key
		c.apiKeyHdr = header
	}
}

// WithRetry configures the maximum number of retries and the base backoff
// duration used for exponential backoff on transient failures.
func WithRetry(maxRetries int, backoff time.Duration) ClientOption {
	return func(c *RESTClient) {
		c.maxRetries = maxRetries
		c.backoff = backoff
	}
}

// WithRateLimit sets the maximum requests per second. Zero or negative disables
// rate limiting.
func WithRateLimit(reqPerSec float64) ClientOption {
	return func(c *RESTClient) {
		c.rateLimit = reqPerSec
	}
}

// WithHTTPClient replaces the default http.Client with a custom one, useful
// for testing or when a connector needs specific transport settings.
func WithHTTPClient(hc *http.Client) ClientOption {
	return func(c *RESTClient) {
		c.client = hc
	}
}

// WithLogger sets a custom logger for request/response logging.
func WithLogger(l *log.Logger) ClientOption {
	return func(c *RESTClient) {
		c.logger = l
	}
}

// WithMaxResponseSize sets the maximum response body size in bytes.
// Responses larger than this limit are rejected to prevent OOM. Default is 50MB.
func WithMaxResponseSize(bytes int64) ClientOption {
	return func(c *RESTClient) {
		c.maxResponseSize = bytes
	}
}

// WithAdaptiveRateLimit enables AIMD-style adaptive rate limiting.
// The rate halves on 429 responses and slowly recovers (additive increase)
// back toward initialRate.
func WithAdaptiveRateLimit(initialRate float64) ClientOption {
	return func(c *RESTClient) {
		c.adaptiveRate = initialRate
		c.adaptiveInitial = initialRate
		c.rateLimit = initialRate
	}
}

// WithDistributedRateLimit sets a shared rate limiter (e.g. Redis-backed)
// that is checked before each request in addition to the local rate limiter.
func WithDistributedRateLimit(limiter DistributedRateLimiter) ClientOption {
	return func(c *RESTClient) {
		c.distLimiter = limiter
	}
}

const defaultMaxResponseSize = 50 * 1024 * 1024 // 50MB

// NewRESTClient constructs a RESTClient for the given base URL.
func NewRESTClient(baseURL string, opts ...ClientOption) *RESTClient {
	c := &RESTClient{
		client: &http.Client{
			Timeout: 30 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        100,
				MaxIdleConnsPerHost: 20,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		baseURL:    strings.TrimRight(baseURL, "/"),
		maxRetries: 3,
		backoff:    500 * time.Millisecond,
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Response wraps the parsed JSON body and the raw HTTP response so callers
// can inspect headers (e.g. for pagination) when needed.
type Response struct {
	StatusCode int
	Headers    http.Header
	Body       map[string]interface{}
	RawBody    []byte
	HTTPResp   *http.Response
}

// Get issues a GET request to baseURL+path and returns parsed JSON.
func (c *RESTClient) Get(ctx context.Context, path string) (*Response, error) {
	return c.do(ctx, http.MethodGet, path, nil)
}

// Post issues a POST request with a JSON body.
func (c *RESTClient) Post(ctx context.Context, path string, body interface{}) (*Response, error) {
	return c.do(ctx, http.MethodPost, path, body)
}

// Put issues a PUT request with a JSON body.
func (c *RESTClient) Put(ctx context.Context, path string, body interface{}) (*Response, error) {
	return c.do(ctx, http.MethodPut, path, body)
}

// Patch issues a PATCH request with a JSON body.
func (c *RESTClient) Patch(ctx context.Context, path string, body interface{}) (*Response, error) {
	return c.do(ctx, http.MethodPatch, path, body)
}

// Delete issues a DELETE request.
func (c *RESTClient) Delete(ctx context.Context, path string) (*Response, error) {
	return c.do(ctx, http.MethodDelete, path, nil)
}

// GetRaw issues a GET and returns the raw bytes (useful for non-JSON endpoints).
func (c *RESTClient) GetRaw(ctx context.Context, path string) ([]byte, http.Header, error) {
	resp, err := c.do(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, nil, err
	}
	return resp.RawBody, resp.Headers, nil
}

// PostRaw issues a POST and returns the raw bytes.
func (c *RESTClient) PostRaw(ctx context.Context, path string, body interface{}) ([]byte, http.Header, error) {
	resp, err := c.do(ctx, http.MethodPost, path, body)
	if err != nil {
		return nil, nil, err
	}
	return resp.RawBody, resp.Headers, nil
}

// do is the core request method with retry, rate-limit, and auth logic.
func (c *RESTClient) do(ctx context.Context, method, path string, body interface{}) (*Response, error) {
	fullURL := c.baseURL + path

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("marshal request body: %w", err)
		}
	}

	maxSize := c.maxResponseSize
	if maxSize <= 0 {
		maxSize = defaultMaxResponseSize
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			wait := c.backoff * time.Duration(math.Pow(2, float64(attempt-1)))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(wait):
			}
		}

		// Distributed rate limiter (Redis-backed) check first.
		if c.distLimiter != nil {
			if err := c.distLimiter.Wait(ctx); err != nil {
				return nil, fmt.Errorf("distributed rate limiter: %w", err)
			}
		}

		c.waitForRateLimit()

		var bodyReader io.Reader
		if bodyBytes != nil {
			bodyReader = bytes.NewReader(bodyBytes)
		}

		req, err := http.NewRequestWithContext(ctx, method, fullURL, bodyReader)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		if bodyBytes != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		req.Header.Set("Accept", "application/json")

		if err := c.applyAuth(ctx, req); err != nil {
			return nil, fmt.Errorf("apply auth: %w", err)
		}

		if c.logger != nil {
			c.logger.Printf("[RESTClient] %s %s (attempt %d)", method, fullURL, attempt+1)
		}

		resp, err := c.client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http request: %w", err)
			continue
		}

		// Read response with size limit to prevent OOM.
		limitedReader := io.LimitReader(resp.Body, maxSize+1)
		respBody, err := io.ReadAll(limitedReader)
		resp.Body.Close()
		if err != nil {
			lastErr = fmt.Errorf("read response: %w", err)
			continue
		}
		if int64(len(respBody)) > maxSize {
			lastErr = fmt.Errorf("response body exceeds max size (%d bytes)", maxSize)
			continue
		}

		if c.logger != nil {
			c.logger.Printf("[RESTClient] %s %s -> %d (%d bytes)", method, fullURL, resp.StatusCode, len(respBody))
		}

		// Handle 429 rate limit with Retry-After and adaptive backoff.
		if resp.StatusCode == http.StatusTooManyRequests {
			c.adaptiveDecrease()
			retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
			if retryAfter > 0 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(retryAfter):
				}
			}
			lastErr = fmt.Errorf("rate limited (429)")
			continue
		}

		// On success, slowly recover adaptive rate.
		c.adaptiveIncrease()

		// Retry on server errors.
		if resp.StatusCode >= 500 {
			lastErr = fmt.Errorf("server error: %d", resp.StatusCode)
			continue
		}

		// Non-retryable client error.
		if resp.StatusCode >= 400 {
			return nil, fmt.Errorf("API error %d: %s", resp.StatusCode, string(respBody))
		}

		result := &Response{
			StatusCode: resp.StatusCode,
			Headers:    resp.Header,
			RawBody:    respBody,
			HTTPResp:   resp,
		}

		// Parse JSON body lazily — only populate Body if needed.
		if len(respBody) > 0 {
			var parsed map[string]interface{}
			if json.Unmarshal(respBody, &parsed) == nil {
				result.Body = parsed
			}
		}

		return result, nil
	}

	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// adaptiveDecrease halves the rate on 429 (AIMD multiplicative decrease).
func (c *RESTClient) adaptiveDecrease() {
	if c.adaptiveInitial <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.adaptiveRate = math.Max(c.adaptiveRate/2, 0.5) // floor at 0.5 req/s
	c.rateLimit = c.adaptiveRate
}

// adaptiveIncrease slowly recovers toward the initial rate (AIMD additive increase).
func (c *RESTClient) adaptiveIncrease() {
	if c.adaptiveInitial <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.adaptiveRate < c.adaptiveInitial {
		c.adaptiveRate = math.Min(c.adaptiveRate+0.5, c.adaptiveInitial)
		c.rateLimit = c.adaptiveRate
	}
}

// applyAuth adds authentication headers to the request.
func (c *RESTClient) applyAuth(_ context.Context, req *http.Request) error {
	if c.tokenSrc != nil {
		tok, err := c.tokenSrc.Token()
		if err != nil {
			return fmt.Errorf("get OAuth token: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+tok.AccessToken)
		return nil
	}
	if c.apiKey != "" {
		req.Header.Set(c.apiKeyHdr, c.apiKey)
	}
	return nil
}

// waitForRateLimit sleeps if necessary to respect the configured rate limit.
func (c *RESTClient) waitForRateLimit() {
	if c.rateLimit <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()

	minInterval := time.Duration(float64(time.Second) / c.rateLimit)
	elapsed := time.Since(c.lastReq)
	if elapsed < minInterval {
		time.Sleep(minInterval - elapsed)
	}
	c.lastReq = time.Now()
}

// parseRetryAfter parses the Retry-After header value as either seconds or
// an HTTP-date and returns the duration to wait.
func parseRetryAfter(val string) time.Duration {
	if val == "" {
		return 0
	}
	// Try seconds first.
	if secs, err := strconv.Atoi(val); err == nil {
		return time.Duration(secs) * time.Second
	}
	// Try HTTP-date.
	if t, err := http.ParseTime(val); err == nil {
		d := time.Until(t)
		if d > 0 {
			return d
		}
	}
	return 0
}

// StreamPages processes pages one at a time via a callback, avoiding unbounded
// slice accumulation. This is the preferred method for paginated reads.
func (c *RESTClient) StreamPages(ctx context.Context, paginator Paginator, handler func(page map[string]interface{}) error) error {
	var lastResp *http.Response

	nextURL, hasMore := paginator.NextPage(lastResp, nil)
	for hasMore {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		path := nextURL
		if strings.HasPrefix(nextURL, "http://") || strings.HasPrefix(nextURL, "https://") {
			path = strings.TrimPrefix(nextURL, c.baseURL)
		}

		resp, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return fmt.Errorf("fetch page: %w", err)
		}

		if resp.Body != nil {
			if err := handler(resp.Body); err != nil {
				return fmt.Errorf("page handler: %w", err)
			}
		}

		lastResp = resp.HTTPResp
		nextURL, hasMore = paginator.NextPage(lastResp, resp.Body)
	}

	return nil
}

// Deprecated: FetchAllPages accumulates all pages in memory. Use StreamPages
// for large datasets to avoid unbounded memory growth.
//
// FetchAllPages retrieves all pages of a paginated endpoint using the given
// Paginator strategy. It collects all response bodies into a slice.
func (c *RESTClient) FetchAllPages(ctx context.Context, paginator Paginator) ([]map[string]interface{}, error) {
	var allPages []map[string]interface{}
	var lastResp *http.Response

	nextURL, hasMore := paginator.NextPage(lastResp, nil)
	for hasMore {
		select {
		case <-ctx.Done():
			return allPages, ctx.Err()
		default:
		}

		// Extract the path from the URL. If the paginator returns an absolute URL,
		// use it directly by temporarily overriding baseURL.
		path := nextURL
		if strings.HasPrefix(nextURL, "http://") || strings.HasPrefix(nextURL, "https://") {
			// Use the full URL as-is.
			path = strings.TrimPrefix(nextURL, c.baseURL)
		}

		resp, err := c.do(ctx, http.MethodGet, path, nil)
		if err != nil {
			return allPages, fmt.Errorf("fetch page: %w", err)
		}

		if resp.Body != nil {
			allPages = append(allPages, resp.Body)
		}

		lastResp = resp.HTTPResp
		nextURL, hasMore = paginator.NextPage(lastResp, resp.Body)
	}

	return allPages, nil
}

// FetchAllPagesPost is like FetchAllPages but uses POST for APIs that require
// POST-based pagination (e.g. search endpoints).
func (c *RESTClient) FetchAllPagesPost(ctx context.Context, path string, buildBody func(cursor string) interface{}, cursorField string) ([]map[string]interface{}, error) {
	var allPages []map[string]interface{}
	cursor := ""

	for {
		select {
		case <-ctx.Done():
			return allPages, ctx.Err()
		default:
		}

		reqBody := buildBody(cursor)
		resp, err := c.do(ctx, http.MethodPost, path, reqBody)
		if err != nil {
			return allPages, fmt.Errorf("fetch page: %w", err)
		}

		if resp.Body != nil {
			allPages = append(allPages, resp.Body)
		}

		nextCursor := extractNestedString(resp.Body, cursorField)
		if nextCursor == "" || nextCursor == cursor {
			break
		}
		cursor = nextCursor
	}

	return allPages, nil
}
