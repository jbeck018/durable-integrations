// Package zoom implements a FlowForge source connector for the Zoom API.
// It reads meetings, webinars, recordings, users, registrants, and participants
// using Zoom's Server-to-Server OAuth2 and REST API. Includes built-in rate
// limiting (10 req/s) and automatic retry on 429 responses.
package zoom

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"golang.org/x/time/rate"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

const (
	connectorName    = "zoom"
	connectorVersion = "1.0.0"
	zoomAPIBase      = "https://api.zoom.us/v2"
	zoomTokenURL     = "https://zoom.us/oauth/token"
	rateLimit        = 8 // Zoom's actual limit is 500/min (~8.3 req/s); use 8 for safety margin
	maxRetries       = 5
	retryBaseDelay   = time.Second
)

// ZoomConfig holds the configuration for the Zoom connector.
type ZoomConfig struct {
	AccountID    string `json:"account_id"`
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	StartDate    string `json:"start_date,omitempty"`
	EndDate      string `json:"end_date,omitempty"`
}

func (c *ZoomConfig) validate() error {
	if c.AccountID == "" {
		return &common.ValidationError{Field: "account_id", Message: "account_id is required"}
	}
	if c.ClientID == "" {
		return &common.ValidationError{Field: "client_id", Message: "client_id is required"}
	}
	if c.ClientSecret == "" {
		return &common.ValidationError{Field: "client_secret", Message: "client_secret is required"}
	}
	return nil
}

// ZoomConnector implements cdk.Source for the Zoom API.
type ZoomConnector struct{}

func init() {
	cdk.RegisterSource(connectorName, cdk.ConnectorMeta{
		Name:        connectorName,
		DisplayName: "Zoom",
		Version:     connectorVersion,
		Category:    "communication",
	}, &ZoomConnector{})
}

// Spec returns the JSON Schema for the Zoom connector configuration.
func (z *ZoomConnector) Spec() (*protocol.Spec, error) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["account_id", "client_id", "client_secret"],
		"properties": {
			"account_id": {
				"type": "string",
				"title": "Account ID",
				"description": "Zoom Server-to-Server OAuth Account ID."
			},
			"client_id": {
				"type": "string",
				"title": "Client ID",
				"description": "Zoom Server-to-Server OAuth Client ID."
			},
			"client_secret": {
				"type": "string",
				"title": "Client Secret",
				"description": "Zoom Server-to-Server OAuth Client Secret.",
				"airbyte_secret": true
			},
			"start_date": {
				"type": "string",
				"title": "Start Date",
				"description": "Earliest date to sync data from (YYYY-MM-DD).",
				"pattern": "^\\d{4}-\\d{2}-\\d{2}$"
			},
			"end_date": {
				"type": "string",
				"title": "End Date",
				"description": "Latest date to sync data to (YYYY-MM-DD). Defaults to today.",
				"pattern": "^\\d{4}-\\d{2}-\\d{2}$"
			}
		}
	}`)
	return &protocol.Spec{
		DocumentationURL: "https://developers.zoom.us/docs/api/",
		ConfigSchema:     schema,
	}, nil
}

// parseConfig deserializes and validates the raw JSON config.
func parseConfig(raw json.RawMessage) (*ZoomConfig, error) {
	var cfg ZoomConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, common.NewConnectorError(connectorName, "parse_config", err)
	}
	if err := cfg.validate(); err != nil {
		return nil, common.NewConnectorError(connectorName, "validate_config", err)
	}
	return &cfg, nil
}

// tokenCache caches the OAuth token with thread-safe access.
type tokenCache struct {
	mu        sync.Mutex
	token     string
	expiresAt time.Time
}

// zoomClient wraps an HTTP client with rate limiting, token management, and retry logic.
type zoomClient struct {
	httpClient *http.Client
	limiter    *rate.Limiter
	cfg        *ZoomConfig
	cache      tokenCache
}

// newZoomClient creates a new Zoom API client with rate limiting.
func newZoomClient(cfg *ZoomConfig) *zoomClient {
	return &zoomClient{
		httpClient: &http.Client{Timeout: 30 * time.Second},
		limiter:    rate.NewLimiter(rate.Limit(rateLimit), rateLimit),
		cfg:        cfg,
	}
}

// getAccessToken obtains or refreshes the Server-to-Server OAuth2 token.
func (c *zoomClient) getAccessToken(ctx context.Context) (string, error) {
	c.cache.mu.Lock()
	defer c.cache.mu.Unlock()

	if c.cache.token != "" && time.Now().Before(c.cache.expiresAt) {
		return c.cache.token, nil
	}

	data := url.Values{}
	data.Set("grant_type", "account_credentials")
	data.Set("account_id", c.cfg.AccountID)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, zoomTokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("create token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(c.cfg.ClientID, c.cfg.ClientSecret)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("token request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return "", fmt.Errorf("token request returned %d: %s", resp.StatusCode, string(body))
	}

	var tokenResp struct {
		AccessToken string `json:"access_token"`
		TokenType   string `json:"token_type"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&tokenResp); err != nil {
		return "", fmt.Errorf("decode token response: %w", err)
	}

	c.cache.token = tokenResp.AccessToken
	// Expire 60 seconds early to account for clock skew and request latency.
	c.cache.expiresAt = time.Now().Add(time.Duration(tokenResp.ExpiresIn-60) * time.Second)
	return c.cache.token, nil
}

// doGet performs an authenticated GET request with rate limiting and retry.
func (c *zoomClient) doGet(ctx context.Context, endpoint string, params url.Values) (json.RawMessage, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return nil, fmt.Errorf("rate limiter: %w", err)
	}

	fullURL := zoomAPIBase + endpoint
	if len(params) > 0 {
		fullURL += "?" + params.Encode()
	}

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		token, err := c.getAccessToken(ctx)
		if err != nil {
			return nil, fmt.Errorf("get access token: %w", err)
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		if err != nil {
			return nil, fmt.Errorf("create request: %w", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(retryBaseDelay * time.Duration(attempt+1))
			continue
		}

		body, readErr := io.ReadAll(resp.Body)
		resp.Body.Close()
		if readErr != nil {
			lastErr = readErr
			continue
		}

		if resp.StatusCode == http.StatusOK {
			return json.RawMessage(body), nil
		}

		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter := retryBaseDelay * time.Duration(attempt+1) * 2
			if raHeader := resp.Header.Get("Retry-After"); raHeader != "" {
				if secs, parseErr := time.ParseDuration(raHeader + "s"); parseErr == nil {
					retryAfter = secs
				}
			}
			lastErr = &common.RetryableError{Err: fmt.Errorf("rate limited (429)"), RetryAfter: int(retryAfter.Seconds())}
			time.Sleep(retryAfter)
			continue
		}

		if resp.StatusCode == http.StatusUnauthorized {
			// Token may have expired; invalidate cache and retry.
			c.cache.mu.Lock()
			c.cache.token = ""
			c.cache.expiresAt = time.Time{}
			c.cache.mu.Unlock()
			lastErr = fmt.Errorf("unauthorized (401): %s", string(body))
			continue
		}

		if resp.StatusCode >= 500 {
			lastErr = &common.RetryableError{Err: fmt.Errorf("server error %d: %s", resp.StatusCode, string(body))}
			time.Sleep(retryBaseDelay * time.Duration(attempt+1))
			continue
		}

		return nil, fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}
	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// Check validates the connection by calling /v2/users/me.
func (z *ZoomConnector) Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error) {
	cfg, err := parseConfig(config)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: err.Error(),
		}, nil
	}
	client := newZoomClient(cfg)
	_, err = client.doGet(ctx, "/users/me", nil)
	if err != nil {
		return &protocol.CheckResult{
			Status:  protocol.CheckStatusFailed,
			Message: fmt.Sprintf("connection check failed: %v", err),
		}, nil
	}
	return &protocol.CheckResult{
		Status:  protocol.CheckStatusSucceeded,
		Message: "Successfully connected to Zoom API",
	}, nil
}

// streamDef defines a Zoom API stream with its endpoint, response key, and schema.
type streamDef struct {
	Name         string
	Endpoint     string
	ResponseKey  string
	Schema       json.RawMessage
	SubResources []subResource
	DateFiltered bool
}

// subResource defines a child resource fetched per parent item.
type subResource struct {
	Name        string
	Endpoint    string // uses %s for parent ID
	ResponseKey string
	ParentIDKey string
	Schema      json.RawMessage
}

// allStreams returns the static catalog of supported Zoom streams.
func allStreams() []streamDef {
	return []streamDef{
		{
			Name:         "users",
			Endpoint:     "/users",
			ResponseKey:  "users",
			DateFiltered: false,
			Schema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"id": {"type": "string"},
					"first_name": {"type": "string"},
					"last_name": {"type": "string"},
					"email": {"type": "string"},
					"type": {"type": "integer"},
					"status": {"type": "string"},
					"created_at": {"type": "string"},
					"last_login_time": {"type": "string"},
					"timezone": {"type": "string"},
					"dept": {"type": "string"},
					"pmi": {"type": "integer"},
					"role_name": {"type": "string"}
				}
			}`),
		},
		{
			Name:         "meetings",
			Endpoint:     "/users/me/meetings",
			ResponseKey:  "meetings",
			DateFiltered: false,
			Schema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"id": {"type": "integer"},
					"uuid": {"type": "string"},
					"topic": {"type": "string"},
					"type": {"type": "integer"},
					"start_time": {"type": "string"},
					"duration": {"type": "integer"},
					"timezone": {"type": "string"},
					"agenda": {"type": "string"},
					"created_at": {"type": "string"},
					"join_url": {"type": "string"},
					"host_id": {"type": "string"},
					"status": {"type": "string"}
				}
			}`),
			SubResources: []subResource{
				{
					Name:        "participants",
					Endpoint:    "/past_meetings/%s/participants",
					ResponseKey: "participants",
					ParentIDKey: "uuid",
					Schema: json.RawMessage(`{
						"type": "object",
						"properties": {
							"id": {"type": "string"},
							"user_id": {"type": "string"},
							"name": {"type": "string"},
							"user_email": {"type": "string"},
							"join_time": {"type": "string"},
							"leave_time": {"type": "string"},
							"duration": {"type": "integer"},
							"meeting_id": {"type": "string"}
						}
					}`),
				},
				{
					Name:        "registrants",
					Endpoint:    "/meetings/%s/registrants",
					ResponseKey: "registrants",
					ParentIDKey: "id",
					Schema: json.RawMessage(`{
						"type": "object",
						"properties": {
							"id": {"type": "string"},
							"email": {"type": "string"},
							"first_name": {"type": "string"},
							"last_name": {"type": "string"},
							"status": {"type": "string"},
							"create_time": {"type": "string"},
							"join_url": {"type": "string"},
							"meeting_id": {"type": "integer"}
						}
					}`),
				},
			},
		},
		{
			Name:         "webinars",
			Endpoint:     "/users/me/webinars",
			ResponseKey:  "webinars",
			DateFiltered: false,
			Schema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"id": {"type": "integer"},
					"uuid": {"type": "string"},
					"topic": {"type": "string"},
					"type": {"type": "integer"},
					"start_time": {"type": "string"},
					"duration": {"type": "integer"},
					"timezone": {"type": "string"},
					"agenda": {"type": "string"},
					"created_at": {"type": "string"},
					"join_url": {"type": "string"},
					"host_id": {"type": "string"}
				}
			}`),
			SubResources: []subResource{
				{
					Name:        "webinar_registrants",
					Endpoint:    "/webinars/%s/registrants",
					ResponseKey: "registrants",
					ParentIDKey: "id",
					Schema: json.RawMessage(`{
						"type": "object",
						"properties": {
							"id": {"type": "string"},
							"email": {"type": "string"},
							"first_name": {"type": "string"},
							"last_name": {"type": "string"},
							"status": {"type": "string"},
							"create_time": {"type": "string"},
							"join_url": {"type": "string"},
							"webinar_id": {"type": "integer"}
						}
					}`),
				},
			},
		},
		{
			Name:         "recordings",
			Endpoint:     "/users/me/recordings",
			ResponseKey:  "meetings",
			DateFiltered: true,
			Schema: json.RawMessage(`{
				"type": "object",
				"properties": {
					"id": {"type": "integer"},
					"uuid": {"type": "string"},
					"topic": {"type": "string"},
					"start_time": {"type": "string"},
					"duration": {"type": "integer"},
					"total_size": {"type": "integer"},
					"recording_count": {"type": "integer"},
					"type": {"type": "integer"},
					"host_id": {"type": "string"},
					"host_email": {"type": "string"},
					"recording_files": {"type": "array", "items": {"type": "object"}}
				}
			}`),
		},
	}
}

// Discover returns a static catalog of supported Zoom streams.
func (z *ZoomConnector) Discover(_ context.Context, _ json.RawMessage) (*protocol.Catalog, error) {
	var streams []protocol.Stream
	for _, sd := range allStreams() {
		streams = append(streams, protocol.Stream{
			Name:               sd.Name,
			Namespace:          "zoom",
			Schema:             sd.Schema,
			SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh, protocol.SyncModeIncremental},
			DefaultCursorField: []string{"created_at"},
		})
		for _, sub := range sd.SubResources {
			streams = append(streams, protocol.Stream{
				Name:               sub.Name,
				Namespace:          "zoom",
				Schema:             sub.Schema,
				SupportedSyncModes: []protocol.SyncMode{protocol.SyncModeFullRefresh},
			})
		}
	}
	return &protocol.Catalog{Streams: streams}, nil
}

// Read fetches data from configured Zoom streams with pagination and date filtering.
func (z *ZoomConnector) Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error {
	cfg, err := parseConfig(config)
	if err != nil {
		return err
	}
	client := newZoomClient(cfg)

	// Index which streams are selected.
	selectedStreams := make(map[string]protocol.ConfiguredStream)
	for _, cs := range catalog.Streams {
		selectedStreams[cs.Stream.Name] = cs
	}

	// Build a map of sub-resource names to their parent stream definitions.
	subToParent := make(map[string]struct {
		parent streamDef
		sub    subResource
	})
	for _, sd := range allStreams() {
		for _, sub := range sd.SubResources {
			subToParent[sub.Name] = struct {
				parent streamDef
				sub    subResource
			}{parent: sd, sub: sub}
		}
	}

	for _, sd := range allStreams() {
		cs, selected := selectedStreams[sd.Name]
		// Even if the parent stream is not selected, we may need it for sub-resources.
		anySubSelected := false
		for _, sub := range sd.SubResources {
			if _, ok := selectedStreams[sub.Name]; ok {
				anySubSelected = true
				break
			}
		}
		if !selected && !anySubSelected {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}

		// Build an onPage callback that processes sub-resources page-by-page,
		// avoiding accumulation of all parent items into memory.
		var onPage func([]map[string]interface{})
		if anySubSelected {
			onPage = func(pageItems []map[string]interface{}) {
				for _, sub := range sd.SubResources {
					subCS, subSel := selectedStreams[sub.Name]
					if !subSel {
						continue
					}
					if subErr := readSubResource(ctx, client, sub, pageItems, subCS, output); subErr != nil {
						output <- protocol.Message{
							Type: protocol.MessageTypeLog,
							Log: &protocol.Log{
								Level:     protocol.LogLevelError,
								Message:   fmt.Sprintf("error reading sub-resource %s: %v", sub.Name, subErr),
								Timestamp: time.Now().UTC(),
							},
						}
					}
				}
			}
		}

		readErr := readZoomStream(ctx, client, cfg, sd, cs, state, output, selected, onPage)
		if readErr != nil {
			output <- protocol.Message{
				Type: protocol.MessageTypeLog,
				Log: &protocol.Log{
					Level:     protocol.LogLevelError,
					Message:   fmt.Sprintf("error reading stream %s: %v", sd.Name, readErr),
					Timestamp: time.Now().UTC(),
				},
			}
			continue
		}
	}
	return nil
}

// readZoomStream reads a top-level Zoom stream with cursor pagination.
// Instead of accumulating all items into memory, it processes sub-resources
// page-by-page via the onPage callback and only keeps items for the current page.
func readZoomStream(ctx context.Context, client *zoomClient, cfg *ZoomConfig, sd streamDef, cs protocol.ConfiguredStream, state map[string]json.RawMessage, output chan<- protocol.Message, emitRecords bool, onPage func(pageItems []map[string]interface{})) error {
	nextPageToken := ""

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		params := url.Values{}
		params.Set("page_size", "300")
		if nextPageToken != "" {
			params.Set("next_page_token", nextPageToken)
		}

		// Apply date filtering for date-filtered streams.
		if sd.DateFiltered {
			from := cfg.StartDate
			if from == "" {
				from = time.Now().AddDate(0, -6, 0).Format("2006-01-02")
			}
			to := cfg.EndDate
			if to == "" {
				to = time.Now().Format("2006-01-02")
			}
			params.Set("from", from)
			params.Set("to", to)
		}

		// For incremental, apply cursor filter from state.
		if cs.SyncMode == protocol.SyncModeIncremental && len(cs.CursorField) > 0 {
			if stateData, ok := state[sd.Name]; ok {
				var stateMap map[string]string
				if json.Unmarshal(stateData, &stateMap) == nil {
					if fromVal, exists := stateMap[cs.CursorField[0]]; exists {
						params.Set("from", fromVal)
					}
				}
			}
		}

		respBody, err := client.doGet(ctx, sd.Endpoint, params)
		if err != nil {
			return fmt.Errorf("fetch %s: %w", sd.Name, err)
		}

		var page map[string]json.RawMessage
		if err := json.Unmarshal(respBody, &page); err != nil {
			return fmt.Errorf("decode %s page: %w", sd.Name, err)
		}

		itemsRaw, hasKey := page[sd.ResponseKey]
		if !hasKey {
			break
		}

		var items []map[string]interface{}
		if err := json.Unmarshal(itemsRaw, &items); err != nil {
			return fmt.Errorf("decode %s items: %w", sd.Name, err)
		}

		var lastCursorValue string
		for _, item := range items {
			if emitRecords {
				itemBytes, _ := json.Marshal(item)
				output <- protocol.Message{
					Type: protocol.MessageTypeRecord,
					Record: &protocol.Record{
						Stream:    sd.Name,
						Namespace: "zoom",
						Data:      itemBytes,
						EmittedAt: time.Now().UTC(),
					},
				}
			}
			if len(cs.CursorField) > 0 {
				if cursorVal, ok := item[cs.CursorField[0]]; ok {
					if sv, ok := cursorVal.(string); ok {
						lastCursorValue = sv
					}
				}
			}
		}

		// Process sub-resources for this page immediately, then discard items.
		if onPage != nil {
			onPage(items)
		}

		// Emit state for incremental.
		if cs.SyncMode == protocol.SyncModeIncremental && len(cs.CursorField) > 0 && lastCursorValue != "" && emitRecords {
			stateData, _ := json.Marshal(map[string]string{
				cs.CursorField[0]: lastCursorValue,
			})
			output <- protocol.Message{
				Type: protocol.MessageTypeState,
				State: &protocol.State{
					Type:   protocol.StateTypeStream,
					Stream: sd.Name,
					Data:   stateData,
				},
			}
		}

		// Check for next page token.
		if nptRaw, ok := page["next_page_token"]; ok {
			var npt string
			if err := json.Unmarshal(nptRaw, &npt); err == nil && npt != "" {
				nextPageToken = npt
				continue
			}
		}
		break
	}
	return nil
}

// readSubResource reads child resources for each parent item.
func readSubResource(ctx context.Context, client *zoomClient, sub subResource, parentItems []map[string]interface{}, cs protocol.ConfiguredStream, output chan<- protocol.Message) error {
	for _, parent := range parentItems {
		if err := ctx.Err(); err != nil {
			return err
		}

		parentID, ok := parent[sub.ParentIDKey]
		if !ok {
			continue
		}
		parentIDStr := fmt.Sprintf("%v", parentID)
		if parentIDStr == "" {
			continue
		}

		// URL-encode the parent ID (important for UUIDs with special characters).
		encodedID := url.PathEscape(url.PathEscape(parentIDStr))
		endpoint := fmt.Sprintf(sub.Endpoint, encodedID)

		nextPageToken := ""
		for {
			params := url.Values{}
			params.Set("page_size", "300")
			if nextPageToken != "" {
				params.Set("next_page_token", nextPageToken)
			}

			respBody, err := client.doGet(ctx, endpoint, params)
			if err != nil {
				output <- protocol.Message{
					Type: protocol.MessageTypeLog,
					Log: &protocol.Log{
						Level:     protocol.LogLevelWarn,
						Message:   fmt.Sprintf("skipping sub-resource %s for parent %s: %v", sub.Name, parentIDStr, err),
						Timestamp: time.Now().UTC(),
					},
				}
				break
			}

			var page map[string]json.RawMessage
			if err := json.Unmarshal(respBody, &page); err != nil {
				break
			}

			itemsRaw, hasKey := page[sub.ResponseKey]
			if !hasKey {
				break
			}

			var items []map[string]interface{}
			if err := json.Unmarshal(itemsRaw, &items); err != nil {
				break
			}

			for _, item := range items {
				// Inject parent reference.
				item["_parent_id"] = parentIDStr
				itemBytes, _ := json.Marshal(item)
				output <- protocol.Message{
					Type: protocol.MessageTypeRecord,
					Record: &protocol.Record{
						Stream:    sub.Name,
						Namespace: "zoom",
						Data:      itemBytes,
						EmittedAt: time.Now().UTC(),
					},
				}
			}

			if nptRaw, ok := page["next_page_token"]; ok {
				var npt string
				if err := json.Unmarshal(nptRaw, &npt); err == nil && npt != "" {
					nextPageToken = npt
					continue
				}
			}
			break
		}
	}
	return nil
}
