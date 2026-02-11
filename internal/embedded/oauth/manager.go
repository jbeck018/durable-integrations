// Package oauth provides an OAuth 2.0 flow manager for embedded integrations.
// It handles the authorization code grant flow: generating redirect URLs,
// exchanging authorization codes for tokens, and refreshing expired tokens.
package oauth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// OAuthConfig holds the OAuth 2.0 client configuration for a connector type.
type OAuthConfig struct {
	ClientID     string   `json:"client_id"`
	ClientSecret string   `json:"client_secret"`
	RedirectURI  string   `json:"redirect_uri"`
	Scopes       []string `json:"scopes"`
	AuthURL      string   `json:"auth_url"`
	TokenURL     string   `json:"token_url"`
}

// OAuthTokens holds the token set returned by an OAuth provider.
type OAuthTokens struct {
	AccessToken   string    `json:"access_token"`
	RefreshToken  string    `json:"refresh_token,omitempty"`
	TokenType     string    `json:"token_type"`
	ExpiresAt     time.Time `json:"expires_at"`
	Scopes        []string  `json:"scopes,omitempty"`
	ConnectorType string    `json:"connector_type,omitempty"`
}

// IsExpired returns true if the access token has expired or will expire
// within the next 60 seconds.
func (t *OAuthTokens) IsExpired() bool {
	return time.Now().After(t.ExpiresAt.Add(-60 * time.Second))
}

// pendingFlow tracks an in-flight OAuth authorization flow.
type pendingFlow struct {
	state         string
	connectorType string
	config        OAuthConfig
	createdAt     time.Time
}

// OAuthManager coordinates OAuth 2.0 flows for embedded integrations.
// It manages pending authorization flows and token storage.
type OAuthManager struct {
	mu           sync.RWMutex
	pendingFlows map[string]*pendingFlow // state -> pending flow
	tokens       map[string]*OAuthTokens // integration ID -> tokens
	configs      map[string]OAuthConfig  // connector type -> oauth config
	httpClient   *http.Client
}

// NewOAuthManager creates a new OAuthManager.
func NewOAuthManager() *OAuthManager {
	return &OAuthManager{
		pendingFlows: make(map[string]*pendingFlow),
		tokens:       make(map[string]*OAuthTokens),
		configs:      make(map[string]OAuthConfig),
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// RegisterConnectorConfig registers an OAuth configuration for a connector type.
func (om *OAuthManager) RegisterConnectorConfig(connectorType string, config OAuthConfig) {
	om.mu.Lock()
	om.configs[connectorType] = config
	om.mu.Unlock()
}

// GetConfig returns the OAuth configuration for a connector type.
func (om *OAuthManager) GetConfig(connectorType string) (OAuthConfig, bool) {
	om.mu.RLock()
	config, ok := om.configs[connectorType]
	om.mu.RUnlock()
	return config, ok
}

// StoreTokens stores OAuth tokens for an integration.
func (om *OAuthManager) StoreTokens(integrationID string, tokens *OAuthTokens) {
	om.mu.Lock()
	om.tokens[integrationID] = tokens
	om.mu.Unlock()
}

// GetTokens retrieves stored tokens for an integration.
func (om *OAuthManager) GetTokens(integrationID string) (*OAuthTokens, bool) {
	om.mu.RLock()
	t, ok := om.tokens[integrationID]
	om.mu.RUnlock()
	return t, ok
}

// StartOAuthFlow initiates an OAuth 2.0 authorization code flow. It generates
// a CSRF state token and returns the authorization URL to redirect the user to.
func (om *OAuthManager) StartOAuthFlow(ctx context.Context, connectorType string, config OAuthConfig) (string, error) {
	if config.AuthURL == "" {
		return "", fmt.Errorf("auth URL is required")
	}
	if config.ClientID == "" {
		return "", fmt.Errorf("client ID is required")
	}
	if config.RedirectURI == "" {
		return "", fmt.Errorf("redirect URI is required")
	}

	state, err := generateState()
	if err != nil {
		return "", fmt.Errorf("generate state: %w", err)
	}

	om.mu.Lock()
	om.pendingFlows[state] = &pendingFlow{
		state:         state,
		connectorType: connectorType,
		config:        config,
		createdAt:     time.Now(),
	}
	om.mu.Unlock()

	// Clean up expired flows older than 10 minutes.
	go om.cleanupExpiredFlows()

	authURL, err := url.Parse(config.AuthURL)
	if err != nil {
		return "", fmt.Errorf("parse auth URL: %w", err)
	}

	q := authURL.Query()
	q.Set("client_id", config.ClientID)
	q.Set("redirect_uri", config.RedirectURI)
	q.Set("response_type", "code")
	q.Set("state", state)
	if len(config.Scopes) > 0 {
		q.Set("scope", strings.Join(config.Scopes, " "))
	}
	authURL.RawQuery = q.Encode()

	return authURL.String(), nil
}

// HandleCallback processes the OAuth callback after the user authorizes.
// It validates the state parameter, exchanges the authorization code for
// tokens, and stores the tokens.
func (om *OAuthManager) HandleCallback(ctx context.Context, state, code string) (*OAuthTokens, error) {
	if state == "" {
		return nil, fmt.Errorf("state parameter is required")
	}
	if code == "" {
		return nil, fmt.Errorf("authorization code is required")
	}

	om.mu.Lock()
	flow, ok := om.pendingFlows[state]
	if ok {
		delete(om.pendingFlows, state)
	}
	om.mu.Unlock()

	if !ok {
		return nil, fmt.Errorf("invalid or expired OAuth state")
	}

	// Check if the flow has expired (10 minute window).
	if time.Since(flow.createdAt) > 10*time.Minute {
		return nil, fmt.Errorf("OAuth flow expired")
	}

	tokens, err := om.exchangeCode(ctx, flow.config, code)
	if err != nil {
		return nil, fmt.Errorf("exchange code: %w", err)
	}

	tokens.ConnectorType = flow.connectorType
	return tokens, nil
}

// RefreshToken refreshes the access token for an integration using the stored
// refresh token. Returns updated tokens.
func (om *OAuthManager) RefreshToken(ctx context.Context, integrationID string) (*OAuthTokens, error) {
	om.mu.RLock()
	existing, ok := om.tokens[integrationID]
	om.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no tokens found for integration %q", integrationID)
	}
	if existing.RefreshToken == "" {
		return nil, fmt.Errorf("no refresh token available for integration %q", integrationID)
	}

	// Look up the OAuth config using the connector type stored with the tokens.
	if existing.ConnectorType == "" {
		return nil, fmt.Errorf("no connector type associated with tokens for integration %q", integrationID)
	}

	om.mu.RLock()
	config, found := om.configs[existing.ConnectorType]
	om.mu.RUnlock()

	if !found {
		return nil, fmt.Errorf("no OAuth config registered for connector type %q", existing.ConnectorType)
	}

	tokens, err := om.doRefresh(ctx, config, existing.RefreshToken)
	if err != nil {
		return nil, fmt.Errorf("refresh token: %w", err)
	}

	// Preserve the refresh token if the provider did not return a new one.
	if tokens.RefreshToken == "" {
		tokens.RefreshToken = existing.RefreshToken
	}

	om.mu.Lock()
	om.tokens[integrationID] = tokens
	om.mu.Unlock()

	return tokens, nil
}

// exchangeCode exchanges an authorization code for tokens.
func (om *OAuthManager) exchangeCode(ctx context.Context, config OAuthConfig, code string) (*OAuthTokens, error) {
	data := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {config.RedirectURI},
		"client_id":     {config.ClientID},
		"client_secret": {config.ClientSecret},
	}

	return om.doTokenRequest(ctx, config.TokenURL, data)
}

// doRefresh performs a refresh token request.
func (om *OAuthManager) doRefresh(ctx context.Context, config OAuthConfig, refreshToken string) (*OAuthTokens, error) {
	data := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {config.ClientID},
		"client_secret": {config.ClientSecret},
	}

	return om.doTokenRequest(ctx, config.TokenURL, data)
}

// tokenResponse is the JSON response from the OAuth token endpoint.
type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in"`
	Scope        string `json:"scope"`
	Error        string `json:"error"`
	ErrorDesc    string `json:"error_description"`
}

// doTokenRequest sends a POST to the token endpoint and parses the response.
func (om *OAuthManager) doTokenRequest(ctx context.Context, tokenURL string, data url.Values) (*OAuthTokens, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := om.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("token request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read token response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		var errResp tokenResponse
		if jsonErr := json.Unmarshal(body, &errResp); jsonErr == nil && errResp.Error != "" {
			return nil, fmt.Errorf("OAuth error: %s - %s", errResp.Error, errResp.ErrorDesc)
		}
		return nil, fmt.Errorf("token request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var tr tokenResponse
	if jsonErr := json.Unmarshal(body, &tr); jsonErr != nil {
		return nil, fmt.Errorf("parse token response: %w", jsonErr)
	}

	if tr.AccessToken == "" {
		return nil, fmt.Errorf("token response missing access_token")
	}

	expiresAt := time.Now().Add(time.Duration(tr.ExpiresIn) * time.Second)
	if tr.ExpiresIn == 0 {
		// Default to 1 hour if not specified.
		expiresAt = time.Now().Add(1 * time.Hour)
	}

	var scopes []string
	if tr.Scope != "" {
		scopes = strings.Split(tr.Scope, " ")
	}

	return &OAuthTokens{
		AccessToken:  tr.AccessToken,
		RefreshToken: tr.RefreshToken,
		TokenType:    tr.TokenType,
		ExpiresAt:    expiresAt,
		Scopes:       scopes,
	}, nil
}

// cleanupExpiredFlows removes pending flows older than 10 minutes.
func (om *OAuthManager) cleanupExpiredFlows() {
	om.mu.Lock()
	defer om.mu.Unlock()

	cutoff := time.Now().Add(-10 * time.Minute)
	for state, flow := range om.pendingFlows {
		if flow.createdAt.Before(cutoff) {
			delete(om.pendingFlows, state)
		}
	}
}

// generateState creates a cryptographically random state string.
func generateState() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
