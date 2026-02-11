package common

import (
	"context"
	"encoding/json"
	"fmt"

	"golang.org/x/oauth2"
)

// OAuthConfig holds the OAuth2 credentials parsed from a connector's config JSON.
type OAuthConfig struct {
	ClientID     string `json:"client_id"`
	ClientSecret string `json:"client_secret"`
	RefreshToken string `json:"refresh_token"`
	AccessToken  string `json:"access_token"`
	TokenURL     string `json:"token_url"`
	AuthURL      string `json:"auth_url"`
	Scopes       []string `json:"scopes"`
	// Extra parameters that some providers require (e.g. Salesforce instance_url).
	Extra map[string]string `json:"extra,omitempty"`
}

// ParseOAuthConfig extracts OAuth2 credentials from a raw connector config.
// It expects the config to contain an "oauth" key with the credentials, or
// to have the fields at the top level.
func ParseOAuthConfig(config json.RawMessage) (*OAuthConfig, error) {
	// First try parsing the whole config as containing an "oauth" sub-object.
	var wrapper struct {
		OAuth *OAuthConfig `json:"oauth"`
	}
	if err := json.Unmarshal(config, &wrapper); err == nil && wrapper.OAuth != nil {
		return wrapper.OAuth, nil
	}

	// Fall back to top-level fields.
	var oc OAuthConfig
	if err := json.Unmarshal(config, &oc); err != nil {
		return nil, fmt.Errorf("failed to parse OAuth config: %w", err)
	}
	if oc.ClientID == "" && oc.AccessToken == "" {
		return nil, fmt.Errorf("OAuth config requires at least client_id or access_token")
	}
	return &oc, nil
}

// TokenSource returns an oauth2.TokenSource that automatically refreshes the
// access token using the refresh token when the current token expires.
// If only an access token is provided (no refresh token), it returns a static
// token source.
func (oc *OAuthConfig) TokenSource(ctx context.Context) oauth2.TokenSource {
	if oc.RefreshToken == "" {
		tok := &oauth2.Token{AccessToken: oc.AccessToken}
		return oauth2.StaticTokenSource(tok)
	}

	cfg := &oauth2.Config{
		ClientID:     oc.ClientID,
		ClientSecret: oc.ClientSecret,
		Endpoint: oauth2.Endpoint{
			AuthURL:  oc.AuthURL,
			TokenURL: oc.TokenURL,
		},
		Scopes: oc.Scopes,
	}

	tok := &oauth2.Token{
		AccessToken:  oc.AccessToken,
		RefreshToken: oc.RefreshToken,
	}

	return cfg.TokenSource(ctx, tok)
}

// OAuth2Config returns the underlying golang.org/x/oauth2.Config for use
// by connectors that need to perform custom token exchanges.
func (oc *OAuthConfig) OAuth2Config() *oauth2.Config {
	return &oauth2.Config{
		ClientID:     oc.ClientID,
		ClientSecret: oc.ClientSecret,
		Endpoint: oauth2.Endpoint{
			AuthURL:  oc.AuthURL,
			TokenURL: oc.TokenURL,
		},
		Scopes: oc.Scopes,
	}
}
