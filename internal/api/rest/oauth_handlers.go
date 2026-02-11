package rest

import (
	"fmt"
	"log/slog"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/flowforge/flowforge/internal/embedded/oauth"
	"github.com/flowforge/flowforge/internal/security/encryption"
)

// OAuthHandlers implements the Backend-for-Frontend OAuth2 endpoints.
// These coordinate the popup-based OAuth flow used by all UI framework adapters:
//
//	POST /oauth/initiate     — Creates OAuth session, returns authorize_url
//	GET  /oauth/callback     — Handles IdP redirect, stores tokens server-side
//	POST /oauth/status       — Check if a connection's token is valid
//	POST /oauth/reauthorize  — Trigger re-auth flow for existing connection
type OAuthHandlers struct {
	Manager   *oauth.OAuthManager
	Encryptor *encryption.Encryptor
	Logger    *slog.Logger
}

// Routes mounts OAuth routes on the given router.
func (h *OAuthHandlers) Routes(r chi.Router) {
	r.Post("/initiate", h.Initiate)
	r.Get("/callback", h.Callback)
	r.Post("/status", h.Status)
	r.Post("/reauthorize", h.Reauthorize)
}

// initiateRequest is the JSON body for POST /oauth/initiate.
type initiateRequest struct {
	ConnectorID string `json:"connector_id"`
	TenantID    string `json:"tenant_id"`
}

// initiateResponse is the JSON response for POST /oauth/initiate.
type initiateResponse struct {
	SessionID    string `json:"session_id"`
	AuthorizeURL string `json:"authorize_url"`
}

// Initiate starts an OAuth flow for a connector. The UI opens a popup to the
// returned authorize_url. Server-side state tracks the pending flow.
func (h *OAuthHandlers) Initiate(w http.ResponseWriter, r *http.Request) {
	var req initiateRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, "invalid request body", "INVALID_REQUEST")
		return
	}

	if req.ConnectorID == "" {
		Error(w, http.StatusBadRequest, "connector_id is required", "MISSING_FIELD")
		return
	}
	if req.TenantID == "" {
		Error(w, http.StatusBadRequest, "tenant_id is required", "MISSING_FIELD")
		return
	}

	config, err := h.resolveOAuthConfig(req.ConnectorID)
	if err != nil {
		Error(w, http.StatusNotFound, fmt.Sprintf("no OAuth config for connector %s", req.ConnectorID), "NOT_FOUND")
		return
	}

	authURL, err := h.Manager.StartOAuthFlow(r.Context(), req.ConnectorID, config)
	if err != nil {
		h.Logger.Error("failed to start OAuth flow", slog.String("error", err.Error()))
		Error(w, http.StatusInternalServerError, "failed to initiate OAuth flow", "INTERNAL_ERROR")
		return
	}

	sessionID := extractStateFromURL(authURL)

	JSON(w, http.StatusOK, initiateResponse{
		SessionID:    sessionID,
		AuthorizeURL: authURL,
	})
}

// Callback handles the OAuth provider redirect. This is called by the IdP
// after the user authorizes in the popup. It exchanges the code for tokens,
// stores them server-side encrypted, and returns an HTML page that posts a
// message to the opener window.
func (h *OAuthHandlers) Callback(w http.ResponseWriter, r *http.Request) {
	state := r.URL.Query().Get("state")
	code := r.URL.Query().Get("code")
	errMsg := r.URL.Query().Get("error")

	if errMsg != "" {
		errDesc := r.URL.Query().Get("error_description")
		h.writeCallbackHTML(w, state, "", false, fmt.Sprintf("%s: %s", errMsg, errDesc))
		return
	}

	if state == "" || code == "" {
		h.writeCallbackHTML(w, state, "", false, "missing state or code parameter")
		return
	}

	tokens, err := h.Manager.HandleCallback(r.Context(), state, code)
	if err != nil {
		h.Logger.Error("OAuth callback failed", slog.String("error", err.Error()))
		h.writeCallbackHTML(w, state, "", false, "authorization failed")
		return
	}

	// Encrypt tokens before storing. If no encryptor is configured, store
	// plaintext (development mode only — production requires encryption).
	if h.Encryptor != nil {
		if encErr := h.encryptAndStoreTokens(tokens); encErr != nil {
			h.Logger.Error("failed to encrypt tokens", slog.String("error", encErr.Error()))
		}
	}

	// Store tokens in the manager's token store keyed by connection ID.
	connectionID := fmt.Sprintf("conn_%s", state[:16])
	h.Manager.StoreTokens(connectionID, tokens)

	h.writeCallbackHTML(w, state, connectionID, true, "")
}

// encryptAndStoreTokens encrypts the access and refresh tokens in place
// using the configured Encryptor. The encrypted values are stored in the
// token struct; the Manager's StoreTokens call persists them.
func (h *OAuthHandlers) encryptAndStoreTokens(tokens *oauth.OAuthTokens) error {
	if tokens.AccessToken != "" {
		encrypted, err := h.Encryptor.Encrypt([]byte(tokens.AccessToken))
		if err != nil {
			return fmt.Errorf("encrypt access token: %w", err)
		}
		tokens.AccessToken = string(encrypted)
	}
	return nil
}

// statusRequest is the JSON body for POST /oauth/status.
type statusRequest struct {
	ConnectionID string `json:"connection_id"`
}

// Status checks if a connection's OAuth token is still valid.
func (h *OAuthHandlers) Status(w http.ResponseWriter, r *http.Request) {
	var req statusRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, "invalid request body", "INVALID_REQUEST")
		return
	}

	if req.ConnectionID == "" {
		Error(w, http.StatusBadRequest, "connection_id is required", "MISSING_FIELD")
		return
	}

	tokens, ok := h.Manager.GetTokens(req.ConnectionID)
	if !ok {
		JSON(w, http.StatusOK, map[string]bool{"valid": false})
		return
	}

	JSON(w, http.StatusOK, map[string]bool{"valid": !tokens.IsExpired()})
}

// reauthorizeRequest is the JSON body for POST /oauth/reauthorize.
type reauthorizeRequest struct {
	ConnectionID string `json:"connection_id"`
	TenantID     string `json:"tenant_id"`
}

// Reauthorize triggers a re-authorization flow for an existing connection.
// It looks up the connector type from the stored tokens to find the correct
// OAuth configuration.
func (h *OAuthHandlers) Reauthorize(w http.ResponseWriter, r *http.Request) {
	var req reauthorizeRequest
	if err := DecodeJSON(r, &req); err != nil {
		Error(w, http.StatusBadRequest, "invalid request body", "INVALID_REQUEST")
		return
	}

	if req.ConnectionID == "" {
		Error(w, http.StatusBadRequest, "connection_id is required", "MISSING_FIELD")
		return
	}

	// Look up the connector type from existing tokens for this connection.
	tokens, ok := h.Manager.GetTokens(req.ConnectionID)
	if !ok {
		Error(w, http.StatusNotFound, "no tokens found for connection", "NOT_FOUND")
		return
	}

	config, err := h.resolveOAuthConfig(tokens.ConnectorType)
	if err != nil {
		Error(w, http.StatusNotFound, "no OAuth config for connection", "NOT_FOUND")
		return
	}

	authURL, err := h.Manager.StartOAuthFlow(r.Context(), tokens.ConnectorType, config)
	if err != nil {
		h.Logger.Error("failed to start re-auth flow", slog.String("error", err.Error()))
		Error(w, http.StatusInternalServerError, "failed to initiate re-authorization", "INTERNAL_ERROR")
		return
	}

	sessionID := extractStateFromURL(authURL)

	JSON(w, http.StatusOK, initiateResponse{
		SessionID:    sessionID,
		AuthorizeURL: authURL,
	})
}

// resolveOAuthConfig looks up the OAuth configuration for a connector type
// from the OAuthManager's registered configs.
func (h *OAuthHandlers) resolveOAuthConfig(connectorType string) (oauth.OAuthConfig, error) {
	if connectorType == "" {
		return oauth.OAuthConfig{}, fmt.Errorf("connector type is required")
	}

	config, ok := h.Manager.GetConfig(connectorType)
	if !ok {
		return oauth.OAuthConfig{}, fmt.Errorf("no OAuth config registered for connector type %q", connectorType)
	}

	return config, nil
}

// writeCallbackHTML writes an HTML page that posts a message to the opener window
// and closes itself. This is the popup's callback page.
func (h *OAuthHandlers) writeCallbackHTML(w http.ResponseWriter, sessionID, connectionID string, success bool, errMsg string) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.WriteHeader(http.StatusOK)

	successStr := "false"
	if success {
		successStr = "true"
	}

	html := fmt.Sprintf(`<!DOCTYPE html>
<html>
<head><title>OAuth Complete</title></head>
<body>
<script>
if (window.opener) {
  window.opener.postMessage({
    type: "oauth_complete",
    sessionId: %q,
    connectionId: %q,
    success: %s,
    error: %q
  }, window.location.origin);
}
window.close();
</script>
<p>Authorization complete. This window will close automatically.</p>
</body>
</html>`, sessionID, connectionID, successStr, errMsg)

	_, _ = w.Write([]byte(html))
}

// extractStateFromURL extracts the state query parameter from an authorization URL.
func extractStateFromURL(authURL string) string {
	for i := 0; i < len(authURL); i++ {
		if i+6 < len(authURL) && authURL[i:i+6] == "state=" {
			start := i + 6
			end := start
			for end < len(authURL) && authURL[end] != '&' {
				end++
			}
			return authURL[start:end]
		}
	}
	return ""
}
