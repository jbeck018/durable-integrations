// Package middleware provides HTTP middleware for the FlowForge REST API.
package middleware

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/flowforge/flowforge/internal/common"
)

// scopeKey is a context key for API key scopes.
type scopeKey struct{}

// ScopesFrom returns the scopes attached to the context by auth middleware.
func ScopesFrom(ctx context.Context) []string {
	v, _ := ctx.Value(scopeKey{}).([]string)
	return v
}

// withScopes attaches scopes to the context.
func withScopes(ctx context.Context, scopes []string) context.Context {
	return context.WithValue(ctx, scopeKey{}, scopes)
}

// APIKeyValidator validates an API key and returns tenant ID, user ID, and scopes.
type APIKeyValidator interface {
	Validate(ctx context.Context, key string) (tenantID, userID string, scopes []string, err error)
}

// APIKeyAuth returns middleware that validates API keys from the Authorization header.
// Expected format: Authorization: Bearer ff_<api_key>
func APIKeyAuth(validator APIKeyValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeAuthError(w, http.StatusUnauthorized, "missing Authorization header", "AUTH_MISSING")
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				writeAuthError(w, http.StatusUnauthorized, "invalid Authorization format, expected: Bearer <token>", "AUTH_INVALID_FORMAT")
				return
			}

			token := parts[1]
			tenantID, userID, scopes, err := validator.Validate(r.Context(), token)
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, "invalid API key", "AUTH_INVALID_KEY")
				return
			}

			ctx := r.Context()
			ctx = common.WithTenantID(ctx, tenantID)
			ctx = common.WithUserID(ctx, userID)
			ctx = withScopes(ctx, scopes)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// JWTConfig holds configuration for JWT validation.
type JWTConfig struct {
	Secret          string
	Issuer          string
	Audience        string
	ClockSkew       time.Duration
}

// jwtHeader is the JWT header structure.
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// jwtClaims holds standard and custom JWT claims for embedded iPaaS.
type jwtClaims struct {
	Sub      string   `json:"sub"`
	TenantID string   `json:"tenant_id"`
	Iss      string   `json:"iss"`
	Aud      string   `json:"aud"`
	Exp      int64    `json:"exp"`
	Iat      int64    `json:"iat"`
	Scopes   []string `json:"scopes"`
}

// JWTAuth returns middleware that validates JWT tokens for the embedded iPaaS flow.
// Expected format: Authorization: Bearer <jwt_token>
func JWTAuth(cfg JWTConfig) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			if authHeader == "" {
				writeAuthError(w, http.StatusUnauthorized, "missing Authorization header", "AUTH_MISSING")
				return
			}

			parts := strings.SplitN(authHeader, " ", 2)
			if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
				writeAuthError(w, http.StatusUnauthorized, "invalid Authorization format", "AUTH_INVALID_FORMAT")
				return
			}

			token := parts[1]
			claims, err := validateJWT(token, cfg)
			if err != nil {
				writeAuthError(w, http.StatusUnauthorized, "invalid JWT token: "+err.Error(), "AUTH_INVALID_JWT")
				return
			}

			ctx := r.Context()
			ctx = common.WithTenantID(ctx, claims.TenantID)
			ctx = common.WithUserID(ctx, claims.Sub)
			ctx = withScopes(ctx, claims.Scopes)

			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// validateJWT parses and validates a JWT token using HS256.
func validateJWT(tokenStr string, cfg JWTConfig) (*jwtClaims, error) {
	segments := strings.Split(tokenStr, ".")
	if len(segments) != 3 {
		return nil, errInvalidToken("malformed JWT: expected 3 segments")
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(segments[0])
	if err != nil {
		return nil, errInvalidToken("invalid header encoding")
	}

	var header jwtHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, errInvalidToken("invalid header JSON")
	}
	if header.Alg != "HS256" {
		return nil, errInvalidToken("unsupported algorithm: " + header.Alg)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(segments[1])
	if err != nil {
		return nil, errInvalidToken("invalid payload encoding")
	}

	signatureBytes, err := base64.RawURLEncoding.DecodeString(segments[2])
	if err != nil {
		return nil, errInvalidToken("invalid signature encoding")
	}

	signingInput := segments[0] + "." + segments[1]
	mac := hmac.New(sha256.New, []byte(cfg.Secret))
	mac.Write([]byte(signingInput))
	expectedSig := mac.Sum(nil)

	if !hmac.Equal(signatureBytes, expectedSig) {
		return nil, errInvalidToken("signature verification failed")
	}

	var claims jwtClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, errInvalidToken("invalid claims JSON")
	}

	now := time.Now().Unix()
	skewSec := int64(cfg.ClockSkew.Seconds())

	if claims.Exp > 0 && now > claims.Exp+skewSec {
		return nil, errInvalidToken("token has expired")
	}

	if cfg.Issuer != "" && claims.Iss != cfg.Issuer {
		return nil, errInvalidToken("invalid issuer")
	}

	if cfg.Audience != "" && claims.Aud != cfg.Audience {
		return nil, errInvalidToken("invalid audience")
	}

	return &claims, nil
}

// jwtError represents a JWT validation error.
type jwtError struct {
	msg string
}

func (e *jwtError) Error() string { return e.msg }

func errInvalidToken(msg string) error {
	return &jwtError{msg: msg}
}

// writeAuthError writes a JSON error response for auth failures.
func writeAuthError(w http.ResponseWriter, status int, message, code string) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(map[string]interface{}{
		"error": map[string]interface{}{
			"code":    code,
			"message": message,
		},
	})
}
