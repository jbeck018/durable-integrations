// Package auth provides authentication services for FlowForge, including API key
// validation, JWT-based authentication, and principal identity management.
package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/common"
)

// PrincipalType identifies the kind of authenticated identity.
type PrincipalType string

const (
	PrincipalAPIKey  PrincipalType = "api_key"
	PrincipalJWT     PrincipalType = "jwt"
	PrincipalService PrincipalType = "service"
)

// Principal represents an authenticated identity within FlowForge.
type Principal struct {
	ID       string        `json:"id"`
	TenantID string        `json:"tenant_id"`
	UserID   string        `json:"user_id"`
	Scopes   []string      `json:"scopes"`
	Type     PrincipalType `json:"type"`
}

// HasScope returns true if the principal possesses the given scope.
// A wildcard scope "*" grants all scopes.
func (p *Principal) HasScope(scope string) bool {
	for _, s := range p.Scopes {
		if s == "*" || s == scope {
			return true
		}
	}
	return false
}

// HasAnyScope returns true if the principal possesses at least one of the given scopes.
func (p *Principal) HasAnyScope(scopes ...string) bool {
	for _, scope := range scopes {
		if p.HasScope(scope) {
			return true
		}
	}
	return false
}

// APIKeyRecord stores the metadata and hash for a generated API key.
type APIKeyRecord struct {
	ID        string     `json:"id"`
	TenantID  string     `json:"tenant_id"`
	Name      string     `json:"name"`
	KeyHash   string     `json:"key_hash"`
	KeyPrefix string     `json:"key_prefix"`
	Scopes    []string   `json:"scopes"`
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	RevokedAt *time.Time `json:"revoked_at,omitempty"`
}

// IsExpired returns true if the API key has passed its expiration time.
func (r *APIKeyRecord) IsExpired() bool {
	if r.ExpiresAt == nil {
		return false
	}
	return time.Now().UTC().After(*r.ExpiresAt)
}

// IsRevoked returns true if the API key has been revoked.
func (r *APIKeyRecord) IsRevoked() bool {
	return r.RevokedAt != nil
}

// APIKeyStore defines the persistence interface for API key records.
// Implementations include in-memory stores for testing and database-backed stores
// for production.
type APIKeyStore interface {
	SaveAPIKey(ctx context.Context, record *APIKeyRecord) error
	GetAPIKeyByHash(ctx context.Context, keyHash string) (*APIKeyRecord, error)
	GetAPIKeyByID(ctx context.Context, keyID string) (*APIKeyRecord, error)
	RevokeAPIKey(ctx context.Context, keyID string) error
	ListAPIKeys(ctx context.Context, tenantID string) ([]*APIKeyRecord, error)
}

// InMemoryAPIKeyStore provides an in-memory implementation of APIKeyStore.
// Suitable for testing and development environments.
type InMemoryAPIKeyStore struct {
	mu      sync.RWMutex
	keys    map[string]*APIKeyRecord // keyed by ID
	byHash  map[string]string        // keyHash -> ID
}

// NewInMemoryAPIKeyStore creates a new in-memory API key store.
func NewInMemoryAPIKeyStore() *InMemoryAPIKeyStore {
	return &InMemoryAPIKeyStore{
		keys:   make(map[string]*APIKeyRecord),
		byHash: make(map[string]string),
	}
}

// SaveAPIKey persists an API key record in memory.
func (s *InMemoryAPIKeyStore) SaveAPIKey(_ context.Context, record *APIKeyRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.keys[record.ID] = record
	s.byHash[record.KeyHash] = record.ID
	return nil
}

// GetAPIKeyByHash looks up an API key record by the SHA-256 hash of the key.
func (s *InMemoryAPIKeyStore) GetAPIKeyByHash(_ context.Context, keyHash string) (*APIKeyRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	id, ok := s.byHash[keyHash]
	if !ok {
		return nil, common.ErrNotFound
	}
	record, ok := s.keys[id]
	if !ok {
		return nil, common.ErrNotFound
	}
	return record, nil
}

// GetAPIKeyByID looks up an API key record by its unique identifier.
func (s *InMemoryAPIKeyStore) GetAPIKeyByID(_ context.Context, keyID string) (*APIKeyRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	record, ok := s.keys[keyID]
	if !ok {
		return nil, common.ErrNotFound
	}
	return record, nil
}

// RevokeAPIKey marks an API key as revoked by setting its RevokedAt timestamp.
func (s *InMemoryAPIKeyStore) RevokeAPIKey(_ context.Context, keyID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, ok := s.keys[keyID]
	if !ok {
		return common.ErrNotFound
	}
	now := time.Now().UTC()
	record.RevokedAt = &now
	return nil
}

// ListAPIKeys returns all API key records for a given tenant.
func (s *InMemoryAPIKeyStore) ListAPIKeys(_ context.Context, tenantID string) ([]*APIKeyRecord, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*APIKeyRecord
	for _, record := range s.keys {
		if record.TenantID == tenantID {
			result = append(result, record)
		}
	}
	return result, nil
}

// AuthService provides authentication operations for API keys and JWTs.
type AuthService struct {
	keyStore   APIKeyStore
	jwtService *JWTService
}

// NewAuthService creates a new authentication service.
func NewAuthService(keyStore APIKeyStore, jwtService *JWTService) *AuthService {
	return &AuthService{
		keyStore:   keyStore,
		jwtService: jwtService,
	}
}

// apiKeyPrefix is prepended to generated API keys for easy identification.
const apiKeyPrefix = "ffk_"

// apiKeyBytes is the number of random bytes used to generate an API key.
const apiKeyBytes = 32

// ValidateAPIKey authenticates an API key by hashing it and looking up the
// corresponding record. It checks for revocation and expiration.
func (s *AuthService) ValidateAPIKey(ctx context.Context, key string) (*Principal, error) {
	if key == "" {
		return nil, fmt.Errorf("empty API key: %w", common.ErrUnauthorized)
	}

	keyHash := hashAPIKey(key)
	record, err := s.keyStore.GetAPIKeyByHash(ctx, keyHash)
	if err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return nil, fmt.Errorf("invalid API key: %w", common.ErrUnauthorized)
		}
		return nil, fmt.Errorf("looking up API key: %w", err)
	}

	if record.IsRevoked() {
		return nil, fmt.Errorf("API key has been revoked: %w", common.ErrUnauthorized)
	}

	if record.IsExpired() {
		return nil, fmt.Errorf("API key has expired: %w", common.ErrUnauthorized)
	}

	// Constant-time comparison of the stored hash against the computed hash
	// to prevent timing side-channel attacks.
	storedHash, err := hex.DecodeString(record.KeyHash)
	if err != nil {
		return nil, fmt.Errorf("corrupt key hash in store: %w", common.ErrUnauthorized)
	}
	computedHash, err := hex.DecodeString(keyHash)
	if err != nil {
		return nil, fmt.Errorf("computing key hash: %w", common.ErrUnauthorized)
	}
	if subtle.ConstantTimeCompare(storedHash, computedHash) != 1 {
		return nil, fmt.Errorf("API key mismatch: %w", common.ErrUnauthorized)
	}

	return &Principal{
		ID:       record.ID,
		TenantID: record.TenantID,
		Scopes:   record.Scopes,
		Type:     PrincipalAPIKey,
	}, nil
}

// ValidateJWT validates a JWT token string and returns the authenticated principal.
func (s *AuthService) ValidateJWT(ctx context.Context, token string) (*Principal, error) {
	if token == "" {
		return nil, fmt.Errorf("empty token: %w", common.ErrUnauthorized)
	}

	// Strip "Bearer " prefix if present.
	token = strings.TrimPrefix(token, "Bearer ")
	token = strings.TrimPrefix(token, "bearer ")

	claims, err := s.jwtService.ValidateToken(token)
	if err != nil {
		return nil, fmt.Errorf("invalid JWT: %w", common.ErrUnauthorized)
	}

	// Verify the tenant context matches if one is set on the request context.
	if ctxTenant := common.TenantIDFrom(ctx); ctxTenant != "" && ctxTenant != claims.TenantID {
		return nil, fmt.Errorf("JWT tenant mismatch: %w", common.ErrForbidden)
	}

	return &Principal{
		ID:       claims.PrincipalID,
		TenantID: claims.TenantID,
		UserID:   claims.UserID,
		Scopes:   claims.Scopes,
		Type:     PrincipalJWT,
	}, nil
}

// GenerateAPIKey creates a new API key for the given tenant. It generates a
// cryptographically random key, stores the SHA-256 hash, and returns the
// plaintext key (which is never stored). The caller must present this key
// to the end user exactly once.
func (s *AuthService) GenerateAPIKey(ctx context.Context, tenantID, name string, scopes []string, expiresAt *time.Time) (string, error) {
	if tenantID == "" {
		return "", fmt.Errorf("tenant ID is required: %w", common.ErrUnauthorized)
	}
	if name == "" {
		return "", fmt.Errorf("key name is required: %w", common.ErrUnauthorized)
	}
	if len(scopes) == 0 {
		return "", fmt.Errorf("at least one scope is required: %w", common.ErrUnauthorized)
	}

	// Generate a cryptographically random key.
	rawKey := make([]byte, apiKeyBytes)
	if _, err := rand.Read(rawKey); err != nil {
		return "", fmt.Errorf("generating random key: %w", err)
	}
	plaintext := apiKeyPrefix + hex.EncodeToString(rawKey)
	keyHash := hashAPIKey(plaintext)

	// Generate a unique ID for the key record.
	idBytes := make([]byte, 16)
	if _, err := rand.Read(idBytes); err != nil {
		return "", fmt.Errorf("generating key ID: %w", err)
	}
	keyID := hex.EncodeToString(idBytes)

	// Store only the first 8 characters of the key (after prefix) as a
	// human-readable prefix for identification in listings.
	keyPrefixStr := plaintext[:len(apiKeyPrefix)+8]

	record := &APIKeyRecord{
		ID:        keyID,
		TenantID:  tenantID,
		Name:      name,
		KeyHash:   keyHash,
		KeyPrefix: keyPrefixStr,
		Scopes:    scopes,
		ExpiresAt: expiresAt,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.keyStore.SaveAPIKey(ctx, record); err != nil {
		return "", fmt.Errorf("saving API key: %w", err)
	}

	return plaintext, nil
}

// RevokeAPIKey marks an API key as revoked so it can no longer be used.
func (s *AuthService) RevokeAPIKey(ctx context.Context, keyID string) error {
	if keyID == "" {
		return fmt.Errorf("key ID is required: %w", common.ErrUnauthorized)
	}
	if err := s.keyStore.RevokeAPIKey(ctx, keyID); err != nil {
		if errors.Is(err, common.ErrNotFound) {
			return fmt.Errorf("API key not found: %w", common.ErrNotFound)
		}
		return fmt.Errorf("revoking API key: %w", err)
	}
	return nil
}

// hashAPIKey returns the hex-encoded SHA-256 hash of an API key string.
func hashAPIKey(key string) string {
	h := sha256.Sum256([]byte(key))
	return hex.EncodeToString(h[:])
}
