// Package secrets provides secret management for FlowForge, including
// HashiCorp Vault integration and a local encrypted file-based fallback.
package secrets

import (
	"context"
	"fmt"
	"sync"

	vault "github.com/hashicorp/vault/api"
)

// VaultClient wraps the HashiCorp Vault API client, providing connection
// pooling and simplified CRUD operations for the KV v2 secrets engine.
type VaultClient struct {
	client    *vault.Client
	mountPath string
	mu        sync.RWMutex
}

// VaultOption configures the VaultClient.
type VaultOption func(*VaultClient)

// WithMountPath sets the KV v2 secrets engine mount path. Defaults to "secret".
func WithMountPath(path string) VaultOption {
	return func(vc *VaultClient) {
		vc.mountPath = path
	}
}

// NewVaultClient creates a new Vault client with connection pooling.
// The addr parameter is the Vault server URL (e.g., "http://127.0.0.1:8200").
// The token parameter is the Vault authentication token.
func NewVaultClient(addr, token string, opts ...VaultOption) (*VaultClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("vault address is required")
	}
	if token == "" {
		return nil, fmt.Errorf("vault token is required")
	}

	config := vault.DefaultConfig()
	config.Address = addr

	// Configure connection pooling through the HTTP transport.
	config.MaxRetries = 3

	client, err := vault.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("creating vault client: %w", err)
	}

	client.SetToken(token)

	vc := &VaultClient{
		client:    client,
		mountPath: "secret",
	}

	for _, opt := range opts {
		opt(vc)
	}

	return vc, nil
}

// kvDataPath returns the KV v2 data path for the given logical path.
func (v *VaultClient) kvDataPath(path string) string {
	return fmt.Sprintf("%s/data/%s", v.mountPath, path)
}

// kvMetadataPath returns the KV v2 metadata path for the given logical path.
func (v *VaultClient) kvMetadataPath(path string) string {
	return fmt.Sprintf("%s/metadata/%s", v.mountPath, path)
}

// StoreSecret writes a secret to Vault at the specified path.
// For KV v2, the data is wrapped in the required {"data": ...} envelope.
func (v *VaultClient) StoreSecret(ctx context.Context, path string, data map[string]interface{}) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	wrappedData := map[string]interface{}{
		"data": data,
	}

	_, err := v.client.Logical().WriteWithContext(ctx, v.kvDataPath(path), wrappedData)
	if err != nil {
		return fmt.Errorf("writing secret to vault path %s: %w", path, err)
	}

	return nil
}

// GetSecret reads a secret from Vault at the specified path.
// Returns the unwrapped data payload from the KV v2 response.
func (v *VaultClient) GetSecret(ctx context.Context, path string) (map[string]interface{}, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	secret, err := v.client.Logical().ReadWithContext(ctx, v.kvDataPath(path))
	if err != nil {
		return nil, fmt.Errorf("reading secret from vault path %s: %w", path, err)
	}

	if secret == nil || secret.Data == nil {
		return nil, fmt.Errorf("secret not found at path %s", path)
	}

	// KV v2 nests the actual data under a "data" key in the response.
	data, ok := secret.Data["data"].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("unexpected data format at vault path %s", path)
	}

	return data, nil
}

// DeleteSecret removes a secret from Vault at the specified path.
// This performs a metadata delete, which destroys all versions.
func (v *VaultClient) DeleteSecret(ctx context.Context, path string) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	_, err := v.client.Logical().DeleteWithContext(ctx, v.kvMetadataPath(path))
	if err != nil {
		return fmt.Errorf("deleting secret at vault path %s: %w", path, err)
	}

	return nil
}

// RotateSecret atomically replaces a secret at the given path with new data.
// Vault KV v2 automatically versions secrets, so the old version is preserved
// and the new data becomes the latest version.
func (v *VaultClient) RotateSecret(ctx context.Context, path string, newData map[string]interface{}) error {
	return v.StoreSecret(ctx, path, newData)
}

// HealthCheck verifies connectivity to the Vault server.
func (v *VaultClient) HealthCheck() error {
	health, err := v.client.Sys().Health()
	if err != nil {
		return fmt.Errorf("vault health check failed: %w", err)
	}
	if health.Sealed {
		return fmt.Errorf("vault is sealed")
	}
	return nil
}
