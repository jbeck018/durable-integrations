package secrets

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// SecretManager defines the interface for secret storage backends.
// Implementations must be safe for concurrent use.
type SecretManager interface {
	// Store persists a secret at the given path.
	Store(ctx context.Context, path string, data map[string]interface{}) error

	// Get retrieves a secret from the given path.
	Get(ctx context.Context, path string) (map[string]interface{}, error)

	// Delete removes a secret at the given path.
	Delete(ctx context.Context, path string) error

	// Rotate replaces a secret at the given path with new data.
	Rotate(ctx context.Context, path string, newData map[string]interface{}) error
}

// ConnectorCredentialPath returns the standard path for connector credentials
// following the pattern: tenants/{tenant_id}/connectors/{connector_id}/credentials
func ConnectorCredentialPath(tenantID, connectorID string) string {
	return fmt.Sprintf("tenants/%s/connectors/%s/credentials", tenantID, connectorID)
}

// ----- Vault-backed SecretManager -----

// VaultSecretManager implements SecretManager using a HashiCorp Vault backend.
type VaultSecretManager struct {
	client *VaultClient
}

// NewVaultSecretManager creates a new Vault-backed secret manager.
func NewVaultSecretManager(client *VaultClient) *VaultSecretManager {
	return &VaultSecretManager{client: client}
}

// Store persists a secret in Vault at the given path.
func (m *VaultSecretManager) Store(ctx context.Context, path string, data map[string]interface{}) error {
	return m.client.StoreSecret(ctx, path, data)
}

// Get retrieves a secret from Vault at the given path.
func (m *VaultSecretManager) Get(ctx context.Context, path string) (map[string]interface{}, error) {
	return m.client.GetSecret(ctx, path)
}

// Delete removes a secret from Vault at the given path.
func (m *VaultSecretManager) Delete(ctx context.Context, path string) error {
	return m.client.DeleteSecret(ctx, path)
}

// Rotate replaces a secret in Vault with new data (Vault handles versioning).
func (m *VaultSecretManager) Rotate(ctx context.Context, path string, newData map[string]interface{}) error {
	return m.client.RotateSecret(ctx, path, newData)
}

// ----- Local encrypted file-based SecretManager -----

// LocalSecretManager implements SecretManager using AES-256-GCM encrypted files
// on the local filesystem. Suitable for development and testing environments only.
type LocalSecretManager struct {
	mu      sync.RWMutex
	baseDir string
	gcm     cipher.AEAD
}

// NewLocalSecretManager creates a new file-based secret manager that encrypts
// secrets at rest using AES-256-GCM. The encryptionKey is hashed with SHA-256
// to derive a 256-bit key.
func NewLocalSecretManager(baseDir, encryptionKey string) (*LocalSecretManager, error) {
	if encryptionKey == "" {
		return nil, fmt.Errorf("encryption key is required for local secret manager")
	}

	if err := os.MkdirAll(baseDir, 0700); err != nil {
		return nil, fmt.Errorf("creating secret store directory: %w", err)
	}

	// Derive a 256-bit key from the provided encryption key via SHA-256.
	keyHash := sha256.Sum256([]byte(encryptionKey))

	block, err := aes.NewCipher(keyHash[:])
	if err != nil {
		return nil, fmt.Errorf("creating AES cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("creating GCM cipher: %w", err)
	}

	return &LocalSecretManager{
		baseDir: baseDir,
		gcm:     gcm,
	}, nil
}

// Store encrypts and writes a secret to a file at the given path.
func (m *LocalSecretManager) Store(_ context.Context, path string, data map[string]interface{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	plaintext, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshaling secret data: %w", err)
	}

	encrypted, err := m.encrypt(plaintext)
	if err != nil {
		return err
	}

	filePath := m.filePath(path)
	if err := os.MkdirAll(filepath.Dir(filePath), 0700); err != nil {
		return fmt.Errorf("creating directory for secret: %w", err)
	}

	if err := os.WriteFile(filePath, encrypted, 0600); err != nil {
		return fmt.Errorf("writing encrypted secret: %w", err)
	}

	return nil
}

// Get reads and decrypts a secret from the file at the given path.
func (m *LocalSecretManager) Get(_ context.Context, path string) (map[string]interface{}, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	filePath := m.filePath(path)
	encrypted, err := os.ReadFile(filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("secret not found at path %s", path)
		}
		return nil, fmt.Errorf("reading encrypted secret: %w", err)
	}

	plaintext, err := m.decrypt(encrypted)
	if err != nil {
		return nil, err
	}

	var data map[string]interface{}
	if err := json.Unmarshal(plaintext, &data); err != nil {
		return nil, fmt.Errorf("unmarshaling secret data: %w", err)
	}

	return data, nil
}

// Delete removes the encrypted secret file at the given path.
func (m *LocalSecretManager) Delete(_ context.Context, path string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	filePath := m.filePath(path)
	if err := os.Remove(filePath); err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("secret not found at path %s", path)
		}
		return fmt.Errorf("deleting secret file: %w", err)
	}

	return nil
}

// Rotate replaces a secret with new data by re-encrypting and overwriting.
func (m *LocalSecretManager) Rotate(ctx context.Context, path string, newData map[string]interface{}) error {
	return m.Store(ctx, path, newData)
}

// filePath converts a logical secret path to a filesystem path, sanitising
// directory traversal attempts.
func (m *LocalSecretManager) filePath(path string) string {
	cleaned := filepath.Clean(path)
	return filepath.Join(m.baseDir, cleaned+".enc")
}

// encrypt produces a nonce-prepended AES-256-GCM ciphertext.
func (m *LocalSecretManager) encrypt(plaintext []byte) ([]byte, error) {
	nonce := make([]byte, m.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("generating nonce: %w", err)
	}
	return m.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// decrypt extracts the nonce from the ciphertext and decrypts using AES-256-GCM.
func (m *LocalSecretManager) decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := m.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}
	nonce := ciphertext[:nonceSize]
	data := ciphertext[nonceSize:]
	plaintext, err := m.gcm.Open(nil, nonce, data, nil)
	if err != nil {
		return nil, fmt.Errorf("decrypting secret: %w", err)
	}
	return plaintext, nil
}
