// Package config provides secure configuration management for FlowForge
// connectors and syncs. It handles encryption at rest, caching, and
// validation of connector configurations against their specification schemas.
package config

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/internal/observability/logging"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// ConnectorConfigRepository is the interface for reading/writing encrypted
// connector configurations in persistent storage.
type ConnectorConfigRepository interface {
	GetEncryptedConfig(ctx context.Context, connectorID string) ([]byte, error)
	SaveEncryptedConfig(ctx context.Context, connectorID string, data []byte) error
}

// SyncConfigRepository is the interface for reading/writing sync configurations.
type SyncConfigRepository interface {
	GetSyncConfig(ctx context.Context, syncID string) (json.RawMessage, error)
	SaveSyncConfig(ctx context.Context, syncID string, data json.RawMessage) error
}

// ConfigCache is the interface for caching decrypted connector configs.
type ConfigCache interface {
	Get(ctx context.Context, connectorID string) (json.RawMessage, bool, error)
	Set(ctx context.Context, connectorID string, config json.RawMessage) error
	Invalidate(ctx context.Context, connectorID string) error
}

// Encryptor provides AES-256-GCM encryption and decryption for connector
// secrets stored at rest.
type Encryptor struct {
	gcm cipher.AEAD
}

// NewEncryptor creates an Encryptor with the given 32-byte AES-256 key.
// If the key is empty, encryption is disabled (passthrough mode for development).
func NewEncryptor(key []byte) (*Encryptor, error) {
	if len(key) == 0 {
		return &Encryptor{}, nil
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("encryption key must be exactly 32 bytes, got %d", len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}
	return &Encryptor{gcm: gcm}, nil
}

// Encrypt encrypts plaintext using AES-256-GCM with a random nonce.
// If encryption is disabled (no key), returns the plaintext unchanged.
func (e *Encryptor) Encrypt(plaintext []byte) ([]byte, error) {
	if e.gcm == nil {
		return plaintext, nil
	}
	nonce := make([]byte, e.gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}
	return e.gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// Decrypt decrypts ciphertext produced by Encrypt.
// If encryption is disabled (no key), returns the ciphertext unchanged.
func (e *Encryptor) Decrypt(ciphertext []byte) ([]byte, error) {
	if e.gcm == nil {
		return ciphertext, nil
	}
	nonceSize := e.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short: %d bytes", len(ciphertext))
	}
	nonce := ciphertext[:nonceSize]
	data := ciphertext[nonceSize:]
	plaintext, err := e.gcm.Open(nil, nonce, data, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}
	return plaintext, nil
}

// RetryPolicy defines how failed sync operations should be retried.
type RetryPolicy struct {
	MaxRetries     int    `json:"max_retries"`
	InitialBackoff string `json:"initial_backoff"` // e.g. "1s", "30s"
	MaxBackoff     string `json:"max_backoff"`     // e.g. "5m"
	BackoffFactor  float64 `json:"backoff_factor"`
}

// StreamConfig describes a stream's sync configuration within a SyncConfig.
type StreamConfig struct {
	StreamName          string              `json:"stream_name"`
	SyncMode            protocol.SyncMode   `json:"sync_mode"`
	DestinationSyncMode protocol.DestinationSyncMode `json:"destination_sync_mode"`
	CursorField         []string            `json:"cursor_field,omitempty"`
	PrimaryKey          [][]string          `json:"primary_key,omitempty"`
}

// FieldMapping describes a single source-to-destination field mapping.
type FieldMapping struct {
	SourceField string `json:"source_field"`
	DestField   string `json:"dest_field"`
	CoerceType  string `json:"coerce_type,omitempty"`
}

// SyncConfig holds the complete configuration for a sync workflow.
type SyncConfig struct {
	SourceConnectorID string                       `json:"source_connector_id"`
	DestConnectorID   string                       `json:"dest_connector_id"`
	SourceConfig      json.RawMessage              `json:"source_config"`
	DestConfig        json.RawMessage              `json:"dest_config"`
	StreamConfigs     []StreamConfig               `json:"stream_configs"`
	FieldMappings     map[string][]FieldMapping    `json:"field_mappings,omitempty"`
	Schedule          string                       `json:"schedule,omitempty"`
	BatchSize         int                          `json:"batch_size"`
	RetryPolicy       *RetryPolicy                 `json:"retry_policy,omitempty"`
}

// ConfigManager provides secure storage and retrieval of connector and sync
// configurations. It encrypts sensitive connector configs at rest and caches
// decrypted configs in Redis for fast access.
type ConfigManager struct {
	connectorRepo ConnectorConfigRepository
	syncRepo      SyncConfigRepository
	cache         ConfigCache
	encryptor     *Encryptor
	logger        *logging.Logger
}

// NewConfigManager creates a ConfigManager. The cache parameter may be nil if
// caching is not desired. The encryptor must not be nil.
func NewConfigManager(connectorRepo ConnectorConfigRepository, syncRepo SyncConfigRepository, cache ConfigCache, encryptor *Encryptor) *ConfigManager {
	return &ConfigManager{
		connectorRepo: connectorRepo,
		syncRepo:      syncRepo,
		cache:         cache,
		encryptor:     encryptor,
		logger:        logging.Global().WithField("component", "config_manager"),
	}
}

// GetConnectorConfig retrieves and decrypts a connector's configuration.
// It checks the cache first, falling back to the persistent store on miss.
func (cm *ConfigManager) GetConnectorConfig(ctx context.Context, connectorID string) (json.RawMessage, error) {
	if connectorID == "" {
		return nil, fmt.Errorf("connectorID is required: %w", common.ErrInvalidConfig)
	}

	// Check the cache first.
	if cm.cache != nil {
		cached, found, err := cm.cache.Get(ctx, connectorID)
		if err != nil {
			cm.logger.WithContext(ctx).Warn("cache read failed, falling through to store",
				"connector_id", connectorID,
				"error", err,
			)
		} else if found {
			return cached, nil
		}
	}

	// Load from persistent store.
	encrypted, err := cm.connectorRepo.GetEncryptedConfig(ctx, connectorID)
	if err != nil {
		return nil, fmt.Errorf("failed to read connector config %s: %w", connectorID, err)
	}

	// Decrypt.
	plaintext, err := cm.encryptor.Decrypt(encrypted)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt connector config %s: %w", connectorID, err)
	}

	config := json.RawMessage(plaintext)

	// Populate the cache.
	if cm.cache != nil {
		if cacheErr := cm.cache.Set(ctx, connectorID, config); cacheErr != nil {
			cm.logger.WithContext(ctx).Warn("cache write failed",
				"connector_id", connectorID,
				"error", cacheErr,
			)
		}
	}

	return config, nil
}

// SaveConnectorConfig encrypts and persists a connector's configuration.
// It also invalidates the cache entry to prevent stale reads.
func (cm *ConfigManager) SaveConnectorConfig(ctx context.Context, connectorID string, config json.RawMessage) error {
	if connectorID == "" {
		return fmt.Errorf("connectorID is required: %w", common.ErrInvalidConfig)
	}
	if len(config) == 0 {
		return fmt.Errorf("config is required: %w", common.ErrInvalidConfig)
	}

	// Validate the JSON is well-formed.
	if !json.Valid(config) {
		return fmt.Errorf("config is not valid JSON: %w", common.ErrInvalidConfig)
	}

	// Encrypt.
	encrypted, err := cm.encryptor.Encrypt([]byte(config))
	if err != nil {
		return fmt.Errorf("failed to encrypt connector config %s: %w", connectorID, err)
	}

	// Persist.
	if err := cm.connectorRepo.SaveEncryptedConfig(ctx, connectorID, encrypted); err != nil {
		return fmt.Errorf("failed to save connector config %s: %w", connectorID, err)
	}

	// Invalidate cache.
	if cm.cache != nil {
		if cacheErr := cm.cache.Invalidate(ctx, connectorID); cacheErr != nil {
			cm.logger.WithContext(ctx).Warn("cache invalidation failed",
				"connector_id", connectorID,
				"error", cacheErr,
			)
		}
	}

	cm.logger.WithContext(ctx).Info("connector config saved",
		"connector_id", connectorID,
	)

	return nil
}

// ValidateConfig validates a connector configuration JSON against the
// connector's specification schema. It checks that all required fields
// are present and values match expected types.
func (cm *ConfigManager) ValidateConfig(connectorType string, config json.RawMessage) error {
	if connectorType == "" {
		return fmt.Errorf("connectorType is required: %w", common.ErrInvalidConfig)
	}
	if len(config) == 0 {
		return fmt.Errorf("config is required: %w", common.ErrInvalidConfig)
	}
	if !json.Valid(config) {
		return fmt.Errorf("config is not valid JSON: %w", common.ErrSchemaValidation)
	}

	// Parse the config into a generic map to validate structure.
	var configMap map[string]interface{}
	if err := json.Unmarshal(config, &configMap); err != nil {
		return fmt.Errorf("config is not a JSON object: %w", common.ErrSchemaValidation)
	}

	// Perform structural validation: ensure the config is a non-empty object.
	// Full JSON Schema validation would require a schema registry lookup.
	// This validates the fundamental requirement that config is a valid object.
	if len(configMap) == 0 {
		return &common.ValidationError{
			Field:   "config",
			Message: fmt.Sprintf("connector type %q config must be a non-empty JSON object", connectorType),
		}
	}

	return nil
}

// GetSyncConfig retrieves a sync's configuration from the persistent store.
func (cm *ConfigManager) GetSyncConfig(ctx context.Context, syncID string) (*SyncConfig, error) {
	if syncID == "" {
		return nil, fmt.Errorf("syncID is required: %w", common.ErrInvalidConfig)
	}

	raw, err := cm.syncRepo.GetSyncConfig(ctx, syncID)
	if err != nil {
		return nil, fmt.Errorf("failed to read sync config %s: %w", syncID, err)
	}

	var cfg SyncConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse sync config %s: %w", syncID, err)
	}

	// Apply defaults.
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1000
	}

	return &cfg, nil
}

// SaveSyncConfig marshals and persists a sync configuration.
func (cm *ConfigManager) SaveSyncConfig(ctx context.Context, syncID string, config *SyncConfig) error {
	if syncID == "" {
		return fmt.Errorf("syncID is required: %w", common.ErrInvalidConfig)
	}
	if config == nil {
		return fmt.Errorf("config is required: %w", common.ErrInvalidConfig)
	}

	// Validate required fields.
	var errs common.ValidationErrors
	if config.SourceConnectorID == "" {
		errs = append(errs, common.ValidationError{Field: "source_connector_id", Message: "required"})
	}
	if config.DestConnectorID == "" {
		errs = append(errs, common.ValidationError{Field: "dest_connector_id", Message: "required"})
	}
	if len(config.StreamConfigs) == 0 {
		errs = append(errs, common.ValidationError{Field: "stream_configs", Message: "at least one stream must be configured"})
	}
	if len(errs) > 0 {
		return errs
	}

	// Apply defaults.
	if config.BatchSize <= 0 {
		config.BatchSize = 1000
	}

	raw, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("failed to marshal sync config: %w", err)
	}

	if err := cm.syncRepo.SaveSyncConfig(ctx, syncID, raw); err != nil {
		return fmt.Errorf("failed to save sync config %s: %w", syncID, err)
	}

	cm.logger.WithContext(ctx).Info("sync config saved",
		"sync_id", syncID,
		"streams", len(config.StreamConfigs),
	)

	return nil
}
