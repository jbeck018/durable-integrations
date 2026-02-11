// Package registry provides a dynamic connector registry that extends the
// static CDK registry with hot-reloading, health checks, and config-driven
// connector loading.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/flowforge/flowforge/pkg/cdk"
)

// ConnectorConfig describes a connector loaded from a configuration file.
type ConnectorConfig struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Version     string `json:"version"`
	Type        string `json:"type"` // "source", "destination", "bidirectional"
	Category    string `json:"category"`
	Icon        string `json:"icon,omitempty"`
	Enabled     bool   `json:"enabled"`
}

// HealthStatus represents the health of a registered connector.
type HealthStatus string

const (
	HealthStatusHealthy   HealthStatus = "healthy"
	HealthStatusUnhealthy HealthStatus = "unhealthy"
	HealthStatusUnknown   HealthStatus = "unknown"
)

// ConnectorHealth holds health check results for a connector.
type ConnectorHealth struct {
	Name       string       `json:"name"`
	Status     HealthStatus `json:"status"`
	Message    string       `json:"message,omitempty"`
	LastCheck  time.Time    `json:"last_check"`
	CheckCount int64        `json:"check_count"`
}

// DynamicRegistry extends the static CDK registry with runtime management
// capabilities: loading connectors from config files, hot-reloading, and
// health monitoring.
type DynamicRegistry struct {
	mu          sync.RWMutex
	configDir   string
	configs     map[string]ConnectorConfig
	health      map[string]*ConnectorHealth
	lastRefresh time.Time
}

// NewDynamicRegistry creates a DynamicRegistry that loads connector
// configurations from the given directory path.
func NewDynamicRegistry(configDir string) *DynamicRegistry {
	return &DynamicRegistry{
		configDir: configDir,
		configs:   make(map[string]ConnectorConfig),
		health:    make(map[string]*ConnectorHealth),
	}
}

// LoadFromConfig reads all JSON connector configuration files from the
// configured directory and registers them in the internal config map.
// Files must have a .json extension and contain a valid ConnectorConfig.
func (dr *DynamicRegistry) LoadFromConfig(path string) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return fmt.Errorf("read config directory %s: %w", path, err)
	}

	dr.mu.Lock()
	defer dr.mu.Unlock()

	var loadErrors []error
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}

		fullPath := filepath.Join(path, entry.Name())
		data, readErr := os.ReadFile(fullPath)
		if readErr != nil {
			loadErrors = append(loadErrors, fmt.Errorf("read %s: %w", fullPath, readErr))
			continue
		}

		var cfg ConnectorConfig
		if unmErr := json.Unmarshal(data, &cfg); unmErr != nil {
			loadErrors = append(loadErrors, fmt.Errorf("parse %s: %w", fullPath, unmErr))
			continue
		}

		if cfg.Name == "" {
			loadErrors = append(loadErrors, fmt.Errorf("config %s: missing name", fullPath))
			continue
		}

		dr.configs[cfg.Name] = cfg
		dr.health[cfg.Name] = &ConnectorHealth{
			Name:   cfg.Name,
			Status: HealthStatusUnknown,
		}
	}

	dr.lastRefresh = time.Now()

	if len(loadErrors) > 0 {
		return fmt.Errorf("loaded with %d errors; first: %w", len(loadErrors), loadErrors[0])
	}
	return nil
}

// Refresh reloads connector configurations from the config directory,
// updating the registry with any new or changed configurations.
func (dr *DynamicRegistry) Refresh() error {
	return dr.LoadFromConfig(dr.configDir)
}

// GetConfig returns the configuration for a connector by name.
func (dr *DynamicRegistry) GetConfig(name string) (ConnectorConfig, bool) {
	dr.mu.RLock()
	defer dr.mu.RUnlock()
	cfg, ok := dr.configs[name]
	return cfg, ok
}

// ListConfigs returns all loaded connector configurations.
func (dr *DynamicRegistry) ListConfigs() []ConnectorConfig {
	dr.mu.RLock()
	defer dr.mu.RUnlock()
	result := make([]ConnectorConfig, 0, len(dr.configs))
	for _, cfg := range dr.configs {
		result = append(result, cfg)
	}
	return result
}

// ListAll returns both statically registered connectors (from CDK) and
// dynamically loaded connector configs, merged by name.
func (dr *DynamicRegistry) ListAll() []cdk.ConnectorMeta {
	static := cdk.ListConnectors()

	dr.mu.RLock()
	defer dr.mu.RUnlock()

	seen := make(map[string]bool, len(static))
	for _, m := range static {
		seen[m.Name] = true
	}

	for _, cfg := range dr.configs {
		if seen[cfg.Name] {
			continue
		}
		static = append(static, cdk.ConnectorMeta{
			Name:        cfg.Name,
			DisplayName: cfg.DisplayName,
			Version:     cfg.Version,
			Type:        cfg.Type,
			Category:    cfg.Category,
			Icon:        cfg.Icon,
		})
	}
	return static
}

// HealthCheck runs a health check for a specific connector by performing a
// Check call with an empty config. The result is cached.
func (dr *DynamicRegistry) HealthCheck(ctx context.Context, name string) (*ConnectorHealth, error) {
	dr.mu.RLock()
	h, exists := dr.health[name]
	dr.mu.RUnlock()

	if !exists {
		// Connector might be in the static registry only.
		_, srcErr := cdk.GetSource(name)
		_, dstErr := cdk.GetDestination(name)
		if srcErr != nil && dstErr != nil {
			return nil, fmt.Errorf("connector %q not found", name)
		}
		h = &ConnectorHealth{Name: name, Status: HealthStatusUnknown}
		dr.mu.Lock()
		dr.health[name] = h
		dr.mu.Unlock()
	}

	status := HealthStatusHealthy
	message := "connector registered and available"

	src, srcErr := cdk.GetSource(name)
	if srcErr == nil {
		_, specErr := src.Spec()
		if specErr != nil {
			status = HealthStatusUnhealthy
			message = fmt.Sprintf("spec error: %v", specErr)
		}
	} else {
		dst, dstErr := cdk.GetDestination(name)
		if dstErr == nil {
			_, specErr := dst.Spec()
			if specErr != nil {
				status = HealthStatusUnhealthy
				message = fmt.Sprintf("spec error: %v", specErr)
			}
		} else {
			// Check dynamic config existence.
			dr.mu.RLock()
			cfg, hasCfg := dr.configs[name]
			dr.mu.RUnlock()
			if hasCfg && cfg.Enabled {
				status = HealthStatusUnknown
				message = "connector configured but not loaded into CDK registry"
			} else if hasCfg {
				status = HealthStatusUnhealthy
				message = "connector is disabled"
			} else {
				status = HealthStatusUnhealthy
				message = "connector not found in any registry"
			}
		}
	}

	dr.mu.Lock()
	h.Status = status
	h.Message = message
	h.LastCheck = time.Now()
	h.CheckCount++
	dr.mu.Unlock()

	return h, nil
}

// HealthCheckAll runs health checks for all known connectors.
func (dr *DynamicRegistry) HealthCheckAll(ctx context.Context) map[string]*ConnectorHealth {
	names := make([]string, 0)

	// Gather all known connector names.
	dr.mu.RLock()
	for name := range dr.configs {
		names = append(names, name)
	}
	dr.mu.RUnlock()

	for _, m := range cdk.ListConnectors() {
		found := false
		for _, n := range names {
			if n == m.Name {
				found = true
				break
			}
		}
		if !found {
			names = append(names, m.Name)
		}
	}

	results := make(map[string]*ConnectorHealth, len(names))
	for _, name := range names {
		h, err := dr.HealthCheck(ctx, name)
		if err != nil {
			results[name] = &ConnectorHealth{
				Name:      name,
				Status:    HealthStatusUnhealthy,
				Message:   err.Error(),
				LastCheck: time.Now(),
			}
			continue
		}
		results[name] = h
	}
	return results
}

// IsEnabled checks whether a connector is enabled in the dynamic config.
// Returns true for connectors only in the static CDK registry (always enabled).
func (dr *DynamicRegistry) IsEnabled(name string) bool {
	dr.mu.RLock()
	cfg, ok := dr.configs[name]
	dr.mu.RUnlock()

	if ok {
		return cfg.Enabled
	}
	// If not in dynamic config, check static registry.
	_, srcErr := cdk.GetSource(name)
	_, dstErr := cdk.GetDestination(name)
	return srcErr == nil || dstErr == nil
}

// LastRefreshTime returns when the registry was last refreshed.
func (dr *DynamicRegistry) LastRefreshTime() time.Time {
	dr.mu.RLock()
	defer dr.mu.RUnlock()
	return dr.lastRefresh
}
