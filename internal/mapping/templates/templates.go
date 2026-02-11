// Package templates provides a template store for reusable field mapping
// configurations, including built-in templates for common integration patterns.
package templates

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/mapping/auto"
)

// TemplateInfo holds metadata about a stored mapping template.
type TemplateInfo struct {
	Name        string    `json:"name"`
	Description string    `json:"description,omitempty"`
	FieldCount  int       `json:"field_count"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	BuiltIn     bool      `json:"built_in"`
}

// storedTemplate is the on-disk / in-memory representation of a template.
type storedTemplate struct {
	Info     TemplateInfo        `json:"info"`
	Mappings []auto.FieldMapping `json:"mappings"`
}

// TemplateStore manages named mapping templates. It supports in-memory storage
// with optional file-system persistence and ships with built-in templates for
// common integration patterns.
type TemplateStore struct {
	mu        sync.RWMutex
	templates map[string]*storedTemplate
	storeDir  string // empty string means in-memory only
}

// NewTemplateStore creates a TemplateStore. If storeDir is non-empty, templates
// are persisted to that directory as JSON files.
func NewTemplateStore(storeDir string) *TemplateStore {
	ts := &TemplateStore{
		templates: make(map[string]*storedTemplate),
		storeDir:  storeDir,
	}
	ts.registerBuiltins()
	return ts
}

// Save persists a mapping template under the given name.
func (ts *TemplateStore) Save(name string, mappings []auto.FieldMapping) error {
	if name == "" {
		return fmt.Errorf("template name cannot be empty")
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	now := time.Now().UTC()
	existing, exists := ts.templates[name]

	st := &storedTemplate{
		Info: TemplateInfo{
			Name:       name,
			FieldCount: len(mappings),
			CreatedAt:  now,
			UpdatedAt:  now,
		},
		Mappings: make([]auto.FieldMapping, len(mappings)),
	}
	copy(st.Mappings, mappings)

	if exists {
		st.Info.CreatedAt = existing.Info.CreatedAt
		st.Info.BuiltIn = existing.Info.BuiltIn
	}

	ts.templates[name] = st

	if ts.storeDir != "" {
		return ts.persistLocked(name, st)
	}
	return nil
}

// Load retrieves a mapping template by name. If the template is not in memory
// but a storeDir is configured, it attempts to load from disk.
func (ts *TemplateStore) Load(name string) ([]auto.FieldMapping, error) {
	ts.mu.RLock()
	st, ok := ts.templates[name]
	ts.mu.RUnlock()

	if ok {
		result := make([]auto.FieldMapping, len(st.Mappings))
		copy(result, st.Mappings)
		return result, nil
	}

	// Try loading from disk if storeDir is configured.
	if ts.storeDir != "" {
		loaded, err := ts.loadFromDisk(name)
		if err == nil {
			result := make([]auto.FieldMapping, len(loaded.Mappings))
			copy(result, loaded.Mappings)
			return result, nil
		}
	}
	return nil, fmt.Errorf("template %q not found", name)
}

// List returns metadata for all stored templates, sorted by name.
func (ts *TemplateStore) List() []TemplateInfo {
	ts.mu.RLock()
	defer ts.mu.RUnlock()

	result := make([]TemplateInfo, 0, len(ts.templates))
	for _, st := range ts.templates {
		result = append(result, st.Info)
	}

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// Delete removes a template by name. Built-in templates cannot be deleted.
func (ts *TemplateStore) Delete(name string) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()

	st, ok := ts.templates[name]
	if !ok {
		return fmt.Errorf("template %q not found", name)
	}
	if st.Info.BuiltIn {
		return fmt.Errorf("cannot delete built-in template %q", name)
	}

	delete(ts.templates, name)

	if ts.storeDir != "" {
		path := filepath.Join(ts.storeDir, name+".json")
		_ = os.Remove(path)
	}
	return nil
}

// persistLocked writes a template to disk. Caller must hold ts.mu.
func (ts *TemplateStore) persistLocked(name string, st *storedTemplate) error {
	if err := os.MkdirAll(ts.storeDir, 0o755); err != nil {
		return fmt.Errorf("create store dir: %w", err)
	}

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal template %q: %w", name, err)
	}

	path := filepath.Join(ts.storeDir, name+".json")
	if wErr := os.WriteFile(path, data, 0o644); wErr != nil {
		return fmt.Errorf("write template %q: %w", name, wErr)
	}
	return nil
}

// loadFromDisk reads a single template from disk and caches it.
func (ts *TemplateStore) loadFromDisk(name string) (*storedTemplate, error) {
	path := filepath.Join(ts.storeDir, name+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var st storedTemplate
	if unmErr := json.Unmarshal(data, &st); unmErr != nil {
		return nil, unmErr
	}

	ts.mu.Lock()
	ts.templates[name] = &st
	ts.mu.Unlock()

	return &st, nil
}

// LoadAllFromDisk loads all template files from the store directory.
func (ts *TemplateStore) LoadAllFromDisk() error {
	if ts.storeDir == "" {
		return nil
	}

	entries, err := os.ReadDir(ts.storeDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read template dir: %w", err)
	}

	ts.mu.Lock()
	defer ts.mu.Unlock()

	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".json" {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".json")
		path := filepath.Join(ts.storeDir, entry.Name())
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			continue
		}
		var st storedTemplate
		if unmErr := json.Unmarshal(data, &st); unmErr != nil {
			continue
		}
		// Do not overwrite built-in templates already loaded.
		if existing, exists := ts.templates[name]; exists && existing.Info.BuiltIn {
			continue
		}
		ts.templates[name] = &st
	}
	return nil
}

// registerBuiltins installs the default mapping templates.
func (ts *TemplateStore) registerBuiltins() {
	now := time.Now().UTC()

	ts.templates["salesforce_to_hubspot_contacts"] = &storedTemplate{
		Info: TemplateInfo{
			Name:        "salesforce_to_hubspot_contacts",
			Description: "Maps Salesforce Contact fields to HubSpot Contact properties",
			FieldCount:  12,
			CreatedAt:   now,
			UpdatedAt:   now,
			BuiltIn:     true,
		},
		Mappings: []auto.FieldMapping{
			{SourceField: "FirstName", DestField: "firstname", Confidence: 1.0, AutoMatched: false},
			{SourceField: "LastName", DestField: "lastname", Confidence: 1.0, AutoMatched: false},
			{SourceField: "Email", DestField: "email", Confidence: 1.0, AutoMatched: false},
			{SourceField: "Phone", DestField: "phone", Confidence: 1.0, AutoMatched: false},
			{SourceField: "MobilePhone", DestField: "mobilephone", Confidence: 0.95, AutoMatched: false},
			{SourceField: "Title", DestField: "jobtitle", Confidence: 0.85, AutoMatched: false},
			{SourceField: "Account.Name", DestField: "company", Confidence: 0.9, AutoMatched: false},
			{SourceField: "MailingStreet", DestField: "address", Confidence: 0.85, AutoMatched: false},
			{SourceField: "MailingCity", DestField: "city", Confidence: 0.95, AutoMatched: false},
			{SourceField: "MailingState", DestField: "state", Confidence: 0.95, AutoMatched: false},
			{SourceField: "MailingPostalCode", DestField: "zip", Confidence: 0.8, AutoMatched: false},
			{SourceField: "MailingCountry", DestField: "country", Confidence: 0.95, AutoMatched: false},
		},
	}

	ts.templates["bigquery_to_snowflake"] = &storedTemplate{
		Info: TemplateInfo{
			Name:        "bigquery_to_snowflake",
			Description: "Maps BigQuery table columns to Snowflake table columns with type conversions",
			FieldCount:  8,
			CreatedAt:   now,
			UpdatedAt:   now,
			BuiltIn:     true,
		},
		Mappings: []auto.FieldMapping{
			{SourceField: "STRING", DestField: "VARCHAR", Transform: "to_string", Confidence: 1.0, AutoMatched: false},
			{SourceField: "INT64", DestField: "NUMBER", Transform: "to_int", Confidence: 0.95, AutoMatched: false},
			{SourceField: "FLOAT64", DestField: "FLOAT", Transform: "to_float", Confidence: 0.95, AutoMatched: false},
			{SourceField: "BOOL", DestField: "BOOLEAN", Confidence: 1.0, AutoMatched: false},
			{SourceField: "TIMESTAMP", DestField: "TIMESTAMP_NTZ", Transform: "format_date", Confidence: 0.9, AutoMatched: false},
			{SourceField: "DATE", DestField: "DATE", Confidence: 1.0, AutoMatched: false},
			{SourceField: "BYTES", DestField: "BINARY", Confidence: 0.9, AutoMatched: false},
			{SourceField: "JSON", DestField: "VARIANT", Confidence: 0.85, AutoMatched: false},
		},
	}
}
