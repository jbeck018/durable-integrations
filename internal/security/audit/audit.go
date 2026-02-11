// Package audit provides structured audit logging for FlowForge, with support
// for batch writing, periodic flushing, and filtered querying.
package audit

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/common"
)

// AuditEvent represents a single auditable action within the system.
type AuditEvent struct {
	ID           string          `json:"id"`
	TenantID     string          `json:"tenant_id"`
	UserID       string          `json:"user_id"`
	Action       string          `json:"action"`
	ResourceType string          `json:"resource_type"`
	ResourceID   string          `json:"resource_id"`
	Details      json.RawMessage `json:"details,omitempty"`
	Timestamp    time.Time       `json:"timestamp"`
	IP           string          `json:"ip,omitempty"`
	UserAgent    string          `json:"user_agent,omitempty"`
}

// AuditFilter specifies criteria for querying audit events.
type AuditFilter struct {
	TenantID     string     `json:"tenant_id,omitempty"`
	UserID       string     `json:"user_id,omitempty"`
	Action       string     `json:"action,omitempty"`
	ResourceType string     `json:"resource_type,omitempty"`
	ResourceID   string     `json:"resource_id,omitempty"`
	Since        *time.Time `json:"since,omitempty"`
	Until        *time.Time `json:"until,omitempty"`
	Limit        int        `json:"limit,omitempty"`
	Offset       int        `json:"offset,omitempty"`
}

// AuditStore defines the persistence interface for audit events.
type AuditStore interface {
	// WriteEvents persists a batch of audit events atomically.
	WriteEvents(ctx context.Context, events []AuditEvent) error

	// QueryEvents retrieves audit events matching the filter.
	QueryEvents(ctx context.Context, filter AuditFilter) ([]AuditEvent, error)
}

// AuditLoggerConfig holds configuration for the AuditLogger.
type AuditLoggerConfig struct {
	// BatchSize is the maximum number of events buffered before an automatic flush.
	// Default: 100.
	BatchSize int

	// FlushInterval is the maximum time between automatic flushes.
	// Default: 5 seconds.
	FlushInterval time.Duration
}

// DefaultAuditLoggerConfig returns sensible defaults for the audit logger.
func DefaultAuditLoggerConfig() AuditLoggerConfig {
	return AuditLoggerConfig{
		BatchSize:     100,
		FlushInterval: 5 * time.Second,
	}
}

// AuditLogger buffers audit events and flushes them to the store in batches.
// It flushes either when the batch size threshold is reached or on a periodic
// timer, whichever comes first. It is safe for concurrent use.
type AuditLogger struct {
	store  AuditStore
	config AuditLoggerConfig

	mu     sync.Mutex
	buffer []AuditEvent

	stopCh chan struct{}
	doneCh chan struct{}
}

// NewAuditLogger creates a new audit logger that writes to the given store.
// Call Close() when finished to ensure all buffered events are flushed.
func NewAuditLogger(store AuditStore, config AuditLoggerConfig) *AuditLogger {
	if config.BatchSize <= 0 {
		config.BatchSize = 100
	}
	if config.FlushInterval <= 0 {
		config.FlushInterval = 5 * time.Second
	}

	al := &AuditLogger{
		store:  store,
		config: config,
		buffer: make([]AuditEvent, 0, config.BatchSize),
		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	go al.flushLoop()
	return al
}

// Log adds an audit event to the buffer. If the event has no timestamp, the
// current time is used. If the buffer reaches the batch size, a flush is
// triggered immediately.
func (al *AuditLogger) Log(ctx context.Context, event AuditEvent) error {
	if event.TenantID == "" {
		event.TenantID = common.TenantIDFrom(ctx)
	}
	if event.UserID == "" {
		event.UserID = common.UserIDFrom(ctx)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	}
	if event.Details == nil {
		event.Details = json.RawMessage("{}")
	}

	al.mu.Lock()
	al.buffer = append(al.buffer, event)
	shouldFlush := len(al.buffer) >= al.config.BatchSize
	al.mu.Unlock()

	if shouldFlush {
		al.flush()
	}

	return nil
}

// Query retrieves audit events matching the given filter from the store.
func (al *AuditLogger) Query(ctx context.Context, filter AuditFilter) ([]AuditEvent, error) {
	// Flush pending events first to ensure query results are up to date.
	al.flush()
	return al.store.QueryEvents(ctx, filter)
}

// Flush forces all buffered events to be written to the store.
func (al *AuditLogger) Flush() {
	al.flush()
}

// Close stops the periodic flush loop and flushes remaining events.
func (al *AuditLogger) Close() error {
	close(al.stopCh)
	<-al.doneCh
	al.flush()
	return nil
}

// flushLoop runs the periodic flush timer.
func (al *AuditLogger) flushLoop() {
	defer close(al.doneCh)
	ticker := time.NewTicker(al.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			al.flush()
		case <-al.stopCh:
			return
		}
	}
}

// flush writes all buffered events to the store.
func (al *AuditLogger) flush() {
	al.mu.Lock()
	if len(al.buffer) == 0 {
		al.mu.Unlock()
		return
	}
	events := al.buffer
	al.buffer = make([]AuditEvent, 0, al.config.BatchSize)
	al.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := al.store.WriteEvents(ctx, events); err != nil {
		// Re-buffer the events on failure so they are retried on the next flush.
		log.Printf("audit: flush failed (%d events): %v", len(events), err)
		al.mu.Lock()
		al.buffer = append(events, al.buffer...)
		// Cap the buffer to prevent unbounded growth on persistent failures.
		if len(al.buffer) > al.config.BatchSize*10 {
			dropped := len(al.buffer) - al.config.BatchSize*10
			al.buffer = al.buffer[dropped:]
			log.Printf("audit: dropped %d oldest events due to buffer overflow", dropped)
		}
		al.mu.Unlock()
	}
}

// ----- In-memory AuditStore implementation -----

// InMemoryAuditStore is an in-memory implementation of AuditStore for testing.
type InMemoryAuditStore struct {
	mu     sync.RWMutex
	events []AuditEvent
}

// NewInMemoryAuditStore creates a new in-memory audit store.
func NewInMemoryAuditStore() *InMemoryAuditStore {
	return &InMemoryAuditStore{}
}

// WriteEvents appends events to the in-memory store.
func (s *InMemoryAuditStore) WriteEvents(_ context.Context, events []AuditEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, events...)
	return nil
}

// QueryEvents filters and returns matching events from the in-memory store.
func (s *InMemoryAuditStore) QueryEvents(_ context.Context, filter AuditFilter) ([]AuditEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var results []AuditEvent
	for i := len(s.events) - 1; i >= 0; i-- {
		event := s.events[i]
		if !matchesFilter(event, filter) {
			continue
		}
		results = append(results, event)
	}

	// Apply offset.
	if filter.Offset > 0 {
		if filter.Offset >= len(results) {
			return nil, nil
		}
		results = results[filter.Offset:]
	}

	// Apply limit.
	if filter.Limit > 0 && len(results) > filter.Limit {
		results = results[:filter.Limit]
	}

	return results, nil
}

// Events returns all stored events (for testing).
func (s *InMemoryAuditStore) Events() []AuditEvent {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]AuditEvent, len(s.events))
	copy(out, s.events)
	return out
}

// matchesFilter checks whether an event satisfies all non-empty filter criteria.
func matchesFilter(event AuditEvent, filter AuditFilter) bool {
	if filter.TenantID != "" && event.TenantID != filter.TenantID {
		return false
	}
	if filter.UserID != "" && event.UserID != filter.UserID {
		return false
	}
	if filter.Action != "" && event.Action != filter.Action {
		return false
	}
	if filter.ResourceType != "" && event.ResourceType != filter.ResourceType {
		return false
	}
	if filter.ResourceID != "" && event.ResourceID != filter.ResourceID {
		return false
	}
	if filter.Since != nil && event.Timestamp.Before(*filter.Since) {
		return false
	}
	if filter.Until != nil && event.Timestamp.After(*filter.Until) {
		return false
	}
	return true
}

// NewEvent is a convenience constructor for creating an AuditEvent with
// details provided as a map that gets marshalled to JSON.
func NewEvent(action, resourceType, resourceID string, details map[string]interface{}) AuditEvent {
	var detailsJSON json.RawMessage
	if details != nil {
		b, err := json.Marshal(details)
		if err != nil {
			detailsJSON = json.RawMessage(fmt.Sprintf(`{"marshal_error":%q}`, err.Error()))
		} else {
			detailsJSON = b
		}
	} else {
		detailsJSON = json.RawMessage("{}")
	}

	return AuditEvent{
		Action:       action,
		ResourceType: resourceType,
		ResourceID:   resourceID,
		Details:      detailsJSON,
		Timestamp:    time.Now().UTC(),
	}
}
