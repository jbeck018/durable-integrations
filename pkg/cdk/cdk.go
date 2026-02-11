// Package cdk provides the Connector Development Kit — the public interfaces
// that all FlowForge connectors implement. This is the primary extension point
// for the platform.
package cdk

import (
	"context"
	"encoding/json"

	"github.com/flowforge/flowforge/pkg/protocol"
)

// Source defines the interface for source connectors that read data from external systems.
type Source interface {
	// Spec returns the JSON Schema describing the connector's configuration.
	Spec() (*protocol.Spec, error)

	// Check validates that the connector can reach the external system with the given config.
	Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error)

	// Discover returns a catalog of available streams and their schemas.
	Discover(ctx context.Context, config json.RawMessage) (*protocol.Catalog, error)

	// Read emits records from the selected streams. For incremental syncs, state
	// contains the last checkpoint. Records, state checkpoints, and logs are sent
	// to the output channel. The caller closes output after Read returns.
	Read(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, state map[string]json.RawMessage, output chan<- protocol.Message) error
}

// Destination defines the interface for destination connectors that write data.
type Destination interface {
	// Spec returns the JSON Schema describing the destination's configuration.
	Spec() (*protocol.Spec, error)

	// Check validates write access to the destination.
	Check(ctx context.Context, config json.RawMessage) (*protocol.CheckResult, error)

	// Write consumes records from the input channel and writes them to the destination.
	// Returns a summary of the write operation.
	Write(ctx context.Context, config json.RawMessage, catalog *protocol.ConfiguredCatalog, input <-chan protocol.Message) (*protocol.WriteResult, error)

	// Capabilities returns the destination sync modes this connector supports.
	Capabilities() []protocol.DestinationSyncMode
}

// Bidirectional extends both Source and Destination with conflict resolution
// and real-time event handling for bidirectional synchronization.
type Bidirectional interface {
	Source
	Destination

	// Resolve handles conflicts when the same record was modified in both systems.
	Resolve(ctx context.Context, conflicts []Conflict) ([]Resolution, error)

	// WebhookHandler processes inbound webhook events for real-time CDC.
	WebhookHandler(ctx context.Context, event WebhookEvent) error
}

// Conflict represents a data conflict between source and destination records.
type Conflict struct {
	Stream       string          `json:"stream"`
	PrimaryKey   json.RawMessage `json:"primary_key"`
	SourceRecord json.RawMessage `json:"source_record"`
	DestRecord   json.RawMessage `json:"dest_record"`
	SourceTS     int64           `json:"source_ts"`
	DestTS       int64           `json:"dest_ts"`
}

// ResolutionStrategy indicates how a conflict should be resolved.
type ResolutionStrategy string

const (
	ResolutionSourceWins ResolutionStrategy = "SOURCE_WINS"
	ResolutionDestWins   ResolutionStrategy = "DEST_WINS"
	ResolutionLastWrite  ResolutionStrategy = "LAST_WRITE_WINS"
	ResolutionMerge      ResolutionStrategy = "MERGE"
	ResolutionSkip       ResolutionStrategy = "SKIP"
)

// Resolution is the decided outcome for a single conflict.
type Resolution struct {
	PrimaryKey   json.RawMessage    `json:"primary_key"`
	Strategy     ResolutionStrategy `json:"strategy"`
	MergedRecord json.RawMessage    `json:"merged_record,omitempty"`
}

// WebhookEvent represents an inbound webhook from an external system.
type WebhookEvent struct {
	Source     string            `json:"source"`
	EventType  string            `json:"event_type"`
	Headers    map[string]string `json:"headers"`
	Body       json.RawMessage   `json:"body"`
	ReceivedAt int64             `json:"received_at"`
}

// ConnectorMeta holds metadata for a registered connector.
type ConnectorMeta struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	Version     string `json:"version"`
	Type        string `json:"type"` // "source", "destination", "bidirectional"
	Category    string `json:"category"`
	Icon        string `json:"icon,omitempty"`
}
