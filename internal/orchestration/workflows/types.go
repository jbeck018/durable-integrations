// Package workflows defines all Temporal workflow implementations for the
// FlowForge orchestration layer, along with shared types used across workflows.
package workflows

import (
	"encoding/json"
	"time"

	otypes "github.com/flowforge/flowforge/internal/orchestration/types"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// Task queue names used for routing work to the appropriate workers.
const (
	SyncTaskQueue   = "flowforge-sync"
	SystemTaskQueue = "flowforge-system"
)

// Workflow names for registration and invocation.
const (
	SyncOrchestratorName        = "SyncOrchestrator"
	ExtractWorkflowName         = "ExtractWorkflow"
	TransformWorkflowName       = "TransformWorkflow"
	LoadWorkflowName            = "LoadWorkflow"
	ScheduledSyncWorkflowName   = "ScheduledSyncWorkflow"
	BidirectionalSyncName       = "BidirectionalSyncWorkflow"
)

// Query names for workflow state introspection.
const (
	QuerySyncStatus = "sync:status"
)

// SyncPhase tracks which stage of the ETL pipeline is currently executing.
type SyncPhase string

const (
	PhaseInitializing SyncPhase = "INITIALIZING"
	PhaseExtracting   SyncPhase = "EXTRACTING"
	PhaseTransforming SyncPhase = "TRANSFORMING"
	PhaseLoading      SyncPhase = "LOADING"
	PhaseCompleted    SyncPhase = "COMPLETED"
	PhaseFailed       SyncPhase = "FAILED"
	PhaseCancelled    SyncPhase = "CANCELLED"
	PhasePaused       SyncPhase = "PAUSED"
)

// FieldMapping describes how a source field maps to a destination field,
// including optional type coercion.
type FieldMapping = otypes.FieldMapping

// SyncParams contains all parameters needed to execute a full sync workflow.
type SyncParams struct {
	SourceConnectorID string                    `json:"source_connector_id"`
	DestConnectorID   string                    `json:"dest_connector_id"`
	SourceConfig      json.RawMessage           `json:"source_config"`
	DestConfig        json.RawMessage           `json:"dest_config"`
	SelectedStreams    []protocol.ConfiguredStream `json:"selected_streams"`
	FieldMappings     map[string][]FieldMapping  `json:"field_mappings"`
	SyncMode          protocol.SyncMode          `json:"sync_mode"`
	LastCheckpoint    map[string]json.RawMessage `json:"last_checkpoint,omitempty"`
	TenantID          string                     `json:"tenant_id"`
	SyncID            string                     `json:"sync_id"`
}

// SyncResult summarizes the outcome of a completed sync workflow.
type SyncResult struct {
	RecordsExtracted   int64                      `json:"records_extracted"`
	RecordsTransformed int64                      `json:"records_transformed"`
	RecordsLoaded      int64                      `json:"records_loaded"`
	NewCheckpoint      map[string]json.RawMessage `json:"new_checkpoint,omitempty"`
	Duration           time.Duration              `json:"duration"`
	Errors             []string                   `json:"errors,omitempty"`
}

// SyncStatus provides a snapshot of the current sync workflow state,
// returned by the status query handler.
type SyncStatus struct {
	Phase            SyncPhase `json:"phase"`
	Progress         float64   `json:"progress"`
	RecordsProcessed int64     `json:"records_processed"`
	Errors           []string  `json:"errors,omitempty"`
	StartedAt        time.Time `json:"started_at"`
}

// PartitionRange defines a slice of data for parallel extraction.
type PartitionRange = otypes.PartitionRange

// ExtractParams contains the parameters for the extract child workflow.
type ExtractParams struct {
	SourceConnectorID string                      `json:"source_connector_id"`
	SourceConfig      json.RawMessage             `json:"source_config"`
	Streams           []protocol.ConfiguredStream `json:"streams"`
	State             map[string]json.RawMessage  `json:"state,omitempty"`
	BatchSize         int                         `json:"batch_size"`
	TenantID          string                      `json:"tenant_id"`
	SyncID            string                      `json:"sync_id"`
}

// ExtractResult holds the outcome of the extract child workflow.
type ExtractResult struct {
	Records        []protocol.Record          `json:"records"`
	RecordsCount   int64                      `json:"records_count"`
	NewState       map[string]json.RawMessage `json:"new_state,omitempty"`
	PartitionsUsed int                        `json:"partitions_used"`
}

// TransformParams contains the parameters for the transform child workflow.
type TransformParams struct {
	Records       []protocol.Record         `json:"records"`
	FieldMappings map[string][]FieldMapping `json:"field_mappings"`
	Streams       []protocol.ConfiguredStream `json:"streams"`
	TenantID      string                    `json:"tenant_id"`
	SyncID        string                    `json:"sync_id"`
}

// TransformResult holds the outcome of the transform child workflow.
type TransformResult struct {
	Records      []protocol.Record `json:"records"`
	RecordsCount int64             `json:"records_count"`
	Dropped      int64             `json:"dropped"`
	Errors       []string          `json:"errors,omitempty"`
}

// LoadParams contains the parameters for the load child workflow.
type LoadParams struct {
	DestConnectorID string                      `json:"dest_connector_id"`
	DestConfig      json.RawMessage             `json:"dest_config"`
	Records         []protocol.Record           `json:"records"`
	Streams         []protocol.ConfiguredStream `json:"streams"`
	BatchSize       int                         `json:"batch_size"`
	TenantID        string                      `json:"tenant_id"`
	SyncID          string                      `json:"sync_id"`
}

// LoadResult holds the outcome of the load child workflow.
type LoadResult struct {
	RecordsLoaded int64    `json:"records_loaded"`
	BatchesWritten int    `json:"batches_written"`
	Errors        []string `json:"errors,omitempty"`
}

// ScheduledSyncParams extends SyncParams with scheduling information.
type ScheduledSyncParams struct {
	SyncParams
	CronExpression string    `json:"cron_expression"`
	NextRunAt      time.Time `json:"next_run_at"`
	RunCount       int       `json:"run_count"`
	MaxRuns        int       `json:"max_runs,omitempty"` // 0 means unlimited
}

// BidirectionalSyncParams extends SyncParams for bidirectional synchronization.
type BidirectionalSyncParams struct {
	SyncParams
	ReverseSourceConfig json.RawMessage `json:"reverse_source_config,omitempty"`
	ReverseDestConfig   json.RawMessage `json:"reverse_dest_config,omitempty"`
	ConflictStrategy    string          `json:"conflict_strategy"` // "source_wins", "dest_wins", "last_write_wins"
	BidirectionalID     string          `json:"bidirectional_connector_id"`
}

// BidirectionalSyncResult holds the outcome of a bidirectional sync.
type BidirectionalSyncResult struct {
	ForwardResult  SyncResult `json:"forward_result"`
	ReverseResult  SyncResult `json:"reverse_result"`
	ConflictsFound int        `json:"conflicts_found"`
	ConflictsResolved int    `json:"conflicts_resolved"`
	Duration       time.Duration `json:"duration"`
	Errors         []string      `json:"errors,omitempty"`
}

// DefaultBatchSize returns the default batch size for processing.
func DefaultBatchSize() int {
	return otypes.DefaultBatchSize()
}

// ParallelPartitionThreshold returns the threshold above which parallel
// partitioned extraction is used.
func ParallelPartitionThreshold() int64 {
	return otypes.ParallelPartitionThreshold()
}

// MaxPartitions returns the maximum number of concurrent partitions.
func MaxPartitions() int {
	return otypes.MaxPartitions()
}
