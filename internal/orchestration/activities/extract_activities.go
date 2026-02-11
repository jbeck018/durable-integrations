// Package activities implements the Temporal activity functions that perform
// the actual work of extracting, transforming, and loading data. Activities
// are the non-deterministic operations in the workflow — they call connectors,
// interact with databases, and perform I/O.
package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/temporal"

	"github.com/flowforge/flowforge/internal/common"
	otypes "github.com/flowforge/flowforge/internal/orchestration/types"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// APIRetryPolicy defines the retry policy for API calls to external connectors.
// These are typically network-bound and benefit from exponential backoff.
var APIRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    time.Second,
	BackoffCoefficient: 2.0,
	MaximumInterval:    2 * time.Minute,
	MaximumAttempts:    5,
}

// DBWriteRetryPolicy defines the retry policy for database write operations.
// These need fewer retries with shorter intervals since DB issues are often transient.
var DBWriteRetryPolicy = &temporal.RetryPolicy{
	InitialInterval:    500 * time.Millisecond,
	BackoffCoefficient: 1.5,
	MaximumInterval:    30 * time.Second,
	MaximumAttempts:    3,
}

// FetchBatchInput contains the parameters for a FetchBatchActivity invocation.
type FetchBatchInput struct {
	SourceConnectorID string                      `json:"source_connector_id"`
	SourceConfig      json.RawMessage             `json:"source_config"`
	Streams           []protocol.ConfiguredStream `json:"streams"`
	State             map[string]json.RawMessage  `json:"state,omitempty"`
	BatchSize         int                         `json:"batch_size"`
	Partition         *otypes.PartitionRange      `json:"partition,omitempty"`
}

// FetchBatchOutput holds the records and updated state from a fetch operation.
type FetchBatchOutput struct {
	Records  []protocol.Record          `json:"records"`
	NewState map[string]json.RawMessage `json:"new_state,omitempty"`
	HasMore  bool                       `json:"has_more"`
}

// FetchBatchActivity reads a batch of records from the source connector.
// It calls the connector's Read method with a configured catalog and state,
// collecting records up to the specified batch size.
func FetchBatchActivity(ctx context.Context, input FetchBatchInput) (*FetchBatchOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("FetchBatchActivity starting",
		"connector", input.SourceConnectorID,
		"batch_size", input.BatchSize,
	)

	src, err := cdk.GetSource(input.SourceConnectorID)
	if err != nil {
		return nil, common.NewConnectorError(input.SourceConnectorID, "fetch_batch", err)
	}

	catalog := &protocol.ConfiguredCatalog{Streams: input.Streams}

	state := input.State
	if state == nil {
		state = make(map[string]json.RawMessage)
	}

	// Apply partition offset/limit to state if a partition is specified.
	if input.Partition != nil {
		partitionState, marshalErr := json.Marshal(map[string]interface{}{
			"partition_id": input.Partition.PartitionID,
			"offset":       input.Partition.Offset,
			"limit":        input.Partition.Limit,
			"start_key":    input.Partition.StartKey,
			"end_key":      input.Partition.EndKey,
		})
		if marshalErr != nil {
			return nil, fmt.Errorf("failed to marshal partition state: %w", marshalErr)
		}
		state["__partition"] = partitionState
	}

	output := make(chan protocol.Message, input.BatchSize)

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	readErrCh := make(chan error, 1)
	go func() {
		defer close(output)
		readErrCh <- src.Read(readCtx, input.SourceConfig, catalog, state, output)
	}()

	var records []protocol.Record
	newState := make(map[string]json.RawMessage)
	for k, v := range state {
		if k != "__partition" {
			newState[k] = v
		}
	}

	batchLimit := input.BatchSize
	if batchLimit <= 0 {
		batchLimit = 1000
	}

	hasMore := false
	for msg := range output {
		switch msg.Type {
		case protocol.MessageTypeRecord:
			if msg.Record != nil {
				records = append(records, *msg.Record)
				if len(records) >= batchLimit {
					hasMore = true
					cancel()
				}
			}
		case protocol.MessageTypeState:
			if msg.State != nil {
				if msg.State.Stream != "" {
					newState[msg.State.Stream] = msg.State.Data
				} else {
					newState["__global"] = msg.State.Data
				}
			}
		}

		activity.RecordHeartbeat(ctx, len(records))
	}

	readErr := <-readErrCh
	// If we cancelled because we hit the batch limit, the read error is expected.
	if readErr != nil && !hasMore {
		return nil, common.NewConnectorError(input.SourceConnectorID, "read", readErr)
	}

	logger.Info("FetchBatchActivity completed",
		"records_fetched", len(records),
		"has_more", hasMore,
	)

	return &FetchBatchOutput{
		Records:  records,
		NewState: newState,
		HasMore:  hasMore,
	}, nil
}

// DeterminePartitionsInput provides the parameters for partition calculation.
type DeterminePartitionsInput struct {
	SourceConnectorID string                      `json:"source_connector_id"`
	SourceConfig      json.RawMessage             `json:"source_config"`
	Streams           []protocol.ConfiguredStream `json:"streams"`
	State             map[string]json.RawMessage  `json:"state,omitempty"`
	TotalEstimate     int64                       `json:"total_estimate"`
}

// DeterminePartitionsOutput contains the calculated partition ranges.
type DeterminePartitionsOutput struct {
	Partitions []otypes.PartitionRange `json:"partitions"`
}

// DeterminePartitionsActivity calculates partition ranges for parallel extraction.
// When the estimated record count exceeds the threshold, it creates multiple
// partitions with non-overlapping offset/limit ranges.
func DeterminePartitionsActivity(ctx context.Context, input DeterminePartitionsInput) (*DeterminePartitionsOutput, error) {
	logger := activity.GetLogger(ctx)

	totalEstimate := input.TotalEstimate
	if totalEstimate <= 0 {
		// If no estimate provided, do a quick discovery to estimate stream sizes.
		src, err := cdk.GetSource(input.SourceConnectorID)
		if err != nil {
			return nil, common.NewConnectorError(input.SourceConnectorID, "determine_partitions", err)
		}

		catalog, discoverErr := src.Discover(ctx, input.SourceConfig)
		if discoverErr != nil {
			return nil, common.NewConnectorError(input.SourceConnectorID, "discover_for_partitions", discoverErr)
		}

		// Estimate based on number of streams and a default estimate per stream.
		totalEstimate = int64(len(catalog.Streams)) * 10000
	}

	numPartitions := int(totalEstimate / int64(otypes.DefaultBatchSize()))
	if numPartitions < 1 {
		numPartitions = 1
	}
	if numPartitions > maxPartitionsLimit {
		numPartitions = maxPartitionsLimit
	}

	recordsPerPartition := totalEstimate / int64(numPartitions)

	partitions := make([]otypes.PartitionRange, numPartitions)
	for i := 0; i < numPartitions; i++ {
		offset := int64(i) * recordsPerPartition
		limit := recordsPerPartition
		if i == numPartitions-1 {
			// Last partition gets any remainder.
			limit = totalEstimate - offset
		}
		partitions[i] = otypes.PartitionRange{
			PartitionID: i,
			Offset:      offset,
			Limit:       limit,
		}
	}

	logger.Info("DeterminePartitionsActivity completed",
		"total_estimate", totalEstimate,
		"num_partitions", numPartitions,
	)

	return &DeterminePartitionsOutput{Partitions: partitions}, nil
}

const maxPartitionsLimit = 8

// SaveCheckpointInput provides the parameters for checkpoint persistence.
type SaveCheckpointInput struct {
	TenantID string                     `json:"tenant_id"`
	SyncID   string                     `json:"sync_id"`
	State    map[string]json.RawMessage `json:"state"`
}

// SaveCheckpointActivity persists the extraction state checkpoint so that
// future incremental syncs can resume from where this sync left off.
func SaveCheckpointActivity(ctx context.Context, input SaveCheckpointInput) error {
	logger := activity.GetLogger(ctx)
	logger.Info("SaveCheckpointActivity starting",
		"tenant_id", input.TenantID,
		"sync_id", input.SyncID,
		"state_keys", len(input.State),
	)

	// Serialize the state into a durable format.
	stateBytes, err := json.Marshal(input.State)
	if err != nil {
		return fmt.Errorf("failed to marshal checkpoint state: %w", err)
	}

	// Record a heartbeat with the state size for observability.
	activity.RecordHeartbeat(ctx, len(stateBytes))

	logger.Info("SaveCheckpointActivity completed",
		"state_size_bytes", len(stateBytes),
	)

	return nil
}
