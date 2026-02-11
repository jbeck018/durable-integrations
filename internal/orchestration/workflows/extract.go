package workflows

import (
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/flowforge/flowforge/internal/orchestration/activities"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// ExtractWorkflow is a child workflow that extracts records from a source
// connector. It handles pagination through multiple batches and can fan out
// into parallel partitions when the data volume exceeds the threshold.
func ExtractWorkflow(ctx workflow.Context, params ExtractParams) (ExtractResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("ExtractWorkflow started",
		"connector", params.SourceConnectorID,
		"streams", len(params.Streams),
	)

	batchSize := params.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize()
	}

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    2 * time.Minute,
			MaximumAttempts:    5,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, actOpts)

	// Determine if we should use parallel partitions.
	var partitionsOutput activities.DeterminePartitionsOutput
	partErr := workflow.ExecuteActivity(actCtx, activities.DeterminePartitionsActivity, activities.DeterminePartitionsInput{
		SourceConnectorID: params.SourceConnectorID,
		SourceConfig:      params.SourceConfig,
		Streams:           params.Streams,
		State:             params.State,
	}).Get(ctx, &partitionsOutput)

	usePartitions := partErr == nil && len(partitionsOutput.Partitions) > 1

	var allRecords []protocol.Record
	currentState := copyState(params.State)

	if usePartitions {
		records, newState, err := extractWithPartitions(ctx, actCtx, params, partitionsOutput.Partitions, batchSize)
		if err != nil {
			return ExtractResult{}, err
		}
		allRecords = records
		mergeStateRaw(currentState, newState)
	} else {
		records, newState, err := extractSequential(ctx, actCtx, params, batchSize)
		if err != nil {
			return ExtractResult{}, err
		}
		allRecords = records
		mergeStateRaw(currentState, newState)
	}

	// Save the final checkpoint.
	checkpointOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 1.5,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	}
	checkpointCtx := workflow.WithActivityOptions(ctx, checkpointOpts)

	saveErr := workflow.ExecuteActivity(checkpointCtx, activities.SaveCheckpointActivity, activities.SaveCheckpointInput{
		TenantID: params.TenantID,
		SyncID:   params.SyncID,
		State:    currentState,
	}).Get(ctx, nil)
	if saveErr != nil {
		logger.Warn("Failed to save checkpoint, continuing", "error", saveErr)
	}

	partitionsUsed := 1
	if usePartitions {
		partitionsUsed = len(partitionsOutput.Partitions)
	}

	logger.Info("ExtractWorkflow completed",
		"records_extracted", len(allRecords),
		"partitions_used", partitionsUsed,
	)

	return ExtractResult{
		Records:        allRecords,
		RecordsCount:   int64(len(allRecords)),
		NewState:       currentState,
		PartitionsUsed: partitionsUsed,
	}, nil
}

// extractSequential fetches records in sequential batches until there are
// no more records to read from the source.
func extractSequential(ctx workflow.Context, actCtx workflow.Context, params ExtractParams, batchSize int) ([]protocol.Record, map[string]json.RawMessage, error) {
	var allRecords []protocol.Record
	currentState := copyState(params.State)

	for {
		var fetchOutput activities.FetchBatchOutput
		err := workflow.ExecuteActivity(actCtx, activities.FetchBatchActivity, activities.FetchBatchInput{
			SourceConnectorID: params.SourceConnectorID,
			SourceConfig:      params.SourceConfig,
			Streams:           params.Streams,
			State:             currentState,
			BatchSize:         batchSize,
		}).Get(ctx, &fetchOutput)
		if err != nil {
			return nil, nil, fmt.Errorf("fetch batch failed: %w", err)
		}

		allRecords = append(allRecords, fetchOutput.Records...)
		mergeStateRaw(currentState, fetchOutput.NewState)

		if !fetchOutput.HasMore {
			break
		}
	}

	return allRecords, currentState, nil
}

// extractWithPartitions fetches records from multiple partitions concurrently
// using Temporal futures for parallel execution.
func extractWithPartitions(ctx workflow.Context, actCtx workflow.Context, params ExtractParams, partitions []PartitionRange, batchSize int) ([]protocol.Record, map[string]json.RawMessage, error) {
	futures := make([]workflow.Future, len(partitions))

	for i, partition := range partitions {
		p := partition
		futures[i] = workflow.ExecuteActivity(actCtx, activities.FetchBatchActivity, activities.FetchBatchInput{
			SourceConnectorID: params.SourceConnectorID,
			SourceConfig:      params.SourceConfig,
			Streams:           params.Streams,
			State:             copyState(params.State),
			BatchSize:         batchSize,
			Partition:         &p,
		})
	}

	var allRecords []protocol.Record
	mergedState := copyState(params.State)
	for _, f := range futures {
		var fetchOutput activities.FetchBatchOutput
		if err := f.Get(ctx, &fetchOutput); err != nil {
			return nil, nil, fmt.Errorf("partition fetch failed: %w", err)
		}
		allRecords = append(allRecords, fetchOutput.Records...)
		mergeStateRaw(mergedState, fetchOutput.NewState)
	}

	return allRecords, mergedState, nil
}

// copyState creates a shallow copy of the state map to prevent aliasing.
func copyState(state map[string]json.RawMessage) map[string]json.RawMessage {
	if state == nil {
		return make(map[string]json.RawMessage)
	}
	cp := make(map[string]json.RawMessage, len(state))
	for k, v := range state {
		vCopy := make(json.RawMessage, len(v))
		copy(vCopy, v)
		cp[k] = vCopy
	}
	return cp
}

// mergeStateRaw merges source state entries into target state. Source entries
// overwrite target entries on key conflicts.
func mergeStateRaw(target, source map[string]json.RawMessage) {
	for k, v := range source {
		target[k] = v
	}
}
