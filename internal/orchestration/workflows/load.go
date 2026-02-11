package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/flowforge/flowforge/internal/orchestration/activities"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// LoadWorkflow is a child workflow that loads transformed records into the
// destination connector. Records are split into configurable batches and
// written in parallel where possible. Errors are collected per-batch so
// partial failures do not prevent other batches from completing.
func LoadWorkflow(ctx workflow.Context, params LoadParams) (LoadResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("LoadWorkflow started",
		"connector", params.DestConnectorID,
		"record_count", len(params.Records),
	)

	batchSize := params.BatchSize
	if batchSize <= 0 {
		batchSize = DefaultBatchSize()
	}

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Minute,
		HeartbeatTimeout:    2 * time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 1.5,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, actOpts)

	// Split records into batches.
	batches := splitIntoBatches(params.Records, batchSize)

	// Execute batch writes concurrently using Temporal futures.
	futures := make([]workflow.Future, len(batches))
	for i, batch := range batches {
		futures[i] = workflow.ExecuteActivity(actCtx, activities.WriteBatchActivity, activities.WriteBatchInput{
			DestConnectorID: params.DestConnectorID,
			DestConfig:      params.DestConfig,
			Records:         batch,
			Streams:         params.Streams,
			BatchIndex:      i,
		})
	}

	// Collect results from all batch writes.
	var totalLoaded int64
	var allErrors []string
	batchesWritten := 0

	for _, f := range futures {
		var writeOutput activities.WriteBatchOutput
		if err := f.Get(ctx, &writeOutput); err != nil {
			allErrors = append(allErrors, err.Error())
			continue
		}
		totalLoaded += writeOutput.RecordsWritten
		allErrors = append(allErrors, writeOutput.Errors...)
		batchesWritten++
	}

	// Confirm the overall write operation.
	confirmOpts := workflow.ActivityOptions{
		StartToCloseTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 1.5,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	}
	confirmCtx := workflow.WithActivityOptions(ctx, confirmOpts)

	var confirmOutput activities.ConfirmWriteOutput
	confirmErr := workflow.ExecuteActivity(confirmCtx, activities.ConfirmWriteActivity, activities.ConfirmWriteInput{
		DestConnectorID: params.DestConnectorID,
		DestConfig:      params.DestConfig,
		ExpectedCount:   int64(len(params.Records)),
		ActualCount:     totalLoaded,
		BatchErrors:     allErrors,
	}).Get(ctx, &confirmOutput)
	if confirmErr != nil {
		allErrors = append(allErrors, confirmErr.Error())
	} else if !confirmOutput.Confirmed {
		if confirmOutput.DiscrepancyMsg != "" {
			allErrors = append(allErrors, confirmOutput.DiscrepancyMsg)
		}
		allErrors = append(allErrors, confirmOutput.Errors...)
	}

	logger.Info("LoadWorkflow completed",
		"records_loaded", totalLoaded,
		"batches_written", batchesWritten,
		"errors", len(allErrors),
	)

	return LoadResult{
		RecordsLoaded:  totalLoaded,
		BatchesWritten: batchesWritten,
		Errors:         allErrors,
	}, nil
}

// splitIntoBatches divides a slice of records into chunks of at most batchSize.
func splitIntoBatches(records []protocol.Record, batchSize int) [][]protocol.Record {
	if batchSize <= 0 {
		batchSize = DefaultBatchSize()
	}

	numBatches := (len(records) + batchSize - 1) / batchSize
	batches := make([][]protocol.Record, 0, numBatches)

	for i := 0; i < len(records); i += batchSize {
		end := i + batchSize
		if end > len(records) {
			end = len(records)
		}
		batches = append(batches, records[i:end])
	}

	return batches
}
