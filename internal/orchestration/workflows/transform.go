package workflows

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/flowforge/flowforge/internal/orchestration/activities"
)

// TransformWorkflow is a child workflow that transforms extracted records by
// applying field mappings, type coercion, and deduplication. Each transform
// step is executed as a separate activity for individual retryability.
func TransformWorkflow(ctx workflow.Context, params TransformParams) (TransformResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("TransformWorkflow started",
		"record_count", len(params.Records),
		"streams", len(params.Streams),
	)

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 5 * time.Minute,
		HeartbeatTimeout:    time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    time.Minute,
			MaximumAttempts:    3,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, actOpts)

	currentRecords := params.Records
	var allErrors []string

	// Step 1: Apply field mappings.
	if len(params.FieldMappings) > 0 {
		var mapOutput activities.MapFieldsOutput
		err := workflow.ExecuteActivity(actCtx, activities.MapFieldsActivity, activities.MapFieldsInput{
			Records:       currentRecords,
			FieldMappings: params.FieldMappings,
		}).Get(ctx, &mapOutput)
		if err != nil {
			return TransformResult{}, err
		}
		currentRecords = mapOutput.Records
		allErrors = append(allErrors, mapOutput.Errors...)
	}

	// Step 2: Apply type coercion for any fields with CoerceType set.
	hasCoercions := false
	for _, mappings := range params.FieldMappings {
		for _, m := range mappings {
			if m.CoerceType != "" {
				hasCoercions = true
				break
			}
		}
		if hasCoercions {
			break
		}
	}

	if hasCoercions {
		var coerceOutput activities.CoerceTypesOutput
		err := workflow.ExecuteActivity(actCtx, activities.CoerceTypesActivity, activities.CoerceTypesInput{
			Records:       currentRecords,
			FieldMappings: params.FieldMappings,
		}).Get(ctx, &coerceOutput)
		if err != nil {
			return TransformResult{}, err
		}
		currentRecords = coerceOutput.Records
		allErrors = append(allErrors, coerceOutput.Errors...)
	}

	// Step 3: Deduplicate records based on primary keys.
	var dedupOutput activities.DeduplicateOutput
	err := workflow.ExecuteActivity(actCtx, activities.DeduplicateActivity, activities.DeduplicateInput{
		Records: currentRecords,
		Streams: params.Streams,
	}).Get(ctx, &dedupOutput)
	if err != nil {
		return TransformResult{}, err
	}
	currentRecords = dedupOutput.Records
	dropped := dedupOutput.DuplicatesFound

	inputCount := int64(len(params.Records))
	outputCount := int64(len(currentRecords))

	logger.Info("TransformWorkflow completed",
		"input_records", inputCount,
		"output_records", outputCount,
		"duplicates_removed", dropped,
		"errors", len(allErrors),
	)

	return TransformResult{
		Records:      currentRecords,
		RecordsCount: outputCount,
		Dropped:      dropped,
		Errors:       allErrors,
	}, nil
}
