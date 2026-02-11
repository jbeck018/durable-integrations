package workflows

import (
	"fmt"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"

	"github.com/flowforge/flowforge/internal/orchestration/activities"
	"github.com/flowforge/flowforge/internal/orchestration/signals"
)

// SyncOrchestrator is the parent workflow that orchestrates the full ETL
// pipeline: Extract -> Transform -> Load. It spawns each phase as a child
// workflow for independent retryability and monitoring.
//
// The orchestrator supports:
//   - Pause/Resume/Cancel signals for runtime control
//   - Modify signal to change parameters mid-flight
//   - Status query for real-time progress monitoring
//   - Phase-specific error handling and recovery
func SyncOrchestrator(ctx workflow.Context, params SyncParams) (SyncResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("SyncOrchestrator started",
		"sync_id", params.SyncID,
		"tenant_id", params.TenantID,
		"source", params.SourceConnectorID,
		"dest", params.DestConnectorID,
	)

	startTime := workflow.Now(ctx)

	// Initialize mutable workflow state.
	status := &SyncStatus{
		Phase:     PhaseInitializing,
		StartedAt: startTime,
	}
	sigState := signals.NewSignalState()

	// Register the status query handler.
	err := workflow.SetQueryHandler(ctx, QuerySyncStatus, func() (SyncStatus, error) {
		return *status, nil
	})
	if err != nil {
		return SyncResult{}, fmt.Errorf("failed to register query handler: %w", err)
	}

	// Register signal handlers on separate goroutines.
	registerSignalHandlers(ctx, sigState)

	// Check for cancellation before starting.
	if err := checkSignals(ctx, sigState, status); err != nil {
		return buildFailedResult(startTime, workflow.Now(ctx), status.Errors), err
	}

	// --- Phase 1: Extract ---
	status.Phase = PhaseExtracting
	extractResult, extractErr := executeExtractPhase(ctx, params)
	if extractErr != nil {
		status.Phase = PhaseFailed
		status.Errors = append(status.Errors, extractErr.Error())
		recordSyncRun(ctx, params, status, startTime, workflow.Now(ctx), SyncResult{Errors: status.Errors})
		alertOnFailure(ctx, params, "extract_failed", extractErr.Error())
		return buildFailedResult(startTime, workflow.Now(ctx), status.Errors), extractErr
	}
	status.RecordsProcessed = extractResult.RecordsCount

	// Check signals between phases.
	if err := checkSignals(ctx, sigState, status); err != nil {
		return buildPartialResult(extractResult, startTime, workflow.Now(ctx), status.Errors), err
	}

	// --- Phase 2: Transform ---
	status.Phase = PhaseTransforming
	transformResult, transformErr := executeTransformPhase(ctx, params, extractResult)
	if transformErr != nil {
		status.Phase = PhaseFailed
		status.Errors = append(status.Errors, transformErr.Error())
		recordSyncRun(ctx, params, status, startTime, workflow.Now(ctx), SyncResult{
			RecordsExtracted: extractResult.RecordsCount,
			Errors:           status.Errors,
		})
		alertOnFailure(ctx, params, "transform_failed", transformErr.Error())
		return buildFailedResult(startTime, workflow.Now(ctx), status.Errors), transformErr
	}
	status.RecordsProcessed = transformResult.RecordsCount

	// Check signals between phases.
	if err := checkSignals(ctx, sigState, status); err != nil {
		return buildPartialResult(extractResult, startTime, workflow.Now(ctx), status.Errors), err
	}

	// --- Phase 3: Load ---
	status.Phase = PhaseLoading
	loadResult, loadErr := executeLoadPhase(ctx, params, transformResult)
	if loadErr != nil {
		status.Phase = PhaseFailed
		status.Errors = append(status.Errors, loadErr.Error())
		recordSyncRun(ctx, params, status, startTime, workflow.Now(ctx), SyncResult{
			RecordsExtracted:   extractResult.RecordsCount,
			RecordsTransformed: transformResult.RecordsCount,
			Errors:             status.Errors,
		})
		alertOnFailure(ctx, params, "load_failed", loadErr.Error())
		return buildFailedResult(startTime, workflow.Now(ctx), status.Errors), loadErr
	}

	// --- Completion ---
	status.Phase = PhaseCompleted
	status.Progress = 1.0
	endTime := workflow.Now(ctx)

	result := SyncResult{
		RecordsExtracted:   extractResult.RecordsCount,
		RecordsTransformed: transformResult.RecordsCount,
		RecordsLoaded:      loadResult.RecordsLoaded,
		NewCheckpoint:      extractResult.NewState,
		Duration:           endTime.Sub(startTime),
		Errors:             collectAllErrors(transformResult.Errors, loadResult.Errors),
	}

	recordSyncRun(ctx, params, status, startTime, endTime, result)

	// If there were non-fatal errors during loading, alert.
	if len(result.Errors) > 0 {
		alertOnFailure(ctx, params, "sync_degraded", fmt.Sprintf("%d non-fatal errors occurred", len(result.Errors)))
	}

	logger.Info("SyncOrchestrator completed",
		"records_extracted", result.RecordsExtracted,
		"records_transformed", result.RecordsTransformed,
		"records_loaded", result.RecordsLoaded,
		"duration", result.Duration,
	)

	return result, nil
}

// registerSignalHandlers sets up goroutines to receive and process workflow
// signals for pause, resume, cancel, and modify operations.
func registerSignalHandlers(ctx workflow.Context, sigState *signals.SignalState) {
	pauseCh := workflow.GetSignalChannel(ctx, signals.SignalPause)
	workflow.Go(ctx, func(gCtx workflow.Context) {
		for {
			var sig signals.PauseSignal
			pauseCh.Receive(gCtx, &sig)
			sigState.ApplyPause(sig)
		}
	})

	resumeCh := workflow.GetSignalChannel(ctx, signals.SignalResume)
	workflow.Go(ctx, func(gCtx workflow.Context) {
		for {
			var sig signals.ResumeSignal
			resumeCh.Receive(gCtx, &sig)
			sigState.ApplyResume()
		}
	})

	cancelCh := workflow.GetSignalChannel(ctx, signals.SignalCancel)
	workflow.Go(ctx, func(gCtx workflow.Context) {
		for {
			var sig signals.CancelSignal
			cancelCh.Receive(gCtx, &sig)
			sigState.ApplyCancel(sig)
		}
	})

	modifyCh := workflow.GetSignalChannel(ctx, signals.SignalModify)
	workflow.Go(ctx, func(gCtx workflow.Context) {
		for {
			var sig signals.ModifySignal
			modifyCh.Receive(gCtx, &sig)
			sigState.ApplyModify(sig)
		}
	})
}

// checkSignals inspects the current signal state and acts on it:
//   - If cancelled, returns an error to terminate the workflow.
//   - If paused, blocks until resumed or cancelled.
func checkSignals(ctx workflow.Context, sigState *signals.SignalState, status *SyncStatus) error {
	if sigState.IsCancelled() {
		status.Phase = PhaseCancelled
		return fmt.Errorf("sync cancelled: %s", sigState.CancelReason)
	}

	if sigState.IsPaused() {
		previousPhase := status.Phase
		status.Phase = PhasePaused

		// Block until either resumed or cancelled.
		workflow.GetSignalChannel(ctx, signals.SignalResume).Receive(ctx, nil)
		sigState.ApplyResume()

		// Check if a cancel came in while we were paused.
		if sigState.IsCancelled() {
			status.Phase = PhaseCancelled
			return fmt.Errorf("sync cancelled while paused: %s", sigState.CancelReason)
		}

		status.Phase = previousPhase
	}

	return nil
}

// executeExtractPhase runs the extract child workflow.
func executeExtractPhase(ctx workflow.Context, params SyncParams) (ExtractResult, error) {
	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID:          fmt.Sprintf("extract-%s-%s", params.TenantID, params.SyncID),
		TaskQueue:           SyncTaskQueue,
		WorkflowRunTimeout:  6 * time.Hour,
		WorkflowTaskTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	}
	childCtx := workflow.WithChildOptions(ctx, childOpts)

	var result ExtractResult
	err := workflow.ExecuteChildWorkflow(childCtx, ExtractWorkflow, ExtractParams{
		SourceConnectorID: params.SourceConnectorID,
		SourceConfig:      params.SourceConfig,
		Streams:           params.SelectedStreams,
		State:             params.LastCheckpoint,
		BatchSize:         DefaultBatchSize(),
		TenantID:          params.TenantID,
		SyncID:            params.SyncID,
	}).Get(ctx, &result)

	return result, err
}

// executeTransformPhase runs the transform child workflow.
func executeTransformPhase(ctx workflow.Context, params SyncParams, extractResult ExtractResult) (TransformResult, error) {
	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID:          fmt.Sprintf("transform-%s-%s", params.TenantID, params.SyncID),
		TaskQueue:           SyncTaskQueue,
		WorkflowRunTimeout:  2 * time.Hour,
		WorkflowTaskTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	}
	childCtx := workflow.WithChildOptions(ctx, childOpts)

	var result TransformResult
	err := workflow.ExecuteChildWorkflow(childCtx, TransformWorkflow, TransformParams{
		Records:       extractResult.Records,
		FieldMappings: params.FieldMappings,
		Streams:       params.SelectedStreams,
		TenantID:      params.TenantID,
		SyncID:        params.SyncID,
	}).Get(ctx, &result)

	return result, err
}

// executeLoadPhase runs the load child workflow.
func executeLoadPhase(ctx workflow.Context, params SyncParams, transformResult TransformResult) (LoadResult, error) {
	childOpts := workflow.ChildWorkflowOptions{
		WorkflowID:          fmt.Sprintf("load-%s-%s", params.TenantID, params.SyncID),
		TaskQueue:           SyncTaskQueue,
		WorkflowRunTimeout:  6 * time.Hour,
		WorkflowTaskTimeout: time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	}
	childCtx := workflow.WithChildOptions(ctx, childOpts)

	var result LoadResult
	err := workflow.ExecuteChildWorkflow(childCtx, LoadWorkflow, LoadParams{
		DestConnectorID: params.DestConnectorID,
		DestConfig:      params.DestConfig,
		Records:         transformResult.Records,
		Streams:         params.SelectedStreams,
		BatchSize:       DefaultBatchSize(),
		TenantID:        params.TenantID,
		SyncID:          params.SyncID,
	}).Get(ctx, &result)

	return result, err
}

// recordSyncRun executes the RecordSyncRunActivity to persist the run results.
// Errors from this activity are logged but do not fail the workflow.
func recordSyncRun(ctx workflow.Context, params SyncParams, status *SyncStatus, startTime, endTime time.Time, result SyncResult) {
	logger := workflow.GetLogger(ctx)

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 1.5,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    3,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, actOpts)

	runStatus := "completed"
	switch status.Phase {
	case PhaseFailed:
		runStatus = "failed"
	case PhaseCancelled:
		runStatus = "cancelled"
	}

	err := workflow.ExecuteActivity(actCtx, activities.RecordSyncRunActivity, activities.SyncRunRecord{
		TenantID:           params.TenantID,
		SyncID:             params.SyncID,
		WorkflowID:         workflow.GetInfo(ctx).WorkflowExecution.ID,
		Status:             runStatus,
		RecordsExtracted:   result.RecordsExtracted,
		RecordsTransformed: result.RecordsTransformed,
		RecordsLoaded:      result.RecordsLoaded,
		NewCheckpoint:      result.NewCheckpoint,
		Duration:           endTime.Sub(startTime),
		Errors:             result.Errors,
		StartedAt:          startTime,
		CompletedAt:        endTime,
	}).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to record sync run", "error", err)
	}
}

// alertOnFailure sends an alert via the AlertActivity.
func alertOnFailure(ctx workflow.Context, params SyncParams, alertType, message string) {
	logger := workflow.GetLogger(ctx)

	actOpts := workflow.ActivityOptions{
		StartToCloseTimeout: 15 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    500 * time.Millisecond,
			BackoffCoefficient: 1.5,
			MaximumInterval:    10 * time.Second,
			MaximumAttempts:    2,
		},
	}
	actCtx := workflow.WithActivityOptions(ctx, actOpts)

	err := workflow.ExecuteActivity(actCtx, activities.AlertActivity, activities.AlertInput{
		TenantID:   params.TenantID,
		SyncID:     params.SyncID,
		WorkflowID: workflow.GetInfo(ctx).WorkflowExecution.ID,
		AlertType:  alertType,
		Message:    message,
		Severity:   alertSeverityForType(alertType),
	}).Get(ctx, nil)
	if err != nil {
		logger.Error("Failed to send alert", "error", err)
	}
}

// alertSeverityForType maps alert types to severity levels.
func alertSeverityForType(alertType string) string {
	switch alertType {
	case "extract_failed", "load_failed":
		return "critical"
	case "transform_failed":
		return "critical"
	case "sync_degraded", "partial_failure":
		return "warning"
	default:
		return "info"
	}
}

// buildFailedResult constructs a SyncResult for a failed workflow.
func buildFailedResult(startTime, endTime time.Time, errors []string) SyncResult {
	return SyncResult{
		Duration: endTime.Sub(startTime),
		Errors:   errors,
	}
}

// buildPartialResult constructs a SyncResult with partial extraction data.
func buildPartialResult(extractResult ExtractResult, startTime, endTime time.Time, errors []string) SyncResult {
	return SyncResult{
		RecordsExtracted: extractResult.RecordsCount,
		NewCheckpoint:    extractResult.NewState,
		Duration:         endTime.Sub(startTime),
		Errors:           errors,
	}
}

// collectAllErrors merges multiple error slices into one.
func collectAllErrors(errorSlices ...[]string) []string {
	var all []string
	for _, errs := range errorSlices {
		all = append(all, errs...)
	}
	return all
}
