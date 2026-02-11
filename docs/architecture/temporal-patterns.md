# Temporal.io Workflow Patterns in FlowForge

This document details how FlowForge uses Temporal.io for durable execution of all data movement operations.

---

## Why Temporal

Temporal.io is the foundational runtime for FlowForge. Every sync, whether ETL extraction, reverse ETL activation, or MCP request, is modeled as a Temporal Workflow. This gives us guarantees that are impossible to achieve with traditional job queues:

1. **Durable Execution**: If a worker crashes mid-sync, Temporal replays the workflow from the last committed state. Zero data loss, zero orphaned records.
2. **Automatic Retries**: Each activity (API call, DB write) has configurable retry policies with exponential backoff, jitter, and max attempts.
3. **Saga Compensation**: If a write succeeds but a downstream step fails, compensation activities undo the partial work.
4. **Long-Running Support**: Syncs running hours or days are first-class. State persists across restarts and deploys.
5. **Full Observability**: Every workflow execution is traceable through Temporal's event history.
6. **Safe Versioning**: Update workflow code without breaking in-flight executions.

---

## Pattern 1: Parent-Child Sync Orchestration

### Overview

The `SyncOrchestrator` is the top-level workflow for every sync operation. It spawns child workflows for each phase (Extract, Transform, Load) and coordinates state between them.

### Workflow Structure

```go
// SyncOrchestrator is the parent workflow for all sync operations.
func SyncOrchestrator(ctx workflow.Context, params SyncParams) (SyncResult, error) {
    // Phase 1: Extract
    extractResult, err := workflow.ExecuteChildWorkflow(ctx, ExtractWorkflow, ExtractParams{
        ConnectorID: params.SourceConnectorID,
        Streams:     params.SelectedStreams,
        State:       params.LastCheckpoint,
    }).Get(ctx, &extractResult)
    if err != nil {
        return SyncResult{}, handleExtractError(ctx, err)
    }

    // Phase 2: Transform
    transformResult, err := workflow.ExecuteChildWorkflow(ctx, TransformWorkflow, TransformParams{
        Records:  extractResult.BufferRef,
        Mappings: params.FieldMappings,
    }).Get(ctx, &transformResult)
    if err != nil {
        return SyncResult{}, handleTransformError(ctx, err)
    }

    // Phase 3: Load
    loadResult, err := workflow.ExecuteChildWorkflow(ctx, LoadWorkflow, LoadParams{
        ConnectorID: params.DestConnectorID,
        Records:     transformResult.BufferRef,
        SyncMode:    params.SyncMode,
    }).Get(ctx, &loadResult)
    if err != nil {
        // Saga compensation: if load partially succeeded, undo
        return SyncResult{}, handleLoadError(ctx, err, loadResult)
    }

    return SyncResult{
        RecordsExtracted:  extractResult.Count,
        RecordsTransformed: transformResult.Count,
        RecordsLoaded:     loadResult.Count,
        NewCheckpoint:     extractResult.Checkpoint,
    }, nil
}
```

### Why Parent-Child

- **Independent retries**: If Transform fails, only Transform retries — Extract doesn't re-run
- **Resource isolation**: Each phase runs on its dedicated task queue with appropriate resource allocation
- **Progress tracking**: Parent workflow tracks overall sync state; each child reports phase progress
- **Cancellation**: Cancelling the parent cascades to all children

---

## Pattern 2: Fan-Out/Fan-In for Parallel Extraction

### Overview

When extracting from high-volume sources (Salesforce with millions of records, BigQuery tables with billions of rows), a single extraction activity would be too slow. The Extract workflow fans out to multiple partition workflows.

### Workflow Structure

```go
func ExtractWorkflow(ctx workflow.Context, params ExtractParams) (ExtractResult, error) {
    // Determine partitions based on source volume
    partitions, err := workflow.ExecuteActivity(ctx, DeterminePartitionsActivity, params).Get(ctx, &partitions)
    if err != nil {
        return ExtractResult{}, err
    }

    // Fan-out: spawn a child workflow per partition
    var futures []workflow.ChildWorkflowFuture
    for _, partition := range partitions {
        future := workflow.ExecuteChildWorkflow(ctx, PartitionExtractWorkflow, PartitionParams{
            ConnectorID: params.ConnectorID,
            Stream:      params.Stream,
            RangeStart:  partition.Start,
            RangeEnd:    partition.End,
        })
        futures = append(futures, future)
    }

    // Fan-in: collect all results
    var totalRecords int64
    for _, future := range futures {
        var result PartitionResult
        if err := future.Get(ctx, &result); err != nil {
            return ExtractResult{}, err
        }
        totalRecords += result.RecordCount
    }

    return ExtractResult{Count: totalRecords}, nil
}
```

### Signal-Based Progress Reporting

Each partition workflow sends Temporal signals to report progress:

```go
func PartitionExtractWorkflow(ctx workflow.Context, params PartitionParams) (PartitionResult, error) {
    var processed int64
    for {
        batch, done, err := workflow.ExecuteActivity(ctx, FetchBatchActivity, FetchParams{
            Cursor: cursor,
            Limit:  1000,
        }).Get(ctx, &batch)

        processed += int64(len(batch.Records))

        // Signal parent with progress
        workflow.SignalExternalWorkflow(ctx, parentWorkflowID, "", "progress", ProgressSignal{
            PartitionID: params.PartitionID,
            Processed:   processed,
        })

        if done {
            break
        }
    }
    return PartitionResult{RecordCount: processed}, nil
}
```

---

## Pattern 3: Continue-As-New for Scheduled Syncs

### Overview

Recurring syncs (e.g., every 15 minutes) must avoid unbounded Temporal event history. After each sync cycle, the workflow "continues as new" — starting a fresh execution with clean history while preserving the schedule.

### Workflow Structure

```go
func ScheduledSyncWorkflow(ctx workflow.Context, params ScheduleParams) error {
    for {
        // Wait until next scheduled time
        nextRun := calculateNextRun(params.CronExpression, params.Timezone)
        if err := workflow.Sleep(ctx, time.Until(nextRun)); err != nil {
            return err
        }

        // Execute the sync
        var result SyncResult
        err := workflow.ExecuteChildWorkflow(ctx, SyncOrchestrator, params.SyncParams).Get(ctx, &result)

        // Record result (even if failed)
        workflow.ExecuteActivity(ctx, RecordSyncRunActivity, RunRecord{
            SyncID:    params.SyncID,
            Result:    result,
            Error:     err,
            Timestamp: workflow.Now(ctx),
        })

        // Continue-as-new to reset event history
        // This prevents unbounded history growth for long-running schedules
        return workflow.NewContinueAsNewError(ctx, ScheduledSyncWorkflow, ScheduleParams{
            SyncID:         params.SyncID,
            CronExpression: params.CronExpression,
            Timezone:       params.Timezone,
            SyncParams:     params.SyncParams,
            LastCheckpoint: result.NewCheckpoint,
        })
    }
}
```

### Why Continue-As-New

- Temporal event history grows with each decision/activity. A schedule running every minute for a month would accumulate ~43,000 events.
- Continue-as-new starts a fresh execution with a new event history, linked to the previous execution for traceability.
- The schedule state (last checkpoint, next run time) is passed to the new execution as input.

---

## Pattern 4: Signal-Based Control

### Overview

Users need real-time control over running syncs: pause, resume, cancel, and modify. Temporal signals provide this without killing and restarting workflows.

### Signal Definitions

```go
const (
    SignalPause   = "pause"
    SignalResume  = "resume"
    SignalCancel  = "cancel"
    SignalModify  = "modify"
)

type ModifySignal struct {
    NewMappings    *FieldMappings    `json:"new_mappings,omitempty"`
    NewSyncMode    *SyncMode         `json:"new_sync_mode,omitempty"`
    NewBatchSize   *int              `json:"new_batch_size,omitempty"`
}
```

### Workflow Integration

```go
func SyncOrchestrator(ctx workflow.Context, params SyncParams) (SyncResult, error) {
    // Set up signal channels
    pauseCh := workflow.GetSignalChannel(ctx, SignalPause)
    resumeCh := workflow.GetSignalChannel(ctx, SignalResume)
    cancelCh := workflow.GetSignalChannel(ctx, SignalCancel)
    modifyCh := workflow.GetSignalChannel(ctx, SignalModify)

    // Check for signals between phases
    if checkPause(ctx, pauseCh) {
        // Block until resume signal
        waitForResume(ctx, resumeCh, cancelCh)
    }

    if checkCancel(ctx, cancelCh) {
        // Run compensation activities
        return runCompensation(ctx, params)
    }

    // Apply any modifications
    if mod, ok := checkModify(ctx, modifyCh); ok {
        params = applyModifications(params, mod)
    }

    // ... proceed with sync phases
}
```

### Query Support

Temporal queries let users check sync status without side effects:

```go
func SyncOrchestrator(ctx workflow.Context, params SyncParams) (SyncResult, error) {
    var status SyncStatus

    // Register query handler
    workflow.SetQueryHandler(ctx, "status", func() (SyncStatus, error) {
        return status, nil
    })

    status = SyncStatus{Phase: "extracting", Progress: 0}
    // ... update status as sync progresses
}
```

---

## Pattern 5: Saga Compensation for Bidirectional Syncs

### Overview

Bidirectional syncs write to two systems. If the write to System A succeeds but System B fails, we must compensate (undo the System A write) to maintain consistency.

### Workflow Structure

```go
func BidirectionalSyncWorkflow(ctx workflow.Context, params BiSyncParams) error {
    // Forward direction: Source A → Destination B
    forwardResult, err := workflow.ExecuteChildWorkflow(ctx, SyncOrchestrator, SyncParams{
        Source: params.SystemA,
        Dest:   params.SystemB,
    }).Get(ctx, &forwardResult)
    if err != nil {
        return err // No compensation needed — nothing written yet
    }

    // Reverse direction: Source B → Destination A
    reverseResult, err := workflow.ExecuteChildWorkflow(ctx, SyncOrchestrator, SyncParams{
        Source: params.SystemB,
        Dest:   params.SystemA,
    }).Get(ctx, &reverseResult)
    if err != nil {
        // Compensation: undo the forward sync
        compensateErr := workflow.ExecuteActivity(ctx, CompensateWriteActivity, CompensateParams{
            System:      params.SystemB,
            WriteResult: forwardResult,
        }).Get(ctx, nil)
        if compensateErr != nil {
            // Log compensation failure for manual intervention
            workflow.ExecuteActivity(ctx, AlertCompensationFailure, AlertParams{
                SyncID: params.SyncID,
                Error:  compensateErr,
            })
        }
        return err
    }

    // Conflict resolution
    return workflow.ExecuteActivity(ctx, ResolveConflictsActivity, ConflictParams{
        Forward: forwardResult,
        Reverse: reverseResult,
        Strategy: params.ConflictStrategy,
    }).Get(ctx, nil)
}
```

---

## Activity Retry Policies

All activities use configured retry policies. Defaults vary by activity type:

```go
var (
    // API calls: retry transient errors with backoff
    APIRetryPolicy = temporal.RetryPolicy{
        InitialInterval:    1 * time.Second,
        BackoffCoefficient: 2.0,
        MaximumInterval:    1 * time.Minute,
        MaximumAttempts:    10,
        NonRetryableErrorTypes: []string{
            "AuthenticationError",     // Don't retry auth failures
            "InvalidConfigError",      // Don't retry config errors
            "SchemaValidationError",   // Don't retry schema mismatches
        },
    }

    // Database writes: fewer retries, longer intervals
    DBWriteRetryPolicy = temporal.RetryPolicy{
        InitialInterval:    2 * time.Second,
        BackoffCoefficient: 2.0,
        MaximumInterval:    5 * time.Minute,
        MaximumAttempts:    5,
    }

    // Heartbeat activities: long-running with frequent heartbeats
    LongRunningRetryPolicy = temporal.RetryPolicy{
        InitialInterval:    5 * time.Second,
        BackoffCoefficient: 1.5,
        MaximumInterval:    2 * time.Minute,
        MaximumAttempts:    3,
    }
)
```

---

## Search Attributes

FlowForge registers custom Temporal search attributes for filtering and querying workflows:

| Attribute | Type | Purpose |
|---|---|---|
| `ConnectorType` | Keyword | Filter by connector (salesforce, bigquery, etc.) |
| `TenantID` | Keyword | Filter by tenant |
| `SyncID` | Keyword | Find specific sync executions |
| `SyncPhase` | Keyword | Filter by phase (extract, transform, load) |
| `ErrorType` | Keyword | Find workflows with specific error types |
| `RecordCount` | Int | Find high-volume syncs |
| `StartedAt` | Datetime | Time-range queries |

Example query:
```
ConnectorType = "salesforce" AND TenantID = "tenant-123" AND ErrorType = "RateLimitError"
```

---

## Workflow Versioning

When updating workflow code, Temporal's versioning API ensures in-flight syncs aren't broken:

```go
func SyncOrchestrator(ctx workflow.Context, params SyncParams) (SyncResult, error) {
    v := workflow.GetVersion(ctx, "add-transform-dedup", workflow.DefaultVersion, 1)

    if v == workflow.DefaultVersion {
        // Original code path (for in-flight workflows)
        return runOriginalSync(ctx, params)
    }

    // New code path (for new workflows)
    // Includes deduplication in transform phase
    return runSyncWithDedup(ctx, params)
}
```

This ensures deterministic replay: existing workflows replay with the original code path, while new workflows use the updated path.
