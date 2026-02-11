---
title: Temporal Patterns
layout: default
parent: Architecture
nav_order: 2
description: "How FlowForge uses Temporal.io for durable execution of all data movement operations."
permalink: /architecture/temporal-patterns/
---

# Temporal.io Workflow Patterns

This document details how FlowForge uses Temporal.io for durable execution of all data movement operations.

---

## Why Temporal

Temporal.io is the foundational runtime for FlowForge. Every sync, whether ETL extraction, reverse ETL activation, or MCP request, is modeled as a Temporal Workflow. This gives us guarantees that are impossible to achieve with traditional job queues:

1. **Durable Execution**: If a worker crashes mid-sync, Temporal replays the workflow from the last committed state
2. **Automatic Retries**: Each activity has configurable retry policies with exponential backoff, jitter, and max attempts
3. **Saga Compensation**: If a write succeeds but a downstream step fails, compensation activities undo the partial work
4. **Long-Running Support**: Syncs running hours or days are first-class. State persists across restarts and deploys
5. **Full Observability**: Every workflow execution is traceable through Temporal's event history
6. **Safe Versioning**: Update workflow code without breaking in-flight executions

---

## Pattern 1: Parent-Child Sync Orchestration

The `SyncOrchestrator` is the top-level workflow for every sync operation. It spawns child workflows for each phase (Extract, Transform, Load) and coordinates state between them.

```go
func SyncOrchestrator(ctx workflow.Context, params SyncParams) (SyncResult, error) {
    // Phase 1: Extract
    extractResult := workflow.ExecuteChildWorkflow(ctx, ExtractWorkflow, extractParams)

    // Phase 2: Transform
    transformResult := workflow.ExecuteChildWorkflow(ctx, TransformWorkflow, transformParams)

    // Phase 3: Load
    loadResult := workflow.ExecuteChildWorkflow(ctx, LoadWorkflow, loadParams)

    return SyncResult{...}, nil
}
```

**Why Parent-Child:**
- **Independent retries**: If Transform fails, only Transform retries — Extract doesn't re-run
- **Resource isolation**: Each phase runs on its dedicated task queue
- **Progress tracking**: Parent workflow tracks overall sync state; each child reports phase progress
- **Cancellation**: Cancelling the parent cascades to all children

---

## Pattern 2: Fan-Out/Fan-In for Parallel Extraction

For high-volume sources (millions of records), the Extract workflow fans out to multiple partition workflows:

```go
func ExtractWorkflow(ctx workflow.Context, params ExtractParams) (ExtractResult, error) {
    partitions := DeterminePartitions(params)

    // Fan-out: spawn a child workflow per partition
    var futures []workflow.ChildWorkflowFuture
    for _, partition := range partitions {
        future := workflow.ExecuteChildWorkflow(ctx, PartitionExtractWorkflow, partition)
        futures = append(futures, future)
    }

    // Fan-in: collect all results
    for _, future := range futures {
        var result PartitionResult
        future.Get(ctx, &result)
    }
}
```

Each partition workflow sends Temporal signals to report progress back to the parent.

---

## Pattern 3: Continue-As-New for Scheduled Syncs

Recurring syncs avoid unbounded Temporal event history:

```go
func ScheduledSyncWorkflow(ctx workflow.Context, params ScheduleParams) error {
    // Wait until next scheduled time
    workflow.Sleep(ctx, time.Until(nextRun))

    // Execute the sync
    workflow.ExecuteChildWorkflow(ctx, SyncOrchestrator, params.SyncParams)

    // Continue-as-new to reset event history
    return workflow.NewContinueAsNewError(ctx, ScheduledSyncWorkflow, updatedParams)
}
```

A schedule running every minute for a month would accumulate ~43,000 events. Continue-as-new starts a fresh execution with clean history.

---

## Pattern 4: Signal-Based Control

Users need real-time control over running syncs. Temporal signals provide this without killing workflows:

| Signal | Action |
|:--|:--|
| `pause` | Workflow blocks until resume signal |
| `resume` | Workflow continues from paused state |
| `cancel` | Workflow runs compensation activities |
| `modify` | Workflow applies new config on next activity |

```go
func SyncOrchestrator(ctx workflow.Context, params SyncParams) (SyncResult, error) {
    pauseCh := workflow.GetSignalChannel(ctx, "pause")
    resumeCh := workflow.GetSignalChannel(ctx, "resume")
    cancelCh := workflow.GetSignalChannel(ctx, "cancel")

    // Check for signals between phases
    if checkPause(ctx, pauseCh) {
        waitForResume(ctx, resumeCh, cancelCh)
    }
    // ... proceed with sync phases
}
```

---

## Pattern 5: Saga Compensation for Bidirectional Syncs

Bidirectional syncs write to two systems. If the write to System A succeeds but System B fails, compensation activities undo the System A write:

```go
func BidirectionalSyncWorkflow(ctx workflow.Context, params BiSyncParams) error {
    forwardResult := workflow.ExecuteChildWorkflow(ctx, SyncOrchestrator, forwardParams)

    reverseResult, err := workflow.ExecuteChildWorkflow(ctx, SyncOrchestrator, reverseParams)
    if err != nil {
        // Compensation: undo the forward sync
        workflow.ExecuteActivity(ctx, CompensateWriteActivity, forwardResult)
        return err
    }

    return workflow.ExecuteActivity(ctx, ResolveConflictsActivity, conflictParams)
}
```

---

## Activity Retry Policies

| Activity Type | Initial Interval | Backoff | Max Interval | Max Attempts | Non-Retryable |
|:--|:--|:--|:--|:--|:--|
| API calls | 1s | 2.0x | 1 min | 10 | Auth errors, config errors, schema errors |
| Database writes | 2s | 2.0x | 5 min | 5 | — |
| Long-running | 5s | 1.5x | 2 min | 3 | — |

---

## Search Attributes

FlowForge registers custom Temporal search attributes for filtering workflows:

| Attribute | Type | Purpose |
|:--|:--|:--|
| `ConnectorType` | Keyword | Filter by connector (salesforce, bigquery) |
| `TenantID` | Keyword | Filter by tenant |
| `SyncID` | Keyword | Find specific sync executions |
| `SyncPhase` | Keyword | Filter by phase (extract, transform, load) |
| `ErrorType` | Keyword | Find workflows with specific error types |
| `RecordCount` | Int | Find high-volume syncs |

Example query:
```
ConnectorType = "salesforce" AND TenantID = "tenant-123" AND ErrorType = "RateLimitError"
```

---

## Workflow Versioning

When updating workflow code, Temporal's versioning API ensures in-flight syncs aren't broken:

```go
v := workflow.GetVersion(ctx, "add-transform-dedup", workflow.DefaultVersion, 1)

if v == workflow.DefaultVersion {
    return runOriginalSync(ctx, params) // For in-flight workflows
}
return runSyncWithDedup(ctx, params) // For new workflows
```

This ensures deterministic replay: existing workflows replay with the original code path, while new workflows use the updated path.
