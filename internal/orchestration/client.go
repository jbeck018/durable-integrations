// Package orchestration provides the Temporal.io-based workflow orchestration
// layer for FlowForge. It manages sync lifecycle, scheduling, and coordination
// of extract-transform-load pipelines through durable workflows.
package orchestration

import (
	"context"
	"fmt"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/internal/orchestration/workflows"
)

// Search attribute keys for workflow visibility and filtering.
const (
	SearchAttrConnectorType = "ConnectorType"
	SearchAttrTenantID      = "TenantID"
	SearchAttrSyncID        = "SyncID"
	SearchAttrSyncPhase     = "SyncPhase"
	SearchAttrErrorType     = "ErrorType"
)

// TemporalClient wraps the Temporal SDK client with FlowForge-specific helpers.
type TemporalClient struct {
	client    client.Client
	namespace string
}

// NewTemporalClient creates a new TemporalClient connected to the Temporal server
// specified in the provided configuration.
func NewTemporalClient(cfg *common.Config) (*TemporalClient, error) {
	opts := client.Options{
		HostPort:  cfg.TemporalAddr(),
		Namespace: cfg.TemporalNamespace,
	}

	c, err := client.Dial(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Temporal at %s: %w", cfg.TemporalAddr(), err)
	}

	return &TemporalClient{
		client:    c,
		namespace: cfg.TemporalNamespace,
	}, nil
}

// Close shuts down the Temporal client connection.
func (tc *TemporalClient) Close() {
	if tc.client != nil {
		tc.client.Close()
	}
}

// Client returns the underlying Temporal SDK client for use by workers
// and other components that need direct access.
func (tc *TemporalClient) Client() client.Client {
	return tc.client
}

// StartSyncWorkflow starts a new sync orchestrator workflow with the given parameters.
// It returns the workflow run handle for tracking and interaction.
func (tc *TemporalClient) StartSyncWorkflow(ctx context.Context, params workflows.SyncParams) (client.WorkflowRun, error) {
	workflowID := fmt.Sprintf("sync-%s-%s", params.TenantID, params.SyncID)

	opts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: workflows.SyncTaskQueue,
		SearchAttributes: map[string]interface{}{
			SearchAttrTenantID:      params.TenantID,
			SearchAttrSyncID:        params.SyncID,
			SearchAttrConnectorType: params.SourceConnectorID,
			SearchAttrSyncPhase:     string(workflows.PhaseInitializing),
		},
		WorkflowExecutionTimeout: 24 * time.Hour,
		WorkflowTaskTimeout:      time.Minute,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2.0,
			MaximumInterval:    5 * time.Minute,
			MaximumAttempts:    3,
		},
	}

	run, err := tc.client.ExecuteWorkflow(ctx, opts, workflows.SyncOrchestratorName, params)
	if err != nil {
		return nil, fmt.Errorf("failed to start sync workflow %s: %w", workflowID, err)
	}

	return run, nil
}

// StartScheduledSyncWorkflow starts a scheduled sync workflow that runs on
// a cron schedule using the continue-as-new pattern.
func (tc *TemporalClient) StartScheduledSyncWorkflow(ctx context.Context, params workflows.ScheduledSyncParams) (client.WorkflowRun, error) {
	workflowID := fmt.Sprintf("scheduled-sync-%s-%s", params.TenantID, params.SyncID)

	opts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: workflows.SyncTaskQueue,
		SearchAttributes: map[string]interface{}{
			SearchAttrTenantID:      params.TenantID,
			SearchAttrSyncID:        params.SyncID,
			SearchAttrConnectorType: params.SourceConnectorID,
		},
		WorkflowExecutionTimeout: 365 * 24 * time.Hour,
		WorkflowTaskTimeout:      time.Minute,
	}

	run, err := tc.client.ExecuteWorkflow(ctx, opts, workflows.ScheduledSyncWorkflowName, params)
	if err != nil {
		return nil, fmt.Errorf("failed to start scheduled sync workflow %s: %w", workflowID, err)
	}

	return run, nil
}

// StartBidirectionalSyncWorkflow starts a bidirectional sync workflow with
// saga compensation for both directions.
func (tc *TemporalClient) StartBidirectionalSyncWorkflow(ctx context.Context, params workflows.BidirectionalSyncParams) (client.WorkflowRun, error) {
	workflowID := fmt.Sprintf("bidi-sync-%s-%s", params.TenantID, params.SyncID)

	opts := client.StartWorkflowOptions{
		ID:        workflowID,
		TaskQueue: workflows.SyncTaskQueue,
		SearchAttributes: map[string]interface{}{
			SearchAttrTenantID:      params.TenantID,
			SearchAttrSyncID:        params.SyncID,
			SearchAttrConnectorType: params.SourceConnectorID,
		},
		WorkflowExecutionTimeout: 24 * time.Hour,
		WorkflowTaskTimeout:      time.Minute,
	}

	run, err := tc.client.ExecuteWorkflow(ctx, opts, workflows.BidirectionalSyncName, params)
	if err != nil {
		return nil, fmt.Errorf("failed to start bidirectional sync workflow %s: %w", workflowID, err)
	}

	return run, nil
}

// SignalWorkflow sends a signal to a running workflow by its ID.
func (tc *TemporalClient) SignalWorkflow(ctx context.Context, workflowID, signalName string, signalArg interface{}) error {
	err := tc.client.SignalWorkflow(ctx, workflowID, "", signalName, signalArg)
	if err != nil {
		return fmt.Errorf("failed to signal workflow %s with %s: %w", workflowID, signalName, err)
	}
	return nil
}

// QueryWorkflow queries a running workflow and decodes the result into valuePtr.
func (tc *TemporalClient) QueryWorkflow(ctx context.Context, workflowID, queryType string, valuePtr interface{}, args ...interface{}) error {
	resp, err := tc.client.QueryWorkflow(ctx, workflowID, "", queryType, args...)
	if err != nil {
		return fmt.Errorf("failed to query workflow %s with %s: %w", workflowID, queryType, err)
	}
	if err := resp.Get(valuePtr); err != nil {
		return fmt.Errorf("failed to decode query result for workflow %s: %w", workflowID, err)
	}
	return nil
}

// CancelWorkflow requests cancellation of a running workflow.
func (tc *TemporalClient) CancelWorkflow(ctx context.Context, workflowID string) error {
	err := tc.client.CancelWorkflow(ctx, workflowID, "")
	if err != nil {
		return fmt.Errorf("failed to cancel workflow %s: %w", workflowID, err)
	}
	return nil
}

// GetWorkflowStatus retrieves the current sync status from a running workflow
// by issuing a status query.
func (tc *TemporalClient) GetWorkflowStatus(ctx context.Context, workflowID string) (*workflows.SyncStatus, error) {
	var status workflows.SyncStatus
	if err := tc.QueryWorkflow(ctx, workflowID, workflows.QuerySyncStatus, &status); err != nil {
		return nil, err
	}
	return &status, nil
}

// RegisterSearchAttributes registers the custom search attributes that
// FlowForge uses for workflow visibility queries. This is idempotent and
// should be called once during service initialization.
func (tc *TemporalClient) RegisterSearchAttributes(ctx context.Context) error {
	attrs := map[string]enums.IndexedValueType{
		SearchAttrConnectorType: enums.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrTenantID:      enums.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrSyncID:        enums.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrSyncPhase:     enums.INDEXED_VALUE_TYPE_KEYWORD,
		SearchAttrErrorType:     enums.INDEXED_VALUE_TYPE_KEYWORD,
	}

	saRequest := make(map[string]enums.IndexedValueType, len(attrs))
	for name, valueType := range attrs {
		saRequest[name] = valueType
	}

	_, err := tc.client.OperatorService().AddSearchAttributes(ctx, &operatorservice.AddSearchAttributesRequest{
		Namespace:        tc.namespace,
		SearchAttributes: saRequest,
	})
	if err != nil {
		return fmt.Errorf("failed to register search attributes: %w", err)
	}
	return nil
}

// WorkflowIDForSync builds a deterministic workflow ID for a sync run.
func WorkflowIDForSync(tenantID, syncID string) string {
	return fmt.Sprintf("sync-%s-%s", tenantID, syncID)
}

// WorkflowIDForScheduledSync builds a deterministic workflow ID for a scheduled sync.
func WorkflowIDForScheduledSync(tenantID, syncID string) string {
	return fmt.Sprintf("scheduled-sync-%s-%s", tenantID, syncID)
}

// WorkflowIDForBidirectionalSync builds a deterministic workflow ID for a bidirectional sync.
func WorkflowIDForBidirectionalSync(tenantID, syncID string) string {
	return fmt.Sprintf("bidi-sync-%s-%s", tenantID, syncID)
}
