// Package orchestration provides the Temporal.io-based workflow orchestration
// layer for FlowForge. It manages sync lifecycle, scheduling, and coordination
// of extract-transform-load pipelines through durable workflows.
package orchestration

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"google.golang.org/protobuf/types/known/durationpb"

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

	// Per-tenant client cache for tenant-isolated namespaces.
	tenantMu      sync.RWMutex
	tenantClients map[string]client.Client
	hostPort      string
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
		client:        c,
		namespace:     cfg.TemporalNamespace,
		tenantClients: make(map[string]client.Client),
		hostPort:      cfg.TemporalAddr(),
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

// TenantNamespace returns the Temporal namespace for a given tenant.
func TenantNamespace(tenantID string) string {
	return fmt.Sprintf("tenant-%s", tenantID)
}

// EnsureTenantNamespace registers a Temporal namespace for the tenant if it
// doesn't already exist. This should be called during tenant provisioning.
func (tc *TemporalClient) EnsureTenantNamespace(ctx context.Context, tenantID string) error {
	ns := TenantNamespace(tenantID)

	_, err := tc.client.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
		Namespace:                        ns,
		WorkflowExecutionRetentionPeriod: durationpb.New(7 * 24 * time.Hour),
		Description:                      fmt.Sprintf("FlowForge tenant namespace for %s", tenantID),
	})
	if err != nil {
		// Namespace may already exist — treat "already exists" as success.
		if isNamespaceAlreadyExistsError(err) {
			return nil
		}
		return fmt.Errorf("register tenant namespace %s: %w", ns, err)
	}

	return nil
}

// ClientForTenant returns a Temporal SDK client connected to the tenant's
// dedicated namespace. Clients are cached for reuse.
func (tc *TemporalClient) ClientForTenant(tenantID string) (client.Client, error) {
	ns := TenantNamespace(tenantID)

	// Fast path: cached client.
	tc.tenantMu.RLock()
	c, ok := tc.tenantClients[tenantID]
	tc.tenantMu.RUnlock()
	if ok {
		return c, nil
	}

	// Slow path: create new client for tenant namespace.
	c, err := client.Dial(client.Options{
		HostPort:  tc.hostPort,
		Namespace: ns,
	})
	if err != nil {
		return nil, fmt.Errorf("dial tenant namespace %s: %w", ns, err)
	}

	tc.tenantMu.Lock()
	// Double-check: another goroutine may have created it.
	if existing, ok := tc.tenantClients[tenantID]; ok {
		tc.tenantMu.Unlock()
		c.Close()
		return existing, nil
	}
	tc.tenantClients[tenantID] = c
	tc.tenantMu.Unlock()

	return c, nil
}

// StartTenantSyncWorkflow starts a sync workflow in the tenant's dedicated
// Temporal namespace, providing workflow isolation between tenants.
func (tc *TemporalClient) StartTenantSyncWorkflow(ctx context.Context, params workflows.SyncParams) (client.WorkflowRun, error) {
	tenantClient, err := tc.ClientForTenant(params.TenantID)
	if err != nil {
		return nil, err
	}

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

	run, err := tenantClient.ExecuteWorkflow(ctx, opts, workflows.SyncOrchestratorName, params)
	if err != nil {
		return nil, fmt.Errorf("start tenant sync workflow %s: %w", workflowID, err)
	}

	return run, nil
}

// CloseAllTenantClients closes all cached per-tenant Temporal clients.
// Call this during shutdown.
func (tc *TemporalClient) CloseAllTenantClients() {
	tc.tenantMu.Lock()
	defer tc.tenantMu.Unlock()

	for id, c := range tc.tenantClients {
		c.Close()
		delete(tc.tenantClients, id)
	}
}

// isNamespaceAlreadyExistsError checks if the error indicates the namespace
// already exists (which is safe to ignore).
func isNamespaceAlreadyExistsError(err error) bool {
	if err == nil {
		return false
	}
	// Temporal returns a specific status code for already-exists errors.
	// The error message contains "already exists" in the gRPC status.
	errStr := err.Error()
	for _, substr := range []string{"already exists", "AlreadyExists", "ALREADY_EXISTS"} {
		if len(errStr) >= len(substr) {
			for i := 0; i <= len(errStr)-len(substr); i++ {
				if errStr[i:i+len(substr)] == substr {
					return true
				}
			}
		}
	}
	return false
}
