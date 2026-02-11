// Package runtime provides the connector execution engine, managing lifecycle,
// sandboxing, resource tracking, and message routing for all connector operations.
package runtime

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// ExecutionStats tracks resource usage for a single connector execution.
type ExecutionStats struct {
	RecordsProcessed int64         `json:"records_processed"`
	BytesProcessed   int64         `json:"bytes_processed"`
	Duration         time.Duration `json:"duration"`
	StartedAt        time.Time     `json:"started_at"`
	CompletedAt      time.Time     `json:"completed_at"`
	ConnectorName    string        `json:"connector_name"`
	Operation        string        `json:"operation"`
}

// Runtime manages connector lifecycle, sandboxing, and resource limits for all
// connector operations. It wraps the CDK registry and applies execution policies.
type Runtime struct {
	cfg            *common.Config
	defaultSandbox SandboxConfig
	mu             sync.RWMutex
	activeExecs    map[string]*executionTracker
	execCounter    uint64
}

// executionTracker monitors a single in-flight execution.
type executionTracker struct {
	id        string
	connector string
	op        string
	startedAt time.Time
	records   atomic.Int64
	bytes     atomic.Int64
	cancel    context.CancelFunc
}

// NewRuntime creates a Runtime with defaults derived from the provided Config.
func NewRuntime(cfg *common.Config) *Runtime {
	return &Runtime{
		cfg: cfg,
		defaultSandbox: SandboxConfig{
			MaxMemoryBytes: 512 * 1024 * 1024, // 512 MB
			MaxDuration:    cfg.RequestTimeout,
			MaxRecords:     int64(cfg.BatchSize) * 1000,
		},
		activeExecs: make(map[string]*executionTracker),
	}
}

// ActiveExecutions returns the count of currently running connector operations.
func (r *Runtime) ActiveExecutions() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.activeExecs)
}

// Stats returns execution stats for all active operations.
func (r *Runtime) Stats() []ExecutionStats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	stats := make([]ExecutionStats, 0, len(r.activeExecs))
	now := time.Now()
	for _, t := range r.activeExecs {
		stats = append(stats, ExecutionStats{
			RecordsProcessed: t.records.Load(),
			BytesProcessed:   t.bytes.Load(),
			Duration:         now.Sub(t.startedAt),
			StartedAt:        t.startedAt,
			ConnectorName:    t.connector,
			Operation:        t.op,
		})
	}
	return stats
}

// startExecution registers a new execution tracker and returns it with a
// sandboxed context.
func (r *Runtime) startExecution(ctx context.Context, connector, op string) (context.Context, *executionTracker) {
	id := fmt.Sprintf("%s-%s-%d", connector, op, atomic.AddUint64(&r.execCounter, 1))
	sandboxCtx, cancel := context.WithTimeout(ctx, r.defaultSandbox.MaxDuration)
	sandboxCtx = common.WithConnectorName(sandboxCtx, connector)

	tracker := &executionTracker{
		id:        id,
		connector: connector,
		op:        op,
		startedAt: time.Now(),
		cancel:    cancel,
	}

	r.mu.Lock()
	r.activeExecs[id] = tracker
	r.mu.Unlock()

	return sandboxCtx, tracker
}

// finishExecution removes the tracker and returns final stats.
func (r *Runtime) finishExecution(tracker *executionTracker) ExecutionStats { //nolint:unparam
	tracker.cancel()
	now := time.Now()
	stats := ExecutionStats{
		RecordsProcessed: tracker.records.Load(),
		BytesProcessed:   tracker.bytes.Load(),
		Duration:         now.Sub(tracker.startedAt),
		StartedAt:        tracker.startedAt,
		CompletedAt:      now,
		ConnectorName:    tracker.connector,
		Operation:        tracker.op,
	}

	r.mu.Lock()
	delete(r.activeExecs, tracker.id)
	r.mu.Unlock()

	return stats
}

// ExecuteCheck runs a connector's Check operation inside the sandbox.
func (r *Runtime) ExecuteCheck(ctx context.Context, connectorName string, config json.RawMessage) (*protocol.CheckResult, error) {
	src, srcErr := cdk.GetSource(connectorName)
	dst, dstErr := cdk.GetDestination(connectorName)
	if srcErr != nil && dstErr != nil {
		return nil, common.NewConnectorError(connectorName, "check", fmt.Errorf("connector not found"))
	}

	sandboxCtx, tracker := r.startExecution(ctx, connectorName, "check")
	defer r.finishExecution(tracker)

	var result *protocol.CheckResult
	err := RunInSandbox(sandboxCtx, r.defaultSandbox, func(sCtx context.Context) error {
		var checkErr error
		if src != nil {
			result, checkErr = src.Check(sCtx, config)
		} else {
			result, checkErr = dst.Check(sCtx, config)
		}
		return checkErr
	})
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "check", err)
	}
	return result, nil
}

// ExecuteDiscover runs a source connector's Discover operation inside the sandbox.
func (r *Runtime) ExecuteDiscover(ctx context.Context, connectorName string, config json.RawMessage) (*protocol.Catalog, error) {
	src, err := cdk.GetSource(connectorName)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "discover", err)
	}

	sandboxCtx, tracker := r.startExecution(ctx, connectorName, "discover")
	defer r.finishExecution(tracker)

	var catalog *protocol.Catalog
	execErr := RunInSandbox(sandboxCtx, r.defaultSandbox, func(sCtx context.Context) error {
		var discErr error
		catalog, discErr = src.Discover(sCtx, config)
		return discErr
	})
	if execErr != nil {
		return nil, common.NewConnectorError(connectorName, "discover", execErr)
	}
	return catalog, nil
}

// ExecuteRead runs a source connector's Read operation inside the sandbox and
// returns a channel of protocol messages. The channel is closed when the read
// completes or an error occurs.
func (r *Runtime) ExecuteRead(
	ctx context.Context,
	connectorName string,
	config json.RawMessage,
	catalog *protocol.ConfiguredCatalog,
	state map[string]json.RawMessage,
) (<-chan protocol.Message, error) {
	src, err := cdk.GetSource(connectorName)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "read", err)
	}

	sandboxCtx, tracker := r.startExecution(ctx, connectorName, "read")
	out := make(chan protocol.Message, r.cfg.BatchSize)

	go func() {
		defer close(out)
		defer r.finishExecution(tracker)

		// Intermediate channel the connector writes to; we intercept for tracking.
		connOut := make(chan protocol.Message, r.cfg.BatchSize)

		errCh := make(chan error, 1)
		go func() {
			defer close(connOut)
			errCh <- RunInSandbox(sandboxCtx, r.defaultSandbox, func(sCtx context.Context) error {
				return src.Read(sCtx, config, catalog, state, connOut)
			})
		}()

		maxRecords := r.defaultSandbox.MaxRecords
		for msg := range connOut {
			if msg.Record != nil {
				count := tracker.records.Add(1)
				tracker.bytes.Add(int64(len(msg.Record.Data)))
				if maxRecords > 0 && count > maxRecords {
					out <- protocol.Message{
						Type: protocol.MessageTypeLog,
						Log: &protocol.Log{
							Level:     protocol.LogLevelWarn,
							Message:   fmt.Sprintf("max records limit (%d) reached, stopping read", maxRecords),
							Timestamp: time.Now(),
						},
					}
					tracker.cancel()
					return
				}
			}
			select {
			case out <- msg:
			case <-sandboxCtx.Done():
				return
			}
		}

		if readErr := <-errCh; readErr != nil {
			out <- protocol.Message{
				Type: protocol.MessageTypeLog,
				Log: &protocol.Log{
					Level:     protocol.LogLevelError,
					Message:   fmt.Sprintf("read error: %v", readErr),
					Timestamp: time.Now(),
				},
			}
		}
	}()

	return out, nil
}

// ExecuteWrite runs a destination connector's Write operation inside the sandbox.
func (r *Runtime) ExecuteWrite(
	ctx context.Context,
	connectorName string,
	config json.RawMessage,
	catalog *protocol.ConfiguredCatalog,
	input <-chan protocol.Message,
) (*protocol.WriteResult, error) {
	dst, err := cdk.GetDestination(connectorName)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "write", err)
	}

	sandboxCtx, tracker := r.startExecution(ctx, connectorName, "write")
	defer r.finishExecution(tracker)

	// Wrap input channel to track bytes/records flowing through.
	tracked := make(chan protocol.Message, r.cfg.BatchSize)
	go func() {
		defer close(tracked)
		for msg := range input {
			if msg.Record != nil {
				tracker.records.Add(1)
				tracker.bytes.Add(int64(len(msg.Record.Data)))
			}
			select {
			case tracked <- msg:
			case <-sandboxCtx.Done():
				return
			}
		}
	}()

	var result *protocol.WriteResult
	execErr := RunInSandbox(sandboxCtx, r.defaultSandbox, func(sCtx context.Context) error {
		var wErr error
		result, wErr = dst.Write(sCtx, config, catalog, tracked)
		return wErr
	})
	if execErr != nil {
		return nil, common.NewConnectorError(connectorName, "write", execErr)
	}
	return result, nil
}

// CancelExecution cancels a running execution by its tracker ID.
func (r *Runtime) CancelExecution(executionID string) bool {
	r.mu.RLock()
	tracker, ok := r.activeExecs[executionID]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	tracker.cancel()
	return true
}

// Shutdown cancels all active executions and waits briefly for them to drain.
func (r *Runtime) Shutdown() {
	r.mu.Lock()
	for _, t := range r.activeExecs {
		t.cancel()
	}
	r.mu.Unlock()
}
