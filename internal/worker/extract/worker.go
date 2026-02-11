// Package extract provides the extraction phase of the FlowForge ETL pipeline.
// It reads data from source connectors via the CDK registry and collects messages
// into batches for downstream processing.
package extract

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"go.temporal.io/sdk/activity"

	restcommon "github.com/flowforge/flowforge/connectors/common"
	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// ExtractWorker reads data from source connectors and collects messages into
// batches. It handles rate limiting signals by pausing reads for the indicated
// duration.
type ExtractWorker struct {
	batchSize      int
	readTimeout    time.Duration
	rateLimitDelay time.Duration

	// DistRateLimiter is an optional distributed rate limiter (e.g. Redis-backed)
	// injected at worker initialization for multi-worker rate limit enforcement.
	DistRateLimiter restcommon.DistributedRateLimiter

	mu        sync.Mutex
	isRunning bool
}

// NewExtractWorker creates an ExtractWorker with the given batch size and read timeout.
func NewExtractWorker(batchSize int, readTimeout time.Duration) *ExtractWorker {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if readTimeout <= 0 {
		readTimeout = 5 * time.Minute
	}
	return &ExtractWorker{
		batchSize:      batchSize,
		readTimeout:    readTimeout,
		rateLimitDelay: 1 * time.Second,
	}
}

// ProcessBatch invokes the source connector's Read method, collects messages
// into a batch, and returns them. It respects rate limiting signals embedded
// in control messages from the connector.
func (w *ExtractWorker) ProcessBatch(
	ctx context.Context,
	connectorName string,
	config json.RawMessage,
	catalog *protocol.ConfiguredCatalog,
	state map[string]json.RawMessage,
) ([]protocol.Message, error) {
	w.mu.Lock()
	if w.isRunning {
		w.mu.Unlock()
		return nil, fmt.Errorf("extract worker already running for connector %s", connectorName)
	}
	w.isRunning = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.isRunning = false
		w.mu.Unlock()
	}()

	src, err := cdk.GetSource(connectorName)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "get_source", err)
	}

	ctx = common.WithConnectorName(ctx, connectorName)
	readCtx, readCancel := context.WithTimeout(ctx, w.readTimeout)
	defer readCancel()

	// Size the output channel to the batch size so the connector can write
	// ahead without blocking immediately.
	output := make(chan protocol.Message, w.batchSize)

	var readErr error
	var readDone sync.WaitGroup
	readDone.Add(1)
	go func() {
		defer readDone.Done()
		defer close(output)
		readErr = src.Read(readCtx, config, catalog, state, output)
	}()

	messages := make([]protocol.Message, 0, w.batchSize)

	// Heartbeat every 30s so Temporal knows the activity is alive.
	heartbeatTicker := time.NewTicker(30 * time.Second)
	defer heartbeatTicker.Stop()

	for msg := range output {
		if msg.Control != nil {
			w.handleControl(readCtx, msg.Control)
			continue
		}
		messages = append(messages, msg)

		// Non-blocking heartbeat check.
		select {
		case <-heartbeatTicker.C:
			activity.RecordHeartbeat(ctx, map[string]interface{}{
				"records_read": len(messages),
				"connector":    connectorName,
			})
		default:
		}
	}

	readDone.Wait()
	if readErr != nil {
		return messages, common.NewConnectorError(connectorName, "read", readErr)
	}

	return messages, nil
}

// handleControl reacts to control signals from the connector. Rate limit and
// backpressure signals cause the worker to sleep before consuming more messages.
func (w *ExtractWorker) handleControl(ctx context.Context, ctrl *protocol.Control) {
	switch ctrl.Type {
	case protocol.ControlTypeRateLimit:
		delay := w.parseRateLimitDelay(ctrl.Data)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
		}
	case protocol.ControlTypeBackpressure:
		select {
		case <-time.After(w.rateLimitDelay):
		case <-ctx.Done():
		}
	case protocol.ControlTypePause:
		// Wait until we receive a resume or the context is cancelled.
		// Since we are consuming from a channel, a RESUME will arrive
		// as another message. Here we simply apply a bounded pause.
		select {
		case <-time.After(30 * time.Second):
		case <-ctx.Done():
		}
	case protocol.ControlTypeResume:
		// No-op: resume normal processing.
	}
}

// parseRateLimitDelay extracts a "retry_after_seconds" value from the control
// data JSON. Falls back to the default delay if parsing fails.
func (w *ExtractWorker) parseRateLimitDelay(data json.RawMessage) time.Duration {
	if len(data) == 0 {
		return w.rateLimitDelay
	}
	var payload struct {
		RetryAfterSeconds float64 `json:"retry_after_seconds"`
	}
	if err := json.Unmarshal(data, &payload); err != nil || payload.RetryAfterSeconds <= 0 {
		return w.rateLimitDelay
	}
	return time.Duration(payload.RetryAfterSeconds * float64(time.Second))
}

// CollectRecords filters record messages from a message batch, returning just
// the Record payloads. This is a zero-allocation filter when the capacity
// matches.
func CollectRecords(messages []protocol.Message) []protocol.Record {
	records := make([]protocol.Record, 0, len(messages))
	for i := range messages {
		if messages[i].Record != nil {
			records = append(records, *messages[i].Record)
		}
	}
	return records
}

// CollectStates filters state messages from a message batch.
func CollectStates(messages []protocol.Message) []protocol.State {
	states := make([]protocol.State, 0)
	for i := range messages {
		if messages[i].State != nil {
			states = append(states, *messages[i].State)
		}
	}
	return states
}
