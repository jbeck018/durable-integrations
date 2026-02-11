// Package mcp provides the MCP (Model Context Protocol) tool execution worker
// for the FlowForge platform. It handles tool calls from AI agents with
// permission validation and per-tenant rate limiting.
package mcp

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// PermissionChecker validates whether an agent/tenant is allowed to execute a tool.
type PermissionChecker interface {
	IsAllowed(ctx context.Context, agentID, tenantID, tool string) (bool, error)
}

// ToolExecutor executes a tool call and returns the raw result content.
type ToolExecutor interface {
	Execute(ctx context.Context, tool string, parameters json.RawMessage) (json.RawMessage, error)
}

// RateLimitConfig configures per-agent/tenant rate limiting.
type RateLimitConfig struct {
	MaxCallsPerMinute int
	BurstSize         int
}

// rateLimitEntry tracks call timestamps for a single agent+tenant.
type rateLimitEntry struct {
	mu        sync.Mutex
	calls     []time.Time
	maxPerMin int
}

// MCPWorker processes MCP tool calls from AI agents. It validates permissions,
// enforces rate limits, executes the tool, and returns structured results.
type MCPWorker struct {
	permissions PermissionChecker
	executor    ToolExecutor
	rlConfig    RateLimitConfig

	mu         sync.RWMutex
	rateLimits map[string]*rateLimitEntry
}

// NewMCPWorker creates an MCPWorker with the given permission checker, tool
// executor, and rate limit configuration.
func NewMCPWorker(permissions PermissionChecker, executor ToolExecutor, rlConfig RateLimitConfig) *MCPWorker {
	if rlConfig.MaxCallsPerMinute <= 0 {
		rlConfig.MaxCallsPerMinute = 60
	}
	if rlConfig.BurstSize <= 0 {
		rlConfig.BurstSize = 10
	}
	return &MCPWorker{
		permissions: permissions,
		executor:    executor,
		rlConfig:    rlConfig,
		rateLimits:  make(map[string]*rateLimitEntry),
	}
}

// ProcessToolCall validates, rate-limits, and executes a tool call.
func (w *MCPWorker) ProcessToolCall(
	ctx context.Context,
	call protocol.MCPToolCall,
) (*protocol.MCPToolResult, error) {
	if call.ID == "" {
		return w.errorResult(call.ID, "missing tool call ID"), nil
	}
	if call.Tool == "" {
		return w.errorResult(call.ID, "missing tool name"), nil
	}

	// Populate context with tenant information if available.
	if call.TenantID != "" {
		ctx = common.WithTenantID(ctx, call.TenantID)
	}

	// Permission check.
	if w.permissions != nil {
		allowed, err := w.permissions.IsAllowed(ctx, call.AgentID, call.TenantID, call.Tool)
		if err != nil {
			return w.errorResult(call.ID, fmt.Sprintf("permission check failed: %v", err)), nil
		}
		if !allowed {
			return w.errorResult(call.ID, fmt.Sprintf("agent %s is not allowed to execute tool %s", call.AgentID, call.Tool)), nil
		}
	}

	// Rate limiting.
	if err := w.checkRateLimit(call.AgentID, call.TenantID); err != nil {
		return w.errorResult(call.ID, err.Error()), nil
	}

	// Execute the tool.
	if w.executor == nil {
		return w.errorResult(call.ID, "no tool executor configured"), nil
	}

	content, err := w.executor.Execute(ctx, call.Tool, call.Parameters)
	if err != nil {
		return w.errorResult(call.ID, fmt.Sprintf("tool execution failed: %v", err)), nil
	}

	return &protocol.MCPToolResult{
		CallID:  call.ID,
		Content: content,
		IsError: false,
	}, nil
}

// checkRateLimit verifies that the agent+tenant has not exceeded their call
// rate. It uses a sliding window of one minute.
func (w *MCPWorker) checkRateLimit(agentID, tenantID string) error {
	key := agentID + ":" + tenantID

	w.mu.RLock()
	entry, exists := w.rateLimits[key]
	w.mu.RUnlock()

	if !exists {
		w.mu.Lock()
		// Double-check after acquiring write lock.
		entry, exists = w.rateLimits[key]
		if !exists {
			entry = &rateLimitEntry{
				maxPerMin: w.rlConfig.MaxCallsPerMinute,
			}
			w.rateLimits[key] = entry
		}
		w.mu.Unlock()
	}

	return entry.tryAcquire(w.rlConfig.BurstSize)
}

// tryAcquire checks whether a new call is allowed under the sliding window
// rate limit. It prunes expired timestamps and checks the count.
func (e *rateLimitEntry) tryAcquire(burstSize int) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now()
	cutoff := now.Add(-1 * time.Minute)

	// Prune expired entries from the front of the sorted slice.
	pruneIdx := 0
	for pruneIdx < len(e.calls) && e.calls[pruneIdx].Before(cutoff) {
		pruneIdx++
	}
	if pruneIdx > 0 {
		e.calls = e.calls[pruneIdx:]
	}

	if len(e.calls) >= e.maxPerMin {
		return &common.RetryableError{
			Err:        common.ErrRateLimited,
			RetryAfter: 1,
		}
	}

	// Check burst: no more than burstSize calls in the last second.
	burstCutoff := now.Add(-1 * time.Second)
	burstCount := 0
	for i := len(e.calls) - 1; i >= 0 && e.calls[i].After(burstCutoff); i-- {
		burstCount++
	}
	if burstCount >= burstSize {
		return &common.RetryableError{
			Err:        common.ErrRateLimited,
			RetryAfter: 1,
		}
	}

	e.calls = append(e.calls, now)
	return nil
}

// errorResult creates an MCPToolResult flagged as an error.
func (w *MCPWorker) errorResult(callID string, msg string) *protocol.MCPToolResult {
	content, _ := json.Marshal(map[string]string{"error": msg})
	return &protocol.MCPToolResult{
		CallID:  callID,
		Content: content,
		IsError: true,
	}
}

// CleanupRateLimits removes stale rate limit entries for agents that have not
// made calls in the last period. Call this periodically to prevent unbounded
// memory growth.
func (w *MCPWorker) CleanupRateLimits(maxAge time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	cutoff := time.Now().Add(-maxAge)
	for key, entry := range w.rateLimits {
		entry.mu.Lock()
		if len(entry.calls) == 0 || entry.calls[len(entry.calls)-1].Before(cutoff) {
			delete(w.rateLimits, key)
		}
		entry.mu.Unlock()
	}
}

// AllowAllPermissions is a PermissionChecker that always allows tool execution.
// Useful for development and testing.
type AllowAllPermissions struct{}

// IsAllowed always returns true.
func (AllowAllPermissions) IsAllowed(_ context.Context, _, _, _ string) (bool, error) {
	return true, nil
}
