package security

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flowforge/flowforge/internal/mcp/tools"
)

// Default sandbox resource limits.
const (
	DefaultMaxDuration   = 30 * time.Second
	DefaultMaxOutputSize = 1 << 20 // 1 MB
	DefaultMaxMemoryMB   = 256
)

// SandboxConfig configures resource limits for tool execution.
type SandboxConfig struct {
	MaxDuration   time.Duration
	MaxOutputSize int
	MaxMemoryMB   int
}

// AuditEntry records a single tool invocation for audit purposes.
type AuditEntry struct {
	Timestamp  time.Time       `json:"timestamp"`
	AgentID    string          `json:"agent_id"`
	TenantID   string          `json:"tenant_id"`
	ToolName   string          `json:"tool_name"`
	Params     json.RawMessage `json:"params,omitempty"`
	DurationMS int64           `json:"duration_ms"`
	OutputSize int             `json:"output_size"`
	Success    bool            `json:"success"`
	Error      string          `json:"error,omitempty"`
}

// AuditLogger is the function type for audit log consumers.
type AuditLogger func(entry AuditEntry)

// Sandbox wraps tool execution with resource limits, panic recovery,
// and audit logging.
type Sandbox struct {
	config      SandboxConfig
	auditLogger AuditLogger
	mu          sync.RWMutex
	auditLog    []AuditEntry
	execCount   atomic.Int64
}

// NewSandbox creates a sandbox with the provided configuration.
// If config fields are zero, defaults are used.
func NewSandbox(config SandboxConfig) *Sandbox {
	if config.MaxDuration == 0 {
		config.MaxDuration = DefaultMaxDuration
	}
	if config.MaxOutputSize == 0 {
		config.MaxOutputSize = DefaultMaxOutputSize
	}
	if config.MaxMemoryMB == 0 {
		config.MaxMemoryMB = DefaultMaxMemoryMB
	}
	return &Sandbox{
		config:   config,
		auditLog: make([]AuditEntry, 0, 1024),
	}
}

// SetAuditLogger sets an external audit logger that receives every audit entry.
func (s *Sandbox) SetAuditLogger(logger AuditLogger) {
	s.mu.Lock()
	s.auditLogger = logger
	s.mu.Unlock()
}

// Execute runs a tool handler within the sandbox constraints.
// It enforces execution timeout, output size limits, panic recovery,
// and records an audit entry for every invocation.
func (s *Sandbox) Execute(ctx context.Context, tool tools.ToolDefinition, params json.RawMessage, agentID, tenantID string) (result json.RawMessage, err error) {
	s.execCount.Add(1)
	start := time.Now()

	entry := AuditEntry{
		Timestamp: start,
		AgentID:   agentID,
		TenantID:  tenantID,
		ToolName:  tool.Name,
		Params:    params,
	}

	defer func() {
		entry.DurationMS = time.Since(start).Milliseconds()
		if result != nil {
			entry.OutputSize = len(result)
		}
		if err != nil {
			entry.Error = err.Error()
			entry.Success = false
		} else {
			entry.Success = true
		}
		s.recordAudit(entry)
	}()

	// Check memory before execution
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	currentMB := int(memStats.Alloc / (1024 * 1024))
	if currentMB > s.config.MaxMemoryMB {
		err = fmt.Errorf("memory limit exceeded: current %dMB > max %dMB", currentMB, s.config.MaxMemoryMB)
		return nil, err
	}

	// Create a timeout context for the tool execution
	execCtx, cancel := context.WithTimeout(ctx, s.config.MaxDuration)
	defer cancel()

	// Execute in a goroutine with panic recovery
	type execResult struct {
		data json.RawMessage
		err  error
	}
	resultCh := make(chan execResult, 1)

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Capture stack trace for logging
				buf := make([]byte, 4096)
				n := runtime.Stack(buf, false)
				stack := string(buf[:n])
				log.Printf("PANIC in tool %s: %v\n%s", tool.Name, r, stack)
				resultCh <- execResult{
					err: fmt.Errorf("tool %s panicked: %v", tool.Name, r),
				}
			}
		}()

		data, execErr := tool.Handler(execCtx, params)
		resultCh <- execResult{data: data, err: execErr}
	}()

	select {
	case <-execCtx.Done():
		if ctx.Err() != nil {
			err = ctx.Err()
		} else {
			err = fmt.Errorf("tool %s execution timed out after %s", tool.Name, s.config.MaxDuration)
		}
		return nil, err
	case res := <-resultCh:
		if res.err != nil {
			return nil, res.err
		}
		// Enforce output size limit
		if len(res.data) > s.config.MaxOutputSize {
			err = fmt.Errorf("tool %s output size %d exceeds limit %d", tool.Name, len(res.data), s.config.MaxOutputSize)
			return nil, err
		}
		return res.data, nil
	}
}

// recordAudit appends an audit entry to the internal log and dispatches
// to the external logger if configured.
func (s *Sandbox) recordAudit(entry AuditEntry) {
	s.mu.Lock()
	s.auditLog = append(s.auditLog, entry)
	logger := s.auditLogger
	s.mu.Unlock()

	if logger != nil {
		logger(entry)
	}
}

// AuditLog returns a copy of all audit entries.
func (s *Sandbox) AuditLog() []AuditEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	entries := make([]AuditEntry, len(s.auditLog))
	copy(entries, s.auditLog)
	return entries
}

// RecentAuditLog returns the most recent n audit entries.
func (s *Sandbox) RecentAuditLog(n int) []AuditEntry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if n > len(s.auditLog) {
		n = len(s.auditLog)
	}
	start := len(s.auditLog) - n
	entries := make([]AuditEntry, n)
	copy(entries, s.auditLog[start:])
	return entries
}

// ExecutionCount returns the total number of tool executions.
func (s *Sandbox) ExecutionCount() int64 {
	return s.execCount.Load()
}
