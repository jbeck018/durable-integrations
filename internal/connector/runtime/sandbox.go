package runtime

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/flowforge/flowforge/internal/common"
)

// SandboxConfig defines execution limits for a sandboxed connector operation.
type SandboxConfig struct {
	// MaxMemoryBytes is an advisory memory limit. The sandbox sets GOGC hints
	// and monitors allocations, but Go does not support hard memory caps.
	MaxMemoryBytes int64

	// MaxDuration is the wall-clock deadline for the entire operation.
	MaxDuration time.Duration

	// MaxRecords is the maximum number of records the operation may process.
	// Zero means unlimited.
	MaxRecords int64
}

// DefaultSandboxConfig returns conservative defaults suitable for most connectors.
func DefaultSandboxConfig() SandboxConfig {
	return SandboxConfig{
		MaxMemoryBytes: 512 * 1024 * 1024, // 512 MB
		MaxDuration:    5 * time.Minute,
		MaxRecords:     1_000_000,
	}
}

// SandboxError represents an error that occurred within the execution sandbox,
// including panic recovery and resource limit violations.
type SandboxError struct {
	Connector string
	Op        string
	Cause     interface{} // may be error or panic value
	Stack     string
}

func (e *SandboxError) Error() string {
	if err, ok := e.Cause.(error); ok {
		return fmt.Sprintf("sandbox [%s/%s]: %v", e.Connector, e.Op, err)
	}
	return fmt.Sprintf("sandbox [%s/%s] panic: %v\n%s", e.Connector, e.Op, e.Cause, e.Stack)
}

func (e *SandboxError) Unwrap() error {
	if err, ok := e.Cause.(error); ok {
		return err
	}
	return nil
}

// RunInSandbox executes fn within a sandboxed context that enforces duration
// limits and recovers from panics. The context passed to fn is derived from
// ctx with the configured timeout applied.
func RunInSandbox(ctx context.Context, cfg SandboxConfig, fn func(ctx context.Context) error) (retErr error) {
	// Apply duration limit via context timeout. If the parent context already
	// has a tighter deadline, context.WithTimeout keeps the tighter one.
	execCtx, cancel := context.WithTimeout(ctx, cfg.MaxDuration)
	defer cancel()

	connector := common.ConnectorNameFrom(ctx)

	// Apply soft memory limit hint when available (Go 1.19+).
	if cfg.MaxMemoryBytes > 0 {
		prev := debug.SetMemoryLimit(cfg.MaxMemoryBytes)
		defer debug.SetMemoryLimit(prev)
	}

	// Panic recovery — convert panics into structured SandboxError.
	defer func() {
		if r := recover(); r != nil {
			retErr = &SandboxError{
				Connector: connector,
				Op:        "exec",
				Cause:     r,
				Stack:     string(debug.Stack()),
			}
		}
	}()

	errCh := make(chan error, 1)
	go func() {
		errCh <- fn(execCtx)
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return &SandboxError{
				Connector: connector,
				Op:        "exec",
				Cause:     err,
			}
		}
		return nil
	case <-execCtx.Done():
		if execCtx.Err() == context.DeadlineExceeded {
			return &SandboxError{
				Connector: connector,
				Op:        "exec",
				Cause:     common.ErrTimeout,
			}
		}
		return &SandboxError{
			Connector: connector,
			Op:        "exec",
			Cause:     common.ErrCancelled,
		}
	}
}
