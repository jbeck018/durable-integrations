// Package common provides shared utilities used across all FlowForge internal packages.
// Centralizing error types, config loading, and context utilities here keeps the
// codebase DRY — every subsystem imports from this single package.
package common

import (
	"errors"
	"fmt"
)

// Sentinel errors for common failure modes across FlowForge.
var (
	ErrNotFound          = errors.New("not found")
	ErrAlreadyExists     = errors.New("already exists")
	ErrUnauthorized      = errors.New("unauthorized")
	ErrForbidden         = errors.New("forbidden")
	ErrRateLimited       = errors.New("rate limited")
	ErrInvalidConfig     = errors.New("invalid configuration")
	ErrConnectionFailed  = errors.New("connection failed")
	ErrSchemaValidation  = errors.New("schema validation failed")
	ErrTimeout           = errors.New("operation timed out")
	ErrCancelled         = errors.New("operation cancelled")
)

// ConnectorError wraps errors originating from a connector with the connector name.
type ConnectorError struct {
	Connector string
	Op        string
	Err       error
}

func (e *ConnectorError) Error() string {
	return fmt.Sprintf("connector %s: %s: %v", e.Connector, e.Op, e.Err)
}

func (e *ConnectorError) Unwrap() error { return e.Err }

// NewConnectorError creates a ConnectorError.
func NewConnectorError(connector, op string, err error) *ConnectorError {
	return &ConnectorError{Connector: connector, Op: op, Err: err}
}

// RetryableError wraps an error to signal that the operation can be retried.
type RetryableError struct {
	Err        error
	RetryAfter int // seconds; 0 means use default backoff
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable: %v", e.Err)
}

func (e *RetryableError) Unwrap() error { return e.Err }

// IsRetryable checks if an error (or any in its chain) is retryable.
func IsRetryable(err error) bool {
	var re *RetryableError
	return errors.As(err, &re)
}

// ValidationError holds one or more validation failures.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error on %s: %s", e.Field, e.Message)
}

// ValidationErrors aggregates multiple validation failures.
type ValidationErrors []ValidationError

func (ve ValidationErrors) Error() string {
	if len(ve) == 1 {
		return ve[0].Error()
	}
	return fmt.Sprintf("%d validation errors (first: %s)", len(ve), ve[0].Error())
}

// TenantError wraps errors with tenant context for multi-tenant isolation.
type TenantError struct {
	TenantID string
	Err      error
}

func (e *TenantError) Error() string {
	return fmt.Sprintf("tenant %s: %v", e.TenantID, e.Err)
}

func (e *TenantError) Unwrap() error { return e.Err }
