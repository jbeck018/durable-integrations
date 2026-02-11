// Package logging provides structured logging for FlowForge built on log/slog.
// It supports JSON and text formats, automatic context enrichment with tenant
// and correlation IDs, and a global logger for use throughout the application.
package logging

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"

	"github.com/flowforge/flowforge/internal/common"
)

// globalMu protects the global logger singleton.
var globalMu sync.RWMutex

// globalLogger is the application-wide logger instance.
var globalLogger *Logger

// Logger wraps slog.Logger with FlowForge-specific context enrichment
// and convenience methods. It is safe for concurrent use.
type Logger struct {
	inner *slog.Logger
}

// NewLogger creates a new Logger with the specified level and format.
// The level parameter accepts "debug", "info", "warn", or "error" (case-insensitive).
// The format parameter accepts "json" or "text" (case-insensitive); any other
// value defaults to "json".
func NewLogger(level, format string) *Logger {
	lvl := parseLevel(level)

	opts := &slog.HandlerOptions{
		Level:     lvl,
		AddSource: lvl == slog.LevelDebug,
	}

	var handler slog.Handler
	var writer io.Writer = os.Stdout
	switch strings.ToLower(format) {
	case "text":
		handler = slog.NewTextHandler(writer, opts)
	default:
		handler = slog.NewJSONHandler(writer, opts)
	}

	return &Logger{inner: slog.New(handler)}
}

// parseLevel converts a string level name to a slog.Level.
func parseLevel(level string) slog.Level {
	switch strings.ToLower(level) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// SetGlobal installs this logger as the global application logger and
// also sets it as the default slog logger.
func (l *Logger) SetGlobal() {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalLogger = l
	slog.SetDefault(l.inner)
}

// Global returns the global logger. If no global logger has been set,
// it returns a default JSON info-level logger.
func Global() *Logger {
	globalMu.RLock()
	defer globalMu.RUnlock()
	if globalLogger == nil {
		return NewLogger("info", "json")
	}
	return globalLogger
}

// WithContext returns a new Logger enriched with tenant_id, correlation_id,
// and connector_name from the context. If those values are absent the
// original logger is returned unchanged.
func (l *Logger) WithContext(ctx context.Context) *Logger {
	if ctx == nil {
		return l
	}

	attrs := make([]any, 0, 6)

	if tenantID := common.TenantIDFrom(ctx); tenantID != "" {
		attrs = append(attrs, slog.String("tenant_id", tenantID))
	}
	if corrID := common.CorrelationIDFrom(ctx); corrID != "" {
		attrs = append(attrs, slog.String("correlation_id", corrID))
	}
	if connector := common.ConnectorNameFrom(ctx); connector != "" {
		attrs = append(attrs, slog.String("connector", connector))
	}

	if len(attrs) == 0 {
		return l
	}

	return &Logger{inner: l.inner.With(attrs...)}
}

// WithField returns a new Logger with an additional key-value pair.
func (l *Logger) WithField(key string, value any) *Logger {
	return &Logger{inner: l.inner.With(slog.Any(key, value))}
}

// WithFields returns a new Logger with multiple additional key-value pairs.
// The args are interpreted as alternating key-value pairs, just like slog.
func (l *Logger) WithFields(args ...any) *Logger {
	return &Logger{inner: l.inner.With(args...)}
}

// Debug logs a message at DEBUG level.
func (l *Logger) Debug(msg string, args ...any) {
	l.inner.Debug(msg, args...)
}

// Info logs a message at INFO level.
func (l *Logger) Info(msg string, args ...any) {
	l.inner.Info(msg, args...)
}

// Warn logs a message at WARN level.
func (l *Logger) Warn(msg string, args ...any) {
	l.inner.Warn(msg, args...)
}

// Error logs a message at ERROR level.
func (l *Logger) Error(msg string, args ...any) {
	l.inner.Error(msg, args...)
}

// DebugContext logs a message at DEBUG level with context enrichment.
func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.inner.DebugContext(ctx, msg, args...)
}

// InfoContext logs a message at INFO level with context enrichment.
func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.inner.InfoContext(ctx, msg, args...)
}

// WarnContext logs a message at WARN level with context enrichment.
func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.inner.WarnContext(ctx, msg, args...)
}

// ErrorContext logs a message at ERROR level with context enrichment.
func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.inner.ErrorContext(ctx, msg, args...)
}

// Slog returns the underlying *slog.Logger for interop with libraries
// that accept slog directly.
func (l *Logger) Slog() *slog.Logger {
	return l.inner
}
