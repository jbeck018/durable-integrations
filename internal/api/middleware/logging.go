package middleware

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/google/uuid"
)

// responseRecorder wraps http.ResponseWriter to capture the status code and bytes written.
type responseRecorder struct {
	http.ResponseWriter
	statusCode   int
	bytesWritten int
	wroteHeader  bool
}

// WriteHeader captures the status code.
func (rr *responseRecorder) WriteHeader(code int) {
	if !rr.wroteHeader {
		rr.statusCode = code
		rr.wroteHeader = true
	}
	rr.ResponseWriter.WriteHeader(code)
}

// Write captures the number of bytes written.
func (rr *responseRecorder) Write(b []byte) (int, error) {
	if !rr.wroteHeader {
		rr.WriteHeader(http.StatusOK)
	}
	n, err := rr.ResponseWriter.Write(b)
	rr.bytesWritten += n
	return n, err
}

// Unwrap exposes the underlying ResponseWriter for middleware that needs it.
func (rr *responseRecorder) Unwrap() http.ResponseWriter {
	return rr.ResponseWriter
}

// RequestLogger returns middleware that logs every HTTP request with structured
// fields including method, path, status, duration, and correlation ID.
func RequestLogger(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Generate or extract correlation ID.
			correlationID := r.Header.Get("X-Correlation-ID")
			if correlationID == "" {
				correlationID = uuid.New().String()
			}

			ctx := common.WithCorrelationID(r.Context(), correlationID)
			r = r.WithContext(ctx)

			// Set correlation ID on response.
			w.Header().Set("X-Correlation-ID", correlationID)

			rec := &responseRecorder{
				ResponseWriter: w,
				statusCode:     http.StatusOK,
			}

			next.ServeHTTP(rec, r)

			duration := time.Since(start)

			tenantID := common.TenantIDFrom(r.Context())
			userID := common.UserIDFrom(r.Context())

			attrs := []slog.Attr{
				slog.String("method", r.Method),
				slog.String("path", r.URL.Path),
				slog.Int("status", rec.statusCode),
				slog.Duration("duration", duration),
				slog.Int("bytes", rec.bytesWritten),
				slog.String("correlation_id", correlationID),
				slog.String("remote_addr", realIP(r)),
			}

			if tenantID != "" {
				attrs = append(attrs, slog.String("tenant_id", tenantID))
			}
			if userID != "" {
				attrs = append(attrs, slog.String("user_id", userID))
			}
			if r.URL.RawQuery != "" {
				attrs = append(attrs, slog.String("query", r.URL.RawQuery))
			}

			// Convert []slog.Attr to []any for LogAttrs.
			level := slog.LevelInfo
			if rec.statusCode >= 500 {
				level = slog.LevelError
			} else if rec.statusCode >= 400 {
				level = slog.LevelWarn
			}

			logger.LogAttrs(r.Context(), level, "http request", attrs...)
		})
	}
}
