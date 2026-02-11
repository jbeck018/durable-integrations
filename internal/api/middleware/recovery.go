package middleware

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime"

	"github.com/flowforge/flowforge/internal/common"
)

// Recovery returns middleware that catches panics, logs the stack trace,
// and returns a 500 Internal Server Error JSON response.
func Recovery(logger *slog.Logger) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if rec := recover(); rec != nil {
					// Capture stack trace.
					buf := make([]byte, 4096)
					n := runtime.Stack(buf, false)
					stackTrace := string(buf[:n])

					correlationID := common.CorrelationIDFrom(r.Context())
					tenantID := common.TenantIDFrom(r.Context())

					logger.Error("panic recovered",
						slog.Any("panic", rec),
						slog.String("stack", stackTrace),
						slog.String("method", r.Method),
						slog.String("path", r.URL.Path),
						slog.String("correlation_id", correlationID),
						slog.String("tenant_id", tenantID),
					)

					w.Header().Set("Content-Type", "application/json; charset=utf-8")
					w.WriteHeader(http.StatusInternalServerError)
					enc := json.NewEncoder(w)
					enc.SetEscapeHTML(false)
					_ = enc.Encode(map[string]interface{}{
						"error": map[string]interface{}{
							"code":    "INTERNAL_ERROR",
							"message": "an unexpected error occurred",
						},
					})
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}
