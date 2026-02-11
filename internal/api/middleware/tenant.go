package middleware

import (
	"encoding/json"
	"net/http"

	"github.com/flowforge/flowforge/internal/common"
)

// TenantIsolation returns middleware that enforces tenant isolation.
// It ensures every request has a tenant ID set in context (by prior auth middleware)
// and rejects requests without one. This guarantees downstream handlers and DB queries
// always operate within a tenant scope.
func TenantIsolation() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenantID := common.TenantIDFrom(r.Context())
			if tenantID == "" {
				w.Header().Set("Content-Type", "application/json; charset=utf-8")
				w.WriteHeader(http.StatusForbidden)
				enc := json.NewEncoder(w)
				enc.SetEscapeHTML(false)
				_ = enc.Encode(map[string]interface{}{
					"error": map[string]interface{}{
						"code":    "TENANT_REQUIRED",
						"message": "tenant context is required for this operation",
					},
				})
				return
			}

			next.ServeHTTP(w, r)
		})
	}
}

// TenantHeader returns middleware that extracts a tenant ID from the
// X-Tenant-ID header, as an alternative to auth-based tenant resolution.
// This is used for internal service-to-service calls.
func TenantHeader() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			tenantID := r.Header.Get("X-Tenant-ID")
			if tenantID != "" {
				ctx := common.WithTenantID(r.Context(), tenantID)
				r = r.WithContext(ctx)
			}
			next.ServeHTTP(w, r)
		})
	}
}
