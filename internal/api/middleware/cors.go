package middleware

import (
	"net/http"

	"github.com/go-chi/cors"
)

// CORSConfig returns a configurable CORS middleware using go-chi/cors.
// It permits common integration patterns including custom headers for
// auth, tenant identification, and correlation tracking.
func CORSConfig() func(http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins: []string{"*"},
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
			http.MethodOptions,
		},
		AllowedHeaders: []string{
			"Accept",
			"Authorization",
			"Content-Type",
			"X-Tenant-ID",
			"X-Correlation-ID",
			"X-Request-ID",
		},
		ExposedHeaders: []string{
			"X-Correlation-ID",
			"X-RateLimit-Limit",
			"X-RateLimit-Remaining",
			"Retry-After",
			"Location",
		},
		AllowCredentials: true,
		MaxAge:           300,
	})
}

// CORSConfigWithOrigins returns CORS middleware restricted to specific allowed origins.
func CORSConfigWithOrigins(origins []string) func(http.Handler) http.Handler {
	return cors.Handler(cors.Options{
		AllowedOrigins: origins,
		AllowedMethods: []string{
			http.MethodGet,
			http.MethodPost,
			http.MethodPut,
			http.MethodPatch,
			http.MethodDelete,
			http.MethodOptions,
		},
		AllowedHeaders: []string{
			"Accept",
			"Authorization",
			"Content-Type",
			"X-Tenant-ID",
			"X-Correlation-ID",
			"X-Request-ID",
		},
		ExposedHeaders: []string{
			"X-Correlation-ID",
			"X-RateLimit-Limit",
			"X-RateLimit-Remaining",
			"Retry-After",
			"Location",
		},
		AllowCredentials: true,
		MaxAge:           300,
	})
}
