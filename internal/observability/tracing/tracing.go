// Package tracing provides OpenTelemetry distributed tracing for FlowForge.
// It configures a TracerProvider with OTLP HTTP export, sampling, and
// resource attributes. Middleware and helpers simplify instrumentation of
// HTTP handlers and internal operations.
package tracing

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/flowforge/flowforge/internal/common"
)

// Common attribute keys for FlowForge spans.
var (
	AttrTenantID  = attribute.Key("flowforge.tenant_id")
	AttrConnector = attribute.Key("flowforge.connector")
	AttrSyncID    = attribute.Key("flowforge.sync_id")
	AttrPhase     = attribute.Key("flowforge.phase")
)

// NewTracerProvider creates and configures an OpenTelemetry TracerProvider
// that exports spans via OTLP/HTTP to the given endpoint. It sets up
// parent-based sampling (always sample when a parent exists, sample 10%
// of root spans) for production-friendly trace volumes.
//
// The caller is responsible for calling Shutdown on the returned provider
// during graceful shutdown to flush pending spans.
func NewTracerProvider(serviceName, endpoint string) (*sdktrace.TracerProvider, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	exporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(endpoint),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create OTLP HTTP exporter: %w", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion("0.1.0"),
		),
		resource.WithHost(),
		resource.WithProcess(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	// Parent-based sampling: inherit the parent's decision when present;
	// otherwise sample 10% of root spans to control trace volume.
	sampler := sdktrace.ParentBased(
		sdktrace.TraceIDRatioBased(0.1),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithMaxQueueSize(2048),
			sdktrace.WithMaxExportBatchSize(512),
			sdktrace.WithBatchTimeout(5*time.Second),
		),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)

	// Set the global TracerProvider and propagator so all instrumentation
	// libraries automatically use our configuration.
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return tp, nil
}

// SpanFromContext returns the current span from the context. This is a
// convenience wrapper around trace.SpanFromContext.
func SpanFromContext(ctx context.Context) trace.Span {
	return trace.SpanFromContext(ctx)
}

// AddSpanAttributes enriches the current span with standard FlowForge
// attributes extracted from the context. This should be called early in
// any operation that has tenant, connector, or sync context.
func AddSpanAttributes(ctx context.Context, syncID string) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	attrs := make([]attribute.KeyValue, 0, 4)

	if tenantID := common.TenantIDFrom(ctx); tenantID != "" {
		attrs = append(attrs, AttrTenantID.String(tenantID))
	}
	if connector := common.ConnectorNameFrom(ctx); connector != "" {
		attrs = append(attrs, AttrConnector.String(connector))
	}
	if syncID != "" {
		attrs = append(attrs, AttrSyncID.String(syncID))
	}

	if len(attrs) > 0 {
		span.SetAttributes(attrs...)
	}
}

// Tracer returns a named tracer from the global TracerProvider.
func Tracer(name string) trace.Tracer {
	return otel.Tracer(name)
}

// StartSpan is a convenience function that starts a new span using the global
// tracer and returns the modified context and span.
func StartSpan(ctx context.Context, tracerName, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, spanName, opts...)
}

// responseWriter wraps http.ResponseWriter to capture the status code.
type responseWriter struct {
	http.ResponseWriter
	statusCode int
	written    bool
}

func newResponseWriter(w http.ResponseWriter) *responseWriter {
	return &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
}

func (rw *responseWriter) WriteHeader(code int) {
	if !rw.written {
		rw.statusCode = code
		rw.written = true
	}
	rw.ResponseWriter.WriteHeader(code)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	if !rw.written {
		rw.written = true
	}
	return rw.ResponseWriter.Write(b)
}

// TracingMiddleware returns HTTP middleware that creates a span for each
// incoming request, propagates trace context from incoming headers, and
// records standard HTTP attributes on the span.
func TracingMiddleware(serviceName string) func(http.Handler) http.Handler {
	tracer := otel.Tracer(serviceName)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Extract trace context from incoming request headers.
			ctx := otel.GetTextMapPropagator().Extract(r.Context(), propagation.HeaderCarrier(r.Header))

			spanName := r.Method + " " + r.URL.Path
			ctx, span := tracer.Start(ctx, spanName,
				trace.WithSpanKind(trace.SpanKindServer),
				trace.WithAttributes(
					semconv.HTTPRequestMethodKey.String(r.Method),
					semconv.URLPath(r.URL.Path),
					semconv.ServerAddress(r.Host),
					semconv.UserAgentOriginal(r.UserAgent()),
				),
			)
			defer span.End()

			// Add FlowForge-specific context attributes.
			if tenantID := common.TenantIDFrom(ctx); tenantID != "" {
				span.SetAttributes(AttrTenantID.String(tenantID))
			}
			if correlationID := common.CorrelationIDFrom(ctx); correlationID != "" {
				span.SetAttributes(attribute.String("flowforge.correlation_id", correlationID))
			}

			// Wrap the response writer to capture the status code.
			rw := newResponseWriter(w)

			// Inject the trace context into the response headers for
			// downstream correlation.
			otel.GetTextMapPropagator().Inject(ctx, propagation.HeaderCarrier(rw.Header()))

			next.ServeHTTP(rw, r.WithContext(ctx))

			// Record the response status on the span.
			span.SetAttributes(semconv.HTTPResponseStatusCode(rw.statusCode))
			if rw.statusCode >= 400 {
				span.SetAttributes(attribute.Bool("error", true))
			}
		})
	}
}
