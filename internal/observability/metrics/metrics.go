// Package metrics provides Prometheus metrics collection for FlowForge.
// All metrics are registered under a configurable namespace and exposed
// via a standard /metrics HTTP endpoint for scraping.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Label names used across FlowForge metrics for dimensional partitioning.
const (
	LabelTenantID  = "tenant_id"
	LabelConnector = "connector"
	LabelSyncID    = "sync_id"
	LabelStatus    = "status"
	LabelMethod    = "method"
	LabelPath      = "path"
	LabelErrorType = "error_type"
)

// Metrics holds all Prometheus metric instruments for the FlowForge platform.
// A single instance is created at startup and shared across the application.
type Metrics struct {
	Registry *prometheus.Registry

	// Counters
	RecordsExtracted   *prometheus.CounterVec
	RecordsTransformed *prometheus.CounterVec
	RecordsLoaded      *prometheus.CounterVec
	SyncRunsTotal      *prometheus.CounterVec
	APIRequestsTotal   *prometheus.CounterVec
	MCPToolCallsTotal  *prometheus.CounterVec
	ErrorsTotal        *prometheus.CounterVec

	// Histograms
	SyncDuration               *prometheus.HistogramVec
	APIRequestDuration         *prometheus.HistogramVec
	ConnectorOperationDuration *prometheus.HistogramVec
	BatchSize                  *prometheus.HistogramVec

	// Gauges
	ActiveSyncs       *prometheus.GaugeVec
	ActiveConnections *prometheus.GaugeVec
	WorkerPoolSize    *prometheus.GaugeVec
	QueueDepth        *prometheus.GaugeVec
}

// NewMetrics creates a fully-registered Metrics instance. The namespace prefixes
// all metric names (e.g. "flowforge_records_extracted_total") for easy filtering
// in Grafana dashboards. A dedicated registry is used so that default Go runtime
// metrics do not pollute the FlowForge namespace.
func NewMetrics(namespace string) *Metrics {
	reg := prometheus.NewRegistry()
	// Include the default process and Go collectors for operational insight.
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	reg.MustRegister(collectors.NewGoCollector())

	m := &Metrics{
		Registry: reg,

		// --- Counters ---

		RecordsExtracted: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "records_extracted_total",
			Help:      "Total number of records extracted from source connectors.",
		}, []string{LabelTenantID, LabelConnector, LabelSyncID}),

		RecordsTransformed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "records_transformed_total",
			Help:      "Total number of records transformed during sync pipelines.",
		}, []string{LabelTenantID, LabelConnector, LabelSyncID}),

		RecordsLoaded: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "records_loaded_total",
			Help:      "Total number of records loaded into destination connectors.",
		}, []string{LabelTenantID, LabelConnector, LabelSyncID}),

		SyncRunsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "sync_runs_total",
			Help:      "Total number of sync runs executed.",
		}, []string{LabelTenantID, LabelSyncID, LabelStatus}),

		APIRequestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "api_requests_total",
			Help:      "Total number of REST API requests received.",
		}, []string{LabelMethod, LabelPath, LabelStatus}),

		MCPToolCallsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "mcp_tool_calls_total",
			Help:      "Total number of MCP tool calls executed by AI agents.",
		}, []string{LabelTenantID, LabelConnector, LabelStatus}),

		ErrorsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "errors_total",
			Help:      "Total number of errors across all subsystems.",
		}, []string{LabelTenantID, LabelConnector, LabelErrorType}),

		// --- Histograms ---

		SyncDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "sync_duration_seconds",
			Help:      "Duration of complete sync runs in seconds.",
			Buckets:   []float64{1, 5, 15, 30, 60, 120, 300, 600, 1800, 3600},
		}, []string{LabelTenantID, LabelSyncID, LabelStatus}),

		APIRequestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "api_request_duration_seconds",
			Help:      "Duration of REST API request handling in seconds.",
			Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		}, []string{LabelMethod, LabelPath}),

		ConnectorOperationDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "connector_operation_duration_seconds",
			Help:      "Duration of individual connector operations (check, discover, read, write) in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 15, 30, 60, 300},
		}, []string{LabelTenantID, LabelConnector, LabelStatus}),

		BatchSize: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "batch_size",
			Help:      "Size of record batches processed during extraction and loading.",
			Buckets:   []float64{1, 10, 50, 100, 250, 500, 1000, 2500, 5000, 10000},
		}, []string{LabelTenantID, LabelConnector, LabelSyncID}),

		// --- Gauges ---

		ActiveSyncs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_syncs",
			Help:      "Number of currently executing sync workflows.",
		}, []string{LabelTenantID}),

		ActiveConnections: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "active_connections",
			Help:      "Number of active connector connections.",
		}, []string{LabelTenantID, LabelConnector}),

		WorkerPoolSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "worker_pool_size",
			Help:      "Current number of worker goroutines in the processing pool.",
		}, []string{LabelTenantID}),

		QueueDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace,
			Name:      "queue_depth",
			Help:      "Number of items waiting in the processing queue.",
		}, []string{LabelTenantID}),
	}

	// Register all collectors with the dedicated registry.
	reg.MustRegister(
		m.RecordsExtracted,
		m.RecordsTransformed,
		m.RecordsLoaded,
		m.SyncRunsTotal,
		m.APIRequestsTotal,
		m.MCPToolCallsTotal,
		m.ErrorsTotal,
		m.SyncDuration,
		m.APIRequestDuration,
		m.ConnectorOperationDuration,
		m.BatchSize,
		m.ActiveSyncs,
		m.ActiveConnections,
		m.WorkerPoolSize,
		m.QueueDepth,
	)

	return m
}

// MetricsHandler returns an http.Handler that serves the /metrics endpoint
// for Prometheus scraping. It uses the dedicated registry so only FlowForge
// metrics (plus process/go collectors) are exposed.
func (m *Metrics) MetricsHandler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{
		EnableOpenMetrics: true,
	})
}

// RecordSyncRun is a convenience method that increments the sync runs counter
// and observes the sync duration in a single call.
func (m *Metrics) RecordSyncRun(tenantID, syncID, status string, durationSecs float64) {
	m.SyncRunsTotal.WithLabelValues(tenantID, syncID, status).Inc()
	m.SyncDuration.WithLabelValues(tenantID, syncID, status).Observe(durationSecs)
}

// RecordAPIRequest is a convenience method that increments the API request counter
// and observes the request duration in a single call.
func (m *Metrics) RecordAPIRequest(method, path, status string, durationSecs float64) {
	m.APIRequestsTotal.WithLabelValues(method, path, status).Inc()
	m.APIRequestDuration.WithLabelValues(method, path).Observe(durationSecs)
}

// RecordError increments the errors_total counter with the given labels.
func (m *Metrics) RecordError(tenantID, connector, errorType string) {
	m.ErrorsTotal.WithLabelValues(tenantID, connector, errorType).Inc()
}

// RecordBatch observes a batch size and increments the appropriate record counters.
func (m *Metrics) RecordBatch(tenantID, connector, syncID string, extracted, transformed, loaded int64) {
	if extracted > 0 {
		m.RecordsExtracted.WithLabelValues(tenantID, connector, syncID).Add(float64(extracted))
	}
	if transformed > 0 {
		m.RecordsTransformed.WithLabelValues(tenantID, connector, syncID).Add(float64(transformed))
	}
	if loaded > 0 {
		m.RecordsLoaded.WithLabelValues(tenantID, connector, syncID).Add(float64(loaded))
		m.BatchSize.WithLabelValues(tenantID, connector, syncID).Observe(float64(loaded))
	}
}
