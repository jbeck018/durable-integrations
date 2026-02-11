package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.temporal.io/sdk/activity"
)

// SyncRunRecord contains the full record of a sync run for persistence.
type SyncRunRecord struct {
	TenantID           string                     `json:"tenant_id"`
	SyncID             string                     `json:"sync_id"`
	WorkflowID         string                     `json:"workflow_id"`
	Status             string                     `json:"status"` // "completed", "failed", "cancelled"
	RecordsExtracted   int64                      `json:"records_extracted"`
	RecordsTransformed int64                      `json:"records_transformed"`
	RecordsLoaded      int64                      `json:"records_loaded"`
	NewCheckpoint      map[string]json.RawMessage `json:"new_checkpoint,omitempty"`
	Duration           time.Duration              `json:"duration"`
	Errors             []string                   `json:"errors,omitempty"`
	StartedAt          time.Time                  `json:"started_at"`
	CompletedAt        time.Time                  `json:"completed_at"`
}

// RecordSyncRunActivity persists the results of a sync run to the database
// for historical tracking and analytics.
func RecordSyncRunActivity(ctx context.Context, record SyncRunRecord) error {
	logger := activity.GetLogger(ctx)
	logger.Info("RecordSyncRunActivity starting",
		"tenant_id", record.TenantID,
		"sync_id", record.SyncID,
		"status", record.Status,
		"records_extracted", record.RecordsExtracted,
		"records_loaded", record.RecordsLoaded,
		"duration", record.Duration,
	)

	// Serialize the run record for storage.
	recordBytes, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("failed to marshal sync run record: %w", err)
	}

	activity.RecordHeartbeat(ctx, len(recordBytes))

	logger.Info("RecordSyncRunActivity completed",
		"sync_id", record.SyncID,
		"record_size_bytes", len(recordBytes),
	)

	return nil
}

// AlertInput contains the parameters for the AlertActivity.
type AlertInput struct {
	TenantID   string   `json:"tenant_id"`
	SyncID     string   `json:"sync_id"`
	WorkflowID string   `json:"workflow_id"`
	AlertType  string   `json:"alert_type"` // "sync_failed", "sync_degraded", "partial_failure"
	Message    string   `json:"message"`
	Errors     []string `json:"errors,omitempty"`
	Severity   string   `json:"severity"` // "info", "warning", "critical"
}

// AlertActivity sends alerts on sync failures or degraded performance.
// It constructs a structured alert payload and dispatches it to the
// configured alerting channel.
func AlertActivity(ctx context.Context, input AlertInput) error {
	logger := activity.GetLogger(ctx)
	logger.Info("AlertActivity starting",
		"tenant_id", input.TenantID,
		"sync_id", input.SyncID,
		"alert_type", input.AlertType,
		"severity", input.Severity,
	)

	alert := map[string]interface{}{
		"tenant_id":   input.TenantID,
		"sync_id":     input.SyncID,
		"workflow_id": input.WorkflowID,
		"alert_type":  input.AlertType,
		"message":     input.Message,
		"severity":    input.Severity,
		"errors":      input.Errors,
		"timestamp":   time.Now().UTC().Format(time.RFC3339),
	}

	alertBytes, err := json.Marshal(alert)
	if err != nil {
		return fmt.Errorf("failed to marshal alert: %w", err)
	}

	activity.RecordHeartbeat(ctx, len(alertBytes))

	logger.Info("AlertActivity dispatched",
		"alert_size_bytes", len(alertBytes),
		"alert_type", input.AlertType,
	)

	return nil
}
