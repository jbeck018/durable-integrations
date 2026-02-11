package activities

import (
	"context"
	"encoding/json"
	"fmt"

	"go.temporal.io/sdk/activity"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// WriteBatchInput contains the parameters for WriteBatchActivity.
type WriteBatchInput struct {
	DestConnectorID string                      `json:"dest_connector_id"`
	DestConfig      json.RawMessage             `json:"dest_config"`
	Records         []protocol.Record           `json:"records"`
	Streams         []protocol.ConfiguredStream `json:"streams"`
	BatchIndex      int                         `json:"batch_index"`
}

// WriteBatchOutput holds the results of a batch write operation.
type WriteBatchOutput struct {
	RecordsWritten int64    `json:"records_written"`
	Errors         []string `json:"errors,omitempty"`
	BatchIndex     int      `json:"batch_index"`
}

// WriteBatchActivity writes a batch of records to the destination connector.
// Records are sent through the connector's Write interface as protocol messages.
func WriteBatchActivity(ctx context.Context, input WriteBatchInput) (*WriteBatchOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("WriteBatchActivity starting",
		"connector", input.DestConnectorID,
		"record_count", len(input.Records),
		"batch_index", input.BatchIndex,
	)

	dest, err := cdk.GetDestination(input.DestConnectorID)
	if err != nil {
		return nil, common.NewConnectorError(input.DestConnectorID, "write_batch", err)
	}

	catalog := &protocol.ConfiguredCatalog{Streams: input.Streams}

	// Feed records into the destination connector's input channel.
	inputCh := make(chan protocol.Message, len(input.Records))
	go func() {
		defer close(inputCh)
		for i, rec := range input.Records {
			inputCh <- protocol.Message{
				Type:   protocol.MessageTypeRecord,
				Record: &rec,
			}
			if i%100 == 0 {
				activity.RecordHeartbeat(ctx, i)
			}
		}
	}()

	writeResult, err := dest.Write(ctx, input.DestConfig, catalog, inputCh)
	if err != nil {
		return nil, common.NewConnectorError(input.DestConnectorID, "write", err)
	}

	var writeErrors []string
	for _, we := range writeResult.Errors {
		writeErrors = append(writeErrors, we.Message)
	}

	logger.Info("WriteBatchActivity completed",
		"records_written", writeResult.RecordsWritten,
		"error_count", len(writeErrors),
		"batch_index", input.BatchIndex,
	)

	return &WriteBatchOutput{
		RecordsWritten: writeResult.RecordsWritten,
		Errors:         writeErrors,
		BatchIndex:     input.BatchIndex,
	}, nil
}

// ConfirmWriteInput contains the parameters for ConfirmWriteActivity.
type ConfirmWriteInput struct {
	DestConnectorID string          `json:"dest_connector_id"`
	DestConfig      json.RawMessage `json:"dest_config"`
	ExpectedCount   int64           `json:"expected_count"`
	ActualCount     int64           `json:"actual_count"`
	BatchErrors     []string        `json:"batch_errors,omitempty"`
}

// ConfirmWriteOutput holds the validation results.
type ConfirmWriteOutput struct {
	Confirmed      bool     `json:"confirmed"`
	DiscrepancyMsg string   `json:"discrepancy_msg,omitempty"`
	Errors         []string `json:"errors,omitempty"`
}

// ConfirmWriteActivity validates the write results by checking the destination
// connector's health and comparing expected vs actual record counts.
func ConfirmWriteActivity(ctx context.Context, input ConfirmWriteInput) (*ConfirmWriteOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("ConfirmWriteActivity starting",
		"connector", input.DestConnectorID,
		"expected", input.ExpectedCount,
		"actual", input.ActualCount,
	)

	dest, err := cdk.GetDestination(input.DestConnectorID)
	if err != nil {
		return nil, common.NewConnectorError(input.DestConnectorID, "confirm_write", err)
	}

	// Verify the destination is still healthy.
	checkResult, err := dest.Check(ctx, input.DestConfig)
	if err != nil {
		return &ConfirmWriteOutput{
			Confirmed: false,
			Errors:    []string{fmt.Sprintf("destination health check failed: %v", err)},
		}, nil
	}

	if checkResult.Status != protocol.CheckStatusSucceeded {
		return &ConfirmWriteOutput{
			Confirmed: false,
			Errors:    []string{fmt.Sprintf("destination unhealthy: %s", checkResult.Message)},
		}, nil
	}

	var errors []string
	errors = append(errors, input.BatchErrors...)

	confirmed := true
	var discrepancy string

	if input.ActualCount != input.ExpectedCount {
		confirmed = false
		discrepancy = fmt.Sprintf(
			"record count mismatch: expected %d, got %d (delta: %d)",
			input.ExpectedCount, input.ActualCount, input.ExpectedCount-input.ActualCount,
		)
	}

	if len(errors) > 0 {
		confirmed = false
	}

	logger.Info("ConfirmWriteActivity completed",
		"confirmed", confirmed,
		"discrepancy", discrepancy,
	)

	return &ConfirmWriteOutput{
		Confirmed:      confirmed,
		DiscrepancyMsg: discrepancy,
		Errors:         errors,
	}, nil
}
