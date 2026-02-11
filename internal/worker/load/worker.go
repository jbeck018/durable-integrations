package load

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/pkg/cdk"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// DeadLetterRecord is a record that could not be written after all retries.
type DeadLetterRecord struct {
	Record    protocol.Record `json:"record"`
	Error     string          `json:"error"`
	Attempts  int             `json:"attempts"`
	Timestamp time.Time       `json:"timestamp"`
}

// LoadWorker writes batches of records to destination connectors with retry
// and dead letter queue support.
type LoadWorker struct {
	batchSize   int
	maxRetries  int
	baseDelay   time.Duration
	maxDelay    time.Duration

	mu         sync.Mutex
	deadLetter []DeadLetterRecord
}

// NewLoadWorker creates a LoadWorker with the given batch size and retry settings.
func NewLoadWorker(batchSize int, maxRetries int) *LoadWorker {
	if batchSize <= 0 {
		batchSize = 1000
	}
	if maxRetries < 0 {
		maxRetries = 3
	}
	return &LoadWorker{
		batchSize:  batchSize,
		maxRetries: maxRetries,
		baseDelay:  100 * time.Millisecond,
		maxDelay:   30 * time.Second,
		deadLetter: make([]DeadLetterRecord, 0),
	}
}

// ProcessBatch writes records to the named destination connector in batches.
// It retries on retryable errors with exponential backoff and sends
// permanently failed records to the dead letter queue.
func (lw *LoadWorker) ProcessBatch(
	ctx context.Context,
	connectorName string,
	config json.RawMessage,
	catalog *protocol.ConfiguredCatalog,
	records []protocol.Record,
) (*protocol.WriteResult, error) {
	dest, err := cdk.GetDestination(connectorName)
	if err != nil {
		return nil, common.NewConnectorError(connectorName, "get_destination", err)
	}

	ctx = common.WithConnectorName(ctx, connectorName)

	totalResult := &protocol.WriteResult{}

	// Process in batches.
	for start := 0; start < len(records); start += lw.batchSize {
		end := start + lw.batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[start:end]

		result, batchErr := lw.writeBatchWithRetry(ctx, dest, config, catalog, batch)
		if batchErr != nil {
			// All records in this batch failed; send to dead letter queue.
			lw.addToDeadLetter(batch, batchErr.Error())
			totalResult.Errors = append(totalResult.Errors, protocol.WriteError{
				Message:   batchErr.Error(),
				ErrorCode: "BATCH_FAILED",
			})
			continue
		}

		totalResult.RecordsWritten += result.RecordsWritten
		totalResult.StateMessages = append(totalResult.StateMessages, result.StateMessages...)

		// Process per-record errors and send failed records to dead letter.
		for _, we := range result.Errors {
			totalResult.Errors = append(totalResult.Errors, we)
			if len(we.Record) > 0 {
				lw.mu.Lock()
				lw.deadLetter = append(lw.deadLetter, DeadLetterRecord{
					Record: protocol.Record{
						Data:      we.Record,
						EmittedAt: time.Now(),
					},
					Error:     we.Message,
					Attempts:  lw.maxRetries + 1,
					Timestamp: time.Now(),
				})
				lw.mu.Unlock()
			}
		}
	}

	return totalResult, nil
}

// writeBatchWithRetry attempts to write a batch to the destination, retrying
// on retryable errors with exponential backoff.
func (lw *LoadWorker) writeBatchWithRetry(
	ctx context.Context,
	dest cdk.Destination,
	config json.RawMessage,
	catalog *protocol.ConfiguredCatalog,
	records []protocol.Record,
) (*protocol.WriteResult, error) {
	var lastErr error
	for attempt := 0; attempt <= lw.maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		result, err := lw.doWrite(ctx, dest, config, catalog, records)
		if err == nil {
			return result, nil
		}

		lastErr = err
		if !common.IsRetryable(err) {
			return nil, err
		}

		// Exponential backoff with jitter.
		delay := lw.backoffDelay(attempt)
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}

// doWrite performs a single write attempt by feeding records into the
// destination via a channel.
func (lw *LoadWorker) doWrite(
	ctx context.Context,
	dest cdk.Destination,
	config json.RawMessage,
	catalog *protocol.ConfiguredCatalog,
	records []protocol.Record,
) (*protocol.WriteResult, error) {
	// Create a buffered input channel sized to the batch to avoid blocking.
	input := make(chan protocol.Message, len(records))

	// Feed records into the channel.
	go func() {
		defer close(input)
		for i := range records {
			rec := records[i]
			msg := protocol.Message{
				Type:   protocol.MessageTypeRecord,
				Record: &rec,
			}
			select {
			case input <- msg:
			case <-ctx.Done():
				return
			}
		}
	}()

	result, err := dest.Write(ctx, config, catalog, input)
	if err != nil {
		return nil, err
	}
	return result, nil
}

// backoffDelay computes exponential backoff capped at maxDelay.
func (lw *LoadWorker) backoffDelay(attempt int) time.Duration {
	delay := time.Duration(float64(lw.baseDelay) * math.Pow(2, float64(attempt)))
	if delay > lw.maxDelay {
		delay = lw.maxDelay
	}
	return delay
}

// addToDeadLetter sends records to the dead letter queue.
func (lw *LoadWorker) addToDeadLetter(records []protocol.Record, errMsg string) {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	for i := range records {
		lw.deadLetter = append(lw.deadLetter, DeadLetterRecord{
			Record:    records[i],
			Error:     errMsg,
			Attempts:  lw.maxRetries + 1,
			Timestamp: time.Now(),
		})
	}
}

// DeadLetterRecords returns a copy of all records in the dead letter queue.
func (lw *LoadWorker) DeadLetterRecords() []DeadLetterRecord {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	out := make([]DeadLetterRecord, len(lw.deadLetter))
	copy(out, lw.deadLetter)
	return out
}

// DrainDeadLetter removes and returns all records from the dead letter queue.
func (lw *LoadWorker) DrainDeadLetter() []DeadLetterRecord {
	lw.mu.Lock()
	defer lw.mu.Unlock()
	out := lw.deadLetter
	lw.deadLetter = make([]DeadLetterRecord, 0)
	return out
}

// FlushBatcher is a convenience function that creates a Batcher, feeds all
// records through it, and collects the write results. This is useful when
// you want size-and-time-based batching in a synchronous call.
func FlushBatcher(
	ctx context.Context,
	connectorName string,
	config json.RawMessage,
	catalog *protocol.ConfiguredCatalog,
	records []protocol.Record,
	batchSize int,
	lw *LoadWorker,
) (*protocol.WriteResult, error) {
	totalResult := &protocol.WriteResult{}
	var resultMu sync.Mutex
	var flushErrors []error

	batcher := NewBatcher(batchSize, 1*time.Second, func(batch []protocol.Record) error {
		r, err := lw.ProcessBatch(ctx, connectorName, config, catalog, batch)
		if err != nil {
			return err
		}
		resultMu.Lock()
		totalResult.RecordsWritten += r.RecordsWritten
		totalResult.Errors = append(totalResult.Errors, r.Errors...)
		totalResult.StateMessages = append(totalResult.StateMessages, r.StateMessages...)
		resultMu.Unlock()
		return nil
	})

	for i := range records {
		if err := batcher.Add(records[i]); err != nil {
			flushErrors = append(flushErrors, err)
		}
	}

	if err := batcher.Close(); err != nil {
		flushErrors = append(flushErrors, err)
	}

	if len(flushErrors) > 0 {
		return totalResult, errors.Join(flushErrors...)
	}
	return totalResult, nil
}
