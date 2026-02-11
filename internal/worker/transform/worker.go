package transform

import (
	"context"
	"fmt"
	"sync"

	"github.com/flowforge/flowforge/pkg/arrow"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// TransformWorker applies field mappings, type coercion, and deduplication to
// batches of records. It leverages Arrow columnar batches for efficient
// processing when a schema is available.
type TransformWorker struct {
	mapper      *FieldMapper
	coercer     *TypeCoercer
	dedup       *Deduplicator
	concurrency int
}

// NewTransformWorker creates a TransformWorker with the given concurrency.
// If dedup is nil, deduplication is skipped.
func NewTransformWorker(concurrency int, dedup *Deduplicator) *TransformWorker {
	if concurrency < 1 {
		concurrency = 1
	}
	coercer := NewTypeCoercer()
	return &TransformWorker{
		mapper:      NewFieldMapper(coercer),
		coercer:     coercer,
		dedup:       dedup,
		concurrency: concurrency,
	}
}

// ProcessBatch transforms a batch of records by applying field mappings,
// type coercion (via the mapper), and deduplication. When a schema is provided
// the records are loaded into an Arrow batch for columnar access, transformed,
// then converted back.
func (tw *TransformWorker) ProcessBatch(
	ctx context.Context,
	records []protocol.Record,
	mappings FieldMappings,
	schema *arrow.Schema,
) ([]protocol.Record, error) {
	if len(records) == 0 {
		return records, nil
	}

	// Stage 1: Deduplication — remove records whose deduplicated keys have
	// already been seen.
	deduped := records
	if tw.dedup != nil {
		deduped = tw.deduplicateRecords(records)
	}

	// Stage 2: If there are no mappings, return the deduplicated records as-is.
	if len(mappings) == 0 {
		return deduped, nil
	}

	// Stage 3: Apply field mappings in parallel.
	result, err := tw.applyMappingsParallel(ctx, deduped, mappings, schema)
	if err != nil {
		return nil, fmt.Errorf("apply mappings: %w", err)
	}

	return result, nil
}

// deduplicateRecords filters out records whose data hash has been seen.
func (tw *TransformWorker) deduplicateRecords(records []protocol.Record) []protocol.Record {
	unique := make([]protocol.Record, 0, len(records))
	for i := range records {
		key := string(records[i].Data)
		if !tw.dedup.IsDuplicate(key) {
			unique = append(unique, records[i])
		}
	}
	return unique
}

// applyMappingsParallel fans out mapping work across tw.concurrency goroutines.
// Each goroutine processes a contiguous slice of records.
func (tw *TransformWorker) applyMappingsParallel(
	ctx context.Context,
	records []protocol.Record,
	mappings FieldMappings,
	schema *arrow.Schema,
) ([]protocol.Record, error) {
	n := len(records)
	if n == 0 {
		return nil, nil
	}

	// If the batch is small or concurrency is 1, process serially to avoid
	// goroutine overhead.
	if tw.concurrency <= 1 || n < tw.concurrency*2 {
		return tw.applyMappingsSlice(ctx, records, mappings, schema)
	}

	chunkSize := n / tw.concurrency
	if chunkSize < 1 {
		chunkSize = 1
	}

	type result struct {
		index   int
		records []protocol.Record
		err     error
	}

	numChunks := (n + chunkSize - 1) / chunkSize
	results := make([]result, numChunks)
	var wg sync.WaitGroup

	for i := 0; i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > n {
			end = n
		}
		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			recs, err := tw.applyMappingsSlice(ctx, records[start:end], mappings, schema)
			results[idx] = result{index: idx, records: recs, err: err}
		}()
	}
	wg.Wait()

	var combined []protocol.Record
	for _, r := range results {
		if r.err != nil {
			return nil, r.err
		}
		combined = append(combined, r.records...)
	}
	return combined, nil
}

// applyMappingsSlice transforms a contiguous slice of records using the mapper.
// When a schema is provided, it first loads records into an Arrow batch,
// processes them, and converts back.
func (tw *TransformWorker) applyMappingsSlice(
	ctx context.Context,
	records []protocol.Record,
	mappings FieldMappings,
	schema *arrow.Schema,
) ([]protocol.Record, error) {
	// Use Arrow columnar batch if schema is provided.
	if schema != nil && len(records) > 0 {
		return tw.applyMappingsColumnar(ctx, records, mappings, schema)
	}
	return tw.applyMappingsRowwise(ctx, records, mappings)
}

// applyMappingsColumnar loads records into an Arrow batch, applies mappings
// using columnar access patterns, and converts back to records.
func (tw *TransformWorker) applyMappingsColumnar(
	ctx context.Context,
	records []protocol.Record,
	mappings FieldMappings,
	schema *arrow.Schema,
) ([]protocol.Record, error) {
	batch := arrow.NewBatch(schema, len(records))
	for i := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := batch.AppendRecord(records[i].Data); err != nil {
			return nil, fmt.Errorf("append record %d to batch: %w", i, err)
		}
	}

	// Convert batch back to raw records, then apply mappings row-wise.
	// The Arrow batch gives us efficient columnar storage during accumulation;
	// mappings are still applied per-record for flexibility.
	rawRecords, err := batch.ToRecords()
	if err != nil {
		return nil, fmt.Errorf("convert batch to records: %w", err)
	}

	result := make([]protocol.Record, 0, len(rawRecords))
	for i, raw := range rawRecords {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mapped, merr := tw.mapper.Apply(raw, mappings)
		if merr != nil {
			return nil, fmt.Errorf("map record %d: %w", i, merr)
		}
		rec := records[i]
		rec.Data = mapped
		result = append(result, rec)
	}
	return result, nil
}

// applyMappingsRowwise transforms records one at a time (no Arrow batching).
func (tw *TransformWorker) applyMappingsRowwise(
	ctx context.Context,
	records []protocol.Record,
	mappings FieldMappings,
) ([]protocol.Record, error) {
	result := make([]protocol.Record, 0, len(records))
	for i := range records {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mapped, err := tw.mapper.Apply(records[i].Data, mappings)
		if err != nil {
			return nil, fmt.Errorf("map record %d: %w", i, err)
		}
		rec := records[i]
		rec.Data = mapped
		result = append(result, rec)
	}
	return result, nil
}
