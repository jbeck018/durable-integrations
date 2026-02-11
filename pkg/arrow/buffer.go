// Package arrow provides Apache Arrow columnar buffer utilities for efficient
// inter-phase data transfer within FlowForge sync pipelines.
//
// Arrow columnar format dramatically reduces memory for typical ETL workloads
// and enables vectorized transforms and zero-copy transfer between phases.
package arrow

import (
	"encoding/json"
	"fmt"
	"sync"
)

// Batch represents a columnar batch of records stored in Arrow format.
// It accumulates records and provides efficient access patterns for
// transform and load operations.
type Batch struct {
	mu       sync.RWMutex
	columns  map[string]*Column
	rowCount int
	schema   *Schema
}

// Schema describes the column layout of a Batch.
type Schema struct {
	Fields []Field `json:"fields"`
}

// Field describes a single column in the schema.
type Field struct {
	Name     string   `json:"name"`
	Type     DataType `json:"type"`
	Nullable bool     `json:"nullable"`
}

// DataType represents supported Arrow data types.
type DataType string

const (
	TypeBool      DataType = "bool"
	TypeInt32     DataType = "int32"
	TypeInt64     DataType = "int64"
	TypeFloat64   DataType = "float64"
	TypeString    DataType = "string"
	TypeBinary    DataType = "binary"
	TypeTimestamp DataType = "timestamp"
	TypeJSON      DataType = "json"
)

// Column stores typed columnar data with null tracking.
type Column struct {
	Name   string
	Type   DataType
	Values []interface{}
	Nulls  []bool // true = null at this index
}

// NewBatch creates a new Batch with the given schema, pre-allocating for capacity rows.
func NewBatch(schema *Schema, capacity int) *Batch {
	cols := make(map[string]*Column, len(schema.Fields))
	for _, f := range schema.Fields {
		cols[f.Name] = &Column{
			Name:   f.Name,
			Type:   f.Type,
			Values: make([]interface{}, 0, capacity),
			Nulls:  make([]bool, 0, capacity),
		}
	}
	return &Batch{
		columns:  cols,
		schema:   schema,
		rowCount: 0,
	}
}

// AppendRecord adds a JSON record to the batch, mapping fields to columns.
func (b *Batch) AppendRecord(data json.RawMessage) error {
	var row map[string]interface{}
	if err := json.Unmarshal(data, &row); err != nil {
		return fmt.Errorf("unmarshal record: %w", err)
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, f := range b.schema.Fields {
		col := b.columns[f.Name]
		val, exists := row[f.Name]
		if !exists || val == nil {
			col.Values = append(col.Values, nil)
			col.Nulls = append(col.Nulls, true)
		} else {
			col.Values = append(col.Values, val)
			col.Nulls = append(col.Nulls, false)
		}
	}
	b.rowCount++
	return nil
}

// RowCount returns the number of records in the batch.
func (b *Batch) RowCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.rowCount
}

// Column returns a column by name.
func (b *Batch) Column(name string) (*Column, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	col, ok := b.columns[name]
	return col, ok
}

// Schema returns the batch schema.
func (b *Batch) Schema() *Schema {
	return b.schema
}

// ToRecords converts the batch back to JSON records for serialization.
func (b *Batch) ToRecords() ([]json.RawMessage, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	records := make([]json.RawMessage, b.rowCount)
	for i := 0; i < b.rowCount; i++ {
		row := make(map[string]interface{}, len(b.schema.Fields))
		for _, f := range b.schema.Fields {
			col := b.columns[f.Name]
			if !col.Nulls[i] {
				row[f.Name] = col.Values[i]
			}
		}
		data, err := json.Marshal(row)
		if err != nil {
			return nil, fmt.Errorf("marshal row %d: %w", i, err)
		}
		records[i] = data
	}
	return records, nil
}

// BufferPool manages reusable Batch instances to reduce GC pressure.
type BufferPool struct {
	pool sync.Pool
}

// NewBufferPool creates a BufferPool that produces batches with the given schema and capacity.
func NewBufferPool(schema *Schema, capacity int) *BufferPool {
	return &BufferPool{
		pool: sync.Pool{
			New: func() interface{} {
				return NewBatch(schema, capacity)
			},
		},
	}
}

// Get retrieves a Batch from the pool (or creates a new one).
func (p *BufferPool) Get() *Batch {
	b, _ := p.pool.Get().(*Batch)
	return b
}

// Put returns a Batch to the pool after resetting it.
func (p *BufferPool) Put(b *Batch) {
	b.mu.Lock()
	for _, col := range b.columns {
		col.Values = col.Values[:0]
		col.Nulls = col.Nulls[:0]
	}
	b.rowCount = 0
	b.mu.Unlock()
	p.pool.Put(b)
}

// InferSchema infers an Arrow schema from a sample JSON record.
func InferSchema(sample json.RawMessage) (*Schema, error) {
	var row map[string]interface{}
	if err := json.Unmarshal(sample, &row); err != nil {
		return nil, fmt.Errorf("unmarshal sample: %w", err)
	}

	fields := make([]Field, 0, len(row))
	for name, val := range row {
		fields = append(fields, Field{
			Name:     name,
			Type:     inferType(val),
			Nullable: true,
		})
	}
	return &Schema{Fields: fields}, nil
}

func inferType(val interface{}) DataType {
	switch val.(type) {
	case bool:
		return TypeBool
	case float64:
		return TypeFloat64
	case string:
		return TypeString
	case map[string]interface{}, []interface{}:
		return TypeJSON
	default:
		return TypeString
	}
}
