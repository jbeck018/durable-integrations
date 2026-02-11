// Package types defines shared types used across the orchestration layer,
// particularly between workflows and activities packages. Placing these types
// in a separate package breaks the import cycle between those two packages.
package types

// FieldMapping describes how a source field maps to a destination field,
// including optional type coercion.
type FieldMapping struct {
	SourceField string `json:"source_field"`
	DestField   string `json:"dest_field"`
	CoerceType  string `json:"coerce_type,omitempty"` // e.g. "string", "int64", "float64", "bool", "timestamp"
}

// PartitionRange defines a slice of data for parallel extraction.
type PartitionRange struct {
	PartitionID int    `json:"partition_id"`
	StartKey    string `json:"start_key,omitempty"`
	EndKey      string `json:"end_key,omitempty"`
	Offset      int64  `json:"offset"`
	Limit       int64  `json:"limit"`
}

// defaultBatchSize is used when no batch size is specified.
const defaultBatchSize = 1000

// parallelPartitionThreshold is the record count above which extraction
// fans out into parallel partitions.
const parallelPartitionThreshold = 10000

// maxPartitions caps the number of concurrent extraction partitions.
const maxPartitions = 8

// DefaultBatchSize returns the default batch size for processing.
func DefaultBatchSize() int {
	return defaultBatchSize
}

// ParallelPartitionThreshold returns the threshold above which parallel
// partitioned extraction is used.
func ParallelPartitionThreshold() int64 {
	return parallelPartitionThreshold
}

// MaxPartitions returns the maximum number of concurrent partitions.
func MaxPartitions() int {
	return maxPartitions
}
