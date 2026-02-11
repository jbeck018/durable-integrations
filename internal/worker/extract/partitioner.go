package extract

import (
	"fmt"
	"time"
)

// Partition represents a unit of work for parallel extraction.
type Partition struct {
	ID    int    `json:"id"`
	Type  string `json:"type"` // "time_range" or "id_range"
	Start string `json:"start"`
	End   string `json:"end"`
}

// Partitioner divides large extraction workloads into parallel chunks.
type Partitioner interface {
	// Partition splits the extraction into chunks. totalRecords is the estimated
	// record count and maxPartitionSize is the upper bound per partition.
	Partition(totalRecords int, maxPartitionSize int) []Partition
}

// TimeRangePartitioner splits an extraction by time ranges. Each partition
// covers a contiguous interval between Start and End.
type TimeRangePartitioner struct {
	Start      time.Time
	End        time.Time
	TimeFormat string // Go time format for serialisation; defaults to RFC3339
}

// Partition divides the time range [Start, End) into chunks of approximately
// maxPartitionSize records each (estimated by dividing the total evenly).
func (p *TimeRangePartitioner) Partition(totalRecords int, maxPartitionSize int) []Partition {
	if maxPartitionSize <= 0 {
		maxPartitionSize = 1
	}
	numPartitions := totalRecords / maxPartitionSize
	if totalRecords%maxPartitionSize != 0 {
		numPartitions++
	}
	if numPartitions < 1 {
		numPartitions = 1
	}

	format := p.TimeFormat
	if format == "" {
		format = time.RFC3339
	}

	totalDuration := p.End.Sub(p.Start)
	partitionDuration := totalDuration / time.Duration(numPartitions)
	if partitionDuration <= 0 {
		partitionDuration = time.Second
	}

	partitions := make([]Partition, 0, numPartitions)
	current := p.Start
	for i := 0; i < numPartitions; i++ {
		end := current.Add(partitionDuration)
		if i == numPartitions-1 || end.After(p.End) {
			end = p.End
		}
		partitions = append(partitions, Partition{
			ID:    i,
			Type:  "time_range",
			Start: current.Format(format),
			End:   end.Format(format),
		})
		current = end
	}
	return partitions
}

// IDRangePartitioner splits an extraction by integer ID ranges. This is
// efficient for databases with monotonically increasing primary keys.
type IDRangePartitioner struct {
	MinID int64
	MaxID int64
}

// Partition divides the ID range [MinID, MaxID] into chunks. Each partition
// covers a contiguous range of IDs.
func (p *IDRangePartitioner) Partition(totalRecords int, maxPartitionSize int) []Partition {
	if maxPartitionSize <= 0 {
		maxPartitionSize = 1
	}
	numPartitions := totalRecords / maxPartitionSize
	if totalRecords%maxPartitionSize != 0 {
		numPartitions++
	}
	if numPartitions < 1 {
		numPartitions = 1
	}

	totalRange := p.MaxID - p.MinID + 1
	rangePerPartition := totalRange / int64(numPartitions)
	if rangePerPartition < 1 {
		rangePerPartition = 1
	}

	partitions := make([]Partition, 0, numPartitions)
	current := p.MinID
	for i := 0; i < numPartitions; i++ {
		end := current + rangePerPartition - 1
		if i == numPartitions-1 || end > p.MaxID {
			end = p.MaxID
		}
		partitions = append(partitions, Partition{
			ID:    i,
			Type:  "id_range",
			Start: fmt.Sprintf("%d", current),
			End:   fmt.Sprintf("%d", end),
		})
		current = end + 1
		if current > p.MaxID {
			break
		}
	}
	return partitions
}

// CalculatePartitions is a convenience function that determines partition
// boundaries given a total record count and maximum partition size.
func CalculatePartitions(totalRecords int, maxPartitionSize int) []Partition {
	if maxPartitionSize <= 0 {
		maxPartitionSize = 1
	}
	numPartitions := totalRecords / maxPartitionSize
	if totalRecords%maxPartitionSize != 0 {
		numPartitions++
	}
	if numPartitions < 1 {
		numPartitions = 1
	}

	recordsPerPartition := totalRecords / numPartitions
	remainder := totalRecords % numPartitions

	partitions := make([]Partition, 0, numPartitions)
	offset := 0
	for i := 0; i < numPartitions; i++ {
		size := recordsPerPartition
		if i < remainder {
			size++
		}
		partitions = append(partitions, Partition{
			ID:    i,
			Type:  "offset_range",
			Start: fmt.Sprintf("%d", offset),
			End:   fmt.Sprintf("%d", offset+size-1),
		})
		offset += size
	}
	return partitions
}
