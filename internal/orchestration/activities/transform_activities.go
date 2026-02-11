package activities

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.temporal.io/sdk/activity"

	otypes "github.com/flowforge/flowforge/internal/orchestration/types"
	"github.com/flowforge/flowforge/pkg/protocol"
)

// MapFieldsInput contains the parameters for MapFieldsActivity.
type MapFieldsInput struct {
	Records       []protocol.Record            `json:"records"`
	FieldMappings map[string][]otypes.FieldMapping `json:"field_mappings"`
}

// MapFieldsOutput holds the mapped records.
type MapFieldsOutput struct {
	Records []protocol.Record `json:"records"`
	Errors  []string          `json:"errors,omitempty"`
}

// MapFieldsActivity applies field mappings to a batch of records, renaming
// source fields to their destination names as defined in the mapping configuration.
// Records from streams without mappings pass through unchanged.
func MapFieldsActivity(ctx context.Context, input MapFieldsInput) (*MapFieldsOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("MapFieldsActivity starting", "record_count", len(input.Records))

	mapped := make([]protocol.Record, 0, len(input.Records))
	var errors []string

	for i, rec := range input.Records {
		mappings, hasMappings := input.FieldMappings[rec.Stream]
		if !hasMappings || len(mappings) == 0 {
			mapped = append(mapped, rec)
			continue
		}

		var srcData map[string]interface{}
		if err := json.Unmarshal(rec.Data, &srcData); err != nil {
			errors = append(errors, fmt.Sprintf("record %d in stream %s: failed to unmarshal: %v", i, rec.Stream, err))
			continue
		}

		destData := make(map[string]interface{}, len(mappings))
		for _, m := range mappings {
			val, exists := srcData[m.SourceField]
			if !exists {
				continue
			}
			destData[m.DestField] = val
		}

		// Preserve any fields not covered by explicit mappings.
		mappedSources := make(map[string]bool, len(mappings))
		for _, m := range mappings {
			mappedSources[m.SourceField] = true
		}
		for k, v := range srcData {
			if !mappedSources[k] {
				if _, alreadyMapped := destData[k]; !alreadyMapped {
					destData[k] = v
				}
			}
		}

		destBytes, err := json.Marshal(destData)
		if err != nil {
			errors = append(errors, fmt.Sprintf("record %d in stream %s: failed to marshal mapped data: %v", i, rec.Stream, err))
			continue
		}

		mapped = append(mapped, protocol.Record{
			Stream:    rec.Stream,
			Namespace: rec.Namespace,
			Data:      destBytes,
			EmittedAt: rec.EmittedAt,
		})

		if i%500 == 0 {
			activity.RecordHeartbeat(ctx, i)
		}
	}

	logger.Info("MapFieldsActivity completed",
		"input_count", len(input.Records),
		"output_count", len(mapped),
		"error_count", len(errors),
	)

	return &MapFieldsOutput{Records: mapped, Errors: errors}, nil
}

// CoerceTypesInput contains the parameters for CoerceTypesActivity.
type CoerceTypesInput struct {
	Records       []protocol.Record                   `json:"records"`
	FieldMappings map[string][]otypes.FieldMapping `json:"field_mappings"`
}

// CoerceTypesOutput holds the type-coerced records.
type CoerceTypesOutput struct {
	Records []protocol.Record `json:"records"`
	Errors  []string          `json:"errors,omitempty"`
}

// CoerceTypesActivity applies type coercion to record fields based on the
// CoerceType specification in the field mappings. Supported coercion targets
// are: string, int64, float64, bool, and timestamp.
func CoerceTypesActivity(ctx context.Context, input CoerceTypesInput) (*CoerceTypesOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("CoerceTypesActivity starting", "record_count", len(input.Records))

	coerced := make([]protocol.Record, 0, len(input.Records))
	var errors []string

	// Build a lookup of field -> coercion type per stream.
	coercionMap := make(map[string]map[string]string)
	for stream, mappings := range input.FieldMappings {
		fieldCoercions := make(map[string]string)
		for _, m := range mappings {
			if m.CoerceType != "" {
				fieldCoercions[m.DestField] = m.CoerceType
			}
		}
		if len(fieldCoercions) > 0 {
			coercionMap[stream] = fieldCoercions
		}
	}

	for i, rec := range input.Records {
		fieldCoercions, hasCoercions := coercionMap[rec.Stream]
		if !hasCoercions {
			coerced = append(coerced, rec)
			continue
		}

		var data map[string]interface{}
		if err := json.Unmarshal(rec.Data, &data); err != nil {
			errors = append(errors, fmt.Sprintf("record %d in stream %s: unmarshal error: %v", i, rec.Stream, err))
			continue
		}

		for field, targetType := range fieldCoercions {
			val, exists := data[field]
			if !exists || val == nil {
				continue
			}

			coercedVal, err := coerceValue(val, targetType)
			if err != nil {
				errors = append(errors, fmt.Sprintf("record %d, field %s: %v", i, field, err))
				continue
			}
			data[field] = coercedVal
		}

		dataBytes, err := json.Marshal(data)
		if err != nil {
			errors = append(errors, fmt.Sprintf("record %d in stream %s: marshal error: %v", i, rec.Stream, err))
			continue
		}

		coerced = append(coerced, protocol.Record{
			Stream:    rec.Stream,
			Namespace: rec.Namespace,
			Data:      dataBytes,
			EmittedAt: rec.EmittedAt,
		})

		if i%500 == 0 {
			activity.RecordHeartbeat(ctx, i)
		}
	}

	logger.Info("CoerceTypesActivity completed",
		"input_count", len(input.Records),
		"output_count", len(coerced),
		"error_count", len(errors),
	)

	return &CoerceTypesOutput{Records: coerced, Errors: errors}, nil
}

// coerceValue converts a value to the specified target type.
func coerceValue(val interface{}, targetType string) (interface{}, error) {
	switch strings.ToLower(targetType) {
	case "string":
		return coerceToString(val), nil
	case "int64", "int", "integer":
		return coerceToInt64(val)
	case "float64", "float", "double", "number":
		return coerceToFloat64(val)
	case "bool", "boolean":
		return coerceToBool(val)
	case "timestamp", "datetime":
		return coerceToTimestamp(val)
	default:
		return val, nil
	}
}

func coerceToString(val interface{}) string {
	switch v := val.(type) {
	case string:
		return v
	case float64:
		if v == float64(int64(v)) {
			return strconv.FormatInt(int64(v), 10)
		}
		return strconv.FormatFloat(v, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(v)
	case nil:
		return ""
	default:
		return fmt.Sprintf("%v", v)
	}
}

func coerceToInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case float64:
		return int64(v), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			f, fErr := strconv.ParseFloat(v, 64)
			if fErr != nil {
				return 0, fmt.Errorf("cannot coerce %q to int64", v)
			}
			return int64(f), nil
		}
		return i, nil
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot coerce %T to int64", val)
	}
}

func coerceToFloat64(val interface{}) (float64, error) {
	switch v := val.(type) {
	case float64:
		return v, nil
	case string:
		return strconv.ParseFloat(v, 64)
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	case nil:
		return 0.0, nil
	default:
		return 0, fmt.Errorf("cannot coerce %T to float64", val)
	}
}

func coerceToBool(val interface{}) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case float64:
		return v != 0, nil
	case string:
		return strconv.ParseBool(v)
	case nil:
		return false, nil
	default:
		return false, fmt.Errorf("cannot coerce %T to bool", val)
	}
}

func coerceToTimestamp(val interface{}) (string, error) {
	switch v := val.(type) {
	case string:
		// Try parsing common timestamp formats.
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
		}
		for _, f := range formats {
			if t, err := time.Parse(f, v); err == nil {
				return t.UTC().Format(time.RFC3339), nil
			}
		}
		return v, nil
	case float64:
		// Treat as Unix timestamp (seconds).
		t := time.Unix(int64(v), 0)
		return t.UTC().Format(time.RFC3339), nil
	case nil:
		return "", nil
	default:
		return fmt.Sprintf("%v", val), nil
	}
}

// DeduplicateInput contains the parameters for DeduplicateActivity.
type DeduplicateInput struct {
	Records []protocol.Record           `json:"records"`
	Streams []protocol.ConfiguredStream `json:"streams"`
}

// DeduplicateOutput holds the deduplicated records.
type DeduplicateOutput struct {
	Records        []protocol.Record `json:"records"`
	DuplicatesFound int64            `json:"duplicates_found"`
}

// DeduplicateActivity removes duplicate records from a batch based on primary
// key fields defined in the configured streams. For streams without a defined
// primary key, all records pass through unchanged.
func DeduplicateActivity(ctx context.Context, input DeduplicateInput) (*DeduplicateOutput, error) {
	logger := activity.GetLogger(ctx)
	logger.Info("DeduplicateActivity starting", "record_count", len(input.Records))

	// Build primary key field lookup per stream.
	streamPKs := make(map[string][][]string)
	for _, cs := range input.Streams {
		pk := cs.PrimaryKey
		if len(pk) == 0 {
			pk = cs.Stream.PrimaryKey
		}
		if len(pk) > 0 {
			streamPKs[cs.Stream.Name] = pk
		}
	}

	seen := make(map[string]map[string]bool)
	deduped := make([]protocol.Record, 0, len(input.Records))
	var duplicates int64

	for _, rec := range input.Records {
		pkFields, hasPK := streamPKs[rec.Stream]
		if !hasPK {
			deduped = append(deduped, rec)
			continue
		}

		key := extractPrimaryKey(rec.Data, pkFields)
		if key == "" {
			deduped = append(deduped, rec)
			continue
		}

		streamSeen, exists := seen[rec.Stream]
		if !exists {
			streamSeen = make(map[string]bool)
			seen[rec.Stream] = streamSeen
		}

		if streamSeen[key] {
			duplicates++
			continue
		}

		streamSeen[key] = true
		deduped = append(deduped, rec)
	}

	logger.Info("DeduplicateActivity completed",
		"input_count", len(input.Records),
		"output_count", len(deduped),
		"duplicates_removed", duplicates,
	)

	return &DeduplicateOutput{
		Records:         deduped,
		DuplicatesFound: duplicates,
	}, nil
}

// extractPrimaryKey builds a composite key string from the record data using
// the given primary key field paths.
func extractPrimaryKey(data json.RawMessage, pkFields [][]string) string {
	var record map[string]interface{}
	if err := json.Unmarshal(data, &record); err != nil {
		return ""
	}

	parts := make([]string, 0, len(pkFields))
	for _, fieldPath := range pkFields {
		val := navigateFieldPath(record, fieldPath)
		parts = append(parts, fmt.Sprintf("%v", val))
	}

	return strings.Join(parts, "|")
}

// navigateFieldPath traverses a nested map using a field path (e.g., ["address", "city"])
// and returns the value at the leaf, or nil if the path doesn't exist.
func navigateFieldPath(data map[string]interface{}, path []string) interface{} {
	if len(path) == 0 {
		return nil
	}

	current := interface{}(data)
	for _, key := range path {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current = m[key]
	}
	return current
}
