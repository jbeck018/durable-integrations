// Package transform provides the transformation phase of the FlowForge ETL
// pipeline: field mapping, type coercion, and deduplication.
package transform

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// TransformType enumerates the supported field transformation operations.
type TransformType string

const (
	TransformRename     TransformType = "rename"
	TransformCast       TransformType = "cast"
	TransformExpression TransformType = "expression"
	TransformDefault    TransformType = "default"
)

// FieldMapping describes how a single source field maps to a destination field,
// with an optional transformation.
type FieldMapping struct {
	SourceField  string        `json:"source_field"`
	DestField    string        `json:"dest_field"`
	Transform    TransformType `json:"transform"`
	CastType     string        `json:"cast_type,omitempty"`     // for TransformCast
	Expression   string        `json:"expression,omitempty"`    // for TransformExpression
	DefaultValue interface{}   `json:"default_value,omitempty"` // for TransformDefault
}

// FieldMappings is a slice of FieldMapping for convenience typing.
type FieldMappings []FieldMapping

// FieldMapper applies field mappings to JSON records. It supports nested paths
// via dot notation (e.g. "address.city") and array indexing ("items[0].name").
type FieldMapper struct {
	coercer *TypeCoercer
}

// NewFieldMapper creates a FieldMapper backed by the given TypeCoercer.
func NewFieldMapper(coercer *TypeCoercer) *FieldMapper {
	return &FieldMapper{coercer: coercer}
}

// Apply transforms a JSON record according to the given mappings and returns
// a new JSON record with the mapped fields.
func (fm *FieldMapper) Apply(record json.RawMessage, mappings []FieldMapping) (json.RawMessage, error) {
	var src map[string]interface{}
	if err := json.Unmarshal(record, &src); err != nil {
		return nil, fmt.Errorf("unmarshal source record: %w", err)
	}

	dest := make(map[string]interface{}, len(mappings))
	for _, m := range mappings {
		val := getNestedValue(src, m.SourceField)

		switch m.Transform {
		case TransformRename:
			// Just move the value to the dest field.
			if val == nil {
				continue
			}
		case TransformCast:
			if val != nil && m.CastType != "" {
				coerced, err := fm.coercer.Coerce(val, inferTypeString(val), m.CastType)
				if err != nil {
					return nil, fmt.Errorf("cast %s to %s: %w", m.SourceField, m.CastType, err)
				}
				val = coerced
			}
		case TransformExpression:
			val = evaluateExpression(m.Expression, val, src)
		case TransformDefault:
			if val == nil {
				val = m.DefaultValue
			}
		default:
			// Identity: carry value as-is.
		}

		if val != nil {
			setNestedValue(dest, m.DestField, val)
		}
	}

	result, err := json.Marshal(dest)
	if err != nil {
		return nil, fmt.Errorf("marshal mapped record: %w", err)
	}
	return result, nil
}

// parsePath splits a dot-notation path that may contain array indices into
// segments. "items[0].name" becomes ["items", "[0]", "name"].
func parsePath(path string) []string {
	var segments []string
	current := strings.Builder{}
	for i := 0; i < len(path); i++ {
		ch := path[i]
		switch ch {
		case '.':
			if current.Len() > 0 {
				segments = append(segments, current.String())
				current.Reset()
			}
		case '[':
			if current.Len() > 0 {
				segments = append(segments, current.String())
				current.Reset()
			}
			// Read the index including brackets.
			current.WriteByte('[')
			i++
			for i < len(path) && path[i] != ']' {
				current.WriteByte(path[i])
				i++
			}
			if i < len(path) {
				current.WriteByte(']')
			}
			segments = append(segments, current.String())
			current.Reset()
		default:
			current.WriteByte(ch)
		}
	}
	if current.Len() > 0 {
		segments = append(segments, current.String())
	}
	return segments
}

// getNestedValue navigates into a nested map/slice following a dot-notation
// path with optional array index segments.
func getNestedValue(data map[string]interface{}, path string) interface{} {
	segments := parsePath(path)
	var current interface{} = data

	for _, seg := range segments {
		if current == nil {
			return nil
		}
		if strings.HasPrefix(seg, "[") && strings.HasSuffix(seg, "]") {
			idxStr := seg[1 : len(seg)-1]
			idx, err := strconv.Atoi(idxStr)
			if err != nil {
				return nil
			}
			arr, ok := current.([]interface{})
			if !ok {
				return nil
			}
			if idx < 0 || idx >= len(arr) {
				return nil
			}
			current = arr[idx]
		} else {
			m, ok := current.(map[string]interface{})
			if !ok {
				return nil
			}
			current, ok = m[seg]
			if !ok {
				return nil
			}
		}
	}
	return current
}

// setNestedValue writes a value into a nested map, creating intermediate maps
// as needed. It supports dot notation but not array indexing for dest paths.
func setNestedValue(data map[string]interface{}, path string, value interface{}) {
	parts := strings.Split(path, ".")
	current := data
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			nested := make(map[string]interface{})
			current[parts[i]] = nested
			current = nested
		} else if m, ok := next.(map[string]interface{}); ok {
			current = m
		} else {
			nested := make(map[string]interface{})
			current[parts[i]] = nested
			current = nested
		}
	}
	current[parts[len(parts)-1]] = value
}

// inferTypeString returns a string label for the Go type of a value.
func inferTypeString(val interface{}) string {
	switch val.(type) {
	case string:
		return "string"
	case float64:
		return "float"
	case int, int64:
		return "int"
	case bool:
		return "bool"
	case json.Number:
		return "string"
	default:
		return "string"
	}
}

// evaluateExpression handles simple built-in expressions. Supported forms:
//   - "upper" / "lower" / "trim" for string operations
//   - "negate" for numeric negation
//   - anything else returns the value unchanged.
func evaluateExpression(expr string, val interface{}, record map[string]interface{}) interface{} {
	if val == nil {
		return nil
	}
	switch strings.ToLower(strings.TrimSpace(expr)) {
	case "upper":
		if s, ok := val.(string); ok {
			return strings.ToUpper(s)
		}
	case "lower":
		if s, ok := val.(string); ok {
			return strings.ToLower(s)
		}
	case "trim":
		if s, ok := val.(string); ok {
			return strings.TrimSpace(s)
		}
	case "negate":
		switch v := val.(type) {
		case float64:
			return -v
		case int:
			return -v
		case int64:
			return -v
		}
	case "tostring":
		return fmt.Sprintf("%v", val)
	case "length":
		if s, ok := val.(string); ok {
			return len(s)
		}
		if arr, ok := val.([]interface{}); ok {
			return len(arr)
		}
	case "concat":
		// concat uses all other source fields matching the expression pattern.
		// For simplicity, treat it as a passthrough if not a string.
		if s, ok := val.(string); ok {
			return s
		}
	}
	return val
}
