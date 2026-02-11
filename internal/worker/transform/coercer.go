package transform

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// CoerceFunc converts a value from one type to another.
type CoerceFunc func(value interface{}) (interface{}, error)

// TypeCoercer provides extensible type conversion with built-in support for
// common type pairs (string, int, float, bool, time). Nullable values (nil)
// pass through unchanged.
type TypeCoercer struct {
	converters map[string]CoerceFunc
}

// NewTypeCoercer creates a TypeCoercer pre-loaded with the built-in converters.
func NewTypeCoercer() *TypeCoercer {
	tc := &TypeCoercer{
		converters: make(map[string]CoerceFunc),
	}
	tc.registerBuiltins()
	return tc
}

// RegisterConverter adds or overrides a converter for the given type pair key.
// The key format is "from->to", e.g. "string->int".
func (tc *TypeCoercer) RegisterConverter(from, to string, fn CoerceFunc) {
	key := coercionKey(from, to)
	tc.converters[key] = fn
}

// Coerce converts a value from fromType to toType. Nil values are passed through.
func (tc *TypeCoercer) Coerce(value interface{}, fromType, toType string) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	from := normalizeType(fromType)
	to := normalizeType(toType)
	if from == to {
		return value, nil
	}

	key := coercionKey(from, to)
	fn, ok := tc.converters[key]
	if !ok {
		return nil, fmt.Errorf("no converter registered for %s -> %s", from, to)
	}
	return fn(value)
}

func coercionKey(from, to string) string {
	return from + "->" + to
}

func normalizeType(t string) string {
	switch strings.ToLower(strings.TrimSpace(t)) {
	case "string", "str", "text", "varchar":
		return "string"
	case "int", "int32", "int64", "integer", "bigint":
		return "int"
	case "float", "float32", "float64", "double", "decimal", "number":
		return "float"
	case "bool", "boolean":
		return "bool"
	case "time", "timestamp", "datetime", "date":
		return "time"
	default:
		return strings.ToLower(strings.TrimSpace(t))
	}
}

// registerBuiltins wires up the standard type converters.
func (tc *TypeCoercer) registerBuiltins() {
	// string -> int
	tc.converters["string->int"] = func(v interface{}) (interface{}, error) {
		s, ok := toString(v)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to string for int parsing", v)
		}
		s = strings.TrimSpace(s)
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			// Try parsing as float then truncating.
			f, ferr := strconv.ParseFloat(s, 64)
			if ferr != nil {
				return nil, fmt.Errorf("cannot parse %q as int: %w", s, err)
			}
			return int64(f), nil
		}
		return i, nil
	}

	// int -> string
	tc.converters["int->string"] = func(v interface{}) (interface{}, error) {
		i, ok := toInt64(v)
		if !ok {
			return fmt.Sprintf("%v", v), nil
		}
		return strconv.FormatInt(i, 10), nil
	}

	// string -> float
	tc.converters["string->float"] = func(v interface{}) (interface{}, error) {
		s, ok := toString(v)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to string for float parsing", v)
		}
		f, err := strconv.ParseFloat(strings.TrimSpace(s), 64)
		if err != nil {
			return nil, fmt.Errorf("cannot parse %q as float: %w", s, err)
		}
		return f, nil
	}

	// float -> string
	tc.converters["float->string"] = func(v interface{}) (interface{}, error) {
		f, ok := toFloat64(v)
		if !ok {
			return fmt.Sprintf("%v", v), nil
		}
		return strconv.FormatFloat(f, 'f', -1, 64), nil
	}

	// string -> bool
	tc.converters["string->bool"] = func(v interface{}) (interface{}, error) {
		s, ok := toString(v)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to string for bool parsing", v)
		}
		b, err := strconv.ParseBool(strings.TrimSpace(s))
		if err != nil {
			lower := strings.ToLower(strings.TrimSpace(s))
			switch lower {
			case "yes", "y", "1", "on":
				return true, nil
			case "no", "n", "0", "off":
				return false, nil
			}
			return nil, fmt.Errorf("cannot parse %q as bool: %w", s, err)
		}
		return b, nil
	}

	// bool -> string
	tc.converters["bool->string"] = func(v interface{}) (interface{}, error) {
		b, ok := v.(bool)
		if !ok {
			return fmt.Sprintf("%v", v), nil
		}
		return strconv.FormatBool(b), nil
	}

	// string -> time
	tc.converters["string->time"] = func(v interface{}) (interface{}, error) {
		s, ok := toString(v)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to string for time parsing", v)
		}
		s = strings.TrimSpace(s)
		formats := []string{
			time.RFC3339,
			time.RFC3339Nano,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
			time.RFC1123,
			time.RFC1123Z,
		}
		for _, f := range formats {
			if t, err := time.Parse(f, s); err == nil {
				return t, nil
			}
		}
		// Try Unix timestamp as integer string.
		if ts, err := strconv.ParseInt(s, 10, 64); err == nil {
			return time.Unix(ts, 0).UTC(), nil
		}
		return nil, fmt.Errorf("cannot parse %q as time: no matching format", s)
	}

	// time -> string
	tc.converters["time->string"] = func(v interface{}) (interface{}, error) {
		switch t := v.(type) {
		case time.Time:
			return t.Format(time.RFC3339), nil
		case string:
			return t, nil
		default:
			return fmt.Sprintf("%v", v), nil
		}
	}

	// int -> float
	tc.converters["int->float"] = func(v interface{}) (interface{}, error) {
		i, ok := toInt64(v)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to int for float conversion", v)
		}
		return float64(i), nil
	}

	// float -> int
	tc.converters["float->int"] = func(v interface{}) (interface{}, error) {
		f, ok := toFloat64(v)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to float for int conversion", v)
		}
		if f != math.Trunc(f) {
			// Truncate toward zero.
		}
		return int64(f), nil
	}

	// int -> bool
	tc.converters["int->bool"] = func(v interface{}) (interface{}, error) {
		i, ok := toInt64(v)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to int for bool conversion", v)
		}
		return i != 0, nil
	}

	// bool -> int
	tc.converters["bool->int"] = func(v interface{}) (interface{}, error) {
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to bool for int conversion", v)
		}
		if b {
			return int64(1), nil
		}
		return int64(0), nil
	}

	// float -> bool
	tc.converters["float->bool"] = func(v interface{}) (interface{}, error) {
		f, ok := toFloat64(v)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to float for bool conversion", v)
		}
		return f != 0, nil
	}

	// bool -> float
	tc.converters["bool->float"] = func(v interface{}) (interface{}, error) {
		b, ok := v.(bool)
		if !ok {
			return nil, fmt.Errorf("cannot convert %T to bool for float conversion", v)
		}
		if b {
			return float64(1), nil
		}
		return float64(0), nil
	}
}

// toString coerces a value to its string representation.
func toString(v interface{}) (string, bool) {
	switch s := v.(type) {
	case string:
		return s, true
	case []byte:
		return string(s), true
	case fmt.Stringer:
		return s.String(), true
	default:
		return fmt.Sprintf("%v", v), true
	}
}

// toInt64 coerces a numeric value to int64.
func toInt64(v interface{}) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int8:
		return int64(n), true
	case int16:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float32:
		return int64(n), true
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

// toFloat64 coerces a numeric value to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float32:
		return float64(n), true
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}
