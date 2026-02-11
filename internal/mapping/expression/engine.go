// Package expression provides a lightweight expression evaluation engine for
// data mapping transforms. It supports field references, built-in functions,
// and basic arithmetic without requiring a full scripting language.
package expression

import (
	"fmt"
	"math"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
)

// ExprFunc is a user-defined or built-in function callable from expressions.
type ExprFunc func(args ...interface{}) (interface{}, error)

// ExprEngine evaluates mapping expressions against data records.
// Expressions support field references ($.field_name), function calls
// (func_name(arg1, arg2)), string literals, numeric literals, and basic
// arithmetic (+, -, *, /).
type ExprEngine struct {
	mu    sync.RWMutex
	funcs map[string]ExprFunc
}

// NewExprEngine creates an ExprEngine pre-loaded with all built-in functions.
func NewExprEngine() *ExprEngine {
	e := &ExprEngine{
		funcs: make(map[string]ExprFunc),
	}
	e.registerBuiltins()
	return e
}

// RegisterFunc adds a custom function to the engine. It can override built-ins.
func (e *ExprEngine) RegisterFunc(name string, fn ExprFunc) {
	e.mu.Lock()
	e.funcs[name] = fn
	e.mu.Unlock()
}

// getFunc returns a function by name.
func (e *ExprEngine) getFunc(name string) (ExprFunc, bool) {
	e.mu.RLock()
	fn, ok := e.funcs[name]
	e.mu.RUnlock()
	return fn, ok
}

// Evaluate parses and evaluates an expression against the provided data map.
// Field references use $.field_name or $.nested.path syntax.
func (e *ExprEngine) Evaluate(expr string, data map[string]interface{}) (interface{}, error) {
	expr = strings.TrimSpace(expr)
	if expr == "" {
		return nil, fmt.Errorf("empty expression")
	}
	tokens, err := tokenize(expr)
	if err != nil {
		return nil, fmt.Errorf("tokenize: %w", err)
	}
	p := &parser{tokens: tokens, pos: 0, engine: e, data: data}
	result, err := p.parseExpression()
	if err != nil {
		return nil, fmt.Errorf("evaluate %q: %w", expr, err)
	}
	return result, nil
}

// token types for the expression parser.
type tokenType int

const (
	tokNumber tokenType = iota
	tokString
	tokField  // $.something
	tokIdent  // function names
	tokLParen // (
	tokRParen // )
	tokComma  // ,
	tokPlus   // +
	tokMinus  // -
	tokStar   // *
	tokSlash  // /
	tokEOF
)

type token struct {
	typ tokenType
	val string
}

// tokenize breaks an expression string into tokens.
func tokenize(expr string) ([]token, error) {
	var tokens []token
	runes := []rune(expr)
	i := 0

	for i < len(runes) {
		ch := runes[i]

		// Skip whitespace.
		if unicode.IsSpace(ch) {
			i++
			continue
		}

		// String literal (double or single quote).
		if ch == '"' || ch == '\'' {
			quote := ch
			i++
			start := i
			for i < len(runes) && runes[i] != quote {
				if runes[i] == '\\' && i+1 < len(runes) {
					i++ // skip escaped char
				}
				i++
			}
			if i >= len(runes) {
				return nil, fmt.Errorf("unterminated string literal")
			}
			tokens = append(tokens, token{typ: tokString, val: string(runes[start:i])})
			i++ // skip closing quote
			continue
		}

		// Field reference: $.field.path
		if ch == '$' && i+1 < len(runes) && runes[i+1] == '.' {
			i += 2 // skip $.
			start := i
			for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_' || runes[i] == '.' || runes[i] == '[' || runes[i] == ']') {
				i++
			}
			tokens = append(tokens, token{typ: tokField, val: string(runes[start:i])})
			continue
		}

		// Number literal.
		if unicode.IsDigit(ch) || (ch == '-' && i+1 < len(runes) && unicode.IsDigit(runes[i+1]) && (len(tokens) == 0 || tokens[len(tokens)-1].typ == tokLParen || tokens[len(tokens)-1].typ == tokComma || tokens[len(tokens)-1].typ == tokPlus || tokens[len(tokens)-1].typ == tokMinus || tokens[len(tokens)-1].typ == tokStar || tokens[len(tokens)-1].typ == tokSlash)) {
			start := i
			if ch == '-' {
				i++
			}
			for i < len(runes) && (unicode.IsDigit(runes[i]) || runes[i] == '.') {
				i++
			}
			tokens = append(tokens, token{typ: tokNumber, val: string(runes[start:i])})
			continue
		}

		// Identifier (function name or keyword).
		if unicode.IsLetter(ch) || ch == '_' {
			start := i
			for i < len(runes) && (unicode.IsLetter(runes[i]) || unicode.IsDigit(runes[i]) || runes[i] == '_') {
				i++
			}
			tokens = append(tokens, token{typ: tokIdent, val: string(runes[start:i])})
			continue
		}

		switch ch {
		case '(':
			tokens = append(tokens, token{typ: tokLParen, val: "("})
		case ')':
			tokens = append(tokens, token{typ: tokRParen, val: ")"})
		case ',':
			tokens = append(tokens, token{typ: tokComma, val: ","})
		case '+':
			tokens = append(tokens, token{typ: tokPlus, val: "+"})
		case '-':
			tokens = append(tokens, token{typ: tokMinus, val: "-"})
		case '*':
			tokens = append(tokens, token{typ: tokStar, val: "*"})
		case '/':
			tokens = append(tokens, token{typ: tokSlash, val: "/"})
		default:
			return nil, fmt.Errorf("unexpected character %q at position %d", string(ch), i)
		}
		i++
	}

	tokens = append(tokens, token{typ: tokEOF, val: ""})
	return tokens, nil
}

// parser evaluates a token stream against data.
type parser struct {
	tokens []token
	pos    int
	engine *ExprEngine
	data   map[string]interface{}
}

func (p *parser) peek() token {
	if p.pos < len(p.tokens) {
		return p.tokens[p.pos]
	}
	return token{typ: tokEOF}
}

func (p *parser) advance() token { //nolint:unparam
	t := p.peek()
	if p.pos < len(p.tokens) {
		p.pos++
	}
	return t
}

// parseExpression handles addition and subtraction (lowest precedence).
func (p *parser) parseExpression() (interface{}, error) {
	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for {
		t := p.peek()
		if t.typ == tokPlus || t.typ == tokMinus {
			p.advance()
			right, rErr := p.parseTerm()
			if rErr != nil {
				return nil, rErr
			}
			if t.typ == tokPlus {
				// String concatenation if either side is a string.
				ls, lok := left.(string)
				rs, rok := right.(string)
				if lok || rok {
					if !lok {
						ls = fmt.Sprintf("%v", left)
					}
					if !rok {
						rs = fmt.Sprintf("%v", right)
					}
					left = ls + rs
					continue
				}
				lf, lfErr := toFloat(left)
				rf, rfErr := toFloat(right)
				if lfErr != nil || rfErr != nil {
					return nil, fmt.Errorf("cannot add %T and %T", left, right)
				}
				left = lf + rf
			} else {
				lf, lfErr := toFloat(left)
				rf, rfErr := toFloat(right)
				if lfErr != nil || rfErr != nil {
					return nil, fmt.Errorf("cannot subtract %T and %T", left, right)
				}
				left = lf - rf
			}
		} else {
			break
		}
	}
	return left, nil
}

// parseTerm handles multiplication and division.
func (p *parser) parseTerm() (interface{}, error) {
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for {
		t := p.peek()
		if t.typ == tokStar || t.typ == tokSlash {
			p.advance()
			right, rErr := p.parseUnary()
			if rErr != nil {
				return nil, rErr
			}
			lf, lfErr := toFloat(left)
			rf, rfErr := toFloat(right)
			if lfErr != nil || rfErr != nil {
				return nil, fmt.Errorf("cannot perform arithmetic on %T and %T", left, right)
			}
			if t.typ == tokStar {
				left = lf * rf
			} else {
				if rf == 0 {
					return nil, fmt.Errorf("division by zero")
				}
				left = lf / rf
			}
		} else {
			break
		}
	}
	return left, nil
}

// parseUnary handles unary minus.
func (p *parser) parseUnary() (interface{}, error) {
	if p.peek().typ == tokMinus {
		p.advance()
		val, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		f, fErr := toFloat(val)
		if fErr != nil {
			return nil, fmt.Errorf("cannot negate %T", val)
		}
		return -f, nil
	}
	return p.parsePrimary()
}

// parsePrimary handles literals, field refs, function calls, and parenthesized exprs.
func (p *parser) parsePrimary() (interface{}, error) {
	t := p.peek()

	switch t.typ {
	case tokNumber:
		p.advance()
		f, err := strconv.ParseFloat(t.val, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid number %q: %w", t.val, err)
		}
		return f, nil

	case tokString:
		p.advance()
		return t.val, nil

	case tokField:
		p.advance()
		return resolveField(p.data, t.val), nil

	case tokIdent:
		p.advance()
		name := t.val

		// Check for boolean literals.
		if name == "true" {
			return true, nil
		}
		if name == "false" {
			return false, nil
		}
		if name == "null" || name == "nil" {
			return nil, nil
		}

		// Must be a function call.
		if p.peek().typ != tokLParen {
			return nil, fmt.Errorf("expected '(' after function name %q", name)
		}
		p.advance() // consume '('

		var args []interface{}
		if p.peek().typ != tokRParen {
			for {
				arg, err := p.parseExpression()
				if err != nil {
					return nil, err
				}
				args = append(args, arg)
				if p.peek().typ == tokComma {
					p.advance()
				} else {
					break
				}
			}
		}
		if p.peek().typ != tokRParen {
			return nil, fmt.Errorf("expected ')' after arguments to %q", name)
		}
		p.advance() // consume ')'

		fn, ok := p.engine.getFunc(name)
		if !ok {
			return nil, fmt.Errorf("unknown function %q", name)
		}
		return fn(args...)

	case tokLParen:
		p.advance()
		val, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if p.peek().typ != tokRParen {
			return nil, fmt.Errorf("expected ')'")
		}
		p.advance()
		return val, nil

	default:
		return nil, fmt.Errorf("unexpected token %q", t.val)
	}
}

// resolveField navigates a dot-delimited path in a nested map.
func resolveField(data map[string]interface{}, path string) interface{} {
	parts := strings.Split(path, ".")
	var current interface{} = data
	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current, ok = m[part]
		if !ok {
			return nil
		}
	}
	return current
}

// toFloat converts a value to float64.
func toFloat(v interface{}) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case string:
		return strconv.ParseFloat(val, 64)
	case bool:
		if val {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func toBool(v interface{}) bool {
	switch val := v.(type) {
	case bool:
		return val
	case float64:
		return val != 0
	case int:
		return val != 0
	case int64:
		return val != 0
	case string:
		return val != "" && val != "false" && val != "0"
	case nil:
		return false
	default:
		return true
	}
}

func toString(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == math.Trunc(val) {
			return strconv.FormatInt(int64(val), 10)
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func toInt(v interface{}) (int64, error) {
	switch val := v.(type) {
	case float64:
		return int64(val), nil
	case int:
		return int64(val), nil
	case int64:
		return val, nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	case bool:
		if val {
			return 1, nil
		}
		return 0, nil
	case nil:
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}

// registerBuiltins registers all built-in functions in the engine.
func (e *ExprEngine) registerBuiltins() {
	e.funcs["concat"] = func(args ...interface{}) (interface{}, error) {
		var sb strings.Builder
		for _, a := range args {
			sb.WriteString(toString(a))
		}
		return sb.String(), nil
	}

	e.funcs["upper"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("upper requires 1 argument, got %d", len(args))
		}
		return strings.ToUpper(toString(args[0])), nil
	}

	e.funcs["lower"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("lower requires 1 argument, got %d", len(args))
		}
		return strings.ToLower(toString(args[0])), nil
	}

	e.funcs["trim"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("trim requires 1 argument, got %d", len(args))
		}
		return strings.TrimSpace(toString(args[0])), nil
	}

	e.funcs["split"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("split requires 2 arguments, got %d", len(args))
		}
		parts := strings.Split(toString(args[0]), toString(args[1]))
		result := make([]interface{}, len(parts))
		for i, p := range parts {
			result[i] = p
		}
		return result, nil
	}

	e.funcs["join"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("join requires 2 arguments (array, separator), got %d", len(args))
		}
		arr, ok := args[0].([]interface{})
		if !ok {
			return nil, fmt.Errorf("join first argument must be an array")
		}
		sep := toString(args[1])
		strs := make([]string, len(arr))
		for i, v := range arr {
			strs[i] = toString(v)
		}
		return strings.Join(strs, sep), nil
	}

	e.funcs["coalesce"] = func(args ...interface{}) (interface{}, error) {
		for _, a := range args {
			if a != nil {
				if s, ok := a.(string); ok && s == "" {
					continue
				}
				return a, nil
			}
		}
		return nil, nil
	}

	e.funcs["if_null"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("if_null requires 2 arguments, got %d", len(args))
		}
		if args[0] == nil {
			return args[1], nil
		}
		return args[0], nil
	}

	e.funcs["to_string"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("to_string requires 1 argument, got %d", len(args))
		}
		return toString(args[0]), nil
	}

	e.funcs["to_int"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("to_int requires 1 argument, got %d", len(args))
		}
		v, err := toInt(args[0])
		if err != nil {
			return nil, fmt.Errorf("to_int: %w", err)
		}
		return float64(v), nil
	}

	e.funcs["to_float"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("to_float requires 1 argument, got %d", len(args))
		}
		f, err := toFloat(args[0])
		if err != nil {
			return nil, fmt.Errorf("to_float: %w", err)
		}
		return f, nil
	}

	e.funcs["to_bool"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 1 {
			return nil, fmt.Errorf("to_bool requires 1 argument, got %d", len(args))
		}
		return toBool(args[0]), nil
	}

	e.funcs["now"] = func(args ...interface{}) (interface{}, error) {
		return time.Now().UTC().Format(time.RFC3339), nil
	}

	e.funcs["format_date"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("format_date requires 2 arguments (date_string, format), got %d", len(args))
		}
		dateStr := toString(args[0])
		format := toString(args[1])
		goFmt := convertDateFormat(format)
		t, err := parseFlexibleDate(dateStr)
		if err != nil {
			return nil, fmt.Errorf("format_date: cannot parse %q: %w", dateStr, err)
		}
		return t.Format(goFmt), nil
	}

	e.funcs["parse_date"] = func(args ...interface{}) (interface{}, error) {
		if len(args) < 1 || len(args) > 2 {
			return nil, fmt.Errorf("parse_date requires 1-2 arguments, got %d", len(args))
		}
		dateStr := toString(args[0])
		t, err := parseFlexibleDate(dateStr)
		if err != nil {
			return nil, fmt.Errorf("parse_date: cannot parse %q: %w", dateStr, err)
		}
		return t.UTC().Format(time.RFC3339), nil
	}

	e.funcs["substring"] = func(args ...interface{}) (interface{}, error) {
		if len(args) < 2 || len(args) > 3 {
			return nil, fmt.Errorf("substring requires 2-3 arguments (string, start[, length]), got %d", len(args))
		}
		s := toString(args[0])
		start, err := toInt(args[1])
		if err != nil {
			return nil, fmt.Errorf("substring start: %w", err)
		}
		runes := []rune(s)
		if start < 0 {
			start = 0
		}
		if int(start) >= len(runes) {
			return "", nil
		}
		if len(args) == 3 {
			length, lErr := toInt(args[2])
			if lErr != nil {
				return nil, fmt.Errorf("substring length: %w", lErr)
			}
			end := int(start) + int(length)
			if end > len(runes) {
				end = len(runes)
			}
			return string(runes[start:end]), nil
		}
		return string(runes[start:]), nil
	}

	e.funcs["replace"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 3 {
			return nil, fmt.Errorf("replace requires 3 arguments (string, old, new), got %d", len(args))
		}
		return strings.ReplaceAll(toString(args[0]), toString(args[1]), toString(args[2])), nil
	}

	e.funcs["regex_match"] = func(args ...interface{}) (interface{}, error) {
		if len(args) != 2 {
			return nil, fmt.Errorf("regex_match requires 2 arguments (string, pattern), got %d", len(args))
		}
		s := toString(args[0])
		pattern := toString(args[1])
		re, err := regexp.Compile(pattern)
		if err != nil {
			return nil, fmt.Errorf("regex_match: invalid pattern %q: %w", pattern, err)
		}
		return re.MatchString(s), nil
	}
}

// parseFlexibleDate tries several common date formats.
func parseFlexibleDate(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02",
		"01/02/2006",
		"02-Jan-2006",
		"Jan 2, 2006",
		time.RFC1123,
		time.RFC822,
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized date format: %s", s)
}

// convertDateFormat converts common date format tokens (YYYY, MM, DD, etc.)
// to Go's reference time format.
func convertDateFormat(format string) string {
	replacer := strings.NewReplacer(
		"YYYY", "2006",
		"YY", "06",
		"MM", "01",
		"DD", "02",
		"HH", "15",
		"mm", "04",
		"ss", "05",
		"SSS", "000",
	)
	return replacer.Replace(format)
}
