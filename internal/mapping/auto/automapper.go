// Package auto provides automatic field mapping between source and destination
// schemas using fuzzy name matching and type compatibility scoring.
package auto

import (
	"encoding/json"
	"math"
	"sort"
	"strings"
	"unicode"

	"github.com/flowforge/flowforge/pkg/protocol"
)

// FieldMapping represents a mapping between a source field and a destination field.
type FieldMapping struct {
	SourceField string  `json:"source_field"`
	DestField   string  `json:"dest_field"`
	Transform   string  `json:"transform,omitempty"`
	Confidence  float64 `json:"confidence"`
	AutoMatched bool    `json:"auto_matched"`
}

// schemaField is an internal representation of a field extracted from a JSON schema.
type schemaField struct {
	Path string // dot-notation path for nested fields
	Type string // JSON schema type
	Name string // leaf name
}

// AutoMapper provides automatic field mapping between source and destination
// schemas. It uses Levenshtein distance for fuzzy name matching and type
// compatibility scoring to produce ranked mapping suggestions.
type AutoMapper struct {
	// MinConfidence is the minimum confidence threshold for including a mapping
	// in the results. Defaults to 0.3.
	MinConfidence float64

	// TypeWeight controls how much type compatibility affects the score.
	// Range 0.0-1.0, default 0.3.
	TypeWeight float64

	// NameWeight controls how much name similarity affects the score.
	// Range 0.0-1.0, default 0.7.
	NameWeight float64
}

// NewAutoMapper creates an AutoMapper with sensible defaults.
func NewAutoMapper() *AutoMapper {
	return &AutoMapper{
		MinConfidence: 0.3,
		TypeWeight:    0.3,
		NameWeight:    0.7,
	}
}

// SuggestMappings analyzes source and destination catalogs and returns
// suggested field mappings with confidence scores. The returned float64
// is the overall confidence for the entire mapping set (average of all
// individual confidences).
func (am *AutoMapper) SuggestMappings(source, dest *protocol.Catalog) ([]FieldMapping, float64) {
	if source == nil || dest == nil || len(source.Streams) == 0 || len(dest.Streams) == 0 {
		return nil, 0.0
	}

	var allMappings []FieldMapping

	for _, srcStream := range source.Streams {
		// Find best matching destination stream by name.
		bestDestIdx := -1
		bestStreamScore := 0.0
		for i, dstStream := range dest.Streams {
			score := am.nameSimilarity(srcStream.Name, dstStream.Name)
			if score > bestStreamScore {
				bestStreamScore = score
				bestDestIdx = i
			}
		}
		if bestDestIdx < 0 {
			continue
		}

		destStream := dest.Streams[bestDestIdx]
		srcFields := extractFields(srcStream.Schema, "")
		destFields := extractFields(destStream.Schema, "")

		mappings := am.matchFields(srcFields, destFields)
		allMappings = append(allMappings, mappings...)
	}

	if len(allMappings) == 0 {
		return nil, 0.0
	}

	totalConf := 0.0
	for _, m := range allMappings {
		totalConf += m.Confidence
	}
	avgConf := totalConf / float64(len(allMappings))

	return allMappings, avgConf
}

// SuggestStreamMappings maps fields between two specific streams.
func (am *AutoMapper) SuggestStreamMappings(srcSchema, destSchema json.RawMessage) []FieldMapping {
	srcFields := extractFields(srcSchema, "")
	destFields := extractFields(destSchema, "")
	return am.matchFields(srcFields, destFields)
}

// matchFields produces the best 1:1 mapping between source and dest fields.
// Uses a greedy approach: score all pairs, sort by score descending, then
// assign each source field to its highest-scoring unassigned dest field.
func (am *AutoMapper) matchFields(srcFields, destFields []schemaField) []FieldMapping {
	type scoredPair struct {
		srcIdx int
		dstIdx int
		score  float64
	}

	pairs := make([]scoredPair, 0, len(srcFields)*len(destFields))
	for si, sf := range srcFields {
		for di, df := range destFields {
			score := am.fieldScore(sf, df)
			if score >= am.MinConfidence {
				pairs = append(pairs, scoredPair{srcIdx: si, dstIdx: di, score: score})
			}
		}
	}

	// Sort by score descending for greedy assignment.
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].score > pairs[j].score
	})

	usedSrc := make(map[int]bool)
	usedDst := make(map[int]bool)
	var mappings []FieldMapping

	for _, p := range pairs {
		if usedSrc[p.srcIdx] || usedDst[p.dstIdx] {
			continue
		}
		usedSrc[p.srcIdx] = true
		usedDst[p.dstIdx] = true

		sf := srcFields[p.srcIdx]
		df := destFields[p.dstIdx]

		mapping := FieldMapping{
			SourceField: sf.Path,
			DestField:   df.Path,
			Confidence:  math.Round(p.score*100) / 100,
			AutoMatched: true,
		}

		// Suggest type coercion transform when types differ.
		if sf.Type != df.Type && sf.Type != "" && df.Type != "" {
			mapping.Transform = typeCoercionTransform(sf.Type, df.Type)
		}

		mappings = append(mappings, mapping)
	}

	return mappings
}

// fieldScore computes a combined name + type similarity score in [0.0, 1.0].
func (am *AutoMapper) fieldScore(src, dest schemaField) float64 {
	nameScore := am.nameSimilarity(src.Name, dest.Name)
	typeScore := typeCompatibility(src.Type, dest.Type)
	return am.NameWeight*nameScore + am.TypeWeight*typeScore
}

// nameSimilarity returns a [0.0, 1.0] score comparing two field names.
// It normalizes names (lowercase, strip separators) and uses Levenshtein distance.
func (am *AutoMapper) nameSimilarity(a, b string) float64 {
	na := normalizeName(a)
	nb := normalizeName(b)

	if na == nb {
		return 1.0
	}
	if na == "" || nb == "" {
		return 0.0
	}

	dist := levenshteinDistance(na, nb)
	maxLen := len(na)
	if len(nb) > maxLen {
		maxLen = len(nb)
	}

	return 1.0 - float64(dist)/float64(maxLen)
}

// normalizeName converts a field name to a canonical form for comparison.
// Strips underscores, hyphens, dots; converts camelCase to lowercase.
func normalizeName(name string) string {
	var buf strings.Builder
	buf.Grow(len(name))
	for i, r := range name {
		if r == '_' || r == '-' || r == '.' || r == ' ' {
			continue
		}
		// Insert boundary at camelCase transitions for comparison purposes;
		// we just lowercase everything.
		_ = i
		buf.WriteRune(unicode.ToLower(r))
	}
	return buf.String()
}

// levenshteinDistance computes the edit distance between two strings.
// Uses the classic dynamic programming approach with O(min(m,n)) space.
func levenshteinDistance(a, b string) int {
	ra := []rune(a)
	rb := []rune(b)
	la := len(ra)
	lb := len(rb)

	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	// Ensure a is the shorter string for space optimization.
	if la > lb {
		ra, rb = rb, ra
		la, lb = lb, la
	}

	// Two-row DP approach.
	prev := make([]int, la+1)
	curr := make([]int, la+1)

	for i := 0; i <= la; i++ {
		prev[i] = i
	}

	for j := 1; j <= lb; j++ {
		curr[0] = j
		for i := 1; i <= la; i++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}
			ins := curr[i-1] + 1
			del := prev[i] + 1
			sub := prev[i-1] + cost

			min := ins
			if del < min {
				min = del
			}
			if sub < min {
				min = sub
			}
			curr[i] = min
		}
		prev, curr = curr, prev
	}
	return prev[la]
}

// typeCompatibility returns a [0.0, 1.0] score for how compatible two JSON
// schema types are. Exact match = 1.0, compatible coercion = 0.5, incompatible = 0.0.
func typeCompatibility(srcType, destType string) float64 {
	if srcType == destType {
		return 1.0
	}

	// Define compatible type pairs (bidirectional coercion).
	compatible := map[[2]string]float64{
		{"string", "number"}:   0.3,
		{"number", "string"}:   0.5,
		{"string", "integer"}:  0.3,
		{"integer", "string"}:  0.5,
		{"integer", "number"}:  0.9,
		{"number", "integer"}:  0.7,
		{"boolean", "string"}:  0.4,
		{"string", "boolean"}:  0.3,
		{"boolean", "integer"}: 0.3,
		{"integer", "boolean"}: 0.3,
	}

	key := [2]string{srcType, destType}
	if score, ok := compatible[key]; ok {
		return score
	}
	return 0.0
}

// typeCoercionTransform returns a suggested transform expression for converting
// between two JSON schema types.
func typeCoercionTransform(srcType, destType string) string {
	switch {
	case destType == "string":
		return "to_string"
	case destType == "integer" || destType == "number":
		if srcType == "string" {
			return "to_float"
		}
		if destType == "integer" {
			return "to_int"
		}
		return "to_float"
	case destType == "boolean":
		return "to_bool"
	default:
		return ""
	}
}

// extractFields recursively extracts field paths and types from a JSON schema.
func extractFields(schema json.RawMessage, prefix string) []schemaField {
	if len(schema) == 0 {
		return nil
	}

	var s map[string]json.RawMessage
	if err := json.Unmarshal(schema, &s); err != nil {
		return nil
	}

	// Check if this is an object with "properties".
	propsRaw, hasProps := s["properties"]
	if !hasProps {
		return nil
	}

	var props map[string]json.RawMessage
	if err := json.Unmarshal(propsRaw, &props); err != nil {
		return nil
	}

	var fields []schemaField
	for name, fieldSchema := range props {
		path := name
		if prefix != "" {
			path = prefix + "." + name
		}

		var fieldDef map[string]json.RawMessage
		if err := json.Unmarshal(fieldSchema, &fieldDef); err != nil {
			fields = append(fields, schemaField{Path: path, Type: "string", Name: name})
			continue
		}

		fieldType := "string"
		if typeRaw, ok := fieldDef["type"]; ok {
			var t string
			if err := json.Unmarshal(typeRaw, &t); err == nil {
				fieldType = t
			}
		}

		// Recurse into nested objects.
		if fieldType == "object" {
			nested := extractFields(fieldSchema, path)
			if len(nested) > 0 {
				fields = append(fields, nested...)
			} else {
				fields = append(fields, schemaField{Path: path, Type: fieldType, Name: name})
			}
		} else {
			fields = append(fields, schemaField{Path: path, Type: fieldType, Name: name})
		}
	}

	return fields
}
