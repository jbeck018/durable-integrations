// Package visual provides the data model for the visual field mapping UI.
// It bridges user-facing drag-and-drop field connections with the executable
// FieldMapping model used by the mapping engine.
package visual

import (
	"fmt"
	"strings"

	"github.com/flowforge/flowforge/internal/mapping/auto"
)

// FieldInfo describes a single field in either the source or destination schema.
type FieldInfo struct {
	Path        string `json:"path"`
	DisplayName string `json:"display_name"`
	Type        string `json:"type"`
	Required    bool   `json:"required"`
	Description string `json:"description,omitempty"`
}

// Connection represents a visual connection between a source field and a
// destination field, optionally with a transform expression.
type Connection struct {
	ID          string `json:"id"`
	SourceField string `json:"source_field"`
	DestField   string `json:"dest_field"`
	Transform   string `json:"transform,omitempty"`
	Validated   bool   `json:"validated"`
}

// VisualMapping is the top-level model for the field mapping UI. It holds the
// source and destination field lists and the set of connections between them.
type VisualMapping struct {
	SourceFields []FieldInfo  `json:"source_fields"`
	DestFields   []FieldInfo  `json:"dest_fields"`
	Connections  []Connection `json:"connections"`
}

// NewVisualMapping creates an empty VisualMapping.
func NewVisualMapping() *VisualMapping {
	return &VisualMapping{
		SourceFields: make([]FieldInfo, 0),
		DestFields:   make([]FieldInfo, 0),
		Connections:  make([]Connection, 0),
	}
}

// AddSourceField adds a field to the source field list.
func (vm *VisualMapping) AddSourceField(f FieldInfo) {
	vm.SourceFields = append(vm.SourceFields, f)
}

// AddDestField adds a field to the destination field list.
func (vm *VisualMapping) AddDestField(f FieldInfo) {
	vm.DestFields = append(vm.DestFields, f)
}

// Connect adds a connection between a source and destination field.
// Returns an error if either field does not exist in the model.
func (vm *VisualMapping) Connect(sourceField, destField, transform string) error {
	if !vm.hasSourceField(sourceField) {
		return fmt.Errorf("source field %q not found", sourceField)
	}
	if !vm.hasDestField(destField) {
		return fmt.Errorf("destination field %q not found", destField)
	}

	// Check for duplicate connection.
	for _, c := range vm.Connections {
		if c.SourceField == sourceField && c.DestField == destField {
			return fmt.Errorf("connection from %q to %q already exists", sourceField, destField)
		}
	}

	id := fmt.Sprintf("%s->%s", sourceField, destField)
	vm.Connections = append(vm.Connections, Connection{
		ID:          id,
		SourceField: sourceField,
		DestField:   destField,
		Transform:   transform,
		Validated:   true,
	})
	return nil
}

// Disconnect removes a connection by source and destination field.
func (vm *VisualMapping) Disconnect(sourceField, destField string) bool {
	for i, c := range vm.Connections {
		if c.SourceField == sourceField && c.DestField == destField {
			vm.Connections = append(vm.Connections[:i], vm.Connections[i+1:]...)
			return true
		}
	}
	return false
}

// ValidationError describes a single validation failure.
type ValidationError struct {
	ConnectionID string `json:"connection_id"`
	Field        string `json:"field"`
	Message      string `json:"message"`
}

// Validate checks all connections for validity: both endpoints must exist in
// their respective field lists. Returns nil if all connections are valid.
func (vm *VisualMapping) Validate() []ValidationError {
	srcIndex := make(map[string]FieldInfo, len(vm.SourceFields))
	for _, f := range vm.SourceFields {
		srcIndex[f.Path] = f
	}
	dstIndex := make(map[string]FieldInfo, len(vm.DestFields))
	for _, f := range vm.DestFields {
		dstIndex[f.Path] = f
	}

	var errs []ValidationError

	// Check that all required dest fields have a connection.
	connectedDest := make(map[string]bool, len(vm.Connections))
	for _, c := range vm.Connections {
		connectedDest[c.DestField] = true
	}
	for _, f := range vm.DestFields {
		if f.Required && !connectedDest[f.Path] {
			errs = append(errs, ValidationError{
				Field:   f.Path,
				Message: fmt.Sprintf("required destination field %q has no mapping", f.Path),
			})
		}
	}

	// Validate each connection.
	destUsed := make(map[string]bool, len(vm.Connections))
	for i := range vm.Connections {
		c := &vm.Connections[i]

		if _, ok := srcIndex[c.SourceField]; !ok {
			c.Validated = false
			errs = append(errs, ValidationError{
				ConnectionID: c.ID,
				Field:        c.SourceField,
				Message:      fmt.Sprintf("source field %q does not exist", c.SourceField),
			})
		}

		if _, ok := dstIndex[c.DestField]; !ok {
			c.Validated = false
			errs = append(errs, ValidationError{
				ConnectionID: c.ID,
				Field:        c.DestField,
				Message:      fmt.Sprintf("destination field %q does not exist", c.DestField),
			})
		}

		// Check for multiple sources mapping to the same dest (warn only).
		if destUsed[c.DestField] {
			errs = append(errs, ValidationError{
				ConnectionID: c.ID,
				Field:        c.DestField,
				Message:      fmt.Sprintf("destination field %q has multiple source mappings", c.DestField),
			})
		}
		destUsed[c.DestField] = true

		if c.Validated {
			// Type compatibility check.
			srcType := srcIndex[c.SourceField].Type
			dstType := dstIndex[c.DestField].Type
			if srcType != "" && dstType != "" && srcType != dstType && c.Transform == "" {
				errs = append(errs, ValidationError{
					ConnectionID: c.ID,
					Field:        c.SourceField,
					Message:      fmt.Sprintf("type mismatch %q -> %q without transform", srcType, dstType),
				})
			}
		}
	}

	if len(errs) == 0 {
		return nil
	}
	return errs
}

// ToFieldMappings converts the visual model to the execution FieldMapping slice.
func (vm *VisualMapping) ToFieldMappings() []auto.FieldMapping {
	mappings := make([]auto.FieldMapping, 0, len(vm.Connections))
	for _, c := range vm.Connections {
		mappings = append(mappings, auto.FieldMapping{
			SourceField: c.SourceField,
			DestField:   c.DestField,
			Transform:   c.Transform,
			Confidence:  1.0, // user-defined connections have full confidence
			AutoMatched: false,
		})
	}
	return mappings
}

// FromFieldMappings populates the Connections list from an execution-model
// FieldMapping slice. Source and destination field lists must already be
// populated. Connections to fields not in the model are still added but
// marked as not validated.
func (vm *VisualMapping) FromFieldMappings(mappings []auto.FieldMapping) {
	srcSet := make(map[string]bool, len(vm.SourceFields))
	for _, f := range vm.SourceFields {
		srcSet[f.Path] = true
	}
	dstSet := make(map[string]bool, len(vm.DestFields))
	for _, f := range vm.DestFields {
		dstSet[f.Path] = true
	}

	vm.Connections = make([]Connection, 0, len(mappings))
	for _, m := range mappings {
		validated := srcSet[m.SourceField] && dstSet[m.DestField]
		id := fmt.Sprintf("%s->%s", m.SourceField, m.DestField)
		vm.Connections = append(vm.Connections, Connection{
			ID:          id,
			SourceField: m.SourceField,
			DestField:   m.DestField,
			Transform:   m.Transform,
			Validated:   validated,
		})
	}
}

// UnmappedSourceFields returns source fields that have no connection.
func (vm *VisualMapping) UnmappedSourceFields() []FieldInfo {
	mapped := make(map[string]bool, len(vm.Connections))
	for _, c := range vm.Connections {
		mapped[c.SourceField] = true
	}
	var result []FieldInfo
	for _, f := range vm.SourceFields {
		if !mapped[f.Path] {
			result = append(result, f)
		}
	}
	return result
}

// UnmappedDestFields returns destination fields that have no connection.
func (vm *VisualMapping) UnmappedDestFields() []FieldInfo {
	mapped := make(map[string]bool, len(vm.Connections))
	for _, c := range vm.Connections {
		mapped[c.DestField] = true
	}
	var result []FieldInfo
	for _, f := range vm.DestFields {
		if !mapped[f.Path] {
			result = append(result, f)
		}
	}
	return result
}

// Summary returns a human-readable summary of the visual mapping.
func (vm *VisualMapping) Summary() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Visual Mapping: %d source fields, %d dest fields, %d connections\n",
		len(vm.SourceFields), len(vm.DestFields), len(vm.Connections)))
	for _, c := range vm.Connections {
		status := "valid"
		if !c.Validated {
			status = "INVALID"
		}
		if c.Transform != "" {
			sb.WriteString(fmt.Sprintf("  %s -> %s [%s] (%s)\n", c.SourceField, c.DestField, c.Transform, status))
		} else {
			sb.WriteString(fmt.Sprintf("  %s -> %s (%s)\n", c.SourceField, c.DestField, status))
		}
	}
	return sb.String()
}

func (vm *VisualMapping) hasSourceField(path string) bool {
	for _, f := range vm.SourceFields {
		if f.Path == path {
			return true
		}
	}
	return false
}

func (vm *VisualMapping) hasDestField(path string) bool {
	for _, f := range vm.DestFields {
		if f.Path == path {
			return true
		}
	}
	return false
}
