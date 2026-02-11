package rest

import (
	"github.com/google/uuid"
)

// generateUUID creates a new random UUID string.
func generateUUID() string {
	return uuid.New().String()
}

// NewID creates and returns a new UUID string. Exported for use by other packages
// (e.g., the GraphQL resolver layer) that need to create domain model instances.
func NewID() string {
	return uuid.New().String()
}
