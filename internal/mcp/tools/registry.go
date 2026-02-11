// Package tools provides the MCP tool registry and built-in tool definitions.
// Tools are the primary mechanism through which AI agents interact with
// FlowForge connectors and system operations via the MCP protocol.
package tools

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"
)

// ToolHandler is the function signature for tool execution.
// It receives the execution context and raw JSON parameters,
// and returns a result or error.
type ToolHandler func(ctx context.Context, params json.RawMessage) (json.RawMessage, error)

// ToolDefinition describes a single MCP tool available to agents.
type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	InputSchema json.RawMessage `json:"inputSchema"`
	Category    string          `json:"category,omitempty"`
	Connector   string          `json:"connector,omitempty"`
	Handler     ToolHandler     `json:"-"`
}

// ToolFilter provides criteria for filtering tools in listing operations.
type ToolFilter struct {
	Category  string `json:"category,omitempty"`
	Connector string `json:"connector,omitempty"`
	Prefix    string `json:"prefix,omitempty"`
}

// ToolRegistry manages all available MCP tools with thread-safe access.
type ToolRegistry struct {
	mu    sync.RWMutex
	tools map[string]ToolDefinition
}

// NewToolRegistry creates an empty tool registry.
func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{
		tools: make(map[string]ToolDefinition, 64),
	}
}

// RegisterTool adds a tool to the registry. If a tool with the same name
// already exists, it is replaced.
func (r *ToolRegistry) RegisterTool(tool ToolDefinition) {
	r.mu.Lock()
	r.tools[tool.Name] = tool
	r.mu.Unlock()
}

// RegisterTools adds multiple tools to the registry in a single lock acquisition.
func (r *ToolRegistry) RegisterTools(tools []ToolDefinition) {
	r.mu.Lock()
	for _, t := range tools {
		r.tools[t.Name] = t
	}
	r.mu.Unlock()
}

// UnregisterTool removes a tool from the registry by name.
func (r *ToolRegistry) UnregisterTool(name string) {
	r.mu.Lock()
	delete(r.tools, name)
	r.mu.Unlock()
}

// GetTool retrieves a tool by its exact name.
func (r *ToolRegistry) GetTool(name string) (ToolDefinition, bool) {
	r.mu.RLock()
	t, ok := r.tools[name]
	r.mu.RUnlock()
	return t, ok
}

// ListTools returns all tools matching the provided filter criteria.
// An empty filter returns all tools. Results are sorted by name.
func (r *ToolRegistry) ListTools(filter ToolFilter) []ToolDefinition {
	r.mu.RLock()
	result := make([]ToolDefinition, 0, len(r.tools))
	for _, t := range r.tools {
		if filter.Category != "" && t.Category != filter.Category {
			continue
		}
		if filter.Connector != "" && t.Connector != filter.Connector {
			continue
		}
		if filter.Prefix != "" && !strings.HasPrefix(t.Name, filter.Prefix) {
			continue
		}
		result = append(result, t)
	}
	r.mu.RUnlock()

	sort.Slice(result, func(i, j int) bool {
		return result[i].Name < result[j].Name
	})
	return result
}

// Count returns the total number of registered tools.
func (r *ToolRegistry) Count() int {
	r.mu.RLock()
	n := len(r.tools)
	r.mu.RUnlock()
	return n
}
