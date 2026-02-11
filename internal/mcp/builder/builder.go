// Package builder provides a fluent API for constructing MCP gateway servers.
// It simplifies the composition of transports, tools, security, and middleware
// into a fully configured gateway instance.
package builder

import (
	"fmt"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/internal/mcp/gateway"
	"github.com/flowforge/flowforge/internal/mcp/gateway/transport"
	"github.com/flowforge/flowforge/internal/mcp/security"
	"github.com/flowforge/flowforge/internal/mcp/tools"
)

// Middleware is a function that wraps a RequestHandler to add cross-cutting behavior.
type Middleware func(next transport.RequestHandler) transport.RequestHandler

// Builder provides a fluent API for assembling an MCP gateway.
type Builder struct {
	config        *common.Config
	transports    []transport.Transport
	toolDefs      []tools.ToolDefinition
	authenticator *security.MCPAuthenticator
	permissions   *security.PermissionChecker
	scopes        *security.ScopeEnforcer
	rateLimiter   *security.RateLimiter
	sandbox       *security.Sandbox
	middlewares   []Middleware
	errors        []error
}

// New creates a new builder with the given configuration.
func New(cfg *common.Config) *Builder {
	return &Builder{
		config: cfg,
	}
}

// WithTransport adds a transport to the gateway.
func (b *Builder) WithTransport(t transport.Transport) *Builder {
	if t == nil {
		b.errors = append(b.errors, fmt.Errorf("transport must not be nil"))
		return b
	}
	b.transports = append(b.transports, t)
	return b
}

// WithSSE adds an SSE transport on the specified address.
func (b *Builder) WithSSE(addr string) *Builder {
	return b.WithTransport(transport.NewSSETransport(addr))
}

// WithWebSocket adds a WebSocket transport on the specified address.
func (b *Builder) WithWebSocket(addr string) *Builder {
	return b.WithTransport(transport.NewWSTransport(addr))
}

// WithStdio adds a stdio transport for local processes.
func (b *Builder) WithStdio() *Builder {
	return b.WithTransport(transport.NewStdioTransport())
}

// WithTool adds a single tool definition to the gateway.
func (b *Builder) WithTool(tool tools.ToolDefinition) *Builder {
	if tool.Name == "" {
		b.errors = append(b.errors, fmt.Errorf("tool name must not be empty"))
		return b
	}
	if tool.Handler == nil {
		b.errors = append(b.errors, fmt.Errorf("tool %q must have a handler", tool.Name))
		return b
	}
	b.toolDefs = append(b.toolDefs, tool)
	return b
}

// WithTools adds multiple tool definitions to the gateway.
func (b *Builder) WithTools(toolDefs []tools.ToolDefinition) *Builder {
	for _, t := range toolDefs {
		b.WithTool(t)
	}
	return b
}

// WithAuth sets the authenticator for the gateway.
func (b *Builder) WithAuth(auth *security.MCPAuthenticator) *Builder {
	b.authenticator = auth
	return b
}

// WithPermissions sets the permission checker for the gateway.
func (b *Builder) WithPermissions(perms *security.PermissionChecker) *Builder {
	b.permissions = perms
	return b
}

// WithScopes sets the scope enforcer for the gateway.
func (b *Builder) WithScopes(scopes *security.ScopeEnforcer) *Builder {
	b.scopes = scopes
	return b
}

// WithRateLimit configures per-agent rate limiting.
func (b *Builder) WithRateLimit(requestsPerMinute int) *Builder {
	if requestsPerMinute <= 0 {
		b.errors = append(b.errors, fmt.Errorf("rate limit must be positive"))
		return b
	}
	b.rateLimiter = security.NewRateLimiter(requestsPerMinute)
	return b
}

// WithSandbox sets the execution sandbox for tool calls.
func (b *Builder) WithSandbox(sandbox *security.Sandbox) *Builder {
	b.sandbox = sandbox
	return b
}

// WithSandboxConfig creates and sets a sandbox with the given configuration.
func (b *Builder) WithSandboxConfig(cfg security.SandboxConfig) *Builder {
	b.sandbox = security.NewSandbox(cfg)
	return b
}

// WithMiddleware adds a middleware function that wraps the request handler.
// Middlewares are applied in the order they are added (first added = outermost).
func (b *Builder) WithMiddleware(mw Middleware) *Builder {
	if mw == nil {
		b.errors = append(b.errors, fmt.Errorf("middleware must not be nil"))
		return b
	}
	b.middlewares = append(b.middlewares, mw)
	return b
}

// WithConnectorTools auto-generates and registers tools for all registered connectors.
func (b *Builder) WithConnectorTools() *Builder {
	connectorTools := tools.GenerateAllConnectorTools()
	b.toolDefs = append(b.toolDefs, connectorTools...)
	return b
}

// WithSystemTools registers all built-in system tools.
func (b *Builder) WithSystemTools() *Builder {
	registry := tools.NewToolRegistry()
	tools.RegisterSystemTools(registry)
	systemTools := registry.ListTools(tools.ToolFilter{Category: "system"})
	b.toolDefs = append(b.toolDefs, systemTools...)
	return b
}

// Build assembles the gateway from all configured components.
// Returns an error if configuration is invalid.
func (b *Builder) Build() (*gateway.Gateway, error) {
	if len(b.errors) > 0 {
		return nil, fmt.Errorf("builder configuration errors: %v", b.errors)
	}

	if b.config == nil {
		return nil, fmt.Errorf("config is required")
	}

	gw := gateway.NewGateway(b.config)

	// Register transports
	for _, t := range b.transports {
		gw.AddTransport(t)
	}

	// Register tools
	for _, td := range b.toolDefs {
		gw.RegisterTool(td)
	}

	// Set security components
	if b.authenticator != nil {
		gw.SetAuthenticator(b.authenticator)
	}
	if b.permissions != nil {
		gw.SetPermissionChecker(b.permissions)
	}
	if b.scopes != nil {
		gw.SetScopeEnforcer(b.scopes)
	}
	if b.rateLimiter != nil {
		gw.SetRateLimiter(b.rateLimiter)
	}
	if b.sandbox != nil {
		gw.SetSandbox(b.sandbox)
	}

	// Apply middlewares
	for _, mw := range b.middlewares {
		gw.AddMiddleware(func(next transport.RequestHandler) transport.RequestHandler {
			return mw(next)
		})
	}

	return gw, nil
}
