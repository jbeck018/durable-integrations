// Package gateway implements the MCP (Model Context Protocol) gateway server.
// It manages transports, routes JSON-RPC requests to the tool registry,
// and enforces security policies for all MCP interactions.
package gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/internal/mcp/gateway/transport"
	"github.com/flowforge/flowforge/internal/mcp/security"
	"github.com/flowforge/flowforge/internal/mcp/tools"
)

// MCP protocol version supported by this gateway.
const mcpProtocolVersion = "2024-11-05"

// Gateway is the central MCP server that manages transports, tool routing,
// and security enforcement.
type Gateway struct {
	cfg         *common.Config
	registry    *tools.ToolRegistry
	transports  []transport.Transport
	auth        *security.MCPAuthenticator
	permissions *security.PermissionChecker
	scopes      *security.ScopeEnforcer
	rateLimiter *security.RateLimiter
	sandbox     *security.Sandbox
	middlewares []func(transport.RequestHandler) transport.RequestHandler
	mu          sync.RWMutex
	started     bool
	serverInfo  serverInfo
}

type serverInfo struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

// NewGateway creates a new MCP gateway with the given configuration.
func NewGateway(cfg *common.Config) *Gateway {
	return &Gateway{
		cfg:      cfg,
		registry: tools.NewToolRegistry(),
		sandbox:  security.NewSandbox(security.SandboxConfig{}),
		serverInfo: serverInfo{
			Name:    cfg.ServiceName,
			Version: "1.0.0",
		},
	}
}

// AddTransport registers a transport with the gateway.
func (g *Gateway) AddTransport(t transport.Transport) {
	g.mu.Lock()
	g.transports = append(g.transports, t)
	g.mu.Unlock()
}

// RegisterTool adds a tool definition to the gateway's registry.
func (g *Gateway) RegisterTool(tool tools.ToolDefinition) {
	g.registry.RegisterTool(tool)
}

// SetAuthenticator sets the authenticator for the gateway.
func (g *Gateway) SetAuthenticator(auth *security.MCPAuthenticator) {
	g.mu.Lock()
	g.auth = auth
	g.mu.Unlock()
}

// SetPermissionChecker sets the permission checker.
func (g *Gateway) SetPermissionChecker(perms *security.PermissionChecker) {
	g.mu.Lock()
	g.permissions = perms
	g.mu.Unlock()
}

// SetScopeEnforcer sets the scope enforcer.
func (g *Gateway) SetScopeEnforcer(scopes *security.ScopeEnforcer) {
	g.mu.Lock()
	g.scopes = scopes
	g.mu.Unlock()
}

// SetRateLimiter sets the rate limiter.
func (g *Gateway) SetRateLimiter(rl *security.RateLimiter) {
	g.mu.Lock()
	g.rateLimiter = rl
	g.mu.Unlock()
}

// SetSandbox sets the execution sandbox.
func (g *Gateway) SetSandbox(sb *security.Sandbox) {
	g.mu.Lock()
	g.sandbox = sb
	g.mu.Unlock()
}

// AddMiddleware appends a middleware to the handler chain.
func (g *Gateway) AddMiddleware(mw func(transport.RequestHandler) transport.RequestHandler) {
	g.mu.Lock()
	g.middlewares = append(g.middlewares, mw)
	g.mu.Unlock()
}

// Start launches all registered transports and begins serving requests.
// It blocks until the context is cancelled.
func (g *Gateway) Start(ctx context.Context) error {
	g.mu.Lock()
	if g.started {
		g.mu.Unlock()
		return fmt.Errorf("gateway already started")
	}
	g.started = true
	transports := make([]transport.Transport, len(g.transports))
	copy(transports, g.transports)
	g.mu.Unlock()

	if len(transports) == 0 {
		return fmt.Errorf("no transports configured")
	}

	// Build the handler chain with middlewares
	handler := g.buildHandler()

	// Start rate limiter cleanup
	if g.rateLimiter != nil {
		go g.runCleanup(ctx)
	}

	errCh := make(chan error, len(transports))
	var wg sync.WaitGroup

	for _, t := range transports {
		wg.Add(1)
		go func(tr transport.Transport) {
			defer wg.Done()
			log.Printf("MCP gateway: starting %s transport", tr.Name())
			if err := tr.Start(ctx, handler); err != nil {
				errCh <- fmt.Errorf("transport %s: %w", tr.Name(), err)
			}
		}(t)
	}

	// Wait for context cancellation or first transport error
	select {
	case <-ctx.Done():
		// Graceful shutdown — stop all transports
		return g.Stop(context.Background())
	case err := <-errCh:
		// One transport failed; stop everything
		_ = g.Stop(context.Background())
		return err
	}
}

// Stop gracefully shuts down all transports.
func (g *Gateway) Stop(ctx context.Context) error {
	g.mu.Lock()
	g.started = false
	transports := make([]transport.Transport, len(g.transports))
	copy(transports, g.transports)
	g.mu.Unlock()

	var firstErr error
	for _, t := range transports {
		log.Printf("MCP gateway: stopping %s transport", t.Name())
		if err := t.Stop(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// HandleRequest processes a single JSON-RPC request through the full
// routing, security, and execution pipeline. Exposed for direct invocation
// by tests and the builder.
func (g *Gateway) HandleRequest(ctx context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	return g.buildHandler()(ctx, req)
}

// buildHandler constructs the request handler chain with all configured middlewares.
func (g *Gateway) buildHandler() transport.RequestHandler {
	var handler transport.RequestHandler = g.routeRequest

	g.mu.RLock()
	mws := make([]func(transport.RequestHandler) transport.RequestHandler, len(g.middlewares))
	copy(mws, g.middlewares)
	g.mu.RUnlock()

	// Apply middlewares in reverse order so the first added is the outermost
	for i := len(mws) - 1; i >= 0; i-- {
		handler = mws[i](handler)
	}
	return handler
}

// routeRequest dispatches a JSON-RPC request to the appropriate handler method.
func (g *Gateway) routeRequest(ctx context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	switch req.Method {
	case "initialize":
		return g.handleInitialize(ctx, req)
	case "tools/list":
		return g.handleToolsList(ctx, req)
	case "tools/call":
		return g.handleToolsCall(ctx, req)
	case "resources/list":
		return g.handleResourcesList(ctx, req)
	case "resources/read":
		return g.handleResourcesRead(ctx, req)
	case "prompts/list":
		return g.handlePromptsList(ctx, req)
	case "ping":
		return g.handlePing(ctx, req)
	case "notifications/initialized":
		// Client acknowledgment of initialization; no response needed for notifications
		return transport.NewResponse(req.ID, map[string]interface{}{})
	default:
		return transport.NewErrorResponse(req.ID, transport.CodeMethodNotFound,
			fmt.Sprintf("method %q not found", req.Method), nil), nil
	}
}

// handleInitialize implements the MCP initialize handshake.
func (g *Gateway) handleInitialize(_ context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	var params struct {
		ProtocolVersion string `json:"protocolVersion"`
		Capabilities    struct {
			Roots *struct {
				ListChanged bool `json:"listChanged"`
			} `json:"roots,omitempty"`
			Sampling *struct{} `json:"sampling,omitempty"`
		} `json:"capabilities"`
		ClientInfo struct {
			Name    string `json:"name"`
			Version string `json:"version"`
		} `json:"clientInfo"`
	}

	if len(req.Params) > 0 {
		if err := json.Unmarshal(req.Params, &params); err != nil {
			return transport.NewErrorResponse(req.ID, transport.CodeInvalidParams,
				"invalid initialize params", nil), nil
		}
	}

	result := map[string]interface{}{
		"protocolVersion": mcpProtocolVersion,
		"capabilities": map[string]interface{}{
			"tools": map[string]interface{}{
				"listChanged": true,
			},
			"resources": map[string]interface{}{
				"subscribe":   false,
				"listChanged": true,
			},
			"prompts": map[string]interface{}{
				"listChanged": false,
			},
			"logging": map[string]interface{}{},
		},
		"serverInfo": g.serverInfo,
	}

	return transport.NewResponse(req.ID, result)
}

// handleToolsList returns all available tools, optionally filtered by cursor pagination.
func (g *Gateway) handleToolsList(_ context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	var params struct {
		Cursor string `json:"cursor,omitempty"`
	}
	if len(req.Params) > 0 {
		_ = json.Unmarshal(req.Params, &params)
	}

	allTools := g.registry.ListTools(tools.ToolFilter{})

	type toolEntry struct {
		Name        string          `json:"name"`
		Description string          `json:"description,omitempty"`
		InputSchema json.RawMessage `json:"inputSchema"`
	}

	entries := make([]toolEntry, 0, len(allTools))
	for _, t := range allTools {
		schema := t.InputSchema
		if len(schema) == 0 {
			schema = json.RawMessage(`{"type":"object"}`)
		}
		entries = append(entries, toolEntry{
			Name:        t.Name,
			Description: t.Description,
			InputSchema: schema,
		})
	}

	result := map[string]interface{}{
		"tools": entries,
	}
	return transport.NewResponse(req.ID, result)
}

// handleToolsCall executes a tool by name with the provided arguments.
func (g *Gateway) handleToolsCall(ctx context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	var params struct {
		Name      string          `json:"name"`
		Arguments json.RawMessage `json:"arguments"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return transport.NewErrorResponse(req.ID, transport.CodeInvalidParams,
			"invalid tool call params", nil), nil
	}

	if params.Name == "" {
		return transport.NewErrorResponse(req.ID, transport.CodeInvalidParams,
			"tool name is required", nil), nil
	}

	tool, ok := g.registry.GetTool(params.Name)
	if !ok {
		return transport.NewErrorResponse(req.ID, transport.CodeInvalidParams,
			fmt.Sprintf("tool %q not found", params.Name), nil), nil
	}

	// Extract identity from context for security checks
	agentID := common.UserIDFrom(ctx)
	tenantID := common.TenantIDFrom(ctx)

	// Rate limiting
	g.mu.RLock()
	rl := g.rateLimiter
	perms := g.permissions
	scopeEnforcer := g.scopes
	auth := g.auth
	sb := g.sandbox
	g.mu.RUnlock()

	if rl != nil && agentID != "" {
		if err := rl.Allow(agentID); err != nil {
			return transport.NewErrorResponse(req.ID, transport.CodeInternalError,
				err.Error(), nil), nil
		}
	}

	// Permission check
	if perms != nil && agentID != "" {
		if !perms.Check(agentID, params.Name) {
			return transport.NewErrorResponse(req.ID, transport.CodeInternalError,
				fmt.Sprintf("agent %q does not have permission to call tool %q", agentID, params.Name), nil), nil
		}
	}

	// Scope enforcement
	if scopeEnforcer != nil && auth != nil && agentID != "" {
		// Look up scopes from the context; the authenticator middleware
		// would have set these before reaching the handler
		scopes := scopesFromContext(ctx)
		if err := scopeEnforcer.Enforce(params.Name, scopes); err != nil {
			return transport.NewErrorResponse(req.ID, transport.CodeInternalError,
				err.Error(), nil), nil
		}
	}

	// Execute through sandbox
	arguments := params.Arguments
	if len(arguments) == 0 {
		arguments = json.RawMessage(`{}`)
	}

	output, err := sb.Execute(ctx, tool, arguments, agentID, tenantID)
	if err != nil {
		result := map[string]interface{}{
			"content": []map[string]interface{}{
				{
					"type": "text",
					"text": fmt.Sprintf("Error: %s", err.Error()),
				},
			},
			"isError": true,
		}
		return transport.NewResponse(req.ID, result)
	}

	// Wrap the output in MCP content format
	result := map[string]interface{}{
		"content": []map[string]interface{}{
			{
				"type": "text",
				"text": string(output),
			},
		},
	}
	return transport.NewResponse(req.ID, result)
}

// handleResourcesList returns available resources (connectors as resources).
func (g *Gateway) handleResourcesList(_ context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	connectorTools := g.registry.ListTools(tools.ToolFilter{Category: "connector"})

	// Group tools by connector to produce resource entries
	connectorSet := make(map[string]bool)
	type resource struct {
		URI         string `json:"uri"`
		Name        string `json:"name"`
		Description string `json:"description,omitempty"`
		MimeType    string `json:"mimeType,omitempty"`
	}

	var resources []resource
	for _, t := range connectorTools {
		if t.Connector != "" && !connectorSet[t.Connector] {
			connectorSet[t.Connector] = true
			resources = append(resources, resource{
				URI:         fmt.Sprintf("flowforge://connector/%s", t.Connector),
				Name:        t.Connector,
				Description: fmt.Sprintf("FlowForge %s connector", t.Connector),
				MimeType:    "application/json",
			})
		}
	}

	result := map[string]interface{}{
		"resources": resources,
	}
	return transport.NewResponse(req.ID, result)
}

// handleResourcesRead reads a specific resource by URI.
func (g *Gateway) handleResourcesRead(ctx context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	var params struct {
		URI string `json:"uri"`
	}
	if err := json.Unmarshal(req.Params, &params); err != nil {
		return transport.NewErrorResponse(req.ID, transport.CodeInvalidParams,
			"invalid resource read params", nil), nil
	}

	// Parse URI: flowforge://connector/{name}
	if !strings.HasPrefix(params.URI, "flowforge://connector/") {
		return transport.NewErrorResponse(req.ID, transport.CodeInvalidParams,
			fmt.Sprintf("unsupported resource URI scheme: %s", params.URI), nil), nil
	}

	connectorName := strings.TrimPrefix(params.URI, "flowforge://connector/")
	connectorTools := g.registry.ListTools(tools.ToolFilter{Connector: connectorName})

	if len(connectorTools) == 0 {
		return transport.NewErrorResponse(req.ID, transport.CodeInvalidParams,
			fmt.Sprintf("connector %q not found", connectorName), nil), nil
	}

	type toolSummary struct {
		Name        string `json:"name"`
		Description string `json:"description"`
	}
	summaries := make([]toolSummary, 0, len(connectorTools))
	for _, t := range connectorTools {
		summaries = append(summaries, toolSummary{
			Name:        t.Name,
			Description: t.Description,
		})
	}

	info := map[string]interface{}{
		"connector": connectorName,
		"tools":     summaries,
	}
	infoBytes, _ := json.Marshal(info)

	result := map[string]interface{}{
		"contents": []map[string]interface{}{
			{
				"uri":      params.URI,
				"mimeType": "application/json",
				"text":     string(infoBytes),
			},
		},
	}
	_ = ctx
	return transport.NewResponse(req.ID, result)
}

// handlePromptsList returns available prompts (currently none built-in).
func (g *Gateway) handlePromptsList(_ context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	result := map[string]interface{}{
		"prompts": []interface{}{},
	}
	return transport.NewResponse(req.ID, result)
}

// handlePing responds to keep-alive pings.
func (g *Gateway) handlePing(_ context.Context, req *transport.JSONRPCRequest) (*transport.JSONRPCResponse, error) {
	return transport.NewResponse(req.ID, map[string]interface{}{})
}

// runCleanup periodically cleans up expired rate limit windows.
func (g *Gateway) runCleanup(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			g.mu.RLock()
			rl := g.rateLimiter
			g.mu.RUnlock()
			if rl != nil {
				rl.Cleanup()
			}
		}
	}
}

// scopeKey is the context key for agent scopes.
type scopeKey struct{}

// WithScopes attaches scopes to a context.
func WithScopes(ctx context.Context, scopes []string) context.Context {
	return context.WithValue(ctx, scopeKey{}, scopes)
}

// scopesFromContext extracts scopes from the context.
func scopesFromContext(ctx context.Context) []string {
	v, _ := ctx.Value(scopeKey{}).([]string)
	return v
}
