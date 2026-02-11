// Package security provides authentication, authorization, rate limiting,
// and sandboxed execution for MCP tool calls.
package security

import (
	"context"
	"crypto/subtle"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/flowforge/flowforge/internal/common"
)

// MCPAuthenticator validates bearer tokens presented by MCP clients.
// Tokens are mapped to agent identities with associated scopes.
type MCPAuthenticator struct {
	mu     sync.RWMutex
	tokens map[string]*AgentIdentity
}

// AgentIdentity represents an authenticated MCP agent.
type AgentIdentity struct {
	AgentID  string   `json:"agent_id"`
	TenantID string   `json:"tenant_id"`
	Scopes   []string `json:"scopes"`
	IssuedAt time.Time `json:"issued_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

// NewMCPAuthenticator creates a new authenticator.
func NewMCPAuthenticator() *MCPAuthenticator {
	return &MCPAuthenticator{
		tokens: make(map[string]*AgentIdentity),
	}
}

// RegisterToken registers an API token mapped to an agent identity.
func (a *MCPAuthenticator) RegisterToken(token string, identity *AgentIdentity) {
	a.mu.Lock()
	a.tokens[token] = identity
	a.mu.Unlock()
}

// RevokeToken removes a token from the authenticator.
func (a *MCPAuthenticator) RevokeToken(token string) {
	a.mu.Lock()
	delete(a.tokens, token)
	a.mu.Unlock()
}

// Authenticate validates a bearer token and returns the associated identity.
// Returns common.ErrUnauthorized if the token is invalid or expired.
func (a *MCPAuthenticator) Authenticate(ctx context.Context, token string) (*AgentIdentity, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	for stored, identity := range a.tokens {
		if subtle.ConstantTimeCompare([]byte(token), []byte(stored)) == 1 {
			if !identity.ExpiresAt.IsZero() && time.Now().After(identity.ExpiresAt) {
				return nil, fmt.Errorf("%w: token expired", common.ErrUnauthorized)
			}
			return identity, nil
		}
	}
	return nil, common.ErrUnauthorized
}

// PermissionChecker verifies whether an agent has permission to invoke a specific tool.
type PermissionChecker struct {
	mu    sync.RWMutex
	rules map[string][]PermissionRule
}

// PermissionRule defines an access rule for a tool pattern.
type PermissionRule struct {
	ToolPattern string `json:"tool_pattern"` // glob-like: "postgres_*", "*", "list_connectors"
	Allow       bool   `json:"allow"`
}

// NewPermissionChecker creates a new permission checker.
func NewPermissionChecker() *PermissionChecker {
	return &PermissionChecker{
		rules: make(map[string][]PermissionRule),
	}
}

// SetRules configures permission rules for a specific agent.
func (p *PermissionChecker) SetRules(agentID string, rules []PermissionRule) {
	p.mu.Lock()
	p.rules[agentID] = rules
	p.mu.Unlock()
}

// Check returns true if the given agent is permitted to call the named tool.
// If no rules are configured for the agent, access is denied by default.
func (p *PermissionChecker) Check(agentID, toolName string) bool {
	p.mu.RLock()
	rules, ok := p.rules[agentID]
	p.mu.RUnlock()

	if !ok || len(rules) == 0 {
		return false
	}

	// Evaluate rules in order; last matching rule wins
	allowed := false
	for _, rule := range rules {
		if matchPattern(rule.ToolPattern, toolName) {
			allowed = rule.Allow
		}
	}
	return allowed
}

// matchPattern performs simple glob matching where * matches any sequence of characters.
func matchPattern(pattern, name string) bool {
	if pattern == "*" {
		return true
	}
	if !strings.Contains(pattern, "*") {
		return pattern == name
	}
	// Handle prefix* patterns
	if strings.HasSuffix(pattern, "*") {
		prefix := strings.TrimSuffix(pattern, "*")
		return strings.HasPrefix(name, prefix)
	}
	// Handle *suffix patterns
	if strings.HasPrefix(pattern, "*") {
		suffix := strings.TrimPrefix(pattern, "*")
		return strings.HasSuffix(name, suffix)
	}
	// Handle prefix*suffix patterns
	parts := strings.SplitN(pattern, "*", 2)
	return strings.HasPrefix(name, parts[0]) && strings.HasSuffix(name, parts[1])
}

// ScopeEnforcer limits tool access based on the scopes associated with an API key.
// Scopes use a hierarchical naming convention: "tools:read", "tools:write",
// "connectors:*", "system:*".
type ScopeEnforcer struct {
	scopeMap map[string][]string // tool name -> required scopes
	mu       sync.RWMutex
}

// NewScopeEnforcer creates a new scope enforcer.
func NewScopeEnforcer() *ScopeEnforcer {
	return &ScopeEnforcer{
		scopeMap: make(map[string][]string),
	}
}

// SetToolScopes defines the required scopes for a specific tool.
func (s *ScopeEnforcer) SetToolScopes(toolName string, requiredScopes []string) {
	s.mu.Lock()
	s.scopeMap[toolName] = requiredScopes
	s.mu.Unlock()
}

// Enforce checks whether the given scopes satisfy the requirements for a tool.
// If no scope requirements are configured for a tool, access is allowed.
func (s *ScopeEnforcer) Enforce(toolName string, agentScopes []string) error {
	s.mu.RLock()
	required, ok := s.scopeMap[toolName]
	s.mu.RUnlock()

	if !ok || len(required) == 0 {
		return nil
	}

	agentSet := make(map[string]struct{}, len(agentScopes))
	for _, scope := range agentScopes {
		agentSet[scope] = struct{}{}
	}

	for _, req := range required {
		if _, found := agentSet[req]; found {
			continue
		}
		// Check for wildcard scopes: "connectors:*" matches "connectors:read"
		parts := strings.SplitN(req, ":", 2)
		if len(parts) == 2 {
			wildcard := parts[0] + ":*"
			if _, found := agentSet[wildcard]; found {
				continue
			}
		}
		// Check for global wildcard
		if _, found := agentSet["*"]; found {
			continue
		}
		return fmt.Errorf("%w: missing scope %q for tool %q", common.ErrForbidden, req, toolName)
	}
	return nil
}

// RateLimiter provides per-agent rate limiting using a sliding window counter.
type RateLimiter struct {
	mu       sync.Mutex
	windows  map[string]*rateLimitWindow
	limit    int
	interval time.Duration
}

type rateLimitWindow struct {
	count     int
	windowStart time.Time
}

// NewRateLimiter creates a rate limiter with the specified requests per interval.
func NewRateLimiter(requestsPerMinute int) *RateLimiter {
	return &RateLimiter{
		windows:  make(map[string]*rateLimitWindow),
		limit:    requestsPerMinute,
		interval: time.Minute,
	}
}

// Allow checks if an agent is within their rate limit. Returns an error
// if the rate limit is exceeded.
func (r *RateLimiter) Allow(agentID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now()
	w, ok := r.windows[agentID]
	if !ok || now.Sub(w.windowStart) >= r.interval {
		r.windows[agentID] = &rateLimitWindow{
			count:       1,
			windowStart: now,
		}
		return nil
	}

	w.count++
	if w.count > r.limit {
		return fmt.Errorf("%w: agent %s exceeded %d requests per %s",
			common.ErrRateLimited, agentID, r.limit, r.interval)
	}
	return nil
}

// Reset clears the rate limit window for a specific agent.
func (r *RateLimiter) Reset(agentID string) {
	r.mu.Lock()
	delete(r.windows, agentID)
	r.mu.Unlock()
}

// Cleanup removes expired windows to prevent memory leaks.
// Should be called periodically.
func (r *RateLimiter) Cleanup() {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	for id, w := range r.windows {
		if now.Sub(w.windowStart) >= r.interval*2 {
			delete(r.windows, id)
		}
	}
}
