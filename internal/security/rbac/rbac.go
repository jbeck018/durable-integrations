// Package rbac provides role-based access control for FlowForge.
// It evaluates whether a principal has permission to perform an action on
// a resource based on their assigned roles and scopes.
package rbac

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/flowforge/flowforge/internal/common"
	"github.com/flowforge/flowforge/internal/security/auth"
)

// Resource types managed within FlowForge.
const (
	ResourceConnector  = "connector"
	ResourceConnection = "connection"
	ResourceSync       = "sync"
	ResourceTenant     = "tenant"
	ResourceSchema     = "schema"
	ResourceMCPServer  = "mcp_server"
	ResourceAuditLog   = "audit_log"
)

// Actions that can be performed on resources.
const (
	ActionCreate  = "create"
	ActionRead    = "read"
	ActionUpdate  = "update"
	ActionDelete  = "delete"
	ActionExecute = "execute"
	ActionManage  = "manage"
)

// Permission represents the allowed actions on a specific resource type.
type Permission struct {
	Resource string   `json:"resource"`
	Actions  []string `json:"actions"`
}

// Role defines a named set of permissions.
type Role struct {
	Name        string       `json:"name"`
	Description string       `json:"description"`
	Permissions []Permission `json:"permissions"`
}

// HasPermission returns true if this role grants the specified action on the resource.
func (r *Role) HasPermission(resource, action string) bool {
	for _, perm := range r.Permissions {
		if perm.Resource == resource || perm.Resource == "*" {
			for _, a := range perm.Actions {
				if a == action || a == "*" {
					return true
				}
			}
		}
	}
	return false
}

// RoleAssignment links a principal to a role within a tenant context.
type RoleAssignment struct {
	PrincipalID string `json:"principal_id"`
	TenantID    string `json:"tenant_id"`
	RoleName    string `json:"role_name"`
}

// RoleStore defines the persistence interface for role assignments.
type RoleStore interface {
	GetRoleAssignments(ctx context.Context, principalID, tenantID string) ([]RoleAssignment, error)
	AssignRole(ctx context.Context, assignment RoleAssignment) error
	RevokeRole(ctx context.Context, principalID, tenantID, roleName string) error
}

// InMemoryRoleStore provides an in-memory implementation of RoleStore.
type InMemoryRoleStore struct {
	mu          sync.RWMutex
	assignments []RoleAssignment
}

// NewInMemoryRoleStore creates a new in-memory role store.
func NewInMemoryRoleStore() *InMemoryRoleStore {
	return &InMemoryRoleStore{}
}

// GetRoleAssignments returns all role assignments for a principal in a tenant.
func (s *InMemoryRoleStore) GetRoleAssignments(_ context.Context, principalID, tenantID string) ([]RoleAssignment, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []RoleAssignment
	for _, a := range s.assignments {
		if a.PrincipalID == principalID && a.TenantID == tenantID {
			result = append(result, a)
		}
	}
	return result, nil
}

// AssignRole adds a role assignment for a principal in a tenant.
func (s *InMemoryRoleStore) AssignRole(_ context.Context, assignment RoleAssignment) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Prevent duplicate assignments.
	for _, a := range s.assignments {
		if a.PrincipalID == assignment.PrincipalID &&
			a.TenantID == assignment.TenantID &&
			a.RoleName == assignment.RoleName {
			return nil
		}
	}
	s.assignments = append(s.assignments, assignment)
	return nil
}

// RevokeRole removes a role assignment for a principal in a tenant.
func (s *InMemoryRoleStore) RevokeRole(_ context.Context, principalID, tenantID, roleName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	filtered := s.assignments[:0]
	found := false
	for _, a := range s.assignments {
		if a.PrincipalID == principalID && a.TenantID == tenantID && a.RoleName == roleName {
			found = true
			continue
		}
		filtered = append(filtered, a)
	}
	s.assignments = filtered
	if !found {
		return common.ErrNotFound
	}
	return nil
}

// RBACService evaluates whether a principal has permission to perform an
// action on a given resource. It considers both scope-based (from API keys
// or JWTs) and role-based access controls.
type RBACService struct {
	roles     map[string]Role
	roleStore RoleStore
}

// NewRBACService creates a new RBAC service with the given policy roles and store.
func NewRBACService(roles map[string]Role, roleStore RoleStore) *RBACService {
	return &RBACService{
		roles:     roles,
		roleStore: roleStore,
	}
}

// CheckPermission verifies that the principal is authorized to perform the
// given action on the specified resource. It first checks scope-based permissions
// (for API keys), then falls back to role-based checks.
//
// Scope format: "resource:action" (e.g., "connector:read") or "*" for full access.
func (s *RBACService) CheckPermission(ctx context.Context, principal *auth.Principal, resource, action string) error {
	if principal == nil {
		return fmt.Errorf("no principal provided: %w", common.ErrUnauthorized)
	}

	// Enforce tenant isolation: the principal's tenant must match the context tenant.
	if ctxTenant := common.TenantIDFrom(ctx); ctxTenant != "" && principal.TenantID != ctxTenant {
		return fmt.Errorf("tenant mismatch: %w", common.ErrForbidden)
	}

	// Check scope-based permissions first. Scopes are typically set on API keys
	// and service principals, formatted as "resource:action".
	if s.checkScopes(principal, resource, action) {
		return nil
	}

	// For JWT and service principals, also check role-based permissions.
	if s.roleStore != nil {
		assignments, err := s.roleStore.GetRoleAssignments(ctx, principal.ID, principal.TenantID)
		if err != nil {
			return fmt.Errorf("fetching role assignments: %w", err)
		}

		for _, assignment := range assignments {
			role, exists := s.roles[assignment.RoleName]
			if !exists {
				continue
			}
			if role.HasPermission(resource, action) {
				return nil
			}
		}
	}

	return fmt.Errorf(
		"principal %s lacks %s permission on %s: %w",
		principal.ID, action, resource, common.ErrForbidden,
	)
}

// checkScopes evaluates scope-based access. Scope format is "resource:action"
// or simply "*" for full access.
func (s *RBACService) checkScopes(principal *auth.Principal, resource, action string) bool {
	requiredScope := resource + ":" + action
	for _, scope := range principal.Scopes {
		if scope == "*" {
			return true
		}
		if scope == requiredScope {
			return true
		}
		// Support wildcard per resource: "connector:*" grants all actions on connectors.
		parts := strings.SplitN(scope, ":", 2)
		if len(parts) == 2 && parts[0] == resource && parts[1] == "*" {
			return true
		}
		// Support wildcard per action: "*:read" grants read on all resources.
		if len(parts) == 2 && parts[0] == "*" && parts[1] == action {
			return true
		}
	}
	return false
}

// AssignRole assigns a role to a principal within a tenant.
func (s *RBACService) AssignRole(ctx context.Context, principalID, tenantID, roleName string) error {
	if _, exists := s.roles[roleName]; !exists {
		return fmt.Errorf("unknown role %q: %w", roleName, common.ErrNotFound)
	}
	return s.roleStore.AssignRole(ctx, RoleAssignment{
		PrincipalID: principalID,
		TenantID:    tenantID,
		RoleName:    roleName,
	})
}

// RevokeRole removes a role assignment from a principal within a tenant.
func (s *RBACService) RevokeRole(ctx context.Context, principalID, tenantID, roleName string) error {
	return s.roleStore.RevokeRole(ctx, principalID, tenantID, roleName)
}

// ListRoles returns all defined roles.
func (s *RBACService) ListRoles() []Role {
	roles := make([]Role, 0, len(s.roles))
	for _, r := range s.roles {
		roles = append(roles, r)
	}
	return roles
}
