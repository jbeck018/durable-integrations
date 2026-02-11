package rbac

// Role name constants for built-in roles.
const (
	RoleAdmin            = "admin"
	RoleEditor           = "editor"
	RoleViewer           = "viewer"
	RoleConnectorManager = "connector_manager"
	RoleSyncOperator     = "sync_operator"
)

// allActions is the complete set of actions that can be performed on a resource.
var allActions = []string{ActionCreate, ActionRead, ActionUpdate, ActionDelete, ActionExecute, ActionManage}

// readOnly grants only the read action.
var readOnly = []string{ActionRead}

// crudActions grants the standard create, read, update, delete set.
var crudActions = []string{ActionCreate, ActionRead, ActionUpdate, ActionDelete}

// DefaultPolicies returns the built-in role definitions for FlowForge.
// These roles form the foundation of the authorization model. Custom roles
// can be added at runtime through the RBACService.
func DefaultPolicies() map[string]Role {
	return map[string]Role{
		RoleAdmin:            adminRole(),
		RoleEditor:           editorRole(),
		RoleViewer:           viewerRole(),
		RoleConnectorManager: connectorManagerRole(),
		RoleSyncOperator:     syncOperatorRole(),
	}
}

// adminRole defines full access to every resource and action, including
// tenant management and audit log access.
func adminRole() Role {
	return Role{
		Name:        RoleAdmin,
		Description: "Full administrative access to all resources and actions",
		Permissions: []Permission{
			{Resource: "*", Actions: []string{"*"}},
		},
	}
}

// editorRole defines CRUD access to the primary data plane resources:
// connectors, connections, syncs, schemas, and MCP servers.
// Editors cannot manage tenants or view audit logs.
func editorRole() Role {
	return Role{
		Name:        RoleEditor,
		Description: "Create, read, update, and delete connectors, connections, syncs, schemas, and MCP servers",
		Permissions: []Permission{
			{Resource: ResourceConnector, Actions: crudActions},
			{Resource: ResourceConnection, Actions: crudActions},
			{Resource: ResourceSync, Actions: append(crudActions, ActionExecute)},
			{Resource: ResourceSchema, Actions: crudActions},
			{Resource: ResourceMCPServer, Actions: crudActions},
		},
	}
}

// viewerRole grants read-only access to all resources.
func viewerRole() Role {
	return Role{
		Name:        RoleViewer,
		Description: "Read-only access to all resources",
		Permissions: []Permission{
			{Resource: ResourceConnector, Actions: readOnly},
			{Resource: ResourceConnection, Actions: readOnly},
			{Resource: ResourceSync, Actions: readOnly},
			{Resource: ResourceTenant, Actions: readOnly},
			{Resource: ResourceSchema, Actions: readOnly},
			{Resource: ResourceMCPServer, Actions: readOnly},
			{Resource: ResourceAuditLog, Actions: readOnly},
		},
	}
}

// connectorManagerRole grants full management of connectors and connections,
// with read-only access to schemas for reference.
func connectorManagerRole() Role {
	return Role{
		Name:        RoleConnectorManager,
		Description: "Manage connectors and connections with full lifecycle control",
		Permissions: []Permission{
			{Resource: ResourceConnector, Actions: allActions},
			{Resource: ResourceConnection, Actions: allActions},
			{Resource: ResourceSchema, Actions: readOnly},
		},
	}
}

// syncOperatorRole grants full management of syncs (including execute),
// with read-only access to connectors, connections, and schemas.
func syncOperatorRole() Role {
	return Role{
		Name:        RoleSyncOperator,
		Description: "Manage and execute syncs, view connectors and connections",
		Permissions: []Permission{
			{Resource: ResourceSync, Actions: allActions},
			{Resource: ResourceConnector, Actions: readOnly},
			{Resource: ResourceConnection, Actions: readOnly},
			{Resource: ResourceSchema, Actions: readOnly},
		},
	}
}
