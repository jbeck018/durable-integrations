// Package graphql provides the FlowForge GraphQL API layer with schema definitions,
// resolvers, and DataLoader-based N+1 prevention.
package graphql

// SchemaString contains the full FlowForge GraphQL schema definition.
// It defines all entity types, queries, mutations, and subscriptions.
const SchemaString = `
scalar JSON
scalar Time

type Connector {
  name: String!
  displayName: String!
  version: String!
  type: String!
  category: String!
  icon: String
  spec: ConnectorSpec
}

type ConnectorSpec {
  documentationUrl: String
  configSchema: JSON
}

type Connection {
  id: ID!
  tenantId: String!
  name: String!
  connectorName: String!
  connectorType: String!
  config: JSON
  status: String!
  createdAt: Time!
  updatedAt: Time!
  connector: Connector
}

type Sync {
  id: ID!
  tenantId: String!
  name: String!
  sourceId: String!
  destinationId: String!
  schedule: String
  status: String!
  createdAt: Time!
  updatedAt: Time!
  source: Connection
  destination: Connection
  fieldMappings: [FieldMapping!]
  runs(page: Int, pageSize: Int): SyncRunConnection!
}

type SyncRun {
  id: ID!
  syncId: String!
  tenantId: String!
  status: String!
  recordsRead: Int!
  recordsWritten: Int!
  bytesRead: Int!
  bytesWritten: Int!
  errorMessage: String
  startedAt: Time!
  finishedAt: Time
  logs(page: Int, pageSize: Int): SyncRunLogConnection!
}

type SyncRunLog {
  id: ID!
  syncRunId: String!
  level: String!
  message: String!
  timestamp: Time!
}

type Stream {
  name: String!
  namespace: String
  displayName: String
  jsonSchema: JSON
  supportedSyncModes: [String!]!
  defaultCursorField: [String!]
  sourceDefinedPrimaryKey: Boolean
  primaryKey: [[String!]!]
}

type Catalog {
  streams: [Stream!]!
}

type Tenant {
  id: ID!
  name: String!
  plan: String!
  settings: JSON
  createdAt: Time!
  updatedAt: Time!
  connections(page: Int, pageSize: Int): ConnectionConnection!
  syncs(page: Int, pageSize: Int): SyncConnection!
}

type FieldMapping {
  id: ID!
  syncId: String
  tenantId: String!
  sourceField: String!
  destField: String!
  transform: String
  createdAt: Time!
  updatedAt: Time!
}

type SchemaVersion {
  id: ID!
  stream: String!
  version: Int!
  schema: JSON!
  createdAt: Time!
}

type SchemaComparison {
  stream: String!
  version1: Int!
  version2: Int!
  changes: [SchemaFieldChange!]!
}

type SchemaFieldChange {
  field: String!
  change: String!
  oldType: String
  newType: String
}

type MCPServer {
  id: ID!
  tenantId: String!
  name: String!
  url: String!
  transport: String!
  tools: [String!]
  metadata: JSON
  status: String!
  createdAt: Time!
  updatedAt: Time!
}

type CheckResult {
  status: String!
  message: String
}

# Pagination types using Relay-style connections
type PageInfo {
  total: Int!
  page: Int!
  pageSize: Int!
  pages: Int!
  hasNextPage: Boolean!
  hasPreviousPage: Boolean!
}

type ConnectionConnection {
  nodes: [Connection!]!
  pageInfo: PageInfo!
}

type SyncConnection {
  nodes: [Sync!]!
  pageInfo: PageInfo!
}

type SyncRunConnection {
  nodes: [SyncRun!]!
  pageInfo: PageInfo!
}

type SyncRunLogConnection {
  nodes: [SyncRunLog!]!
  pageInfo: PageInfo!
}

type MCPServerConnection {
  nodes: [MCPServer!]!
  pageInfo: PageInfo!
}

type FieldMappingConnection {
  nodes: [FieldMapping!]!
  pageInfo: PageInfo!
}

# Root Query type — all read operations
type Query {
  # Connectors
  connectors: [Connector!]!
  connector(name: String!): Connector

  # Connections
  connection(id: ID!): Connection
  connections(page: Int, pageSize: Int): ConnectionConnection!

  # Syncs
  sync(id: ID!): Sync
  syncs(page: Int, pageSize: Int): SyncConnection!

  # Sync Runs
  syncRun(id: ID!): SyncRun
  syncRuns(syncId: String, page: Int, pageSize: Int): SyncRunConnection!

  # Streams
  streams(connectionId: String!): [Stream!]!
  streamSchema(connectionId: String!, streamName: String!): JSON

  # Tenants
  tenant(id: ID!): Tenant
  tenants(page: Int, pageSize: Int): [Tenant!]!

  # Field Mappings
  fieldMapping(id: ID!): FieldMapping

  # Schemas
  schemaVersions(stream: String!): [SchemaVersion!]!
  schemaComparison(stream: String!, v1: Int!, v2: Int!): SchemaComparison

  # MCP
  mcpServers(page: Int, pageSize: Int): MCPServerConnection!
  mcpServer(id: ID!): MCPServer
}

# Root Mutation type — all write operations
type Mutation {
  # Connections
  createConnection(input: CreateConnectionInput!): Connection!
  updateConnection(id: ID!, input: UpdateConnectionInput!): Connection!
  deleteConnection(id: ID!): Boolean!
  testConnection(connectorName: String!, config: JSON!): CheckResult!

  # Syncs
  createSync(input: CreateSyncInput!): Sync!
  updateSync(id: ID!, input: UpdateSyncInput!): Sync!
  deleteSync(id: ID!): Boolean!
  triggerSync(id: ID!): SyncRun!
  pauseSync(id: ID!): Sync!
  resumeSync(id: ID!): Sync!

  # Tenants
  createTenant(input: CreateTenantInput!): Tenant!
  updateTenant(id: ID!, input: UpdateTenantInput!): Tenant!

  # Field Mappings
  createFieldMapping(input: CreateFieldMappingInput!): FieldMapping!
  updateFieldMapping(id: ID!, input: UpdateFieldMappingInput!): FieldMapping!
  autoMapFields(syncId: String!, streamName: String!): [FieldMapping!]!

  # MCP
  registerMCPServer(input: RegisterMCPServerInput!): MCPServer!
  updateMCPServer(id: ID!, input: UpdateMCPServerInput!): MCPServer!
  deleteMCPServer(id: ID!): Boolean!

  # Streams
  discoverStreams(connectionId: String!): Catalog!
}

# Input types for mutations
input CreateConnectionInput {
  name: String!
  connectorName: String!
  config: JSON!
}

input UpdateConnectionInput {
  name: String
  config: JSON
}

input CreateSyncInput {
  name: String!
  sourceId: String!
  destinationId: String!
  schedule: String
}

input UpdateSyncInput {
  name: String
  schedule: String
}

input CreateTenantInput {
  name: String!
  plan: String
  settings: JSON
}

input UpdateTenantInput {
  name: String
  plan: String
  settings: JSON
}

input CreateFieldMappingInput {
  syncId: String!
  sourceField: String!
  destField: String!
  transform: String
}

input UpdateFieldMappingInput {
  sourceField: String
  destField: String
  transform: String
}

input RegisterMCPServerInput {
  name: String!
  url: String!
  transport: String!
  metadata: JSON
}

input UpdateMCPServerInput {
  name: String
  url: String
  transport: String
  metadata: JSON
}

# Subscription type for real-time sync status updates
type Subscription {
  syncStatusChanged(syncId: String): SyncStatusEvent!
  syncRunProgress(syncRunId: String!): SyncRunProgressEvent!
}

type SyncStatusEvent {
  syncId: String!
  previousStatus: String!
  newStatus: String!
  timestamp: Time!
}

type SyncRunProgressEvent {
  syncRunId: String!
  status: String!
  recordsRead: Int!
  recordsWritten: Int!
  bytesRead: Int!
  bytesWritten: Int!
  timestamp: Time!
}
`
