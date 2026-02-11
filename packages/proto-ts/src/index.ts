/**
 * @flowforge/proto-ts — Auto-generated TypeScript types, Connect client stubs,
 * and TanStack Query hooks from FlowForge protobuf definitions.
 *
 * This package replaces the hand-written types in ui/src/lib/types.ts and the
 * hand-written API client in ui/src/lib/api.ts. All types, service clients,
 * and query hooks are generated from the proto source of truth.
 *
 * Usage:
 *   import { FlowForgeService } from "@flowforge/proto-ts";
 *   import { createConnectTransport } from "@connectrpc/connect-web";
 *   import { createClient } from "@connectrpc/connect";
 *
 *   const transport = createConnectTransport({ baseUrl: "/api/v1" });
 *   const client = createClient(FlowForgeService, transport);
 *   const { connectors } = await client.listConnectors({});
 */

// Re-export all generated types and services.
// These paths are populated by `buf generate` and should not be manually edited.
export * from "./gen/flowforge/api/v1/api_pb.js";
export * from "./gen/flowforge/protocol/protocol_pb.js";
