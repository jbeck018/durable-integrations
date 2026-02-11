/**
 * Client re-exports — bridges to @flowforge/proto-ts Connect client.
 * Framework adapters import the client through ui-core to avoid
 * direct dependency on proto-ts.
 */

export {
  // Re-export types and services from proto-ts when generated.
  // For now this serves as the integration point:
  //
  // export { FlowForgeService } from "@flowforge/proto-ts";
  // export { createConnectTransport } from "@connectrpc/connect-web";
  // export type { Transport } from "@connectrpc/connect";
} from "@flowforge/proto-ts";
