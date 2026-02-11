/**
 * Angular service providing access to ui-core stores and logic.
 */

import { Injectable, type Signal } from "@angular/core";
import {
  createConnectorSelectorStore,
  createFieldMapperStore,
  createSyncMonitorStore,
  createSchemaViewerStore,
  type ConnectorSelectorStore,
  type FieldMapperStore,
  type SyncMonitorStore,
  type SchemaViewerStore,
  type JSONSchema,
  type MappingPair,
  type Connector,
} from "@flowforge/ui-core";

@Injectable({ providedIn: "root" })
export class FlowForgeService {
  createConnectorSelector(initialConnectors: Connector[] = []): ConnectorSelectorStore {
    return createConnectorSelectorStore(initialConnectors);
  }

  createFieldMapper(
    sourceSchema: JSONSchema,
    destSchema: JSONSchema,
    initialMappings: MappingPair[] = [],
  ): FieldMapperStore {
    return createFieldMapperStore(sourceSchema, destSchema, initialMappings);
  }

  createSyncMonitor(): SyncMonitorStore {
    return createSyncMonitorStore();
  }

  createSchemaViewer(schema: JSONSchema, compareSchema: JSONSchema | null = null): SchemaViewerStore {
    return createSchemaViewerStore(schema, compareSchema);
  }
}
