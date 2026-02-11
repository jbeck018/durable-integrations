/**
 * Field mapper state — manages mappings, drag state, and mapping operations.
 * Framework-agnostic.
 */

import type { DragState, JSONSchema, MappingPair, SchemaField } from "../types/index.js";
import { flattenSchema } from "../logic/schema-flatten.js";
import { Store } from "./store.js";

export interface FieldMapperState {
  sourceSchema: JSONSchema;
  destSchema: JSONSchema;
  mappings: MappingPair[];
  selectedMappingId: string | null;
  dragState: DragState | null;
  activeField: string | null;
}

export function createFieldMapperStore(
  sourceSchema: JSONSchema,
  destSchema: JSONSchema,
  initialMappings: MappingPair[] = [],
) {
  const store = new Store<FieldMapperState>({
    sourceSchema,
    destSchema,
    mappings: initialMappings,
    selectedMappingId: null,
    dragState: null,
    activeField: null,
  });

  return {
    store,

    getSourceFields(): SchemaField[] {
      return flattenSchema(store.getState().sourceSchema);
    },

    getDestFields(): SchemaField[] {
      return flattenSchema(store.getState().destSchema);
    },

    getMappedSourceFields(): Set<string> {
      return new Set(store.getState().mappings.map((m) => m.sourceField));
    },

    getMappedDestFields(): Set<string> {
      return new Set(store.getState().mappings.map((m) => m.destinationField));
    },

    getErrorCount(): number {
      return store.getState().mappings.filter((m) => m.status === "error").length;
    },

    getSelectedMapping(): MappingPair | null {
      const state = store.getState();
      return state.mappings.find((m) => m.id === state.selectedMappingId) ?? null;
    },

    setMappings(mappings: MappingPair[]) {
      store.setState((s) => ({ ...s, mappings }));
    },

    addMapping(sourceField: string, destField: string): MappingPair | null {
      const state = store.getState();
      const alreadyMapped = state.mappings.some(
        (m) => m.sourceField === sourceField && m.destinationField === destField,
      );
      if (alreadyMapped) return null;

      const newMapping: MappingPair = {
        id: `mapping_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
        sourceField,
        destinationField: destField,
        transform: null,
        isAutoMapped: false,
        status: "valid",
      };

      store.setState((s) => ({ ...s, mappings: [...s.mappings, newMapping] }));
      return newMapping;
    },

    deleteMapping(id: string) {
      store.setState((s) => ({
        ...s,
        mappings: s.mappings.filter((m) => m.id !== id),
        selectedMappingId: s.selectedMappingId === id ? null : s.selectedMappingId,
      }));
    },

    clearAll() {
      store.setState((s) => ({ ...s, mappings: [], selectedMappingId: null }));
    },

    selectMapping(id: string | null) {
      store.setState((s) => ({
        ...s,
        selectedMappingId: s.selectedMappingId === id ? null : id,
      }));
    },

    setTransformType(type: string | null) {
      const state = store.getState();
      if (!state.selectedMappingId) return;
      store.setState((s) => ({
        ...s,
        mappings: s.mappings.map((m) =>
          m.id === s.selectedMappingId
            ? {
                ...m,
                transform: type
                  ? { type, expression: m.transform?.expression ?? "", config: {} }
                  : null,
              }
            : m,
        ),
      }));
    },

    setTransformExpression(expression: string) {
      const state = store.getState();
      if (!state.selectedMappingId) return;
      store.setState((s) => ({
        ...s,
        mappings: s.mappings.map((m) =>
          m.id === s.selectedMappingId && m.transform
            ? { ...m, transform: { ...m.transform, expression } }
            : m,
        ),
      }));
    },

    startDrag(field: SchemaField, side: "source" | "destination", startX: number, startY: number) {
      store.setState((s) => ({
        ...s,
        dragState: { sourceField: field, side, startX, startY, currentX: startX, currentY: startY },
      }));
    },

    updateDrag(currentX: number, currentY: number) {
      store.setState((s) => {
        if (!s.dragState) return s;
        return { ...s, dragState: { ...s.dragState, currentX, currentY } };
      });
    },

    endDrag() {
      store.setState((s) => ({ ...s, dragState: null }));
    },

    /**
     * Complete a drag-to-connect operation. Returns the new mapping if created.
     */
    completeDrag(targetField: SchemaField, targetSide: "source" | "destination"): MappingPair | null {
      const state = store.getState();
      if (!state.dragState || !state.dragState.sourceField) return null;
      if (state.dragState.side === targetSide) {
        this.endDrag();
        return null;
      }

      const sourceField = state.dragState.side === "source"
        ? state.dragState.sourceField.path
        : targetField.path;
      const destField = state.dragState.side === "destination"
        ? state.dragState.sourceField.path
        : targetField.path;

      this.endDrag();
      return this.addMapping(sourceField, destField);
    },

    setActiveField(path: string | null) {
      store.setState((s) => ({
        ...s,
        activeField: s.activeField === path ? null : path,
      }));
    },
  };
}

export type FieldMapperStore = ReturnType<typeof createFieldMapperStore>;
