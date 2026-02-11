/**
 * Flatten a JSON Schema into a tree of SchemaField nodes.
 * Used by FieldMapper and SchemaViewer across all framework adapters.
 */

import type { JSONSchema, SchemaField } from "../types/index.js";

export function flattenSchema(
  schema: JSONSchema,
  parentPath: string = "",
  depth: number = 0,
  parentRequired: string[] = [],
): SchemaField[] {
  const properties = schema.properties ?? {};
  const requiredSet = new Set(schema.required ?? parentRequired);

  return Object.entries(properties).map(([name, propSchema]) => {
    const path = parentPath ? `${parentPath}.${name}` : name;
    const fieldType =
      typeof propSchema.type === "string"
        ? propSchema.type
        : Array.isArray(propSchema.type)
          ? propSchema.type.filter((t) => t !== "null").join(" | ")
          : "any";

    const isNullable =
      propSchema.nullable === true ||
      (Array.isArray(propSchema.type) && propSchema.type.includes("null"));

    const children =
      fieldType === "object" || (propSchema.properties && Object.keys(propSchema.properties).length > 0)
        ? flattenSchema(propSchema, path, depth + 1, propSchema.required ?? [])
        : fieldType === "array" && propSchema.items?.properties
          ? flattenSchema(propSchema.items, path, depth + 1, propSchema.items.required ?? [])
          : [];

    return {
      path,
      name,
      type: fieldType,
      required: requiredSet.has(name),
      nullable: isNullable,
      description: propSchema.description ?? "",
      children,
      depth,
      schema: propSchema,
    };
  });
}

/**
 * Count total fields in a schema (recursive).
 */
export function countFields(schema: JSONSchema): number {
  let count = 0;
  const props = schema.properties ?? {};
  for (const key of Object.keys(props)) {
    count += 1;
    const child = props[key];
    if (child.properties) {
      count += countFields(child);
    } else if (child.items?.properties) {
      count += countFields(child.items);
    }
  }
  return count;
}
