/**
 * TypeScript client code generator
 * Generates data-specific types that work with the package runtime
 */

import { json2ts } from "json-ts";
import type { DataRecord, Manifest, Schema } from "../../types/index.js";

/**
 * Post-process json-ts output to clean up interface names
 * and add index signature for Record<string, unknown> compatibility
 */
function cleanupTypes(types: string): string {
  return types
    .replace(/^type IItem = IItemItem\[\];\n/m, "")
    .replace(/IItemItem/g, "Item")
    .replace(/\bI([A-Z][a-z_]+)/g, "$1")
    .replace(/^interface /gm, "export interface ")
    // Add index signature to Item interface for Record<string, unknown> compatibility
    .replace(
      /^(export interface Item \{[\s\S]*?)(^\})/m,
      "$1    [key: string]: unknown;\n$2"
    );
}

/**
 * Generate field names type from schema
 */
function generateFieldNamesType(schema: Schema): string {
  const names = schema.fields.map((f) => `"${f.name}"`).join(" | ");
  return `export type FieldName = ${names};`;
}

/**
 * Generate field accessors for the client
 */
function generateFieldAccessors(schema: Schema): string {
  const accessors = schema.fields.map((f) => {
    const fieldName = f.name;
    if (f.type === "number") {
      return `  ${fieldName} = numericField("${fieldName}");`;
    } else if (f.type === "boolean") {
      return `  ${fieldName} = booleanField("${fieldName}");`;
    } else {
      // string, date, or other types default to string accessor
      return `  ${fieldName} = stringField("${fieldName}");`;
    }
  });
  return accessors.join("\n");
}

/**
 * Generate the client code - now imports runtime from package
 */
export function generateClient(
  schema: Schema,
  manifest: Manifest,
  samples: DataRecord[]
): string {
  // Use json-ts to generate the Item interface from actual data samples
  const rawTypes = json2ts(JSON.stringify(samples), { rootName: "Item" });
  const itemInterface = cleanupTypes(rawTypes);

  const fieldNamesType = generateFieldNamesType(schema);
  const fieldAccessors = generateFieldAccessors(schema);

  const sortableFields = schema.fields
    .filter((f) => f.type === "number" || f.type === "string" || f.type === "date")
    .map((f) => `"${f.name}"`)
    .join(" | ");

  return `/**
 * Auto-generated types for static-shard
 * Generated at: ${manifest.generatedAt}
 * Total records: ${manifest.totalRecords}
 * Chunks: ${manifest.chunks.length}
 *
 * Usage:
 *   import { db } from './client'
 *
 *   // Query with field accessors - no imports needed!
 *   const results = await db.query()
 *     .where(db.category.eq('electronics'))
 *     .where(db.price.gte(100))
 *     .orderBy('price', 'desc')
 *     .execute()
 */

import {
  StaticShardClient,
  QueryBuilder,
  createClient as createBaseClient,
  stringField,
  numericField,
  booleanField,
  type Condition,
  type ClientOptions,
  type ClientQueryOptions,
} from "static-shard";

// ============================================================================
// Data Types (generated from your data)
// ============================================================================

${itemInterface}

${fieldNamesType}

export type SortableField = ${sortableFields || "string"};

// Typed condition for this schema
export type ItemCondition = Condition<FieldName>;

// ============================================================================
// Typed Client with Field Accessors
// ============================================================================

export type TypedQueryOptions = ClientQueryOptions<SortableField>;
export type TypedQueryBuilder = QueryBuilder<Item, ItemCondition, SortableField>;

export class Client extends StaticShardClient<Item, ItemCondition, SortableField> {
  // Field accessors for building conditions
${fieldAccessors}
}

export function createClient(options: ClientOptions): Client {
  return new Client(options);
}

// Default client for current directory
export const db = createClient({ basePath: "." });
`;
}
