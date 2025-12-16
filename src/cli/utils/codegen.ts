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
 * Generate where clause types based on schema field types
 */
function generateWhereTypes(schema: Schema): string {
  return `export type WhereClause = {
${schema.fields
  .map((f) => {
    if (f.type === "number") {
      return `  ${f.name}?: number | NumericOperators;`;
    } else if (f.type === "boolean") {
      return `  ${f.name}?: boolean;`;
    } else {
      return `  ${f.name}?: string | StringOperators;`;
    }
  })
  .join("\n")}
};`;
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
  const whereTypes = generateWhereTypes(schema);

  const sortableFields = schema.fields
    .filter((f) => f.type === "number" || f.type === "string" || f.type === "date")
    .map((f) => `"${f.name}"`)
    .join(" | ");

  return `/**
 * Auto-generated types for static-shard
 * Generated at: ${manifest.generatedAt}
 * Total records: ${manifest.totalRecords}
 * Chunks: ${manifest.chunks.length}
 */

import {
  StaticShardClient,
  QueryBuilder,
  createClient as createBaseClient,
  StringOperators,
  NumericOperators,
  type ClientOptions,
  type ClientQueryOptions,
} from "static-shard";

// ============================================================================
// Data Types (generated from your data)
// ============================================================================

${itemInterface}

${fieldNamesType}

${whereTypes}

export type SortableField = ${sortableFields || "string"};

// ============================================================================
// Typed Client
// ============================================================================

export type TypedQueryOptions = ClientQueryOptions<WhereClause, SortableField>;
export type TypedQueryBuilder = QueryBuilder<Item, WhereClause, SortableField>;

export class Client extends StaticShardClient<Item, WhereClause, SortableField> {}

export function createClient(options: ClientOptions): Client {
  return createBaseClient<Item, WhereClause, SortableField>(options);
}

// Default client for current directory
export const db = createClient({ basePath: "." });
`;
}
