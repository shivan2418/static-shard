/**
 * TypeScript client code generator
 * Uses json-ts for accurate type inference
 */

import { json2ts } from "json-ts";
import type { DataRecord, Manifest, Schema } from "../../types/index.js";

/**
 * Post-process json-ts output to clean up interface names
 */
function cleanupTypes(types: string): string {
  return types
    // Remove the array type wrapper (we want the item type)
    .replace(/^type IItem = IItemItem\[\];\n/m, "")
    // Rename IItemItem to Item (both references and declarations)
    .replace(/IItemItem/g, "Item")
    // Remove I prefix from all interface names (both references and declarations)
    .replace(/\bI([A-Z][a-z_]+)/g, "$1")
    // Export all interfaces
    .replace(/^interface /gm, "export interface ");
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
  return `
export type StringOperators = {
  eq?: string;
  neq?: string;
  contains?: string;
  startsWith?: string;
  endsWith?: string;
  in?: string[];
};

export type NumericOperators = {
  eq?: number;
  neq?: number;
  gt?: number;
  gte?: number;
  lt?: number;
  lte?: number;
  in?: number[];
};

export type WhereClause = {
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
 * Generate the full client code
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
 * Auto-generated client for static-shard
 * Generated at: ${manifest.generatedAt}
 * Total records: ${manifest.totalRecords}
 * Chunks: ${manifest.chunks.length}
 */

// ============================================================================
// Types
// ============================================================================

${itemInterface}

${fieldNamesType}

${whereTypes}

export type SortableField = ${sortableFields || "string"};

export interface QueryOptions {
  where?: WhereClause;
  orderBy?: SortableField | { field: SortableField; direction: "asc" | "desc" };
  limit?: number;
  offset?: number;
}

export interface Manifest {
  version: string;
  schema: {
    fields: Array<{
      name: string;
      type: string;
      nullable: boolean;
      indexed: boolean;
    }>;
    primaryField: string | null;
  };
  chunks: Array<{
    id: string;
    path: string;
    count: number;
    byteSize: number;
    fieldRanges: { [field: string]: { min: unknown; max: unknown } };
  }>;
  indices: { [field: string]: { [value: string]: string[] } };
  totalRecords: number;
}

// ============================================================================
// Runtime
// ============================================================================

interface ClientOptions {
  basePath: string;
}

class StaticShardClient {
  private basePath: string;
  private manifest: Manifest | null = null;
  private chunkCache: Map<string, Item[]> = new Map();

  constructor(options: ClientOptions) {
    this.basePath = options.basePath.replace(/\\/$/, "");
  }

  /**
   * Load the manifest file
   */
  private async loadManifest(): Promise<Manifest> {
    if (this.manifest) return this.manifest;

    const response = await fetch(\`\${this.basePath}/manifest.json\`);
    if (!response.ok) {
      throw new Error(\`Failed to load manifest: \${response.statusText}\`);
    }

    this.manifest = await response.json();
    return this.manifest!;
  }

  /**
   * Load a chunk by ID
   */
  private async loadChunk(chunkId: string): Promise<Item[]> {
    const cached = this.chunkCache.get(chunkId);
    if (cached) return cached;

    const manifest = await this.loadManifest();
    const chunkMeta = manifest.chunks.find((c) => c.id === chunkId);
    if (!chunkMeta) {
      throw new Error(\`Chunk not found: \${chunkId}\`);
    }

    const response = await fetch(\`\${this.basePath}/\${chunkMeta.path}\`);
    if (!response.ok) {
      throw new Error(\`Failed to load chunk \${chunkId}: \${response.statusText}\`);
    }

    const records = await response.json();
    this.chunkCache.set(chunkId, records);
    return records;
  }

  /**
   * Find chunk IDs that might contain matching records
   */
  private findCandidateChunks(manifest: Manifest, where?: WhereClause): string[] {
    if (!where) {
      return manifest.chunks.map((c) => c.id);
    }

    let candidateChunks: Set<string> | null = null;

    for (const [field, condition] of Object.entries(where)) {
      // Check if we can use the index
      const index = manifest.indices[field];

      if (index && (typeof condition === "string" || typeof condition === "number" || typeof condition === "boolean")) {
        const value = String(condition);
        const chunks = index[value] || [];

        if (candidateChunks === null) {
          candidateChunks = new Set(chunks);
        } else {
          candidateChunks = new Set(Array.from(candidateChunks).filter((c) => chunks.includes(c)));
        }
      } else if (typeof condition === "object" && condition !== null && "eq" in condition) {
        const value = String(condition.eq);
        const chunks = index?.[value] || [];

        if (index) {
          if (candidateChunks === null) {
            candidateChunks = new Set(chunks);
          } else {
            candidateChunks = new Set(Array.from(candidateChunks).filter((c) => chunks.includes(c)));
          }
        }
      }

      // Range pruning using fieldRanges
      if (typeof condition === "object" && condition !== null) {
        const rangeCondition = condition as { gt?: number; gte?: number; lt?: number; lte?: number };
        const hasRangeOp = "gt" in rangeCondition || "gte" in rangeCondition || "lt" in rangeCondition || "lte" in rangeCondition;

        if (hasRangeOp) {
          const matchingChunks = manifest.chunks
            .filter((chunk) => {
              const range = chunk.fieldRanges[field];
              if (!range) return true; // Can't prune, include chunk

              const min = range.min as number;
              const max = range.max as number;

              if (rangeCondition.gt !== undefined && max <= rangeCondition.gt) return false;
              if (rangeCondition.gte !== undefined && max < rangeCondition.gte) return false;
              if (rangeCondition.lt !== undefined && min >= rangeCondition.lt) return false;
              if (rangeCondition.lte !== undefined && min > rangeCondition.lte) return false;

              return true;
            })
            .map((c) => c.id);

          if (candidateChunks === null) {
            candidateChunks = new Set(matchingChunks);
          } else {
            candidateChunks = new Set(Array.from(candidateChunks).filter((c) => matchingChunks.includes(c)));
          }
        }
      }
    }

    return candidateChunks ? Array.from(candidateChunks) : manifest.chunks.map((c) => c.id);
  }

  /**
   * Check if a record matches the where clause
   */
  private matchesWhere(record: Item, where?: WhereClause): boolean {
    if (!where) return true;

    for (const [field, condition] of Object.entries(where)) {
      const value = record[field as keyof Item];

      // Direct value comparison
      if (typeof condition !== "object" || condition === null) {
        if (value !== condition) return false;
        continue;
      }

      const ops = condition as StringOperators & NumericOperators;

      if ("eq" in ops && value !== ops.eq) return false;
      if ("neq" in ops && value === ops.neq) return false;
      if ("gt" in ops && (typeof value !== "number" || value <= ops.gt!)) return false;
      if ("gte" in ops && (typeof value !== "number" || value < ops.gte!)) return false;
      if ("lt" in ops && (typeof value !== "number" || value >= ops.lt!)) return false;
      if ("lte" in ops && (typeof value !== "number" || value > ops.lte!)) return false;
      if ("contains" in ops && (typeof value !== "string" || !value.includes(ops.contains!))) return false;
      if ("startsWith" in ops && (typeof value !== "string" || !value.startsWith(ops.startsWith!))) return false;
      if ("endsWith" in ops && (typeof value !== "string" || !value.endsWith(ops.endsWith!))) return false;
      if ("in" in ops && !(ops.in as unknown[])!.includes(value)) return false;
    }

    return true;
  }

  /**
   * Query records
   */
  async query(options: QueryOptions = {}): Promise<Item[]> {
    const manifest = await this.loadManifest();

    // Find candidate chunks
    const candidateChunkIds = this.findCandidateChunks(manifest, options.where);

    // Load chunks in parallel
    const chunkPromises = candidateChunkIds.map((id) => this.loadChunk(id));
    const chunks = await Promise.all(chunkPromises);

    // Flatten and filter
    let results: Item[] = [];
    for (const chunk of chunks) {
      for (const record of chunk) {
        if (this.matchesWhere(record, options.where)) {
          results.push(record);
        }
      }
    }

    // Sort
    if (options.orderBy) {
      const field = typeof options.orderBy === "string" ? options.orderBy : options.orderBy.field;
      const direction = typeof options.orderBy === "string" ? "asc" : options.orderBy.direction;

      results.sort((a, b) => {
        const aVal = a[field as keyof Item];
        const bVal = b[field as keyof Item];

        if (aVal === bVal) return 0;
        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;

        const cmp = aVal < bVal ? -1 : 1;
        return direction === "asc" ? cmp : -cmp;
      });
    }

    // Pagination
    const offset = options.offset || 0;
    const limit = options.limit;

    if (offset > 0 || limit !== undefined) {
      results = results.slice(offset, limit !== undefined ? offset + limit : undefined);
    }

    return results;
  }

  /**
   * Get a single record by primary key
   */
  async get(id: string | number): Promise<Item | null> {
    const manifest = await this.loadManifest();
    const primaryField = manifest.schema.primaryField;

    if (!primaryField) {
      throw new Error("No primary field defined in schema");
    }

    const results = await this.query({
      where: { [primaryField]: id } as WhereClause,
      limit: 1,
    });

    return results[0] || null;
  }

  /**
   * Count records matching a query
   */
  async count(options: { where?: WhereClause } = {}): Promise<number> {
    const manifest = await this.loadManifest();

    if (!options.where) {
      return manifest.totalRecords;
    }

    // For complex queries, we need to load and count
    const results = await this.query({ where: options.where });
    return results.length;
  }

  /**
   * Get schema information
   */
  async getSchema(): Promise<Manifest["schema"]> {
    const manifest = await this.loadManifest();
    return manifest.schema;
  }

  /**
   * Clear the chunk cache
   */
  clearCache(): void {
    this.chunkCache.clear();
  }
}

// ============================================================================
// Export
// ============================================================================

export function createClient(options: ClientOptions): StaticShardClient {
  return new StaticShardClient(options);
}

// Default export for convenience
export const db = createClient({ basePath: "." });
`;
}
