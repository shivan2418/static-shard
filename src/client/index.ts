/**
 * Static Shard Client Runtime
 * Generic client and types that can be imported from the package
 */

import type { ChunkMeta, Manifest, Schema } from "../types/index.js";

// Re-export types from core types module
export type { ChunkMeta, Manifest, Schema };

// ============================================================================
// Operator Types
// ============================================================================

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

// ============================================================================
// Client Types
// ============================================================================

export interface ClientQueryOptions<TWhere = Record<string, unknown>, TSortable extends string = string> {
  where?: TWhere;
  orderBy?: TSortable | { field: TSortable; direction: "asc" | "desc" };
  limit?: number;
  offset?: number;
}

export interface ClientOptions {
  basePath: string;
}

// ============================================================================
// Query Builder
// ============================================================================

export class QueryBuilder<
  TItem extends Record<string, unknown>,
  TWhereClause,
  TSortableField extends string
> {
  private client: StaticShardClient<TItem, TWhereClause, TSortableField>;
  private _where: TWhereClause | undefined;
  private _orderBy: { field: TSortableField; direction: "asc" | "desc" } | undefined;
  private _limit: number | undefined;
  private _offset: number | undefined;

  constructor(client: StaticShardClient<TItem, TWhereClause, TSortableField>) {
    this.client = client;
  }

  /**
   * Add where conditions. Multiple calls merge conditions (AND logic).
   */
  where(conditions: Partial<TWhereClause>): this {
    if (this._where) {
      this._where = { ...this._where, ...conditions } as TWhereClause;
    } else {
      this._where = conditions as TWhereClause;
    }
    return this;
  }

  /**
   * Set sort order
   */
  orderBy(field: TSortableField, direction: "asc" | "desc" = "asc"): this {
    this._orderBy = { field, direction };
    return this;
  }

  /**
   * Limit the number of results
   */
  limit(count: number): this {
    this._limit = count;
    return this;
  }

  /**
   * Skip a number of results
   */
  offset(count: number): this {
    this._offset = count;
    return this;
  }

  /**
   * Build the options object for internal use
   */
  private buildOptions(): ClientQueryOptions<TWhereClause, TSortableField> {
    return {
      where: this._where,
      orderBy: this._orderBy,
      limit: this._limit,
      offset: this._offset,
    };
  }

  /**
   * Execute the query and return all matching results
   */
  async execute(): Promise<TItem[]> {
    return this.client.executeQuery(this.buildOptions());
  }

  /**
   * Execute the query and return only the first result (or null)
   */
  async first(): Promise<TItem | null> {
    const results = await this.client.executeQuery({
      ...this.buildOptions(),
      limit: 1,
    });
    return results[0] ?? null;
  }

  /**
   * Get the count of matching records
   */
  async count(): Promise<number> {
    return this.client.executeCount({ where: this._where });
  }
}

// ============================================================================
// Generic Client
// ============================================================================

export class StaticShardClient<
  TItem extends Record<string, unknown> = Record<string, unknown>,
  TWhereClause = Record<string, unknown>,
  TSortableField extends string = string
> {
  private basePath: string;
  private manifest: Manifest | null = null;
  private chunkCache: Map<string, TItem[]> = new Map();

  constructor(options: ClientOptions) {
    this.basePath = options.basePath.replace(/\/$/, "");
  }

  /**
   * Load the manifest file
   */
  private async loadManifest(): Promise<Manifest> {
    if (this.manifest) return this.manifest;

    const response = await fetch(`${this.basePath}/manifest.json`);
    if (!response.ok) {
      throw new Error(`Failed to load manifest: ${response.statusText}`);
    }

    this.manifest = (await response.json()) as Manifest;
    return this.manifest;
  }

  /**
   * Load a chunk by ID
   */
  private async loadChunk(chunkId: string): Promise<TItem[]> {
    const cached = this.chunkCache.get(chunkId);
    if (cached) return cached;

    const manifest = await this.loadManifest();
    const chunkMeta = manifest.chunks.find((c) => c.id === chunkId);
    if (!chunkMeta) {
      throw new Error(`Chunk not found: ${chunkId}`);
    }

    const response = await fetch(`${this.basePath}/${chunkMeta.path}`);
    if (!response.ok) {
      throw new Error(`Failed to load chunk ${chunkId}: ${response.statusText}`);
    }

    const records = (await response.json()) as TItem[];
    this.chunkCache.set(chunkId, records);
    return records;
  }

  /**
   * Find chunk IDs that might contain matching records
   */
  private findCandidateChunks(manifest: Manifest, where?: TWhereClause): string[] {
    if (!where) {
      return manifest.chunks.map((c) => c.id);
    }

    let candidateChunks: Set<string> | null = null;

    for (const [field, condition] of Object.entries(where as Record<string, unknown>)) {
      const index = manifest.indices[field];

      if (index && (typeof condition === "string" || typeof condition === "number" || typeof condition === "boolean")) {
        const value = String(condition);
        const chunks = index[value] || [];

        if (candidateChunks === null) {
          candidateChunks = new Set(chunks);
        } else {
          const current: Set<string> = candidateChunks;
          candidateChunks = new Set(chunks.filter((c) => current.has(c)));
        }
      } else if (typeof condition === "object" && condition !== null && "eq" in condition) {
        const value = String((condition as { eq: unknown }).eq);
        const chunks = index?.[value] || [];

        if (index) {
          if (candidateChunks === null) {
            candidateChunks = new Set(chunks);
          } else {
            const current: Set<string> = candidateChunks;
            candidateChunks = new Set(chunks.filter((c) => current.has(c)));
          }
        }
      }

      // Range pruning
      if (typeof condition === "object" && condition !== null) {
        const rangeCondition = condition as { gt?: number; gte?: number; lt?: number; lte?: number };
        const hasRangeOp = "gt" in rangeCondition || "gte" in rangeCondition || "lt" in rangeCondition || "lte" in rangeCondition;

        if (hasRangeOp) {
          const matchingChunks = manifest.chunks
            .filter((chunk) => {
              const range = chunk.fieldRanges[field];
              if (!range) return true;

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
            const current: Set<string> = candidateChunks;
            candidateChunks = new Set(matchingChunks.filter((c) => current.has(c)));
          }
        }
      }
    }

    return candidateChunks ? [...candidateChunks] : manifest.chunks.map((c) => c.id);
  }

  /**
   * Check if a record matches the where clause
   */
  private matchesWhere(record: TItem, where?: TWhereClause): boolean {
    if (!where) return true;

    for (const [field, condition] of Object.entries(where as Record<string, unknown>)) {
      const value = record[field];

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
   * Start a chainable query
   */
  query(): QueryBuilder<TItem, TWhereClause, TSortableField> {
    return new QueryBuilder(this);
  }

  /**
   * Execute a query with options (internal, used by QueryBuilder)
   */
  async executeQuery(options: ClientQueryOptions<TWhereClause, TSortableField> = {}): Promise<TItem[]> {
    const manifest = await this.loadManifest();
    const candidateChunkIds = this.findCandidateChunks(manifest, options.where);

    const chunkPromises = candidateChunkIds.map((id) => this.loadChunk(id));
    const chunks = await Promise.all(chunkPromises);

    let results: TItem[] = [];
    for (const chunk of chunks) {
      for (const record of chunk) {
        if (this.matchesWhere(record, options.where)) {
          results.push(record);
        }
      }
    }

    if (options.orderBy) {
      const field = typeof options.orderBy === "string" ? options.orderBy : options.orderBy.field;
      const direction = typeof options.orderBy === "string" ? "asc" : options.orderBy.direction;

      results.sort((a, b) => {
        const aVal = a[field];
        const bVal = b[field];

        if (aVal === bVal) return 0;
        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;

        const cmp = (aVal as string | number) < (bVal as string | number) ? -1 : 1;
        return direction === "asc" ? cmp : -cmp;
      });
    }

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
  async get(id: string | number): Promise<TItem | null> {
    const manifest = await this.loadManifest();
    const primaryField = manifest.schema.primaryField;

    if (!primaryField) {
      throw new Error("No primary field defined in schema");
    }

    const results = await this.executeQuery({
      where: { [primaryField]: id } as TWhereClause,
      limit: 1,
    });

    return results[0] || null;
  }

  /**
   * Count records matching a query (internal, used by QueryBuilder)
   */
  async executeCount(options: { where?: TWhereClause } = {}): Promise<number> {
    const manifest = await this.loadManifest();

    if (!options.where) {
      return manifest.totalRecords;
    }

    const results = await this.executeQuery({ where: options.where });
    return results.length;
  }

  /**
   * Get schema information
   */
  async getSchema(): Promise<Schema> {
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

/**
 * Create a typed client instance
 */
export function createClient<
  TItem extends Record<string, unknown> = Record<string, unknown>,
  TWhereClause = Record<string, unknown>,
  TSortableField extends string = string
>(options: ClientOptions): StaticShardClient<TItem, TWhereClause, TSortableField> {
  return new StaticShardClient<TItem, TWhereClause, TSortableField>(options);
}
