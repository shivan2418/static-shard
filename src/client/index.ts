/**
 * Static Shard Client Runtime
 * Generic client and types that can be imported from the package
 */

import type { ChunkMeta, Manifest, Schema } from "../types/index.js";

// Re-export types from core types module
export type { ChunkMeta, Manifest, Schema };

// ============================================================================
// Condition Types and Operator Functions
// ============================================================================

export type OperatorType =
  | "eq"
  | "neq"
  | "gt"
  | "gte"
  | "lt"
  | "lte"
  | "contains"
  | "startsWith"
  | "endsWith"
  | "in";

export interface Condition<TField extends string = string, TValue = unknown> {
  readonly field: TField;
  readonly operator: OperatorType;
  readonly value: TValue;
}

// Operator functions
export function eq<F extends string, V>(field: F, value: V): Condition<F, V> {
  return { field, operator: "eq", value };
}

export function neq<F extends string, V>(field: F, value: V): Condition<F, V> {
  return { field, operator: "neq", value };
}

export function gt<F extends string>(field: F, value: number): Condition<F, number> {
  return { field, operator: "gt", value };
}

export function gte<F extends string>(field: F, value: number): Condition<F, number> {
  return { field, operator: "gte", value };
}

export function lt<F extends string>(field: F, value: number): Condition<F, number> {
  return { field, operator: "lt", value };
}

export function lte<F extends string>(field: F, value: number): Condition<F, number> {
  return { field, operator: "lte", value };
}

export function contains<F extends string>(field: F, value: string): Condition<F, string> {
  return { field, operator: "contains", value };
}

export function startsWith<F extends string>(field: F, value: string): Condition<F, string> {
  return { field, operator: "startsWith", value };
}

export function endsWith<F extends string>(field: F, value: string): Condition<F, string> {
  return { field, operator: "endsWith", value };
}

export function inArray<F extends string, V>(field: F, values: V[]): Condition<F, V[]> {
  return { field, operator: "in", value: values };
}

// ============================================================================
// Field Accessor Types (for generated clients)
// ============================================================================

/** Field accessor for string fields */
export interface StringFieldAccessor<F extends string> {
  eq(value: string): Condition<F, string>;
  neq(value: string): Condition<F, string>;
  contains(value: string): Condition<F, string>;
  startsWith(value: string): Condition<F, string>;
  endsWith(value: string): Condition<F, string>;
  in(values: string[]): Condition<F, string[]>;
}

/** Field accessor for numeric fields */
export interface NumericFieldAccessor<F extends string> {
  eq(value: number): Condition<F, number>;
  neq(value: number): Condition<F, number>;
  gt(value: number): Condition<F, number>;
  gte(value: number): Condition<F, number>;
  lt(value: number): Condition<F, number>;
  lte(value: number): Condition<F, number>;
  in(values: number[]): Condition<F, number[]>;
}

/** Field accessor for boolean fields */
export interface BooleanFieldAccessor<F extends string> {
  eq(value: boolean): Condition<F, boolean>;
  neq(value: boolean): Condition<F, boolean>;
}

/** Create a string field accessor */
export function stringField<F extends string>(field: F): StringFieldAccessor<F> {
  return {
    eq: (value: string) => eq(field, value),
    neq: (value: string) => neq(field, value),
    contains: (value: string) => contains(field, value),
    startsWith: (value: string) => startsWith(field, value),
    endsWith: (value: string) => endsWith(field, value),
    in: (values: string[]) => inArray(field, values),
  };
}

/** Create a numeric field accessor */
export function numericField<F extends string>(field: F): NumericFieldAccessor<F> {
  return {
    eq: (value: number) => eq(field, value),
    neq: (value: number) => neq(field, value),
    gt: (value: number) => gt(field, value),
    gte: (value: number) => gte(field, value),
    lt: (value: number) => lt(field, value),
    lte: (value: number) => lte(field, value),
    in: (values: number[]) => inArray(field, values),
  };
}

/** Create a boolean field accessor */
export function booleanField<F extends string>(field: F): BooleanFieldAccessor<F> {
  return {
    eq: (value: boolean) => eq(field, value),
    neq: (value: boolean) => neq(field, value),
  };
}

// ============================================================================
// Client Types
// ============================================================================

export interface ClientQueryOptions<TSortable extends string = string> {
  conditions?: Condition[];
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
  TCondition extends Condition,
  TSortableField extends string
> {
  private client: StaticShardClient<TItem, TCondition, TSortableField>;
  private _conditions: Condition[] = [];
  private _orderBy: { field: TSortableField; direction: "asc" | "desc" } | undefined;
  private _limit: number | undefined;
  private _offset: number | undefined;

  constructor(client: StaticShardClient<TItem, TCondition, TSortableField>) {
    this.client = client;
  }

  /**
   * Add a where condition. Multiple calls use AND logic.
   */
  where(condition: TCondition): this {
    this._conditions.push(condition);
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
  private buildOptions(): ClientQueryOptions<TSortableField> {
    return {
      conditions: this._conditions.length > 0 ? this._conditions : undefined,
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
    return this.client.executeCount({
      conditions: this._conditions.length > 0 ? this._conditions : undefined,
    });
  }
}

// ============================================================================
// Generic Client
// ============================================================================

export class StaticShardClient<
  TItem extends Record<string, unknown> = Record<string, unknown>,
  TCondition extends Condition = Condition,
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
  private findCandidateChunks(manifest: Manifest, conditions?: Condition[]): string[] {
    if (!conditions || conditions.length === 0) {
      return manifest.chunks.map((c) => c.id);
    }

    let candidateChunks: Set<string> | null = null;

    for (const condition of conditions) {
      const { field, operator, value } = condition;
      const index = manifest.indices[field];

      // Index lookup for eq operator
      if (operator === "eq" && index) {
        const strValue = String(value);
        const chunks = index[strValue] || [];

        if (candidateChunks === null) {
          candidateChunks = new Set(chunks);
        } else {
          const current: Set<string> = candidateChunks;
          candidateChunks = new Set(chunks.filter((c) => current.has(c)));
        }
      }

      // Range pruning for gt, gte, lt, lte operators
      if (["gt", "gte", "lt", "lte"].includes(operator)) {
        const rangeValue = value as number;

        const matchingChunks = manifest.chunks
          .filter((chunk) => {
            const range = chunk.fieldRanges[field];
            if (!range) return true;

            const min = range.min as number;
            const max = range.max as number;

            switch (operator) {
              case "gt":
                return max > rangeValue;
              case "gte":
                return max >= rangeValue;
              case "lt":
                return min < rangeValue;
              case "lte":
                return min <= rangeValue;
            }
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

    return candidateChunks ? [...candidateChunks] : manifest.chunks.map((c) => c.id);
  }

  /**
   * Check if a record matches all conditions
   */
  private matchesConditions(record: TItem, conditions?: Condition[]): boolean {
    if (!conditions || conditions.length === 0) return true;

    for (const condition of conditions) {
      const { field, operator, value: condValue } = condition;
      const recordValue = record[field];

      switch (operator) {
        case "eq":
          if (recordValue !== condValue) return false;
          break;
        case "neq":
          if (recordValue === condValue) return false;
          break;
        case "gt":
          if (typeof recordValue !== "number" || recordValue <= (condValue as number)) return false;
          break;
        case "gte":
          if (typeof recordValue !== "number" || recordValue < (condValue as number)) return false;
          break;
        case "lt":
          if (typeof recordValue !== "number" || recordValue >= (condValue as number)) return false;
          break;
        case "lte":
          if (typeof recordValue !== "number" || recordValue > (condValue as number)) return false;
          break;
        case "contains":
          if (typeof recordValue !== "string" || !recordValue.includes(condValue as string)) return false;
          break;
        case "startsWith":
          if (typeof recordValue !== "string" || !recordValue.startsWith(condValue as string)) return false;
          break;
        case "endsWith":
          if (typeof recordValue !== "string" || !recordValue.endsWith(condValue as string)) return false;
          break;
        case "in":
          if (!(condValue as unknown[]).includes(recordValue)) return false;
          break;
      }
    }

    return true;
  }

  /**
   * Start a chainable query
   */
  query(): QueryBuilder<TItem, TCondition, TSortableField> {
    return new QueryBuilder(this);
  }

  /**
   * Execute a query with options (internal, used by QueryBuilder)
   */
  async executeQuery(options: ClientQueryOptions<TSortableField> = {}): Promise<TItem[]> {
    const manifest = await this.loadManifest();
    const candidateChunkIds = this.findCandidateChunks(manifest, options.conditions);

    const chunkPromises = candidateChunkIds.map((id) => this.loadChunk(id));
    const chunks = await Promise.all(chunkPromises);

    let results: TItem[] = [];
    for (const chunk of chunks) {
      for (const record of chunk) {
        if (this.matchesConditions(record, options.conditions)) {
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
      conditions: [eq(primaryField, id)],
      limit: 1,
    });

    return results[0] || null;
  }

  /**
   * Count records matching a query (internal, used by QueryBuilder)
   */
  async executeCount(options: { conditions?: Condition[] } = {}): Promise<number> {
    const manifest = await this.loadManifest();

    if (!options.conditions || options.conditions.length === 0) {
      return manifest.totalRecords;
    }

    const results = await this.executeQuery({ conditions: options.conditions });
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
  TCondition extends Condition = Condition,
  TSortableField extends string = string
>(options: ClientOptions): StaticShardClient<TItem, TCondition, TSortableField> {
  return new StaticShardClient<TItem, TCondition, TSortableField>(options);
}
