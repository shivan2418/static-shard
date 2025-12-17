/**
 * Core type definitions for static-shard
 */
type FieldType = "string" | "number" | "boolean" | "date" | "null";
interface FieldSchema {
    name: string;
    type: FieldType;
    nullable: boolean;
    indexed: boolean;
    stats: FieldStats;
}
interface FieldStats {
    min?: string | number;
    max?: string | number;
    cardinality: number;
    nullCount: number;
    sampleValues?: (string | number | boolean | null)[];
}
interface Schema {
    fields: FieldSchema[];
    primaryField: string | null;
}
interface ChunkMeta {
    id: string;
    path: string;
    count: number;
    byteSize: number;
    fieldRanges: Record<string, {
        min: unknown;
        max: unknown;
    }>;
}
interface Manifest {
    version: string;
    generatedAt: string;
    schema: Schema;
    chunks: ChunkMeta[];
    indices: Record<string, Record<string, string[]>>;
    totalRecords: number;
    config: BuildConfig;
}
interface BuildConfig {
    chunkSize: number;
    chunkBy: string | null;
    indexedFields: string[];
}
type DataRecord = {
    [key: string]: unknown;
};
type DataFormat = "json" | "ndjson" | "csv";
interface ParseResult {
    records: DataRecord[];
    format: DataFormat;
}
interface BuildOptions {
    output: string;
    chunkSize: string;
    chunkBy?: string;
    index?: string;
    format?: DataFormat;
}
interface InspectOptions {
    sample?: number;
    format?: DataFormat;
    fast?: boolean;
}

/**
 * Static Shard Client Runtime
 * Generic client and types that can be imported from the package
 */

type OperatorType = "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "contains" | "startsWith" | "endsWith" | "in";
interface Condition<TField extends string = string, TValue = unknown> {
    readonly field: TField;
    readonly operator: OperatorType;
    readonly value: TValue;
}
declare function eq<F extends string, V>(field: F, value: V): Condition<F, V>;
declare function neq<F extends string, V>(field: F, value: V): Condition<F, V>;
declare function gt<F extends string>(field: F, value: number): Condition<F, number>;
declare function gte<F extends string>(field: F, value: number): Condition<F, number>;
declare function lt<F extends string>(field: F, value: number): Condition<F, number>;
declare function lte<F extends string>(field: F, value: number): Condition<F, number>;
declare function contains<F extends string>(field: F, value: string): Condition<F, string>;
declare function startsWith<F extends string>(field: F, value: string): Condition<F, string>;
declare function endsWith<F extends string>(field: F, value: string): Condition<F, string>;
declare function inArray<F extends string, V>(field: F, values: V[]): Condition<F, V[]>;
/** Field accessor for string fields */
interface StringFieldAccessor<F extends string> {
    eq(value: string): Condition<F, string>;
    neq(value: string): Condition<F, string>;
    contains(value: string): Condition<F, string>;
    startsWith(value: string): Condition<F, string>;
    endsWith(value: string): Condition<F, string>;
    in(values: string[]): Condition<F, string[]>;
}
/** Field accessor for numeric fields */
interface NumericFieldAccessor<F extends string> {
    eq(value: number): Condition<F, number>;
    neq(value: number): Condition<F, number>;
    gt(value: number): Condition<F, number>;
    gte(value: number): Condition<F, number>;
    lt(value: number): Condition<F, number>;
    lte(value: number): Condition<F, number>;
    in(values: number[]): Condition<F, number[]>;
}
/** Field accessor for boolean fields */
interface BooleanFieldAccessor<F extends string> {
    eq(value: boolean): Condition<F, boolean>;
    neq(value: boolean): Condition<F, boolean>;
}
/** Create a string field accessor */
declare function stringField<F extends string>(field: F): StringFieldAccessor<F>;
/** Create a numeric field accessor */
declare function numericField<F extends string>(field: F): NumericFieldAccessor<F>;
/** Create a boolean field accessor */
declare function booleanField<F extends string>(field: F): BooleanFieldAccessor<F>;
interface ClientQueryOptions<TSortable extends string = string> {
    conditions?: Condition[];
    orderBy?: TSortable | {
        field: TSortable;
        direction: "asc" | "desc";
    };
    limit?: number;
    offset?: number;
}
interface ClientOptions {
    basePath: string;
}
declare class QueryBuilder<TItem extends Record<string, unknown>, TCondition extends Condition, TSortableField extends string> {
    private client;
    private _conditions;
    private _orderBy;
    private _limit;
    private _offset;
    constructor(client: StaticShardClient<TItem, TCondition, TSortableField>);
    /**
     * Add a where condition. Multiple calls use AND logic.
     */
    where(condition: TCondition): this;
    /**
     * Set sort order
     */
    orderBy(field: TSortableField, direction?: "asc" | "desc"): this;
    /**
     * Limit the number of results
     */
    limit(count: number): this;
    /**
     * Skip a number of results
     */
    offset(count: number): this;
    /**
     * Build the options object for internal use
     */
    private buildOptions;
    /**
     * Execute the query and return all matching results
     */
    execute(): Promise<TItem[]>;
    /**
     * Execute the query and return only the first result (or null)
     */
    first(): Promise<TItem | null>;
    /**
     * Get the count of matching records
     */
    count(): Promise<number>;
}
declare class StaticShardClient<TItem extends Record<string, unknown> = Record<string, unknown>, TCondition extends Condition = Condition, TSortableField extends string = string> {
    private basePath;
    private manifest;
    private chunkCache;
    constructor(options: ClientOptions);
    /**
     * Load the manifest file
     */
    private loadManifest;
    /**
     * Load a chunk by ID
     */
    private loadChunk;
    /**
     * Find chunk IDs that might contain matching records
     */
    private findCandidateChunks;
    /**
     * Check if a record matches all conditions
     */
    private matchesConditions;
    /**
     * Start a chainable query
     */
    query(): QueryBuilder<TItem, TCondition, TSortableField>;
    /**
     * Execute a query with options (internal, used by QueryBuilder)
     */
    executeQuery(options?: ClientQueryOptions<TSortableField>): Promise<TItem[]>;
    /**
     * Get a single record by primary key
     */
    get(id: string | number): Promise<TItem | null>;
    /**
     * Count records matching a query (internal, used by QueryBuilder)
     */
    executeCount(options?: {
        conditions?: Condition[];
    }): Promise<number>;
    /**
     * Get schema information
     */
    getSchema(): Promise<Schema>;
    /**
     * Clear the chunk cache
     */
    clearCache(): void;
}
/**
 * Create a typed client instance
 */
declare function createClient<TItem extends Record<string, unknown> = Record<string, unknown>, TCondition extends Condition = Condition, TSortableField extends string = string>(options: ClientOptions): StaticShardClient<TItem, TCondition, TSortableField>;

export { type BuildConfig as B, type BooleanFieldAccessor, type ChunkMeta, type ClientOptions, type ClientQueryOptions, type Condition, type DataRecord as D, type FieldType as F, type InspectOptions as I, type Manifest, type NumericFieldAccessor, type OperatorType, type ParseResult as P, QueryBuilder, type Schema, StaticShardClient, type StringFieldAccessor, type FieldSchema as a, type FieldStats as b, booleanField, type DataFormat as c, contains, createClient, type BuildOptions as d, endsWith, eq, gt, gte, inArray, lt, lte, neq, numericField, startsWith, stringField };
