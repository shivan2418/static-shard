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

type StringOperators = {
    eq?: string;
    neq?: string;
    contains?: string;
    startsWith?: string;
    endsWith?: string;
    in?: string[];
};
type NumericOperators = {
    eq?: number;
    neq?: number;
    gt?: number;
    gte?: number;
    lt?: number;
    lte?: number;
    in?: number[];
};
interface ClientQueryOptions<TWhere = Record<string, unknown>, TSortable extends string = string> {
    where?: TWhere;
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
declare class StaticShardClient<TItem extends Record<string, unknown> = Record<string, unknown>, TWhereClause = Record<string, unknown>, TSortableField extends string = string> {
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
     * Check if a record matches the where clause
     */
    private matchesWhere;
    /**
     * Query records
     */
    query(options?: ClientQueryOptions<TWhereClause, TSortableField>): Promise<TItem[]>;
    /**
     * Get a single record by primary key
     */
    get(id: string | number): Promise<TItem | null>;
    /**
     * Count records matching a query
     */
    count(options?: {
        where?: TWhereClause;
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
declare function createClient<TItem extends Record<string, unknown> = Record<string, unknown>, TWhereClause = Record<string, unknown>, TSortableField extends string = string>(options: ClientOptions): StaticShardClient<TItem, TWhereClause, TSortableField>;

export { type BuildConfig as B, type ChunkMeta, type ClientOptions, type ClientQueryOptions, type DataRecord as D, type FieldType as F, type InspectOptions as I, type Manifest, type NumericOperators, type ParseResult as P, type Schema, StaticShardClient, type StringOperators, type FieldSchema as a, type FieldStats as b, type DataFormat as c, createClient, type BuildOptions as d };
