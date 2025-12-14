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
type ComparisonOperator = "eq" | "neq" | "gt" | "gte" | "lt" | "lte";
type StringOperator = "contains" | "startsWith" | "endsWith";
type FieldCondition = string | number | boolean | null | {
    eq?: unknown;
} | {
    neq?: unknown;
} | {
    gt?: number | string;
} | {
    gte?: number | string;
} | {
    lt?: number | string;
} | {
    lte?: number | string;
} | {
    contains?: string;
} | {
    startsWith?: string;
} | {
    endsWith?: string;
} | {
    in?: unknown[];
};
interface WhereClause {
    [field: string]: FieldCondition;
}
interface QueryOptions {
    where?: WhereClause;
    orderBy?: string | {
        field: string;
        direction: "asc" | "desc";
    };
    limit?: number;
    offset?: number;
}
type DataRecord = {
    [key: string]: unknown;
};
type DataFormat = "json" | "ndjson" | "csv";
interface ParseResult {
    records: DataRecord[];
    format: DataFormat;
}
interface StreamParseOptions {
    format?: DataFormat;
    onRecord?: (record: DataRecord, index: number) => void;
    onError?: (error: Error) => void;
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
}

export type { BuildConfig, BuildOptions, ChunkMeta, ComparisonOperator, DataFormat, DataRecord, FieldCondition, FieldSchema, FieldStats, FieldType, InspectOptions, Manifest, ParseResult, QueryOptions, Schema, StreamParseOptions, StringOperator, WhereClause };
