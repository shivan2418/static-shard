/**
 * Core type definitions for static-shard
 */

// Schema types
export type FieldType = "string" | "number" | "boolean" | "date" | "null";

export interface FieldSchema {
  name: string;
  type: FieldType;
  nullable: boolean;
  indexed: boolean;
  stats: FieldStats;
}

export interface FieldStats {
  min?: string | number;
  max?: string | number;
  cardinality: number;
  nullCount: number;
  sampleValues?: (string | number | boolean | null)[];
}

export interface Schema {
  fields: FieldSchema[];
  primaryField: string | null;
}

// Manifest types
export interface ChunkMeta {
  id: string;
  path: string;
  count: number;
  byteSize: number;
  fieldRanges: Record<string, { min: unknown; max: unknown }>;
}

export interface Manifest {
  version: string;
  generatedAt: string;
  schema: Schema;
  chunks: ChunkMeta[];
  indices: Record<string, Record<string, string[]>>;
  totalRecords: number;
  config: BuildConfig;
}

// Build configuration
export interface BuildConfig {
  chunkSize: number; // target bytes per chunk
  chunkBy: string | null; // field to chunk by
  indexedFields: string[];
}

// Query types
export type ComparisonOperator = "eq" | "neq" | "gt" | "gte" | "lt" | "lte";
export type StringOperator = "contains" | "startsWith" | "endsWith";

export type FieldCondition =
  | string
  | number
  | boolean
  | null
  | { eq?: unknown }
  | { neq?: unknown }
  | { gt?: number | string }
  | { gte?: number | string }
  | { lt?: number | string }
  | { lte?: number | string }
  | { contains?: string }
  | { startsWith?: string }
  | { endsWith?: string }
  | { in?: unknown[] };

export interface WhereClause {
  [field: string]: FieldCondition;
}

export interface QueryOptions {
  where?: WhereClause;
  orderBy?: string | { field: string; direction: "asc" | "desc" };
  limit?: number;
  offset?: number;
}

// DataRecord type (generic for user data)
export type DataRecord = { [key: string]: unknown };

// Parser types
export type DataFormat = "json" | "ndjson" | "csv";

export interface ParseResult {
  records: DataRecord[];
  format: DataFormat;
}

export interface StreamParseOptions {
  format?: DataFormat;
  onRecord?: (record: DataRecord, index: number) => void;
  onError?: (error: Error) => void;
}

// CLI options
export interface BuildOptions {
  output: string;
  chunkSize: string;
  chunkBy?: string;
  index?: string;
  format?: DataFormat;
}

export interface InspectOptions {
  sample?: number;
  format?: DataFormat;
}
