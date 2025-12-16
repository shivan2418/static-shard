/**
 * static-shard - Query large static datasets efficiently
 *
 * This module exports the client runtime and types.
 * Use the CLI to build your dataset, then import from this package.
 */

// Core types (for CLI and internal use)
export type {
  FieldType,
  FieldSchema,
  FieldStats,
  Schema,
  ChunkMeta,
  Manifest,
  BuildConfig,
  DataRecord,
  DataFormat,
  ParseResult,
  BuildOptions,
  InspectOptions,
} from "./types/index.js";

// Client runtime (for generated clients to import)
export {
  StaticShardClient,
  QueryBuilder,
  createClient,
  type StringOperators,
  type NumericOperators,
  type ClientQueryOptions,
  type ClientOptions,
} from "./client/index.js";
