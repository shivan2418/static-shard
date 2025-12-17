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
  // Field accessor factories
  stringField,
  numericField,
  booleanField,
  // Types
  type Condition,
  type OperatorType,
  type ClientQueryOptions,
  type ClientOptions,
  type StringFieldAccessor,
  type NumericFieldAccessor,
  type BooleanFieldAccessor,
} from "./client/index.js";
