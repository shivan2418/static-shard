export type FieldKind = "string" | "number" | "boolean" | "date";

/** Input file shapes accepted by `build` (T9). NDJSON/JSONL is the preferred format (= shard payload format). */
export type InputFormat = "ndjson" | "json" | "csv" | "tsv";

export interface FieldConfig {
  kind: FieldKind;
  /** Opt-in secondary index (ADR-0003): builds a chunked inverted index + zonemap for this non-sort field. */
  indexed?: boolean;
  /** Opt-in reversed-value index — unlocks `endsWith` (ADR-0003 §7). Requires `kind: "string"` and `indexed: true`. */
  endsWith?: boolean;
  /** Opt-in trigram index — unlocks `contains` (ADR-0003 §7). Requires `kind: "string"` and `indexed: true`. */
  contains?: boolean;
  /** Value may be missing from a record (absent ≠ null) — unlocks `isNull`/`isAbsent`/`exists` (T7). Requires `indexed: true`. */
  absent?: boolean;
  /** Scalar leaf under an object-array — record value is `string[]`, matched existentially via `some` (T7). Requires `kind: "string"` and `indexed: true`. */
  multi?: boolean;
}

export interface StaticShardConfig {
  /** Name the generated collection is exposed under, e.g. `db.movies`. */
  collection: string;
  input: {
    /** A single file path, or a glob pattern matching same-format files to merge then shard as one dataset (T9). */
    path: string;
    /** Defaults to "ndjson". */
    format?: InputFormat;
    /** Delimited (csv/tsv) column delimiter override. Default: "," for csv, "\t" for tsv. Only valid for those formats (T9). */
    delimiter?: string;
    /** JSON only: dot-path to the array/map of records nested within the parsed document — the record selector for nested JSON (T9). Lands on exactly one node; no array-flattening. */
    records?: string;
  };
  /** Served data tree. Default `public/shard-data`. */
  output?: string;
  /** Generated client dir. Default `src/shard-db`. */
  clientOut?: string;
  /** Baked default for the generated `connect()`. Default `/shard-data`. */
  basePath?: string;
  /** Target compressed shard size in bytes. Default 2 MiB. */
  shardBytes?: number;
  /** Target gzipped size per secondary-index chunk, in bytes. Default ~45 KB (ADR-0003 §5). */
  indexChunkBytes?: number;
  schema: {
    /** Must name a `number` or `date` field (T2: the sole indexed field). */
    sortField: string;
    /** Names a field as the user PK — unlocks the generated client's `get(id)` (T8). */
    pk?: string;
    fields: Record<string, FieldConfig>;
  };
}

export interface ResolvedConfig {
  collection: string;
  inputPath: string;
  inputFormat: InputFormat;
  /** Resolved delimiter for csv/tsv; irrelevant for ndjson/json but always populated for simplicity. */
  inputDelimiter: string;
  inputRecordsPath?: string;
  output: string;
  clientOut: string;
  basePath: string;
  shardBytes: number;
  indexChunkBytes: number;
  sortField: string;
  pk?: string;
  fields: Record<string, FieldConfig>;
}

export interface ShardDescriptor {
  hash: string;
  bytes: number;
  count: number;
}

export interface FieldSchemaEntry {
  kind: FieldKind;
  isDate: boolean;
  indexed: boolean;
  operators: readonly string[];
  /** Present (`true`) only for fields opted into presence semantics (T7) — omitted otherwise, mirroring the runtime's optional `FieldMeta.absent`. */
  absent?: true;
  /** Present (`true`) only for multi-valued (object-array scalar-leaf) fields (T7) — omitted otherwise, mirroring the runtime's optional `FieldMeta.multi`. */
  multi?: true;
  /** Present (`true`) only for the user PK field (T8) — omitted otherwise, mirroring the runtime's optional `FieldMeta.pk`. */
  pk?: true;
}

export interface SchemaDescriptor {
  collection: string;
  sortField: string;
  /** Names the user PK field, if declared (T8) — unlocks the generated client's `get(id)`. */
  pk?: string;
  fields: Record<string, FieldSchemaEntry>;
}

/** Sort field: N+1 monotonic split-points, binary-searchable (ADR-0003 §2). */
export interface SplitPointZonemapEntry {
  splitPoints: unknown[];
}

/** Secondary field: per-shard [min,max] pairs, ordinal-aligned with `shards[]` (ADR-0003 §2/§9). */
export interface PairZonemapEntry {
  pairs: [unknown, unknown][];
  /** String min/max are truncated with a next-string-after upper bound (ADR-0003 §2). */
  truncated?: boolean;
}

export type ZonemapEntry = SplitPointZonemapEntry | PairZonemapEntry;

/** One index chunk's value-range coverage — routing metadata only (ADR-0003 §9). */
export interface IndexChunkDirEntry {
  from: unknown;
  to: unknown;
  file: string;
}

export interface IndexDescriptor {
  /** Enabled operator set for this field's index — drives T5 codegen. */
  operators: readonly string[];
  chunks: IndexChunkDirEntry[];
  /** Reversed-value index chunk directory — present iff `endsWith` opted in (ADR-0003 §7/§9). */
  reversed?: { chunks: IndexChunkDirEntry[] };
  /** Trigram index chunk directory — present iff `contains` opted in (ADR-0003 §7/§9). */
  trigram?: { chunks: IndexChunkDirEntry[] };
}

export interface Manifest {
  formatVersion: number;
  generatorVersion: string;
  dataset: {
    collection: string;
    recordCount: number;
    shardCount: number;
    sortField: string;
  };
  schema: SchemaDescriptor;
  shards: ShardDescriptor[];
  zonemap: Record<string, ZonemapEntry>;
  /** One entry per indexed non-sort field (ADR-0003). */
  indexes: Record<string, IndexDescriptor>;
}
