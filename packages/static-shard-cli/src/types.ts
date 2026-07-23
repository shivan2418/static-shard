export type FieldKind = "string" | "number" | "boolean" | "date";

export interface FieldConfig {
  kind: FieldKind;
}

export interface StaticShardConfig {
  /** Name the generated collection is exposed under, e.g. `db.movies`. */
  collection: string;
  input: {
    path: string;
    /** T2 supports NDJSON only; other formats land in a later ticket. */
    format?: "ndjson";
  };
  /** Served data tree. Default `public/shard-data`. */
  output?: string;
  /** Generated client dir. Default `src/shard-db`. */
  clientOut?: string;
  /** Baked default for the generated `connect()`. Default `/shard-data`. */
  basePath?: string;
  /** Target compressed shard size in bytes. Default 2 MiB. */
  shardBytes?: number;
  schema: {
    /** Must name a `number` or `date` field (T2: the sole indexed field). */
    sortField: string;
    fields: Record<string, FieldConfig>;
  };
}

export interface ResolvedConfig {
  collection: string;
  inputPath: string;
  output: string;
  clientOut: string;
  basePath: string;
  shardBytes: number;
  sortField: string;
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
}

export interface SchemaDescriptor {
  collection: string;
  sortField: string;
  fields: Record<string, FieldSchemaEntry>;
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
  zonemap: Record<string, { splitPoints: unknown[] }>;
}
