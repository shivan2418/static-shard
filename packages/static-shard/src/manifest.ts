export interface ShardDescriptor {
  hash: string;
  bytes: number;
  count: number;
}

export interface FieldSchemaEntry {
  kind: string;
  isDate: boolean;
  indexed: boolean;
  operators: readonly string[];
}

export interface SchemaDescriptor {
  collection: string;
  sortField: string;
  fields: Record<string, FieldSchemaEntry>;
}

/** Sort field: N+1 monotonic split-points, binary-searchable (ADR-0003 §2). */
export interface SplitPointZonemapEntry {
  splitPoints: unknown[];
}

/** Secondary field: per-shard [min,max] pairs, ordinal-aligned with `shards[]` (ADR-0003 §2/§9). */
export interface PairZonemapEntry {
  pairs: [unknown, unknown][];
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
  operators: readonly string[];
  chunks: IndexChunkDirEntry[];
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
  indexes: Record<string, IndexDescriptor>;
}

export async function fetchManifest(basePath: string, fetchImpl: typeof fetch): Promise<Manifest> {
  const response = await fetchImpl(`${basePath}/manifest.json`);
  if (!response.ok) {
    throw new Error(`static-shard: failed to fetch manifest.json at "${basePath}" (status ${response.status})`);
  }
  return (await response.json()) as Manifest;
}
