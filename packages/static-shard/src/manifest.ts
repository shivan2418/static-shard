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

export async function fetchManifest(basePath: string, fetchImpl: typeof fetch): Promise<Manifest> {
  const response = await fetchImpl(`${basePath}/manifest.json`);
  if (!response.ok) {
    throw new Error(`static-shard: failed to fetch manifest.json at "${basePath}" (status ${response.status})`);
  }
  return (await response.json()) as Manifest;
}
