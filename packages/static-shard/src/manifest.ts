import { ShardError } from "./errors.js";
import { fetchJson } from "./fetch-file.js";
import { FORMAT_VERSION } from "./version.js";

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
  const url = `${basePath}/manifest.json`;
  const parsed = (await fetchJson(url, "manifest", fetchImpl)) as Manifest;
  // JSON-valid but not a manifest — the body "won't parse" into one (ADR-0007 §5).
  if (typeof parsed.formatVersion !== "number") {
    throw new ShardError({
      code: "CORRUPT_DATA",
      url,
      message: `static-shard: the manifest at "${url}" parsed as JSON but has no numeric formatVersion — the deploy is corrupt. Re-run \`static-shard build\` and redeploy.`,
    });
  }
  // ADR-0005: same major → always compatible (SemVer); major mismatch → fail loud.
  if (parsed.formatVersion !== FORMAT_VERSION) {
    throw new ShardError({
      code: "FORMAT_VERSION",
      url,
      message:
        `static-shard: the dataset at "${url}" was built with static-shard major ${String(parsed.formatVersion)} ` +
        `but this runtime is major ${FORMAT_VERSION} — align versions and re-run \`static-shard build\`.`,
    });
  }
  return parsed;
}
