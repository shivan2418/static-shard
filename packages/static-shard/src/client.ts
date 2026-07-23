import { fetchIndexChunk } from "./index-fetch.js";
import { matchesWhere } from "./filter.js";
import { fetchManifest, type Manifest } from "./manifest.js";
import { chunksForFilter, decodeIndexChunk, shardIndicesForFilter, type IndexChunkFile, type SecondaryFieldFilter } from "./secondary-index.js";
import { fetchShardRecords } from "./shard-fetch.js";
import {
  assertWhereHasPruning,
  type ClientOptions,
  type CollectionMeta,
  type CountResult,
  type FieldKind,
  type GenericClient,
  type SchemaMeta,
} from "./types.js";
import { candidateShardIndices, type SortFieldFilter, type SortValue } from "./zonemap.js";

interface RawFindManyArgs {
  where?: Record<string, Record<string, unknown>>;
  orderBy?: Record<string, "asc" | "desc">;
  limit?: number;
  offset?: number;
}

/** Only equals/in/startsWith prune via the inverted index (ADR-0003 §7); other keys (e.g. `not`) don't. */
function secondaryFilterOf(rawFilter: Record<string, unknown>): SecondaryFieldFilter | undefined {
  const { equals, in: inValues, startsWith } = rawFilter;
  if (equals === undefined && inValues === undefined && startsWith === undefined) return undefined;
  return { equals, in: inValues as unknown[] | undefined, startsWith: startsWith as string | undefined };
}

/** The plumbing every chunk/shard fetch in one query shares — travels as a unit rather than three loose params. */
interface FetchContext {
  basePath: string;
  fetchImpl: typeof fetch;
  chunkCache: Map<string, Promise<IndexChunkFile>>;
}

/** Fetches+intersects the index chunks covering one secondary field's filter into its candidate shard set. */
async function secondaryFieldCandidates(
  manifest: Manifest,
  ctx: FetchContext,
  field: string,
  rawFilter: Record<string, unknown>,
): Promise<Set<number> | undefined> {
  const indexDescriptor = manifest.indexes[field];
  const secondaryFilter = indexDescriptor && secondaryFilterOf(rawFilter);
  if (!indexDescriptor || !secondaryFilter) return undefined;

  const kind = manifest.schema.fields[field]!.kind as FieldKind;
  const shardIndices = new Set<number>();
  for (const chunkDir of chunksForFilter(indexDescriptor.chunks, secondaryFilter)) {
    let chunkPromise = ctx.chunkCache.get(chunkDir.file);
    if (!chunkPromise) {
      chunkPromise = fetchIndexChunk(ctx.basePath, chunkDir.file, ctx.fetchImpl);
      ctx.chunkCache.set(chunkDir.file, chunkPromise);
    }
    const decoded = decodeIndexChunk(await chunkPromise, kind);
    for (const shardIndex of shardIndicesForFilter(decoded, secondaryFilter)) shardIndices.add(shardIndex);
  }
  return shardIndices;
}

/**
 * Shard ordinals surviving zonemap + postings pruning for `where`, ascending.
 * Free zonemap pruning on the sort field first (ADR-0003 §6 step 1), then
 * fetch+intersect the index chunks for every equals/in/startsWith-constrained
 * secondary field (step 2) — cheap chunk fetches before shards.
 */
async function candidateIndicesForWhere(
  manifest: Manifest,
  ctx: FetchContext,
  where: Record<string, Record<string, unknown>> | undefined,
): Promise<number[]> {
  const sortField = manifest.dataset.sortField;
  const sortFieldFilter = where?.[sortField] as SortFieldFilter | undefined;
  const sortZonemap = manifest.zonemap[sortField] as { splitPoints?: SortValue[] } | undefined;
  const splitPoints = sortZonemap?.splitPoints ?? [];

  let candidateSet = new Set(candidateShardIndices(splitPoints, sortFieldFilter));
  const secondaryEntries = Object.entries(where ?? {}).filter(([field]) => field !== sortField);
  const secondarySets = await Promise.all(
    secondaryEntries.map(([field, filter]) => secondaryFieldCandidates(manifest, ctx, field, filter)),
  );
  for (const set of secondarySets) {
    if (set === undefined) continue;
    candidateSet = new Set([...candidateSet].filter((index) => set.has(index)));
  }
  return [...candidateSet].sort((a, b) => a - b);
}

/**
 * Approximate upper bound with zero data-shard fetches (ADR-0008 §2): sum
 * `manifest.shards[i].count` over the shards surviving zonemap + postings
 * pruning. `exact: true` only for an empty where and pruned-to-zero (§3).
 */
async function executeCount(
  manifest: Manifest,
  ctx: FetchContext,
  where: Record<string, Record<string, unknown>> | undefined,
): Promise<CountResult> {
  if (!where || Object.keys(where).length === 0) {
    return { count: manifest.dataset.recordCount, exact: true };
  }
  const candidateIndices = await candidateIndicesForWhere(manifest, ctx, where);
  if (candidateIndices.length === 0) return { count: 0, exact: true };
  let count = 0;
  for (const index of candidateIndices) count += manifest.shards[index]!.count;
  return { count, exact: false };
}

async function executeFindMany(
  manifest: Manifest,
  ctx: FetchContext,
  args: RawFindManyArgs | undefined,
): Promise<{ records: Record<string, unknown>[]; hasMore: boolean }> {
  const candidateIndices = await candidateIndicesForWhere(manifest, ctx, args?.where);
  const fetched = await Promise.all(
    candidateIndices.map(async (index) => ({
      index,
      records: await fetchShardRecords(ctx.basePath, manifest.shards[index]!.hash, ctx.fetchImpl),
    })),
  );
  fetched.sort((a, b) => a.index - b.index);

  // Fetched shards are individually sorted ascending by the sort field, and
  // fetched/concatenated in ascending shard-index order — the concatenation
  // is already globally ascending; filtering never reorders it.
  let matches: Record<string, unknown>[] = [];
  for (const { records } of fetched) {
    for (const record of records) {
      if (matchesWhere(record, args?.where)) matches.push(record);
    }
  }

  const orderDirection = args?.orderBy?.[manifest.dataset.sortField];
  if (orderDirection === "desc") matches = matches.reverse();

  const offset = args?.offset ?? 0;
  const windowed = matches.slice(offset);
  if (args?.limit === undefined) {
    return { records: windowed, hasMore: false };
  }
  return { records: windowed.slice(0, args.limit), hasMore: windowed.length > args.limit };
}

export function createClient<S extends SchemaMeta, Records>(
  schema: S,
  opts: ClientOptions,
): GenericClient<S, Records> {
  const basePath = opts.basePath.replace(/\/+$/, "");
  const fetchImpl = opts.fetch ?? fetch;
  let manifestPromise: Promise<Manifest> | undefined;
  const getManifest = (): Promise<Manifest> => (manifestPromise ??= fetchManifest(basePath, fetchImpl));

  const makeCollection = (meta: CollectionMeta) => ({
    findMany: async (args?: RawFindManyArgs) => {
      assertWhereHasPruning(args?.where);
      const manifest = await getManifest();
      const ctx: FetchContext = { basePath, fetchImpl, chunkCache: new Map() };
      return executeFindMany(manifest, ctx, args);
    },
    count: async (where?: Record<string, Record<string, unknown>>) => {
      const manifest = await getManifest();
      const ctx: FetchContext = { basePath, fetchImpl, chunkCache: new Map() };
      return executeCount(manifest, ctx, where);
    },
    getSchema: () => meta,
  });

  const out: Record<string, unknown> = {};
  for (const name of Object.keys(schema)) out[name] = makeCollection(schema[name]!);
  return out as GenericClient<S, Records>;
}
