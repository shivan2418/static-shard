import { fetchIndexChunk } from "./index-fetch.js";
import { ShardError } from "./errors.js";
import { parseCorruptible } from "./fetch-file.js";
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
  /** Passed to every fetch in this query; fired on the first failure (ADR-0007 §7). */
  signal: AbortSignal;
  /** First-failure-wins: aborts the shared controller, then rethrows — so Promise.all rejects fast and outstanding fetches cancel. */
  track<T>(promise: Promise<T>): Promise<T>;
  /** Sync counterpart of track, for post-fetch decode/parse steps — the same first-failure abort must fire (ADR-0007 §7). */
  trackSync<T>(fn: () => T): T;
}

function makeFetchContext(basePath: string, fetchImpl: typeof fetch): FetchContext {
  const controller = new AbortController();
  const abortAndRethrow = (error: unknown): never => {
    controller.abort(error);
    throw error;
  };
  return {
    basePath,
    fetchImpl,
    chunkCache: new Map(),
    signal: controller.signal,
    track: (promise) => promise.catch(abortAndRethrow),
    trackSync: (fn) => {
      try {
        return fn();
      } catch (error) {
        return abortAndRethrow(error);
      }
    },
  };
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
      chunkPromise = ctx.track(fetchIndexChunk(ctx.basePath, chunkDir.file, ctx.fetchImpl, ctx.signal));
      ctx.chunkCache.set(chunkDir.file, chunkPromise);
    }
    const awaitedChunk = await chunkPromise;
    const decoded = ctx.trackSync(() =>
      parseCorruptible(`${ctx.basePath}/${chunkDir.file}`, () => decodeIndexChunk(awaitedChunk, kind)),
    );
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

const DEFAULT_MAX_RESULTS = 10_000;

/** The explicit-limit half of the maxResults guardrail — pure validation, runs before any fetch. */
function assertLimitWithinCeiling(limit: number | undefined, maxResults: number): void {
  if (limit !== undefined && limit > maxResults) {
    throw new ShardError({
      code: "LIMIT_EXCEEDED",
      message:
        `static-shard: limit ${limit} exceeds the maxResults ceiling ${maxResults} — ` +
        `lower the query's limit, or raise maxResults in connect() if you truly need more.`,
    });
  }
}

async function executeFindMany(
  manifest: Manifest,
  ctx: FetchContext,
  args: RawFindManyArgs | undefined,
  maxResults: number,
): Promise<{ records: Record<string, unknown>[]; hasMore: boolean }> {
  const candidateIndices = await candidateIndicesForWhere(manifest, ctx, args?.where);
  const fetched = await Promise.all(
    candidateIndices.map(async (index) => ({
      index,
      records: await ctx.track(fetchShardRecords(ctx.basePath, manifest.shards[index]!.hash, ctx.fetchImpl, ctx.signal)),
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

  // The unbounded half of the maxResults guardrail: a query with no explicit
  // limit that would exceed the ceiling throws rather than silently truncating
  // (ADR-0004 — partial results are indistinguishable from smaller correct ones).
  if (args?.limit === undefined && matches.length > maxResults) {
    throw new ShardError({
      code: "LIMIT_EXCEEDED",
      message:
        `static-shard: this unbounded query matched more than the maxResults ceiling of ${maxResults} records — ` +
        `add a limit ≤ ${maxResults} to paginate, or raise maxResults in connect(). Refusing to silently truncate.`,
    });
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
  const maxResults = opts.maxResults ?? DEFAULT_MAX_RESULTS;
  let manifestPromise: Promise<Manifest> | undefined;
  const getManifest = (): Promise<Manifest> => (manifestPromise ??= fetchManifest(basePath, fetchImpl));

  const makeCollection = (meta: CollectionMeta) => ({
    findMany: async (args?: RawFindManyArgs) => {
      assertWhereHasPruning(args?.where);
      assertLimitWithinCeiling(args?.limit, maxResults);
      const manifest = await getManifest();
      const ctx = makeFetchContext(basePath, fetchImpl);
      return executeFindMany(manifest, ctx, args, maxResults);
    },
    count: async (where?: Record<string, Record<string, unknown>>) => {
      const manifest = await getManifest();
      const ctx = makeFetchContext(basePath, fetchImpl);
      return executeCount(manifest, ctx, where);
    },
    getSchema: () => meta,
  });

  const out: Record<string, unknown> = {};
  for (const name of Object.keys(schema)) out[name] = makeCollection(schema[name]!);
  return out as GenericClient<S, Records>;
}
