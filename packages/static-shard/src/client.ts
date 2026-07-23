import { matchesWhere } from "./filter.js";
import { fetchManifest, type Manifest } from "./manifest.js";
import { fetchShardRecords } from "./shard-fetch.js";
import { assertWhereHasPruning, type ClientOptions, type CollectionMeta, type GenericClient, type SchemaMeta } from "./types.js";
import { candidateShardIndices, type SortFieldFilter, type SortValue } from "./zonemap.js";

interface RawFindManyArgs {
  where?: Record<string, Record<string, unknown>>;
  orderBy?: Record<string, "asc" | "desc">;
  limit?: number;
  offset?: number;
}

async function executeFindMany(
  manifest: Manifest,
  basePath: string,
  fetchImpl: typeof fetch,
  args: RawFindManyArgs | undefined,
): Promise<{ records: Record<string, unknown>[]; hasMore: boolean }> {
  const sortField = manifest.dataset.sortField;
  const sortFieldFilter = args?.where?.[sortField] as SortFieldFilter | undefined;
  const splitPoints = (manifest.zonemap[sortField]?.splitPoints ?? []) as SortValue[];

  const candidateIndices = candidateShardIndices(splitPoints, sortFieldFilter);
  const fetched = await Promise.all(
    candidateIndices.map(async (index) => ({
      index,
      records: await fetchShardRecords(basePath, manifest.shards[index]!.hash, fetchImpl),
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

  const orderDirection = args?.orderBy?.[sortField];
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
      return executeFindMany(manifest, basePath, fetchImpl, args);
    },
    getSchema: () => meta,
  });

  const out: Record<string, unknown> = {};
  for (const name of Object.keys(schema)) out[name] = makeCollection(schema[name]!);
  return out as GenericClient<S, Records>;
}
