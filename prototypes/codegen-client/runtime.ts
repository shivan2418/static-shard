// ============================================================================
// PROTOTYPE — THROWAWAY. Answers wayfinder ticket T5 (Codegen & typed-client
// shape, #8). This file is the DATASET-AGNOSTIC RUNTIME — the single npm
// package `static-shard` that every consumer installs. It is NOT generated.
//
// The blend being prototyped (from the T5 discussion):
//   * spine  = tRPC-style: a generic runtime parameterized by a generated
//              `as const` schema; all typing done here by mapped types.
//   * skin   = hey-api/Prisma-style: codegen emits a *thin concrete facade*
//              (generated/client.ts) so `db.movies` is a real named member
//              with clean error messages — not raw generic index access.
//
// Runtime dependency footprint: ZERO third-party deps, NO WASM. Pure TS +
// fetch + (optionally) DecompressionStream. The planner below is stubbed to
// return empty data — only the TYPES are real, which is the whole point.
// ============================================================================

// ---------------------------------------------------------------------------
// 1. The shape a generated schema const must have (the codegen contract).
//    `operators` is the ADR-0003 §7 lever: operator availability is DATA, read
//    off the manifest/config per field — not implied by `kind` alone.
// ---------------------------------------------------------------------------
export type FieldKind = "string" | "number" | "date" | "boolean";

export interface FieldMeta {
  readonly kind: FieldKind;
  readonly operators: readonly string[]; // the enabled operator names for this field
  readonly multi?: boolean; // multi-valued (string[]) → existential `some`
  readonly pk?: boolean; // this field is the user PK
  readonly absent?: boolean; // value can be missing → is null / is absent / exists surface
}

export interface CollectionMeta {
  readonly pk?: string; // present ⟺ a user PK was declared (T1) → get(id) emitted
  readonly fields: { readonly [field: string]: FieldMeta };
}

export interface SchemaMeta {
  readonly [collection: string]: CollectionMeta;
}

// ---------------------------------------------------------------------------
// 2. Per-kind operator → value-type tables. The FULL set; a field exposes only
//    the subset its `operators` tuple names (config-driven, ADR-0003 §7).
// ---------------------------------------------------------------------------
type AllStringOps = {
  equals: string;
  not: string; // filter-only rider
  in: string[];
  startsWith: string;
  contains: string; // opt-in (trigram index) + filter-only rider
  endsWith: string; // opt-in (reversed index) + filter-only rider
};
type AllNumberOps = {
  equals: number;
  not: number; // filter-only rider
  in: number[];
  gt: number;
  gte: number;
  lt: number;
  lte: number;
};
type AllDateOps = {
  // dates compare as ISO strings
  equals: string;
  not: string; // filter-only rider
  in: string[];
  gt: string;
  gte: string;
  lt: string;
  lte: string;
};
type AllBoolOps = {
  equals: boolean;
  not: boolean; // filter-only rider
};

// Pick only the operators this field enabled.
type PickOps<All, Ops extends string> = {
  [K in Extract<keyof All, Ops>]?: All[K];
};

// The is-null / is-absent / exists surface (ADR-0002): only for absentable fields.
type AbsentOps<F> = F extends { absent: true }
  ? { isNull?: true; isAbsent?: true; exists?: boolean }
  : {};

// One field's filter object: value types by kind, operators by config.
type FilterFor<F extends FieldMeta> = F extends { kind: "string"; multi: true }
  ? { some?: PickOps<AllStringOps, F["operators"][number]> | string }
  : F extends { kind: "string" }
    ? PickOps<AllStringOps, F["operators"][number]> & AbsentOps<F>
    : F extends { kind: "number" }
      ? PickOps<AllNumberOps, F["operators"][number]> & AbsentOps<F>
      : F extends { kind: "date" }
        ? PickOps<AllDateOps, F["operators"][number]> & AbsentOps<F>
        : F extends { kind: "boolean" }
          ? PickOps<AllBoolOps, F["operators"][number]> & AbsentOps<F>
          : never;

// The where type: ONLY indexed fields, each with ONLY its valid operators.
export type WhereOf<C extends CollectionMeta> = {
  [K in keyof C["fields"]]?: FilterFor<C["fields"][K]>;
};

// orderBy / limit / offset over indexed fields.
export type OrderByOf<C extends CollectionMeta> = {
  [K in keyof C["fields"]]?: "asc" | "desc";
};

// ---------------------------------------------------------------------------
// 3. EXACT-TYPE validation. A generic `where?: W` alone would disable
//    TypeScript's excess-property check (letting `genres.equals`, unknown
//    fields, or disabled operators slip through). So we capture the literal as
//    W and re-implement every check by hand: any illegal field, disabled
//    operator, or wrong value type is mapped to `never`, so the literal fails
//    to assign. This keeps ALL the checks a fixed `WhereOf<C>` gave us AND lets
//    us encode the cross-field rider rule below — which a fixed type cannot.
// ---------------------------------------------------------------------------
// Per field: keep only operator keys valid for that field, and force each to
// its ALLOWED value type (so wrong value types collapse to never on intersect).
type ValidateFilter<F, Allowed> = { [Op in keyof F]: Op extends keyof Allowed ? Allowed[Op] : never };
// Per where: valid field → validated filter; unknown field → never.
type ValidateWhere<W, C extends CollectionMeta> = {
  [K in keyof W]: K extends keyof C["fields"]
    ? ValidateFilter<NonNullable<W[K]>, FilterFor<C["fields"][K]>>
    : never;
};

// ---------------------------------------------------------------------------
//    Filter-only rider rule (ADR-0003 §7). ONLY `not` / negation is a rider:
//    it matches nearly every shard, no structure can prune it, so a where whose
//    only constraint is `not` would force a full scan (T1 forbids).
//    NOTE: `contains` (trigram index) and `endsWith` (reversed-value index) DO
//    prune once their per-field index is built — trigram postings / a reversed
//    prefix range narrow the candidate shards — so they are NOT riders and are
//    valid as a sole constraint. (The T5 ticket body mis-grouped them; ADR-0003
//    §7 is authoritative and classifies only `not` as the rider.)
//    Encoded at the TYPE level: if `not` appears with no pruning constraint
//    anywhere, RiderGuard injects a required branded property whose NAME is the
//    fix, so the call fails with a readable message. Empty where is fine.
// ---------------------------------------------------------------------------
type RiderOp = "not";
type FieldHasPruning<F> = F extends object ? (Exclude<keyof F, RiderOp> extends never ? false : true) : false;
type FieldHasRider<F> = F extends object ? (Extract<keyof F, RiderOp> extends never ? false : true) : false;
type AnyPrunes<W> = true extends { [K in keyof W]: FieldHasPruning<NonNullable<W[K]>> }[keyof W] ? true : false;
type AnyRides<W> = true extends { [K in keyof W]: FieldHasRider<NonNullable<W[K]>> }[keyof W] ? true : false;
type RiderGuard<W> = AnyRides<W> extends true
  ? AnyPrunes<W> extends true
    ? {}
    : { "❌ add a pruning filter — `not` cannot be the only constraint": never }
  : {};

// Defense-in-depth: the SAME rule at runtime, for untyped JS callers and
// dynamically-built where objects the compiler never sees.
const RIDER_OPS = new Set<string>(["not"]);
export function assertWhereHasPruning(where: Record<string, Record<string, unknown>> | undefined): void {
  if (!where) return; // findMany-all
  const fields = Object.values(where);
  if (fields.length === 0) return;
  const hasPruning = fields.some((filter) =>
    Object.keys(filter ?? {}).some((op) => !RIDER_OPS.has(op)),
  );
  if (!hasPruning) {
    throw new Error(
      "static-shard: `not` cannot be the only constraint — " +
        "add a pruning filter (equals / in / startsWith / contains / endsWith / range / some).",
    );
  }
}

// ---------------------------------------------------------------------------
// 4. The collection surface. `get(id)` exists ONLY when the collection meta
//    carries a `pk` (T1). `findMany`/`count` are generic over the where literal
//    W so the exact-type + rider checks above bite on the actual call.
// ---------------------------------------------------------------------------
export interface FindManyArgs<C extends CollectionMeta, W extends WhereOf<C>> {
  where?: W & ValidateWhere<W, C> & RiderGuard<W>;
  orderBy?: OrderByOf<C>;
  limit?: number;
  offset?: number;
}

interface BaseCollection<C extends CollectionMeta, Rec> {
  findMany<W extends WhereOf<C>>(args?: FindManyArgs<C, W>): Promise<Rec[]>;
  count<W extends WhereOf<C>>(args?: { where?: W & ValidateWhere<W, C> & RiderGuard<W> }): Promise<number>;
  getSchema(): C;
}
interface PkCollection<Rec> {
  // Emitted ONLY when a user PK exists. `get` is equality-on-PK (ADR-0002).
  get(id: string): Promise<Rec | null>;
}
export type Collection<C extends CollectionMeta, Rec> = C extends { pk: string }
  ? BaseCollection<C, Rec> & PkCollection<Rec>
  : BaseCollection<C, Rec>;

// ---------------------------------------------------------------------------
// 5. The public factory. `createClient(schema, { basePath })`.
//    Resolution (documented, stubbed): manifest at `${basePath}/manifest.json`,
//    lazy index chunks + content-hashed NDJSON shards resolved relative to
//    basePath from the manifest. Returns a generic client; the generated facade
//    (generated/client.ts) casts it to the concrete named-collection `Db`.
// ---------------------------------------------------------------------------
/** Default client-level result ceiling if the caller doesn't set one. */
export const DEFAULT_MAX_RESULTS = 10_000;

export interface ClientOptions {
  basePath: string;
  fetch?: typeof fetch; // injectable for non-browser / testing; defaults to global fetch
  // Hard ceiling on records any single findMany may return — a safety rail
  // against a broad query pulling a huge number of whole shards into the
  // browser. Distinct from per-query `limit`: `limit` is the caller's intent,
  // `maxResults` is the client's guardrail. Semantics (fail loud, per T1 ethos):
  //   * no `limit`      → effective limit = maxResults; if the query WOULD match
  //                       more than maxResults, throw (don't silently truncate).
  //   * `limit` ≤ max   → honored as-is.
  //   * `limit` > max   → throw (the guardrail wins over the per-query ask).
  maxResults?: number;
}

export type GenericClient<S extends SchemaMeta, Records> = {
  [K in keyof S]: K extends keyof Records ? Collection<S[K], Records[K]> : never;
};

/**
 * Resolve the effective per-query limit against the client ceiling.
 * Throws if an explicit `limit` exceeds `maxResults` (guardrail wins, fail loud).
 * `matchCount` (from the planner's cheap postings/zonemap count) lets an
 * UNBOUNDED query throw before fetching shards rather than silently truncate.
 */
export function resolveEffectiveLimit(
  limit: number | undefined,
  maxResults: number,
  matchCount?: number,
): number {
  if (limit != null && limit > maxResults) {
    throw new Error(
      `static-shard: limit ${limit} exceeds client maxResults ${maxResults} — ` +
        `raise maxResults or lower the limit.`,
    );
  }
  if (limit == null && matchCount != null && matchCount > maxResults) {
    throw new Error(
      `static-shard: query matches ${matchCount} records, over client maxResults ${maxResults} — ` +
        `pass an explicit \`limit\` or narrow the query (no silent truncation).`,
    );
  }
  return limit ?? maxResults;
}

export function createClient<S extends SchemaMeta, Records>(
  schema: S,
  opts: ClientOptions,
): GenericClient<S, Records> {
  const maxResults = opts.maxResults ?? DEFAULT_MAX_RESULTS;
  // --- STUBBED RUNTIME. Real impl: load manifest, zonemap-prune, fetch index
  //     chunks, intersect postings, fetch shards, post-filter `not`, decode. ---
  const makeCollection = (meta: CollectionMeta) => {
    const base = {
      findMany: async (args?: {
        where?: Record<string, Record<string, unknown>>;
        limit?: number;
      }) => {
        assertWhereHasPruning(args?.where); // runtime rider guard
        resolveEffectiveLimit(args?.limit, maxResults); // runtime maxResults guard
        return [];
      },
      count: async (args?: { where?: Record<string, Record<string, unknown>> }) => {
        assertWhereHasPruning(args?.where);
        return 0;
      },
      getSchema: () => meta,
    };
    if (meta.pk) return { ...base, get: async () => null };
    return base;
  };
  const out: Record<string, unknown> = {};
  for (const name of Object.keys(schema)) out[name] = makeCollection(schema[name]);
  return out as GenericClient<S, Records>;
}
