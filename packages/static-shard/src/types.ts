// The dataset-agnostic runtime's type machinery (ADR-0004). Ported from
// prototypes/codegen-client/runtime.ts: a generic runtime parameterized by a
// generated `as const` schema — all typing lives here as mapped types. The
// generated facade (schema.ts + client.ts, emitted by static-shard-cli) only
// narrows this generic surface to named, go-to-definition collections.

export type FieldKind = "string" | "number" | "date" | "boolean";

export interface FieldMeta {
  readonly kind: FieldKind;
  /** The enabled operator names for this field — data, not implied by `kind` (ADR-0003 §7). */
  readonly operators: readonly string[];
  /** Multi-valued (string[]) → existential `some`. */
  readonly multi?: boolean;
  /** This field is the user PK. */
  readonly pk?: boolean;
  /** Value can be missing → is null / is absent / exists surface. */
  readonly absent?: boolean;
}

export interface CollectionMeta {
  /** Present ⟺ a user PK was declared → `get(id)` is emitted. */
  readonly pk?: string;
  readonly fields: { readonly [field: string]: FieldMeta };
}

export interface SchemaMeta {
  readonly [collection: string]: CollectionMeta;
}

// ---------------------------------------------------------------------------
// Per-kind operator → value-type tables. The FULL set; a field exposes only
// the subset its `operators` tuple names (config-driven, ADR-0003 §7).
// ---------------------------------------------------------------------------
type AllStringOps = {
  equals: string;
  not: string; // filter-only rider
  in: string[];
  startsWith: string;
  contains: string; // opt-in (trigram index) + prunes
  endsWith: string; // opt-in (reversed index) + prunes
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

type PickOps<All, Ops extends string> = {
  [K in Extract<keyof All, Ops>]?: All[K];
};

type AbsentOps<F> = F extends { absent: true } ? { isNull?: true; isAbsent?: true; exists?: boolean } : {};

/** `{ some: value }` ≡ `{ some: { equals: value } }` (ADR-0001) — only offered where `equals` is itself enabled. */
type SomeShorthand<F extends FieldMeta> = "equals" extends F["operators"][number] ? string : never;

type FilterFor<F extends FieldMeta> = F extends { kind: "string"; multi: true }
  ? { some?: PickOps<AllStringOps, F["operators"][number]> | SomeShorthand<F> }
  : F extends { kind: "string" }
    ? PickOps<AllStringOps, F["operators"][number]> & AbsentOps<F>
    : F extends { kind: "number" }
      ? PickOps<AllNumberOps, F["operators"][number]> & AbsentOps<F>
      : F extends { kind: "date" }
        ? PickOps<AllDateOps, F["operators"][number]> & AbsentOps<F>
        : F extends { kind: "boolean" }
          ? PickOps<AllBoolOps, F["operators"][number]> & AbsentOps<F>
          : never;

/** The where type: ONLY indexed fields, each with ONLY its valid operators. */
export type WhereOf<C extends CollectionMeta> = {
  [K in keyof C["fields"]]?: FilterFor<C["fields"][K]>;
};

/** orderBy over indexed fields only. */
export type OrderByOf<C extends CollectionMeta> = {
  [K in keyof C["fields"]]?: "asc" | "desc";
};

// ---------------------------------------------------------------------------
// EXACT-TYPE validation. A generic `where?: W` alone would disable excess-
// property checking, silently admitting unknown fields / disabled operators.
// Capture the query literal as W and re-implement every check by hand.
// ---------------------------------------------------------------------------
type ValidateFilter<F, Allowed> = { [Op in keyof F]: Op extends keyof Allowed ? Allowed[Op] : never };
export type ValidateWhere<W, C extends CollectionMeta> = {
  [K in keyof W]: K extends keyof C["fields"]
    ? ValidateFilter<NonNullable<W[K]>, FilterFor<C["fields"][K]>>
    : never;
};

// ---------------------------------------------------------------------------
// Filter-only rider rule (ADR-0003 §7): only `not`/negation cannot prune, so a
// where whose sole constraint is `not` would force a full scan. Encoded at the
// type level via a branded required property whose NAME is the fix message.
// ---------------------------------------------------------------------------
type RiderOp = "not";
type FieldHasPruning<F> = F extends object ? (Exclude<keyof F, RiderOp> extends never ? false : true) : false;
type FieldHasRider<F> = F extends object ? (Extract<keyof F, RiderOp> extends never ? false : true) : false;
type AnyPrunes<W> = true extends { [K in keyof W]: FieldHasPruning<NonNullable<W[K]>> }[keyof W] ? true : false;
type AnyRides<W> = true extends { [K in keyof W]: FieldHasRider<NonNullable<W[K]>> }[keyof W] ? true : false;
export type RiderGuard<W> = AnyRides<W> extends true
  ? AnyPrunes<W> extends true
    ? {}
    : { "❌ add a pruning filter — `not` cannot be the only constraint": never }
  : {};

// Defense-in-depth: the SAME rule at runtime, for untyped JS callers and
// dynamically-built where objects the compiler never sees.
const RIDER_OPS = new Set<string>(["not"]);
export function assertWhereHasPruning(where: Record<string, Record<string, unknown>> | undefined): void {
  if (!where) return;
  const fields = Object.values(where);
  if (fields.length === 0) return;
  const hasPruning = fields.some((filter) => Object.keys(filter ?? {}).some((op) => !RIDER_OPS.has(op)));
  if (!hasPruning) {
    throw new Error(
      "static-shard: `not` cannot be the only constraint — " +
        "add a pruning filter (equals / in / startsWith / contains / endsWith / range / some).",
    );
  }
}

// ---------------------------------------------------------------------------
// The collection surface: `findMany` (T2) + `count` (T4) + `getSchema`.
// `get(id)` (user PK) lands in a later ticket that extends this contract.
// ---------------------------------------------------------------------------
export interface FindManyArgs<C extends CollectionMeta, W extends WhereOf<C>> {
  where?: W & ValidateWhere<W, C> & RiderGuard<W>;
  orderBy?: OrderByOf<C>;
  limit?: number;
  offset?: number;
}

export interface FindManyResult<Rec> {
  records: Rec[];
  hasMore: boolean;
}

/**
 * Approximate upper bound for pagination totals (ADR-0008 §2/§3): `exact: true`
 * only for an empty where (→ recordCount) and pruned-to-zero (→ 0), so
 * `count === 0` is always a trustworthy existence check.
 */
export interface CountResult {
  count: number;
  exact: boolean;
}

/**
 * Reserved for the deferred v2 exact mode — 1.0 locks the slot to `false`, so
 * passing `exact: true` is a compile-time error (ADR-0008 §4).
 */
export interface CountOptions {
  exact?: false;
}

export interface Collection<C extends CollectionMeta, Rec> {
  findMany<W extends WhereOf<C>>(args?: FindManyArgs<C, W>): Promise<FindManyResult<Rec>>;
  // No RiderGuard here, deliberately: a `not`-only where cannot refine an
  // un-fetched count, so it just widens the upper bound (ADR-0008 §3) — count
  // never full-scans, so the rider rule has nothing to guard.
  count<W extends WhereOf<C>>(where?: W & ValidateWhere<W, C>, opts?: CountOptions): Promise<CountResult>;
  getSchema(): C;
}

export interface ClientOptions {
  basePath: string;
  /** Injectable for non-browser / testing; defaults to global `fetch`. */
  fetch?: typeof fetch;
  /**
   * Client-level result ceiling (default 10_000), a guardrail distinct from
   * per-query `limit` — fail-loud (ADR-0004/0007): an explicit `limit` above
   * it throws `LIMIT_EXCEEDED`, and an unbounded query that would match more
   * than it throws rather than silently truncating.
   */
  maxResults?: number;
}

export type GenericClient<S extends SchemaMeta, Records> = {
  [K in keyof S]: K extends keyof Records ? Collection<S[K], Records[K]> : never;
};
