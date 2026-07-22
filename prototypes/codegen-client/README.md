# PROTOTYPE — Codegen & typed-client shape (wayfinder T5, issue #8)

**Throwaway.** Answers: *what shape does the generated, typed client take — how is it delivered, how are types bound to the query API, and what's the runtime footprint?* Not production code. Types are real; the runtime planner is stubbed to return empty data.

## The decision this prototypes: the "blend"

From the T5 discussion (tRPC vs hey-api as reference points):

- **spine = tRPC-style** — a **generic runtime** (`runtime.ts`, the one npm package everyone installs) parameterized by a generated `as const` schema. All the typing is done here by mapped types.
- **skin = hey-api / Prisma-style** — codegen emits a **thin concrete facade** (`generated/client.ts`) so `db.movies` is a real, named, go-to-definition member with clean hover types — not raw generic index access.

Industry moved *away* from the "single generated file, everything inlined" model (old `openapi-generator`); both references confirm the thin-runtime + generated-surface split.

## What's here

| File | Role | Generated? |
|---|---|---|
| `runtime.ts` | dataset-agnostic runtime: type machinery + `createClient` + rider guard | **no** — the npm package |
| `generated/schema.ts` | full record interfaces + `as const` schema (PK, indexed fields, per-field enabled operators) | **yes** — tiny |
| `generated/client.ts` | thin concrete facade (`Db` interface + `connect()`) | **yes** — ~20 lines |
| `consumer.ts` | the app author's import/call experience + compile-time rejections | no (example) |
| `runtime-guard.ts` | runnable demo of the one runtime-only constraint | no (example) |

## Sample queries

The consumer experience (from `consumer.ts`). `connect()` once, then query named collections. Every field → operator → value autocompletes, and illegal queries are compile errors.

```ts
import { connect } from "./generated/client";

const db = connect({ basePath: "/data", maxResults: 500 });

// Sci-fi films from the 2000s rated above 8, newest first, top 10.
await db.movies.findMany({
  where: { year: { gte: 2000, lt: 2010 }, rating: { gt: 8 }, genres: { some: "Sci-Fi" } },
  orderBy: { year: "desc" },
  limit: 10,
});

// Films by either of two directors (implicit AND across fields; `in` for OR-within-a-field).
await db.movies.findMany({
  where: { director: { in: ["Villeneuve", "Nolan"] }, inPrint: { equals: true } },
});

// Title substring — `contains` is enabled on `title` (trigram index), so it prunes and stands alone.
await db.movies.findMany({ where: { title: { contains: "Matrix" } } });

// Everything except a value: `not` is a post-filter rider, so it needs a pruning constraint alongside it.
await db.movies.findMany({ where: { year: { gte: 2000 }, inPrint: { not: false } } });

// Missing sort values are a real, queryable state (ADR-0002).
await db.movies.findMany({ where: { releaseDate: { isAbsent: true } } });

// How many, not the rows.
await db.movies.count({ where: { rating: { gte: 9 } } });

// Point lookup by the user PK — emitted only because `movies` declared `imdbId`.
const heat = await db.movies.get("tt0113277");

// Fetch-all (no where) — bounded by the client `maxResults` ceiling.
await db.movies.findMany();

// The generated runtime schema, for introspection.
db.movies.getSchema();
```

These are all rejected at compile time (see `consumer.ts` for the full `@ts-expect-error` set):

```ts
db.movies.findMany({ where: { plot: { contains: "hacker" } } });   // plot isn't indexed
db.movies.findMany({ where: { rating: { contains: "8" } } });      // rating didn't build a trigram index
db.movies.findMany({ where: { year: { gt: "2000" } } });           // year is a number
db.movies.findMany({ where: { genres: { equals: "Sci-Fi" } } });   // multi-valued → must use `some`
db.movies.findMany({ where: { inPrint: { not: true } } });         // sole rider → needs a pruning filter
db.screenings.get("anything");                                     // screenings has no PK → no get()
```

## Combining conditions

`where` is **not chained** (no `.where().where()` — that's the builder surface ADR-0001 rejected). It's a single object, and everything in it is **AND**ed together. That's the only combinator in v1.0 — "implicit-AND only; OR/nesting deferred" (ADR-0001). It composes three ways:

```ts
// 1. Several operators on ONE field → AND within the field (a bounded range)
await db.movies.findMany({ where: { year: { gte: 2000, lt: 2010 } } });
//  year >= 2000 AND year < 2010

// 2. Several fields → AND across fields
await db.movies.findMany({
  where: {
    year:    { gte: 2000 },
    rating:  { gt: 8 },
    inPrint: { equals: true },
  },
});
//  year >= 2000 AND rating > 8 AND inPrint = true

// 3. `in` → the one "OR", but only within a single field
await db.movies.findMany({ where: { director: { in: ["Villeneuve", "Nolan"] } } });
//  director = "Villeneuve" OR director = "Nolan"
```

### Nesting: multi-valued `some`

The one nested filter in v1.0 is `some` on a multi-valued field — "at least one element matches this sub-filter." Its inner operators AND together too, and it composes with the outer fields:

```ts
// A genre starting with "Sci", released 2015 or later, in print.
await db.movies.findMany({
  where: {
    genres:  { some: { startsWith: "Sci" } },
    year:    { gte: 2015 },
    inPrint: { equals: true },
  },
});

// Shorthand: a bare string means { equals }.
await db.movies.findMany({ where: { genres: { some: "Sci-Fi" } } });
```

### A worked page: filter + sort + paginate + total

```ts
const where = {
  year:   { gte: 2000, lt: 2020 },
  rating: { gte: 7.5 },
  genres: { some: "Thriller" },
} as const;

const page  = await db.movies.findMany({ where, orderBy: { rating: "desc" }, limit: 20, offset: 40 });
const total = await db.movies.count({ where });   // "showing 41–60 of {total}"
```

### Not in v1.0: OR / AND / NOT across fields

Arbitrary boolean nesting — OR between *different* fields, or NOT-groups — is deferred (ADR-0001). The object shape was chosen so it slots in later **without a breaking change**; this is what it will look like when added, not something wired up for 1.0:

```ts
// ❌ NOT SUPPORTED in v1.0 — top-level OR / AND / NOT keys are a v2 addition
await db.movies.findMany({
  where: {
    OR: [
      { director: { equals: "Nolan" } },
      { rating:   { gt: 9 } },
    ],
  },
});
```

**Workaround today:** queries are cheap and stateless, so OR-across-fields is "run each branch, merge by `id`" — `findMany({director:{equals:"Nolan"}})` + `findMany({rating:{gt:9}})`, then dedupe. Fine for a couple of branches; the native `OR` key is the real fix, post-1.0.

## Run it

```
cd prototypes/codegen-client
pnpm --package=typescript dlx tsc -p tsconfig.json      # types verdict — exit 0
pnpm --package=tsx dlx tsx runtime-guard.ts             # defense-in-depth — "ALL RUNTIME GUARDS OK"
```

**`tsc` exit 0 is the headline result.** tsc errors on an *unused* `@ts-expect-error`, so a silent pass proves every invalid query in `consumer.ts` was actually rejected.

## Everything bites at COMPILE time (via `consumer.ts` `@ts-expect-error`)

- Non-indexed field not filterable (`plot`) — queryable ⟺ indexed (T1).
- Per-type operators (numeric `gt` rejected on a string field; wrong value type).
- **Config-driven operator availability (ADR-0003 §7):** `contains`/`endsWith` present *only* on fields that opted in (`title` has `contains`, `director` has `endsWith`; others reject them).
- Multi-valued fields force existential `some` (scalar `equals` on `genres` rejected).
- Absent surface (`isNull`/`isAbsent`/`exists`) only on absentable fields (ADR-0002).
- `get(id)` emitted **only** when a user PK exists — present on `movies` (`imdbId`), absent on `screenings`.
- `orderBy` restricted to indexed fields.
- **Filter-only rider rule (ADR-0003 §7):** `not` as the *sole* constraint is rejected — the error message names the fix. (`contains`/`endsWith` prune via their trigram/reversed index, so they are valid alone — only `not` is a rider.)

## The one subtle bit: the rider rule IS type-level (via "exact types")

The rider rule needs the *literal's* type (it's cross-field: a pruning op on field A licenses a `not` on field B). The naive way to get the literal — a generic `where?: W` — **disables TypeScript's excess-property check**, which silently lets `genres.equals`, unknown fields, and disabled operators through (verified empirically).

The fix is the **exact-type** technique (runtime.ts §3): capture the literal as generic `W`, then re-implement every check by hand — `ValidateWhere<W,C>` maps any illegal field / disabled operator / wrong value type to `never`, and `RiderGuard<W>` injects a required branded property (whose name is the error message) when `not` appears with no pruning constraint. This keeps **all** the checks a fixed `WhereOf<C>` gave us *and* adds the rider rule.

**Cost of going generic:** noisier error first-line (prints the expanded intersection type before the actionable line), and a compile-time cost bounded by query size (not dataset size). Autocomplete flows through `W`'s constraint (`WhereOf<C>`) — should stay crisp; worth a final in-editor confirmation.

## Defense-in-depth at runtime

`assertWhereHasPruning` (runtime.ts §3) re-checks the rider rule at runtime for untyped JS callers and dynamically-built `where` objects the compiler never sees. Demonstrated in `runtime-guard.ts`.

## The answers (for the ticket)

1. **Delivery:** thin runtime npm package (`static-shard`) + generated `schema.ts` (types) + generated thin facade (`client.ts`). Not a single inlined file.
2. **Types → query API binding:** generated `as const` schema drives runtime mapped types (`WhereOf`/`FilterFor`/`Collection`); facade casts the generic client to a concrete named-collection `Db`.
3. **Runtime footprint:** zero third-party deps, no WASM — pure TS + `fetch` (+ optional `DecompressionStream`).
4. **Public factory:** `connect({ basePath, maxResults? })` (wrapping `createClient(schema, opts)`); manifest at `${basePath}/manifest.json`, shards/chunks resolved relative to `basePath`. `maxResults` is a client-level result ceiling (default 10_000) — a guardrail distinct from per-query `limit`: an explicit `limit` above it throws, and an unbounded query matching more than it throws rather than silently truncating (fail loud, per T1).
5. **Operator availability is data** (per field `operators` tuple), realizing ADR-0003 §7's "operator available ⟺ its structure was built."
