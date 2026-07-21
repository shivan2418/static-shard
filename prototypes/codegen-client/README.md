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
