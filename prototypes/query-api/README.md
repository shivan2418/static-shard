# PROTOTYPE — Query API surface (wayfinder T2, issue #3)

**Throwaway.** Answers: *what should the v1.0 query API look like, and does it read well with the typed codegen client?* Not production code.

## What's here

A **mock of the generated client** (`generated-client.ts`) for one example dataset (a movie catalogue) — the types are real, the runtime is stubbed. Then the same set of queries written three ways:

- `callsites-A-object-where.ts` — Prisma-style `{ where: { year: { gte: 2000 } } }`
- `callsites-B-builder.ts` — Kysely-style `.where("year", "gte", 2000).many()`
- `callsites-C-hybrid.ts` — object-where canonical + builder as a dynamic-composition escape hatch

Each call-site file ends with `@ts-expect-error` lines asserting the type system **rejects** invalid queries (filtering a non-indexed field, wrong operator for a type, wrong value type, ordering by a non-indexed field, `equals` on a multi-valued field).

## Run it

```
cd prototypes/query-api
pnpm --package=typescript dlx tsc -p tsconfig.json
```

**Exit 0 is the result.** It means every valid call site type-checks AND every `@ts-expect-error` fired (tsc fails on an *unused* expect-error). So the "queryable ⟺ indexed, per-type operators" guarantee holds at compile time.

## The decision to make (from the ticket)

- Operators: eq / not / in / numeric gt·gte·lt·lte / string contains·startsWith·endsWith
- Features: orderBy(+dir), limit, offset, count, get(id) [PK-gated], getSchema
- **Canonical shape: A, B, or C?**
