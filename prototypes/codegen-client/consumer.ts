// ============================================================================
// PROTOTYPE — THROWAWAY. THE CONSUMER EXPERIENCE. This is what an app author
// writes. It imports ONLY the generated facade; the runtime is a transitive
// dep. Valid queries must type-check; every `@ts-expect-error` asserts the
// type system REJECTS an invalid one. `tsc --noEmit` passing (exit 0) is the
// verdict: tsc errors on an UNUSED @ts-expect-error, so a silent pass proves
// each rejection actually fired.
// ============================================================================
import { connect } from "./generated/client";

// Client-level result ceiling (guardrail). Optional; defaults to 10_000.
const db = connect({ basePath: "/data", maxResults: 500 });

// ---------------------------------------------------------------------------
// VALID — the happy path. All of these autocomplete field → operator → value.
// ---------------------------------------------------------------------------
async function valid() {
  // Numeric range + implicit AND (ADR-0001).
  await db.movies.findMany({ where: { year: { gte: 2000 }, rating: { gt: 8 } } });

  // String equals + `in`, ordering, paging.
  await db.movies.findMany({
    where: { director: { in: ["Villeneuve", "Nolan"] } },
    orderBy: { year: "desc" },
    limit: 10,
    offset: 20,
  });

  // Opted-in operators: title.contains (trigram) alongside a pruning startsWith.
  await db.movies.findMany({ where: { title: { startsWith: "The", contains: "Matrix" } } });

  // Opted-in director.endsWith alongside a pruning equals.
  await db.movies.findMany({ where: { director: { equals: "Scott", endsWith: "eve" } } });

  // contains / endsWith PRUNE (trigram / reversed index) → valid as a SOLE
  // constraint once the field opted in (ADR-0003 §7 — NOT riders).
  await db.movies.findMany({ where: { title: { contains: "Matrix" } } });
  await db.movies.findMany({ where: { director: { endsWith: "eve" } } });

  // Multi-valued existential `some` (+ shorthand string form).
  await db.movies.findMany({ where: { genres: { some: { equals: "Sci-Fi" } } } });
  await db.movies.findMany({ where: { genres: { some: "Sci-Fi" } } });

  // Absentable date field → is null / is absent / exists surface (ADR-0002).
  await db.movies.findMany({ where: { releaseDate: { isAbsent: true } } });
  await db.movies.findMany({ where: { releaseDate: { exists: true, gte: "2000-01-01" } } });

  // Boolean.
  await db.movies.findMany({ where: { inPrint: { equals: true } } });

  // `not` (the only rider) WITH a pruning constraint elsewhere → allowed.
  await db.movies.findMany({ where: { year: { gte: 2000 }, inPrint: { not: false } } });

  // Empty / no where = findMany-all → allowed.
  await db.movies.findMany();
  await db.movies.count({ where: { year: { gte: 2000 } } });

  // PK-gated get(id): movies HAS a user PK (imdbId) → get is present.
  const m = await db.movies.get("tt0133093");

  // getSchema returns the const.
  db.movies.getSchema();

  return m;
}

// ---------------------------------------------------------------------------
// INVALID — each must be rejected. tsc fails if any @ts-expect-error is unused.
// ---------------------------------------------------------------------------
async function invalid() {
  // Non-indexed field (plot) is not filterable — queryable ⟺ indexed (T1).
  // @ts-expect-error
  await db.movies.findMany({ where: { plot: { contains: "hacker" } } });

  // Operator not enabled for this field: `rating` did NOT opt into `contains`.
  // @ts-expect-error
  await db.movies.findMany({ where: { rating: { contains: "8" } } });

  // `endsWith` not enabled on `title` (only director opted in).
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { endsWith: "Reloaded" } } });

  // Wrong value type: year is a number.
  // @ts-expect-error
  await db.movies.findMany({ where: { year: { gt: "2000" } } });

  // Wrong operator for type: numeric `gt` on a string field.
  // @ts-expect-error
  await db.movies.findMany({ where: { director: { gt: "N" } } });

  // orderBy a non-indexed field.
  // @ts-expect-error
  await db.movies.findMany({ orderBy: { plot: "asc" } });

  // `equals` scalar on a multi-valued field — must go through `some`.
  // @ts-expect-error
  await db.movies.findMany({ where: { genres: { equals: "Sci-Fi" } } });

  // `not` (the ONLY filter-only rider) as the sole constraint → rejected at
  // COMPILE time; it can't prune, so it needs a pruning constraint alongside.
  // @ts-expect-error
  await db.movies.findMany({ where: { inPrint: { not: true } } });

  // Absent surface only on absentable fields: `year` is not absentable.
  // @ts-expect-error
  await db.movies.findMany({ where: { year: { isNull: true } } });

  // get(id) is NOT emitted for screenings (no user PK declared).
  // @ts-expect-error
  await db.screenings.get("anything");
}

void valid;
void invalid;
