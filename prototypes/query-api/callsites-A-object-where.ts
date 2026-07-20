// PROTOTYPE — STYLE A: object / where-clause (Prisma-flavoured). THROWAWAY.
// Read this top-to-bottom as if you were the developer using the generated client.
import { db } from "./generated-client.js";

// --- The headline query: "Sci-Fi films from the 2000s rated 8+, best first" ---
const hits = await db.movies.findMany({
  where: {
    year: { gte: 2000, lt: 2010 },
    rating: { gte: 8 },
    genres: { some: "Sci-Fi" }, // shorthand for { some: { equals: "Sci-Fi" } }
    title: { contains: "Star" },
    director: { not: "Uwe Boll" },
  },
  orderBy: { rating: "desc" },
  limit: 20,
  offset: 0,
});

// --- Range + set membership + string ops ---
await db.movies.findMany({
  where: {
    runtime: { gte: 90, lte: 180 },
    imdbId: { in: ["tt0133093", "tt0111161"] },
    title: { startsWith: "The" },
    releaseDate: { gte: "2001-01-01", lt: "2002-01-01" },
  },
});

// --- Other query features ---
const total = await db.movies.count({ where: { year: { gte: 2000 } } });
const one = await db.movies.get("tt0133093"); // by user PK
const schema = db.movies.getSchema();

// --- Conditional / dynamic building (does the object shape read okay here?) ---
function search(term: string, minYear?: number) {
  const where: Parameters<typeof db.movies.findMany>[0] = { where: {} };
  if (term) where.where!.title = { contains: term };
  if (minYear !== undefined) where.where!.year = { gte: minYear };
  return db.movies.findMany(where);
}

void [hits, total, one, schema, search];

// ============================================================================
// The type constraints MUST reject these. Each @ts-expect-error asserts that.
// If any line stops erroring, `tsc` fails — proving the guarantee is real.
// ============================================================================

// @ts-expect-error — `plot` is not indexed, so it is not filterable at all.
await db.movies.findMany({ where: { plot: { contains: "hacker" } } });

// @ts-expect-error — `contains` is a string op; `year` is a number.
await db.movies.findMany({ where: { year: { contains: "20" } } });

// @ts-expect-error — `gt` is a number/date op; not valid on a plain string field.
await db.movies.findMany({ where: { director: { gt: "M" } } });

// @ts-expect-error — cannot order by a non-indexed field.
await db.movies.findMany({ orderBy: { plot: "desc" } });

// @ts-expect-error — genres is multi-valued: must go through `some`, not equals.
await db.movies.findMany({ where: { genres: { equals: "Sci-Fi" } } });

// @ts-expect-error — nonexistent field is rejected.
await db.movies.findMany({ where: { rating: { gte: 8 }, nope: { equals: 1 } } });
