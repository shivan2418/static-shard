// PROTOTYPE — STYLE D: typed field-proxy / expression builder (Drizzle-flavoured).
// The goal this style is built for: crisp intellisense on BOTH field and operator.
//   f.        -> completion lists every indexed field (property access)
//   f.year.   -> completion lists ONLY number ops: eq not in gt gte lt lte (methods)
//   f.title.  -> completion lists ONLY string ops: eq not in contains startsWith endsWith
//   f.genres. -> `.some` then string ops (existential)
// THROWAWAY.
import { db, and, or, not } from "./generated-client.js";

// --- The headline query ---
const hits = await db.moviesD.findMany({
  where: (f) =>
    and(
      f.year.gte(2000),
      f.year.lt(2010),
      f.rating.gte(8),
      f.genres.some.eq("Sci-Fi"),
      f.title.contains("Star"),
      not(f.director.eq("Uwe Boll")),
    ),
  orderBy: (o) => o.rating.desc(),
  limit: 20,
  offset: 0,
});

// --- Boolean composition is first-class (the object/builder styles can't `or` cleanly) ---
await db.moviesD.findMany({
  where: (f) =>
    or(
      f.director.eq("Christopher Nolan"),
      and(f.rating.gte(9), f.year.gte(2010)),
    ),
  orderBy: (o) => [o.year.desc(), o.rating.desc()], // multi-key sort
});

// --- Range + set membership + string ops ---
await db.moviesD.findMany({
  where: (f) =>
    and(
      f.runtime.gte(90),
      f.runtime.lte(180),
      f.imdbId.in(["tt0133093", "tt0111161"]),
      f.title.startsWith("The"),
      f.releaseDate.gte("2001-01-01"),
    ),
});

// --- Other query features ---
const total = await db.moviesD.count({ where: (f) => f.year.gte(2000) });
const one = await db.moviesD.get("tt0133093");
const schema = db.moviesD.getSchema();

// --- Conditional / dynamic building: push Conditions into an array, then `and` ---
function search(opts: { term?: string; minYear?: number; genre?: string }) {
  return db.moviesD.findMany({
    where: (f) => {
      const cs = [];
      if (opts.term) cs.push(f.title.contains(opts.term));
      if (opts.minYear !== undefined) cs.push(f.year.gte(opts.minYear));
      if (opts.genre) cs.push(f.genres.some.eq(opts.genre));
      return and(...cs);
    },
    orderBy: (o) => o.rating.desc(),
  });
}

void [hits, total, one, schema, search];

// ============================================================================
// The type constraints MUST reject these too.
// ============================================================================

// @ts-expect-error — `plot` is not indexed → not a property on the field proxy.
await db.moviesD.findMany({ where: (f) => f.plot.contains("hacker") });

// @ts-expect-error — `contains` is a string op; `year` is a NumberColumn (no contains).
await db.moviesD.findMany({ where: (f) => f.year.contains("20") });

// @ts-expect-error — `gt` is not on a plain StringColumn.
await db.moviesD.findMany({ where: (f) => f.director.gt("M") });

// @ts-expect-error — value type must match: year.gte takes a number.
await db.moviesD.findMany({ where: (f) => f.year.gte("2000") });

// @ts-expect-error — genres is multi-valued: must go through `.some`.
await db.moviesD.findMany({ where: (f) => f.genres.eq("Sci-Fi") });

// @ts-expect-error — cannot order by a non-indexed field.
await db.moviesD.findMany({ orderBy: (o) => o.plot.desc() });
