// PROTOTYPE — STYLE B: builder / chain (Kysely/Knex-flavoured). THROWAWAY.
import { db } from "./generated-client.js";

// --- The same headline query, chained ---
const hits = await db.moviesB
  .query()
  .where("year", "gte", 2000)
  .where("year", "lt", 2010)
  .where("rating", "gte", 8)
  .whereSome("genres", "equals", "Sci-Fi")
  .where("title", "contains", "Star")
  .where("director", "not", "Uwe Boll")
  .orderBy("rating", "desc")
  .limit(20)
  .offset(0)
  .many();

// --- Range + set membership + string ops ---
await db.moviesB
  .query()
  .where("runtime", "gte", 90)
  .where("runtime", "lte", 180)
  .where("imdbId", "in", ["tt0133093", "tt0111161"])
  .where("title", "startsWith", "The")
  .where("releaseDate", "gte", "2001-01-01")
  .many();

// --- Other query features ---
const total = await db.moviesB.query().where("year", "gte", 2000).count();
const one = await db.moviesB.get("tt0133093");
const schema = db.moviesB.getSchema();

// --- Conditional / dynamic building (this is where the builder shines) ---
function search(term: string, minYear?: number) {
  let q = db.moviesB.query();
  if (term) q = q.where("title", "contains", term);
  if (minYear !== undefined) q = q.where("year", "gte", minYear);
  return q.many();
}

void [hits, total, one, schema, search];

// ============================================================================
// The type constraints MUST reject these too.
// ============================================================================

// @ts-expect-error — `plot` is not indexed, so it is not a valid field literal.
await db.moviesB.query().where("plot", "contains", "hacker").many();

// @ts-expect-error — `contains` is not a valid op for the numeric field `year`.
await db.moviesB.query().where("year", "contains", "20").many();

// @ts-expect-error — value type must match the field: `year` takes a number.
await db.moviesB.query().where("year", "gte", "2000").many();

// @ts-expect-error — multi-valued field must use whereSome, not where.
await db.moviesB.query().where("genres", "equals", "Sci-Fi").many();
