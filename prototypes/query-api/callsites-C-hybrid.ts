// PROTOTYPE — STYLE C: BOTH. Object-where is canonical; the builder is offered
// as sugar for the one thing object-where is clumsy at: dynamic composition.
// THROWAWAY.
import { db } from "./generated-client.js";

// Canonical: static, fully-known queries use the object form (best-typed, flattest).
const hits = await db.movies.findMany({
  where: {
    year: { gte: 2000, lt: 2010 },
    rating: { gte: 8 },
    genres: { some: "Sci-Fi" },
  },
  orderBy: { rating: "desc" },
  limit: 20,
});

// Escape hatch: when the filter set is built from user input at runtime, reach for
// the builder so you are not mutating a nested object literal by hand.
function search(opts: { term?: string; minYear?: number; genre?: string }) {
  let q = db.moviesB.query();
  if (opts.term) q = q.where("title", "contains", opts.term);
  if (opts.minYear !== undefined) q = q.where("year", "gte", opts.minYear);
  if (opts.genre) q = q.whereSome("genres", "equals", opts.genre);
  return q.orderBy("rating", "desc").many();
}

void [hits, search];

// The verdict this style forces: shipping two surfaces doubles the codegen output,
// the docs, and the "which one do I use?" decision every developer must make.
// Style C is only worth it if dynamic composition is common enough to justify that.
