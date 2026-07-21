// ============================================================================
// PROTOTYPE — THROWAWAY. This is the ENTIRE dataset-specific GENERATED output
// #1 of 2: the "types file". Codegen emits this from the SchemaDescriptor IR
// (R2). It is TINY — a full record interface per collection (for results) plus
// one `as const` schema describing PK + indexed fields + enabled operators.
// Everything filterable/typed flows from here into the generic runtime.
//
// Example dataset: a movie catalogue + a PK-less screenings collection (to show
// get(id) is omitted when no user PK exists).
// ============================================================================

// ---- Full record shapes (entire nested payload preserved; drives results) ----
export interface Movie {
  id: string; // internal content-hash id (always present)
  imdbId: string; // user PK
  title: string;
  year: number;
  rating: number;
  director: string;
  genres: string[]; // multi-valued
  releaseDate: string; // isDate — ISO 8601 string; absentable (missing sort values, ADR-0002)
  inPrint: boolean;
  plot: string; // NOT indexed → in results, NOT filterable
  boxOffice: { domestic: number; worldwide: number }; // NOT indexed (nested)
}

export interface Screening {
  id: string; // internal content-hash id — but NO user PK declared
  movieId: string;
  venue: string;
  startsAt: string;
}

// Maps collection name → its full record type (the facade needs this).
export interface Records {
  movies: Movie;
  screenings: Screening;
}

// ---- The schema const. `operators` per field = ADR-0003 §7 (config-driven). ----
//   title:    `contains` opted in (trigram index built)
//   director: `endsWith` opted in (reversed index built)
//   releaseDate: absentable → gets is null / is absent / exists
export const schema = {
  movies: {
    pk: "imdbId",
    fields: {
      imdbId: { kind: "string", pk: true, operators: ["equals", "in", "startsWith"] },
      title: { kind: "string", operators: ["equals", "in", "startsWith", "contains", "not"] },
      year: { kind: "number", operators: ["equals", "in", "gt", "gte", "lt", "lte", "not"] },
      rating: { kind: "number", operators: ["equals", "in", "gt", "gte", "lt", "lte"] },
      director: { kind: "string", operators: ["equals", "in", "startsWith", "endsWith"] },
      genres: { kind: "string", multi: true, operators: ["equals", "in", "startsWith"] },
      releaseDate: {
        kind: "date",
        absent: true,
        operators: ["equals", "in", "gt", "gte", "lt", "lte"],
      },
      inPrint: { kind: "boolean", operators: ["equals", "not"] },
    },
  },
  screenings: {
    // no `pk` key → get(id) is NOT emitted for this collection
    fields: {
      movieId: { kind: "string", operators: ["equals", "in"] },
      venue: { kind: "string", operators: ["equals", "in", "startsWith"] },
      startsAt: { kind: "date", operators: ["equals", "gt", "gte", "lt", "lte"] },
    },
  },
} as const;

export type Schema = typeof schema;
