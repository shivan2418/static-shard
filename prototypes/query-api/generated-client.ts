// ============================================================================
// PROTOTYPE — THROWAWAY. Answers wayfinder ticket T2 (Query API surface, #3).
// This is a MOCK of what `static-shard` codegen would emit for one dataset.
// The TYPES are real and honest (proving the type constraints bite is the whole
// point). The runtime is stubbed to return empty/mock data.
// ============================================================================
//
// Example dataset: a movie catalogue. Chosen because it exercises every T1 type:
//   strings, numbers, a multi-valued string[] (genres), a date, a bigint-as-string,
//   a NON-indexed free-text field (plot), and a NON-indexed nested object (boxOffice).
//
// T1 decisions this honours:
//   - internal content-hash `id` always present; optional user PK (`imdbId`) unlocks get(id)
//   - indexing is opt-in per scalar-leaf path; QUERYABLE ⟺ INDEXED (no scan fallback)
//   - multi-valued fields match existentially ("some")
//   - absent ≠ null; date = string + isDate flag; bigint = string

// ---- The full record shape (entire nested payload is preserved) ----
export interface Movie {
  id: string; // internal content-hash id (always present, never filtered on directly)
  imdbId: string; // user PK
  title: string;
  year: number;
  rating: number;
  runtime: number;
  director: string;
  genres: string[]; // multi-valued
  releaseDate: string; // isDate — ISO 8601 string
  budget: string; // bigint-as-string
  plot: string; // NOT indexed → present in results, NOT filterable
  boxOffice: { domestic: number; worldwide: number }; // NOT indexed (nested)
}

// ---- What the build actually indexed, and how. Codegen emits this from the
//      SchemaDescriptor IR (see R2). It is the SOLE source of what is filterable. ----
export interface MovieIndex {
  imdbId: { kind: "string"; pk: true };
  title: { kind: "string" };
  year: { kind: "number" };
  rating: { kind: "number" };
  runtime: { kind: "number" };
  director: { kind: "string" };
  genres: { kind: "string"; multi: true };
  releaseDate: { kind: "date" };
  // plot, boxOffice, budget, id are absent here → not filterable by construction.
}

// ---- Per-type operator sets (this is R2's "operator-constraint typing") ----
export interface StringFilter {
  equals?: string;
  not?: string;
  in?: string[];
  contains?: string;
  startsWith?: string;
  endsWith?: string;
}
export interface NumberFilter {
  equals?: number;
  not?: number;
  in?: number[];
  gt?: number;
  gte?: number;
  lt?: number;
  lte?: number;
}
export interface DateFilter {
  // dates compare as ISO strings
  equals?: string;
  not?: string;
  gt?: string;
  gte?: string;
  lt?: string;
  lte?: string;
}
// multi-valued: existential match. Shorthand `"Sci-Fi"` === `{ equals: "Sci-Fi" }`.
export interface MultiStringFilter {
  some?: StringFilter | string;
}

// ---- Map each index entry to its operator set ----
type FilterFor<E> = E extends { kind: "string"; multi: true }
  ? MultiStringFilter
  : E extends { kind: "string" }
    ? StringFilter
    : E extends { kind: "number" }
      ? NumberFilter
      : E extends { kind: "date" }
        ? DateFilter
        : never;

// The where type: ONLY indexed fields, each with ONLY its valid operators.
export type MovieWhere = {
  [K in keyof MovieIndex]?: FilterFor<MovieIndex[K]>;
};

// orderBy: only indexed fields (object form allows multi-key sort).
export type MovieOrderBy = { [K in keyof MovieIndex]?: "asc" | "desc" };

export interface FindManyArgs {
  where?: MovieWhere;
  orderBy?: MovieOrderBy;
  limit?: number;
  offset?: number;
}

// The runtime schema object returned by getSchema() (shape only; stubbed value).
export const MOVIE_SCHEMA = {
  record: "Movie",
  pk: "imdbId",
  indexed: {
    imdbId: "string",
    title: "string",
    year: "number",
    rating: "number",
    runtime: "number",
    director: "string",
    genres: "string[]",
    releaseDate: "date",
  },
} as const;

// ============================================================================
// STYLE A — object / where-clause (canonical candidate)
// ============================================================================
export interface MoviesCollectionA {
  findMany(args?: FindManyArgs): Promise<Movie[]>;
  count(args?: { where?: MovieWhere }): Promise<number>;
  // get(id) exists ONLY because a user PK (imdbId) was declared. No PK → codegen omits it.
  get(id: string): Promise<Movie | null>;
  getSchema(): typeof MOVIE_SCHEMA;
}

// ============================================================================
// STYLE B — builder / chain
// ============================================================================
type NumericField = {
  [K in keyof MovieIndex]: MovieIndex[K] extends { kind: "number" } ? K : never;
}[keyof MovieIndex];
type PlainStringField = {
  [K in keyof MovieIndex]: MovieIndex[K] extends { kind: "string"; multi: true }
    ? never
    : MovieIndex[K] extends { kind: "string" }
      ? K
      : never;
}[keyof MovieIndex];
type DateField = {
  [K in keyof MovieIndex]: MovieIndex[K] extends { kind: "date" } ? K : never;
}[keyof MovieIndex];
type MultiField = {
  [K in keyof MovieIndex]: MovieIndex[K] extends { multi: true } ? K : never;
}[keyof MovieIndex];

export interface MovieQuery {
  // one overload per type family — the cost of the builder shape shows up here
  where(field: NumericField, op: "equals" | "not" | "gt" | "gte" | "lt" | "lte", value: number): MovieQuery;
  where(field: NumericField, op: "in", value: number[]): MovieQuery;
  where(field: PlainStringField, op: "equals" | "not" | "contains" | "startsWith" | "endsWith", value: string): MovieQuery;
  where(field: PlainStringField, op: "in", value: string[]): MovieQuery;
  where(field: DateField, op: "equals" | "not" | "gt" | "gte" | "lt" | "lte", value: string): MovieQuery;
  whereSome(field: MultiField, op: "equals" | "contains" | "startsWith" | "endsWith", value: string): MovieQuery;
  orderBy(field: keyof MovieIndex, dir?: "asc" | "desc"): MovieQuery;
  limit(n: number): MovieQuery;
  offset(n: number): MovieQuery;
  many(): Promise<Movie[]>;
  count(): Promise<number>;
}
export interface MoviesCollectionB {
  query(): MovieQuery;
  get(id: string): Promise<Movie | null>;
  getSchema(): typeof MOVIE_SCHEMA;
}

// ============================================================================
// STYLE D — typed field-proxy / expression builder (Drizzle-flavoured)
// The operator is a METHOD on a typed column, so BOTH the field (property access)
// and the operator (method access) complete crisply, each narrowed by type.
// ============================================================================
export interface Condition {
  readonly __brand: "condition";
}
export interface NumberColumn {
  eq(v: number): Condition;
  not(v: number): Condition;
  in(v: number[]): Condition;
  gt(v: number): Condition;
  gte(v: number): Condition;
  lt(v: number): Condition;
  lte(v: number): Condition;
}
export interface StringColumn {
  eq(v: string): Condition;
  not(v: string): Condition;
  in(v: string[]): Condition;
  contains(v: string): Condition;
  startsWith(v: string): Condition;
  endsWith(v: string): Condition;
}
export interface DateColumn {
  eq(v: string): Condition;
  not(v: string): Condition;
  gt(v: string): Condition;
  gte(v: string): Condition;
  lt(v: string): Condition;
  lte(v: string): Condition;
}
export interface MultiStringColumn {
  some: StringColumn; // f.genres.some.eq("Sci-Fi")
}

// Map each index entry to its column type.
type ColumnFor<E> = E extends { kind: "string"; multi: true }
  ? MultiStringColumn
  : E extends { kind: "string" }
    ? StringColumn
    : E extends { kind: "number" }
      ? NumberColumn
      : E extends { kind: "date" }
        ? DateColumn
        : never;

// The `f` proxy: every indexed field, each exposing only its valid operators.
export type MovieFields = { [K in keyof MovieIndex]: ColumnFor<MovieIndex[K]> };

export interface OrderTerm {
  readonly __brand: "order";
}
export type MovieOrder = {
  [K in keyof MovieIndex]: { asc(): OrderTerm; desc(): OrderTerm };
};

// Boolean composition helpers (also exported top-level for dynamic building).
export const and = (..._cs: Condition[]): Condition => ({ __brand: "condition" });
export const or = (..._cs: Condition[]): Condition => ({ __brand: "condition" });
export const not = (_c: Condition): Condition => ({ __brand: "condition" });

export interface FindManyArgsD {
  where?: (f: MovieFields) => Condition;
  orderBy?: (o: MovieOrder) => OrderTerm | OrderTerm[];
  limit?: number;
  offset?: number;
}
export interface MoviesCollectionD {
  findMany(args?: FindManyArgsD): Promise<Movie[]>;
  count(args?: { where?: (f: MovieFields) => Condition }): Promise<number>;
  get(id: string): Promise<Movie | null>;
  getSchema(): typeof MOVIE_SCHEMA;
}

// ============================================================================
// The mock `db`. All surfaces are attached so each call-site file can import one.
// ============================================================================
const stubQuery: MovieQuery = {
  where: () => stubQuery,
  whereSome: () => stubQuery,
  orderBy: () => stubQuery,
  limit: () => stubQuery,
  offset: () => stubQuery,
  many: async () => [],
  count: async () => 0,
};

export const db = {
  // Style A surface
  movies: {
    findMany: async (_args: FindManyArgs = {}): Promise<Movie[]> => [],
    count: async (_args: { where?: MovieWhere } = {}): Promise<number> => 0,
    get: async (_id: string): Promise<Movie | null> => null,
    getSchema: () => MOVIE_SCHEMA,
  } satisfies MoviesCollectionA,

  // Style B surface (same data, builder entrypoint)
  moviesB: {
    query: () => stubQuery,
    get: async (_id: string): Promise<Movie | null> => null,
    getSchema: () => MOVIE_SCHEMA,
  } satisfies MoviesCollectionB,

  // Style D surface (field-proxy expression builder)
  moviesD: {
    findMany: async (_args: FindManyArgsD = {}): Promise<Movie[]> => [],
    count: async (_args: { where?: (f: MovieFields) => Condition } = {}): Promise<number> => 0,
    get: async (_id: string): Promise<Movie | null> => null,
    getSchema: () => MOVIE_SCHEMA,
  } satisfies MoviesCollectionD,
};
