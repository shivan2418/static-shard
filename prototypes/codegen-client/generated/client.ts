// ============================================================================
// PROTOTYPE — THROWAWAY. GENERATED output #2 of 2: the thin CONCRETE FACADE
// (the "skin"). This is what makes `db.movies` a real, named, go-to-definition
// member with clean hover types — instead of the runtime's generic index
// access. It is deliberately thin: it declares the concrete collection members
// and delegates ALL behaviour to the generic runtime's createClient.
//
// Note how little code this is: one interface listing the collections, and one
// `connect()` that just casts the generic client. No query logic lives here —
// that's the whole point of the blend (runtime owns logic, facade owns names).
// ============================================================================
import {
  createClient,
  type Collection,
  type ClientOptions,
  type GenericClient,
} from "../runtime";
import { schema, type Schema, type Records, type Movie, type Screening } from "./schema";

// Concrete, named collections — clean hovers, real go-to-definition.
export interface Db {
  movies: Collection<Schema["movies"], Movie>;
  screenings: Collection<Schema["screenings"], Screening>;
}

// The consumer's single entry point. Same ergonomics as tRPC's createClient /
// hey-api's createClient({ baseUrl }) — but fully typed to THIS dataset.
export function connect(opts: ClientOptions): Db {
  // The generic runtime does all the work; the facade only narrows the type.
  const generic: GenericClient<Schema, Records> = createClient<Schema, Records>(schema, opts);
  return generic as unknown as Db;
}
