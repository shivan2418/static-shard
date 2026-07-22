// PROTOTYPE — throwaway. A REAL "detected dataset" the wizard reacts to.
//
// These numbers were produced by profiling an actual file the way `init` would:
//   default-cards-20260721211623.jsonl.gz  — Scryfall bulk export, 116,138 MTG
//   cards, 72.5 MB gzipped (see profile-scryfall.py for the extractor).
// Field types, cardinality, average value bytes, p95 record size, and the PK
// guess are all measured, not invented — so the live estimates move against
// real shape.
//
// DELIBERATELY CURATED to 12 fields for a legible flow demo. The real record
// has 63 top-level keys → ~91 scalar-leaf paths once nested objects are expanded
// (legalities.*, prices.*, image_uris.*) plus 9 multi-valued arrays. A real
// `init` surfaces ALL of them — which is why the spec (not this prototype) must
// give the field-list + operator steps a scrollable/filterable list and default
// to a SMALL recommended index set (opt-in), not opt-out-of-everything.

export type FieldType = "string" | "number" | "boolean" | "date";

export interface FieldProfile {
  name: string;
  type: FieldType;
  /** distinct values across the whole dataset */
  cardinality: number;
  /** mean serialized value size in bytes */
  avgValueBytes: number;
  /** mean string length in chars — drives the trigram (`contains`) estimate */
  avgLen: number;
  /** multi-valued (array) field → existential `some` only (T1) */
  multiValued?: boolean;
  /** has absent/null records → unlocks isAbsent/isNull surface (ADR-0002) */
  absentable?: boolean;
  /** the tool would never recommend indexing this (huge free text) */
  discourageIndex?: boolean;
}

export interface DatasetProfile {
  label: string;
  recordCount: number;
  /** whole dataset, gzip-compressed, in bytes (what shardCount divides) */
  datasetBytesCompressed: number;
  /** 95th-percentile single record size, compressed — the shard-size floor */
  p95RecordCompressed: number;
  /** field the tool guesses is the user PK (unique, stable) — may be null */
  pkGuess: string | null;
  fields: FieldProfile[];
}

const MB = 1024 * 1024;
const KB = 1024;

export const CARDS: DatasetProfile = {
  label: "default-cards.jsonl  (detected: NDJSON, 116,138 records, 72.5 MB)",
  recordCount: 116_138,
  datasetBytesCompressed: 72_572_552,
  p95RecordCompressed: 697,
  pkGuess: "id",
  fields: [
    { name: "id",          type: "string",  cardinality: 116_138, avgValueBytes: 36,  avgLen: 36  },
    { name: "name",        type: "string",  cardinality: 37_889,  avgValueBytes: 16,  avgLen: 16  },
    { name: "released_at", type: "date",    cardinality: 1_266,   avgValueBytes: 10,  avgLen: 10  },
    { name: "cmc",         type: "number",  cardinality: 19,      avgValueBytes: 3,   avgLen: 3, absentable: true },
    { name: "rarity",      type: "string",  cardinality: 6,       avgValueBytes: 6,   avgLen: 6   },
    { name: "set",         type: "string",  cardinality: 1_047,   avgValueBytes: 3,   avgLen: 3   },
    { name: "artist",      type: "string",  cardinality: 2_524,   avgValueBytes: 13,  avgLen: 13  },
    { name: "type_line",   type: "string",  cardinality: 4_937,   avgValueBytes: 18,  avgLen: 18, absentable: true },
    { name: "colors",      type: "string",  cardinality: 5,       avgValueBytes: 1,   avgLen: 1, multiValued: true, absentable: true },
    { name: "keywords",    type: "string",  cardinality: 885,     avgValueBytes: 7,   avgLen: 7, multiValued: true },
    { name: "reprint",     type: "boolean", cardinality: 2,       avgValueBytes: 4,   avgLen: 4   },
    { name: "oracle_text", type: "string",  cardinality: 31_885,  avgValueBytes: 145, avgLen: 145, absentable: true, discourageIndex: true },
  ],
};

export const BYTES = { MB, KB };
