// PROTOTYPE — throwaway TUI, but THIS module is the reusable core.
//
// Pure implementation of the ADR-0003 §11 cost-estimation formulas (which fold
// in ADR-0002 §5's shard-size rule). No I/O, no terminal code. The wizard's
// entire "live consequence estimate" is `estimate(profile, config)`.
//
// Every formula line is annotated with the ADR reference it realizes so the
// numbers can be checked against the spec, not just eyeballed.

import type { DatasetProfile, FieldProfile } from "./profile.ts";
import { BYTES } from "./profile.ts";

const { MB, KB } = BYTES;

// ---- knobs the wizard controls (the config being built) --------------------

export type Operator = "endsWith" | "contains";

export interface Config {
  sortField: string;
  indexed: Set<string>;              // opt-in indexed set (T1)
  chunkTargetBytes: number;          // knob 3 — shard byte target T
  /** per-field opt-in operators beyond the free set (ADR-0003 §7) */
  operators: Record<string, Set<Operator>>;
}

// ---- derived estimates -----------------------------------------------------

export interface FieldIndexCost {
  name: string;
  isSortField: boolean;
  baseIndexBytes: number;    // equals/in/startsWith — free & on
  endsWithBytes: number;     // reversed index — opt-in
  containsBytes: number;     // trigram index — opt-in, usually huge
  columnBytes: number;       // raw column size, for the contains warning
  chunkCount: number;        // base index chunks (dir entries in root)
  containsExceedsColumn: boolean;
}

export interface Estimate {
  // Axis 1 — shards
  shardTargetBytes: number;
  shardCount: number;
  recordsPerShard: number;
  // Axis 2 — root manifest
  manifestBytesGzip: number;
  manifestBudgetBytes: number;
  manifestOverBudget: boolean;
  spilledFields: string[];   // secondary zonemaps forced to sidecars
  // Axis 3 — per-query cost (representative queries)
  equalityQuery: { bytes: number; requests: number };
  rangeQuery: { bytes: number; requests: number };
  // per-field operator index costs
  perField: FieldIndexCost[];
  // warnings (ADR-0002 §6)
  warnings: Warning[];
}

export interface Warning {
  level: "warn" | "danger";
  field?: string;
  text: string;
}

const HASH_BYTES = 12;
const INT_BYTES = 3;                 // varint-ish average for shards[] counts/bytes
const TRUNC_VALUE_BYTES = 10;        // truncated zonemap min/max bound (ADR-0003 §2)
const DIR_ENTRY_BYTES = 40;          // one index-chunk directory entry
const CHUNK_TARGET = 45 * KB;        // ADR-0003 §5
const GZIP_RATIO = 0.35;             // ADR-0003 §11 (M_gzip ≈ 0.35·M)
const MANIFEST_BUDGET = 1 * MB;      // ADR-0003 §3
const SCHEMA_CONST = 2 * KB;

const field = (p: DatasetProfile, name: string): FieldProfile =>
  p.fields.find((f) => f.name === name)!;

// postings_f = Σ_value min(occurrences(value), shardCount)  (ADR-0003 §11)
// uniform-occurrence approximation: occ ≈ recordCount / cardinality
function postings(f: FieldProfile, recordCount: number, shardCount: number): number {
  const occ = recordCount / f.cardinality;
  return f.cardinality * Math.min(occ, shardCount);
}

function baseIndexBytes(f: FieldProfile, recordCount: number, shardCount: number): number {
  const frontCoded = f.avgValueBytes * 0.5;          // shared-prefix elision ≈ halves term bytes
  return f.cardinality * frontCoded + postings(f, recordCount, shardCount) * 2;
}

// contains_f ≈ min(Σ_value (len-2)·occ , trigramCount·shardCount) · 2B  (ADR-0003 §11)
function containsBytes(f: FieldProfile, recordCount: number, shardCount: number): number {
  const perValueTrigrams = Math.max(f.avgLen - 2, 1) * (recordCount / f.cardinality);
  const totalTrigramPostings = f.cardinality * perValueTrigrams;      // ≈ (len-2)·recordCount
  // a common trigram appears in nearly every shard → up to shardCount postings;
  // total postings ≈ distinctTrigrams·shardCount once trigrams are widespread.
  const distinctTrigrams = Math.min(f.cardinality, 80_000);
  return Math.min(totalTrigramPostings, distinctTrigrams * shardCount) * 2;
}

function chunks(bytes: number): number {
  return Math.max(1, Math.ceil((bytes * GZIP_RATIO) / CHUNK_TARGET));
}

export function estimate(p: DatasetProfile, cfg: Config): Estimate {
  // ---- Axis 1: shards (ADR-0002 §5) ----
  const shardTargetBytes = cfg.chunkTargetBytes;
  const shardCount = Math.max(1, Math.ceil(p.datasetBytesCompressed / shardTargetBytes));
  const recordsPerShard = Math.round(p.recordCount / shardCount);

  const indexedNonSort = [...cfg.indexed].filter((n) => n !== cfg.sortField);
  const sortF = field(p, cfg.sortField);

  // ---- per-field index costs (ADR-0003 §7 + §11) ----
  const perField: FieldIndexCost[] = [...cfg.indexed].map((name) => {
    const f = field(p, name);
    const isSort = name === cfg.sortField;
    const ops = cfg.operators[name] ?? new Set<Operator>();
    const base = isSort ? 0 : baseIndexBytes(f, p.recordCount, shardCount); // sort field needs no inverted index
    const ends = ops.has("endsWith") ? base : 0;                            // reversed ≈ +1× base
    const cont = ops.has("contains") ? containsBytes(f, p.recordCount, shardCount) : 0;
    const columnBytes = p.recordCount * f.avgValueBytes;
    return {
      name,
      isSortField: isSort,
      baseIndexBytes: base,
      endsWithBytes: ends,
      containsBytes: cont,
      columnBytes,
      chunkCount: isSort ? 0 : chunks(base + ends + cont),
      containsExceedsColumn: cont > columnBytes,
    };
  });

  // ---- Axis 2: root manifest (ADR-0003 §11) ----
  const totalChunkDirEntries = perField.reduce((n, f) => n + f.chunkCount, 0);
  const rawManifest =
    SCHEMA_CONST +
    shardCount * (HASH_BYTES + 2 * INT_BYTES) +          // shards[] identity
    (shardCount + 1) * sortF.avgValueBytes +             // sort split-points
    indexedNonSort.length * shardCount * 2 * TRUNC_VALUE_BYTES + // zonemap pairs (dominant)
    totalChunkDirEntries * DIR_ENTRY_BYTES;              // index directories
  const manifestBytesGzip = rawManifest * GZIP_RATIO;
  const manifestOverBudget = manifestBytesGzip > MANIFEST_BUDGET;

  // spill: when over budget, secondary zonemaps move to sidecars, heaviest first
  const spilledFields: string[] = [];
  if (manifestOverBudget) {
    let over = manifestBytesGzip - MANIFEST_BUDGET;
    const perFieldZonemap = shardCount * 2 * TRUNC_VALUE_BYTES * GZIP_RATIO;
    for (const name of indexedNonSort) {
      if (over <= 0) break;
      spilledFields.push(name);
      over -= perFieldZonemap;
    }
  }

  // ---- Axis 3: per-query cost (ADR-0003 §11) ----
  // representative equality query on the highest-cardinality secondary index
  const secondary = perField.filter((f) => !f.isSortField && f.baseIndexBytes > 0);
  const eqField = secondary.slice().sort((a, b) => b.baseIndexBytes - a.baseIndexBytes)[0];
  const eqChunks = eqField ? Math.min(eqField.chunkCount, 1) : 0;
  // a SELECTIVE equality (the representative case) lands on a handful of shards
  const eqCandidateShards = eqField ? Math.min(3, Math.max(1, Math.round(shardCount * 0.001))) : 1;
  const equalityQuery = {
    bytes: eqChunks * CHUNK_TARGET + eqCandidateShards * shardTargetBytes,
    requests: eqChunks + eqCandidateShards + (spilledFields.length ? 1 : 0),
  };
  // representative range query on the sort field — pure zonemap, no chunk fetch
  const rangeCandidateShards = Math.min(8, Math.max(2, Math.round(shardCount * 0.01)));
  const rangeQuery = {
    bytes: rangeCandidateShards * shardTargetBytes,
    requests: rangeCandidateShards,
  };

  // ---- warnings (ADR-0002 §5/§6, ADR-0003 §7) ----
  const warnings: Warning[] = [];
  if (sortF.cardinality < shardCount) {
    warnings.push({
      level: "warn",
      field: sortF.name,
      text: `low-cardinality sort field (${sortF.cardinality} distinct < ${shardCount} shards): equal-key runs stay contiguous, some shards may exceed target. Discouraged, not forbidden.`,
    });
  }
  if (sortF.multiValued) {
    warnings.push({ level: "danger", field: sortF.name, text: `sort field is multi-valued — records scatter across ranges; pick a scalar.` });
  }
  if (p.p95RecordCompressed > shardTargetBytes) {
    warnings.push({ level: "warn", text: `p95 record (${fmtBytes(p.p95RecordCompressed)}) exceeds chunk target — oversized records get their own flagged shards.` });
  }
  for (const f of perField) {
    if (f.containsExceedsColumn) {
      warnings.push({
        level: "danger",
        field: f.name,
        text: `contains(${f.name}): trigram index ${fmtBytes(f.containsBytes)} is LARGER than the raw column ${fmtBytes(f.columnBytes)}. This is the single biggest build-output cost.`,
      });
    }
  }
  if (manifestOverBudget) {
    warnings.push({
      level: "warn",
      text: `root manifest ${fmtBytes(manifestBytesGzip)} over ~1 MB budget — ${spilledFields.length} secondary zonemap(s) spill to sidecars (pay-on-use).`,
    });
  }

  return {
    shardTargetBytes, shardCount, recordsPerShard,
    manifestBytesGzip, manifestBudgetBytes: MANIFEST_BUDGET, manifestOverBudget, spilledFields,
    equalityQuery, rangeQuery, perField, warnings,
  };
}

// auto-heuristic default (ADR-0002 §2): PREFER number/date types, then highest
// cardinality within that tier; only fall back to strings if no number/date
// field exists; the PK is the final tiebreak. Type-tiered, not a weighted blend,
// so a unique string PK doesn't outrank a good range field on raw cardinality.
export function recommendSortField(p: DatasetProfile): string {
  const cands = p.fields.filter((f) => !f.multiValued && !f.discourageIndex);
  const rank = (f: DatasetProfile["fields"][number]) => {
    const typeTier = f.type === "number" || f.type === "date" ? 2 : 1;
    return { typeTier, card: f.cardinality, pk: f.name === p.pkGuess ? 1 : 0 };
  };
  return cands
    .slice()
    .sort((a, b) => {
      const ra = rank(a), rb = rank(b);
      return rb.typeTier - ra.typeTier || rb.card - ra.card || rb.pk - ra.pk;
    })[0].name;
}

export function recommendIndexed(p: DatasetProfile): Set<string> {
  return new Set(p.fields.filter((f) => !f.discourageIndex).map((f) => f.name));
}

export function defaultChunkTarget(p: DatasetProfile): number {
  // T = clamp(max(2MB, p95), 512KB, 8MB)  (ADR-0002 §5)
  const t = Math.max(2 * MB, p.p95RecordCompressed);
  return Math.min(Math.max(t, 512 * KB), 8 * MB);
}

// ---- formatting helpers (shared by the TUI) --------------------------------

export function fmtBytes(n: number): string {
  if (n >= MB) return `${(n / MB).toFixed(n < 10 * MB ? 1 : 0)} MB`;
  if (n >= KB) return `${(n / KB).toFixed(n < 10 * KB ? 1 : 0)} KB`;
  return `${Math.round(n)} B`;
}

export function fmtInt(n: number): string {
  return Math.round(n).toLocaleString("en-US");
}
