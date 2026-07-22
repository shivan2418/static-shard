// PROTOTYPE — shared wizard logic for the Shape-A UX reference (wizard-ink.tsx).
// It imports the pure estimator core, so presentation and behaviour stay cleanly
// separated — the real dep-light CLI would reuse this same logic under a
// hand-rolled renderer instead of Ink.

import { CARDS, BYTES, type FieldProfile } from "./profile.ts";
import {
  estimate, recommendSortField, recommendIndexed, defaultChunkTarget,
  fmtBytes, fmtInt, type Config, type Operator, type Estimate,
} from "./estimator.ts";

const { MB, KB } = BYTES;

export const profile = CARDS;
export const CHUNK_STEPS = [64 * KB, 128 * KB, 256 * KB, 512 * KB, 1 * MB, 2 * MB, 4 * MB, 8 * MB];
export { estimate, recommendSortField, recommendIndexed, defaultChunkTarget, fmtBytes, fmtInt };
export type { Config, Operator, Estimate };

export const sortCandidates = profile.fields.filter((f) => !f.multiValued && !f.discourageIndex);
export const indexable = profile.fields.filter((f) => !f.discourageIndex);

export function nearestStep(x: number): number {
  return CHUNK_STEPS.reduce((best, s) => (Math.abs(s - x) < Math.abs(best - x) ? s : best), CHUNK_STEPS[0]);
}

export function freshConfig(): Config {
  const operators: Record<string, Set<Operator>> = {};
  for (const f of profile.fields) operators[f.name] = new Set();
  return {
    sortField: recommendSortField(profile),
    indexed: recommendIndexed(profile),
    chunkTargetBytes: nearestStep(defaultChunkTarget(profile)),
    operators,
  };
}

export function eligibleOpFields(cfg: Config): FieldProfile[] {
  return [...cfg.indexed]
    .filter((n) => n !== cfg.sortField)
    .map((n) => profile.fields.find((f) => f.name === n)!)
    .filter((f) => f.type === "string" && !f.multiValued);
}

// what enabling `contains` on a field WOULD cost (and whether it dwarfs the column)
export function containsProbe(cfg: Config, name: string): { bytes: number; big: boolean; column: number } {
  const probe: Config = { ...cfg, operators: { ...cfg.operators, [name]: new Set<Operator>([...cfg.operators[name], "contains"]) } };
  const fc = estimate(profile, probe).perField.find((x) => x.name === name)!;
  return { bytes: fc.containsBytes, big: fc.containsBytes > fc.columnBytes, column: fc.columnBytes };
}
export function endsWithProbe(cfg: Config, name: string): number {
  return estimate(profile, cfg).perField.find((x) => x.name === name)!.baseIndexBytes;
}

// the three consequence axes in plain language — one structure, rendered by both UIs
export interface Axis { title: string; hint: string; body: string[]; warn?: boolean }
export function plainAxes(e: Estimate): Axis[] {
  return [
    {
      title: "Data files",
      hint: "your data, split into many small files",
      body: [`${fmtInt(e.shardCount)} files`, `~${fmtInt(e.recordsPerShard)} records each · ${fmtBytes(e.shardTargetBytes)} each`],
    },
    {
      title: "First download",
      hint: "everyone loads this once, before any query",
      warn: e.manifestOverBudget,
      body: [
        `${fmtBytes(e.manifestBytesGzip)} of a ${fmtBytes(e.manifestBudgetBytes)} comfort limit`,
        e.manifestOverBudget ? "getting big — some loads on demand instead" : "comfortably small",
      ],
    },
    {
      title: "Download per query",
      hint: "what a typical query pulls down",
      body: [
        `by exact value: ${fmtBytes(e.equalityQuery.bytes)} in ${e.equalityQuery.requests} req`,
        `by a range: ${fmtBytes(e.rangeQuery.bytes)} in ${e.rangeQuery.requests} req`,
      ],
    },
  ];
}

// plain config summary lines (for the review screen)
export function summaryLines(cfg: Config): string[] {
  const e = estimate(profile, cfg);
  const extra = Object.entries(cfg.operators).filter(([, s]) => s.size).map(([f, s]) => `${f} (${[...s].join(", ")})`);
  return [
    `Sorted by ${cfg.sortField} — ${fmtInt(e.shardCount)} files, ~${fmtInt(e.recordsPerShard)} records each`,
    `Filterable fields: ${[...cfg.indexed].join(", ")}`,
    `Extra text search: ${extra.length ? extra.join("; ") : "none"}`,
    `File size target: ${fmtBytes(cfg.chunkTargetBytes)}`,
  ];
}

export function configJson(cfg: Config): string {
  const schema: Record<string, unknown> = {};
  for (const f of profile.fields) {
    if (!cfg.indexed.has(f.name)) continue;
    const ops = cfg.operators[f.name];
    schema[f.name] = {
      type: f.type,
      ...(f.multiValued ? { multiValued: true } : {}),
      operators: [
        ...(f.name === cfg.sortField ? ["sort"] : []),
        ...(f.multiValued ? ["some"] : f.type === "number" || f.type === "date"
          ? ["equals", "in", "gt", "gte", "lt", "lte"] : ["equals", "in", "startsWith"]),
        ...(ops.has("endsWith") ? ["endsWith"] : []),
        ...(ops.has("contains") ? ["contains"] : []),
      ],
    };
  }
  return JSON.stringify({
    $schema: "./node_modules/static-shard-cli/config.schema.json",
    formatVersion: 1,
    input: "default-cards.jsonl",
    sortField: cfg.sortField,
    shardTargetBytes: cfg.chunkTargetBytes,
    pk: profile.pkGuess,
    schema,
  }, null, 2);
}

export function warningsFor(cfg: Config): { level: "warn" | "danger"; field?: string; text: string }[] {
  return estimate(profile, cfg).warnings;
}
