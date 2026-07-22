// PROTOTYPE — throwaway interactive shell for wayfinder T7 (#10).
//
// QUESTION: how should the static-shard `init` config wizard PRESENT the three
// knobs (sort field, indexed set, chunk size), their live consequence estimates,
// the per-field operator toggles (with the loud `contains` warning), and the
// skew/oversized/low-card warnings — and what FLOW ties them together?
//
// This isn't asking "does the estimator work" (that's estimator.ts, the reusable
// core). It's a UI/UX question in a terminal medium, so — adapting the /prototype
// UI branch — it offers THREE radically different shapes you flip between and
// steal from:
//
//   Shape A — LINEAR WIZARD     detect → recommend → confirm → persist, one
//                               decision per screen. Maximum hand-holding.
//   Shape B — LIVE COCKPIT      every knob + the field/operator table on one
//                               dense screen; estimates recompute on every key.
//   Shape C — REVIEW & OVERRIDE the tool decides everything up front, shows the
//                               finished config as a report; you intervene by
//                               exception via a short list of framed overrides.
//
// Run:   node prototypes/config-wizard/tui.ts        (Node >= 22, strips types)
// Flip:  [Tab] cycle shape   [1/2/3] jump   [q] quit
//
// No persistence: "persist" just prints the JSON it WOULD write. Nothing on disk.

import readline from "node:readline";
import { CARDS, BYTES, type DatasetProfile, type FieldProfile } from "./profile.ts";
import {
  estimate, recommendSortField, recommendIndexed, defaultChunkTarget,
  fmtBytes, fmtInt, type Config, type Operator, type Estimate, type Warning,
} from "./estimator.ts";

const { MB, KB } = BYTES;
// spans below the ADR floor too, so you can drive shardCount up and watch the
// contains index cross its column / the manifest approach budget (live estimate).
const CHUNK_STEPS = [64 * KB, 128 * KB, 256 * KB, 512 * KB, 1 * MB, 2 * MB, 4 * MB, 8 * MB];

// ---- ANSI ------------------------------------------------------------------
const E = "\x1b[";
const c = {
  reset: `${E}0m`, bold: `${E}1m`, dim: `${E}2m`, ital: `${E}3m`, under: `${E}4m`,
  inv: `${E}7m`,
  red: `${E}31m`, grn: `${E}32m`, yel: `${E}33m`, blu: `${E}34m`, mag: `${E}35m`, cyn: `${E}36m`, gry: `${E}90m`,
  bgBlu: `${E}44m`, bgGry: `${E}100m`,
};
const clear = () => process.stdout.write(`${E}2J${E}H`);
const b = (s: string) => c.bold + s + c.reset;
const dim = (s: string) => c.dim + s + c.reset;
const col = (color: string, s: string) => color + s + c.reset;
const pad = (s: string, n: number) => {
  const len = stripLen(s);
  return len >= n ? s : s + " ".repeat(n - len);
};
const stripLen = (s: string) => s.replace(/\x1b\[[0-9;]*m/g, "").length;
const bar = (filled: number, total: number, width = 18) => {
  const f = Math.max(0, Math.min(width, Math.round((filled / total) * width)));
  return col(c.cyn, "█".repeat(f)) + dim("░".repeat(width - f));
};

// ---- state -----------------------------------------------------------------
const profile: DatasetProfile = CARDS;

interface State {
  cfg: Config;
  variant: "A" | "B" | "C";
  // Shape A
  stepA: number;      // 0 detect .. 5 review
  cursorA: number;
  // Shape B
  rowB: number;       // 0 sortField, 1 chunkSize, 2.. fields
  colB: number;       // 0 index, 1 endsWith, 2 contains (within a field row)
  // Shape C
  overrideOpen: boolean;
  cursorC: number;
  persisted: boolean;
}

function freshConfig(): Config {
  const sortField = recommendSortField(profile);
  const indexed = recommendIndexed(profile);
  const operators: Record<string, Set<Operator>> = {};
  for (const f of profile.fields) operators[f.name] = new Set();
  // find nearest chunk step to the ADR default
  const def = defaultChunkTarget(profile);
  const chunkTargetBytes = CHUNK_STEPS.reduce((best, s) =>
    Math.abs(s - def) < Math.abs(best - def) ? s : best, CHUNK_STEPS[0]);
  return { sortField, indexed, chunkTargetBytes, operators };
}

const st: State = {
  cfg: freshConfig(),
  variant: "A",
  stepA: 0, cursorA: 0,
  rowB: 0, colB: 0,
  overrideOpen: false, cursorC: 0, persisted: false,
};

// candidate fields for sorting/indexing (exclude free-text discouraged + multivalued for sort)
const indexable = profile.fields.filter((f) => !f.discourageIndex);
const sortCandidates = indexable.filter((f) => !f.multiValued);

// ---- shared render pieces --------------------------------------------------
function opInfo(f: FieldProfile): { free: string; multi: boolean } {
  const free = f.multiValued
    ? "some (existential)"
    : f.type === "number" || f.type === "date"
      ? "equals in gt/gte/lt/lte"
      : "equals in startsWith";
  return { free, multi: !!f.multiValued };
}

function warningLines(ws: Warning[]): string[] {
  if (!ws.length) return [dim("  no warnings — config within budget")];
  return ws.map((w) => {
    const tag = w.level === "danger" ? col(c.red, "▲ danger") : col(c.yel, "△ warn  ");
    const where = w.field ? col(c.mag, ` ${w.field}`) : "";
    return `  ${tag}${where}  ${w.text}`;
  });
}

function estimateBlock(e: Estimate): string[] {
  const eq = e.equalityQuery, rg = e.rangeQuery;
  return [
    b("  Data files") + dim("  your data, split into many small files"),
    `    ${col(c.cyn, fmtInt(e.shardCount) + " files")} ${dim("· about " + fmtInt(e.recordsPerShard) + " records each · " + fmtBytes(e.shardTargetBytes) + " each")}`,
    b("  First download") + dim("  everyone loads this once, before any query"),
    `    ${col(e.manifestOverBudget ? c.yel : c.grn, fmtBytes(e.manifestBytesGzip))} ${dim("of a " + fmtBytes(e.manifestBudgetBytes) + " comfort limit")}  ${bar(e.manifestBytesGzip, e.manifestBudgetBytes)}`,
    e.manifestOverBudget
      ? dim(`    getting big — some of it will load on demand instead (${e.spilledFields.join(", ")})`)
      : dim(`    comfortably small`),
    b("  Download per query") + dim("  what a typical query pulls down"),
    `    ${pad("filter by exact value:", 24)} ${col(c.cyn, pad(fmtBytes(eq.bytes), 8))} ${dim("in " + eq.requests + " request" + (eq.requests > 1 ? "s" : ""))}`,
    `    ${pad("filter by a range:", 24)} ${col(c.cyn, pad(fmtBytes(rg.bytes), 8))} ${dim("in " + rg.requests + " request" + (rg.requests > 1 ? "s" : ""))}`,
  ];
}

// plain per-field cost line
function fieldCostLine(name: string, e: Estimate, cfg: Config): string {
  const fc = e.perField.find((x) => x.name === name)!;
  if (fc.isSortField) return dim("sorted field — free range filtering, instant lookups by this field");
  const payOnUse = fmtBytes(fc.baseIndexBytes + fc.endsWithBytes + fc.containsBytes);
  const ops = cfg.operators[name];
  const extra = [
    ops.has("endsWith") ? col(c.blu, "+ends-with") : "",
    ops.has("contains") ? col(fc.containsExceedsColumn ? c.red : c.blu, "+contains") : "",
  ].filter(Boolean).join(" ");
  return `${dim("index loads only when you filter on it:")} ${col(c.gry, payOnUse)} ${extra}`;
}

// =====================================================================
// SHAPE A — LINEAR WIZARD
// =====================================================================
const STEPS_A = ["Detect", "Sort field", "Indexed set", "Operators", "Chunk size", "Review & persist"];

function renderA(): string {
  const e = estimate(profile, st.cfg);
  const L: string[] = [];
  // stepper header
  const crumbs = STEPS_A.map((s, i) =>
    i === st.stepA ? col(c.bgBlu, b(` ${i + 1} ${s} `)) : dim(` ${i + 1} ${s} `)).join(dim("→"));
  L.push(crumbs, "");

  if (st.stepA === 0) {
    L.push(b(`Detected  ${profile.label}`), "");
    L.push(dim("  field          type      cardinality    role"));
    for (const f of profile.fields) {
      const role = f.name === profile.pkGuess ? col(c.grn, "PK guess")
        : f.multiValued ? col(c.mag, "multi-valued")
        : f.discourageIndex ? dim("free text — won't index") : "";
      L.push(`  ${pad(f.name, 14)} ${pad(f.type, 9)} ${pad(fmtInt(f.cardinality), 14)} ${role}`);
    }
    L.push("", dim(`  p95 record ${fmtBytes(profile.p95RecordCompressed)} · dataset ${fmtBytes(profile.datasetBytesCompressed)} compressed`));
    L.push("", col(c.cyn, "  [Enter] looks right, continue →"));
  } else if (st.stepA === 1) {
    const rec = recommendSortField(profile);
    L.push(b("Pick the ONE field to sort by."),
      dim("  This field gets fast filtering for free — including before/after and less/greater ranges."),
      dim("  Every other field needs an index. You can only pick one, so pick what you filter on most."), "");
    sortCandidates.forEach((f, i) => {
      const sel = st.cfg.sortField === f.name;
      const cur = i === st.cursorA;
      const marker = sel ? col(c.grn, "◉") : "○";
      const recTag = f.name === rec ? col(c.grn, " ★ recommended") : "";
      const line = `  ${marker} ${pad(f.name, 14)} ${dim(pad(f.type + " · " + fmtInt(f.cardinality) + " distinct", 26))}${recTag}`;
      L.push(cur ? col(c.inv, pad(line, 62)) : line);
    });
    L.push("", ...estimateBlock(e).slice(0, 3));
    const sf = st.cfg.sortField;
    L.push(dim(`  Looking up one record by ${profile.pkGuess}: ${sf === profile.pkGuess ? col(c.grn, "instant") + dim(" (it's the sorted field)") : "one extra fetch (fine, just not free)"}`));
    e.warnings.filter((w) => w.field === sf).forEach((w) => L.push(...warningLines([w])));
    L.push("", dim("  [↑/↓] move  [space] choose  [n] next →  [b] ← back"));
  } else if (st.stepA === 2) {
    L.push(b("Which fields do you want to filter on?"),
      dim("  You can only filter on fields you index. Each one adds a little to the first download,"),
      dim("  plus an index that loads only when a query actually uses that field."), "");
    indexable.forEach((f, i) => {
      const on = st.cfg.indexed.has(f.name);
      const cur = i === st.cursorA;
      const box = on ? col(c.grn, "[✓]") : dim("[ ]");
      const isSort = f.name === st.cfg.sortField;
      const cost = on ? "  " + fieldCostLine(f.name, e, st.cfg) : dim("  not indexed");
      const line = `  ${box} ${pad(f.name, 12)}${isSort ? col(c.cyn, " (sort)") : ""}${cost}`;
      L.push(cur ? col(c.inv, pad(line, 78)) : line);
    });
    L.push("", ...estimateBlock(e).slice(3, 7));
    L.push("", dim("  [↑/↓] move  [space] toggle  [n] next →  [b] ← back"));
  } else if (st.stepA === 3) {
    L.push(b("Extra ways to search text"),
      dim("  Exact match, \"is one of\", and starts-with are already on for every text field."),
      dim("  These two add more — each builds an extra index that only downloads when you use it."), "");
    const eligible = opEligibleFields();
    if (eligible.length === 0) {
      L.push(dim("  No text fields indexed — nothing to add here. [n] to continue."));
    } else {
      if (st.cursorA >= eligible.length) st.cursorA = 0;
      // compact field list with badges for what's enabled
      eligible.forEach((f, i) => {
        const ops = st.cfg.operators[f.name];
        const badge = (on: boolean, ch: string) => on ? col(c.grn, ch) : dim("·");
        const cur = i === st.cursorA;
        const line = `  ${cur ? col(c.cyn, "▸") : " "} ${pad(f.name, 12)} ${badge(ops.has("endsWith"), "ends-with")}  ${badge(ops.has("contains"), "contains")}`;
        L.push(cur ? b(line) : line);
      });
      // focused-field detail card
      const f = eligible[st.cursorA];
      const ops = st.cfg.operators[f.name];
      const fc = e.perField.find((x) => x.name === f.name)!;
      const endsCost = fc.baseIndexBytes;
      const containsCost = ops.has("contains") ? fc.containsBytes : estimateContains(f.name);
      const containsBig = containsCost > fc.columnBytes;
      L.push("", dim("  ── ") + b(f.name) + dim(" ──────────────────────────────────────────"));
      L.push(dim("  already on:  exact match · is one of · starts with"), dim("  add:"));
      const endBox = ops.has("endsWith") ? col(c.grn, "[✓]") : dim("[ ]");
      const conBox = ops.has("contains") ? col(containsBig ? c.red : c.grn, "[✓]") : dim("[ ]");
      L.push(`    ${endBox} ${b("ends with")}   ${dim("match the end of the value")}        ${dim("adds " + fmtBytes(endsCost))}`);
      L.push(`    ${conBox} ${b("contains")}    ${dim("match text anywhere in the value")}   ${dim("adds " + fmtBytes(containsCost))}`);
      if (containsBig) L.push(col(c.red, `        ⚠ that index is bigger than ${f.name}'s own data (${fmtBytes(fc.columnBytes)}) — usually not worth it`));
      L.push("", dim("  number, date, boolean and multi-value fields don't have these text operators."));
    }
    L.push("", dim("  [↑/↓] pick field   [e] ends-with   [c] contains   [n] next →   [b] ← back"));
  } else if (st.stepA === 4) {
    L.push(b("How big should each data file be?"),
      dim("  Smaller files = each query grabs less it doesn't need, but takes more requests to do it."),
      dim("  Bigger files = fewer requests, but each query downloads more than it strictly needs."), "");
    const cur = CHUNK_STEPS.indexOf(st.cfg.chunkTargetBytes);
    const slider = CHUNK_STEPS.map((s, i) => i === cur ? col(c.cyn, b(`◆ ${fmtBytes(s)}`)) : dim(fmtBytes(s))).join("   ");
    L.push("  " + slider, "");
    L.push(...estimateBlock(e));
    e.warnings.filter((w) => w.text.includes("record") || w.text.includes("cardinality")).forEach((w) => L.push(...warningLines([w])));
    L.push("", dim("  [←/→] adjust  [n] next →  [b] ← back"));
  } else {
    L.push(b("Review — this is what static-shard.config.json will contain."), "");
    L.push(...configJson(st.cfg).split("\n").map((l) => "  " + col(c.gry, l)));
    L.push("", ...estimateBlock(e));
    L.push("", b("  Warnings"));
    L.push(...warningLines(e.warnings));
    L.push("");
    if (st.persisted) L.push(col(c.grn, "  ✓ wrote static-shard.config.json  (prototype: printed above, nothing on disk)"));
    else L.push(col(c.cyn, "  [Enter] persist config  ") + dim("  [b] ← back"));
  }
  return L.join("\n");
}
// indexed, non-sort, scalar-string fields — the only ones ends-with/contains apply to
function opEligibleFields(): FieldProfile[] {
  return [...st.cfg.indexed]
    .filter((n) => n !== st.cfg.sortField)
    .map((n) => profile.fields.find((f) => f.name === n)!)
    .filter((f) => f.type === "string" && !f.multiValued);
}
// helper so step-3 can show the WOULD-BE contains cost before enabling
function estimateContains(name: string): number {
  const probe: Config = { ...st.cfg, operators: { ...st.cfg.operators, [name]: new Set<Operator>(["contains"]) } };
  return estimate(profile, probe).perField.find((x) => x.name === name)!.containsBytes;
}

// =====================================================================
// SHAPE B — LIVE COCKPIT
// =====================================================================
function renderB(): string {
  const e = estimate(profile, st.cfg);
  const idxFields = indexable;
  const rows = 2 + idxFields.length; // sortField, chunk, fields
  // LEFT column — controls
  const left: string[] = [];
  left.push(b("KNOBS"), "");
  // sort field (row 0)
  const sfSel = st.rowB === 0;
  left.push(`${sfSel ? col(c.cyn, "▶") : " "} ${b("sort by")}  ${col(c.grn, st.cfg.sortField)} ${dim("◂ ▸ cycle")}`);
  // chunk (row 1)
  const chSel = st.rowB === 1;
  const ci = CHUNK_STEPS.indexOf(st.cfg.chunkTargetBytes);
  left.push(`${chSel ? col(c.cyn, "▶") : " "} ${b("shard")}    ${col(c.grn, fmtBytes(st.cfg.chunkTargetBytes))} ${dim("◂ ▸")} ${dim("[" + CHUNK_STEPS.map((_, i) => i === ci ? "◆" : "·").join("") + "]")}`);
  left.push("", dim(" field        idx  ends  cont"));
  idxFields.forEach((f, i) => {
    const row = 2 + i;
    const sel = st.rowB === row;
    const on = st.cfg.indexed.has(f.name);
    const ops = st.cfg.operators[f.name];
    const fc = e.perField.find((x) => x.name === f.name);
    const isStr = f.type === "string" && !f.multiValued;
    const cell = (active: boolean, avail: boolean, dangerous = false) =>
      !avail ? dim(" — ") : active ? col(dangerous ? c.red : c.grn, "[x]") : dim("[ ]");
    const idxCell = f.name === st.cfg.sortField ? col(c.cyn, "srt") : cell(on, true);
    const endCell = cell(ops.has("endsWith"), on && isStr);
    const conCell = cell(ops.has("contains"), on && isStr, fc?.containsExceedsColumn);
    // highlight the focused column when this row selected
    const hi = (s: string, col2: number) => sel && st.colB === col2 ? col(c.inv, s) : s;
    const line = ` ${pad(f.name, 12)} ${hi(idxCell, 0)}  ${hi(endCell, 1)}   ${hi(conCell, 2)}`;
    left.push(sel ? col(c.bold, "▶") + line : "  " + line.slice(0));
  });
  left.push("", dim(" [↑↓] row  [←→] value/col  [space] toggle  [q] quit"));

  // RIGHT column — always-live estimates + warnings
  const right: string[] = [];
  right.push(b("LIVE ESTIMATE"), "");
  right.push(...estimateBlock(e));
  right.push("", b("  Warnings"));
  right.push(...warningLines(e.warnings));

  // focused-field detail strip
  const detail: string[] = [""];
  if (st.rowB >= 2) {
    const f = idxFields[st.rowB - 2];
    detail.push(dim("focus: ") + b(f.name) + dim(`  ${f.type} · ${fmtInt(f.cardinality)} distinct · free ops: `) + col(c.gry, opInfo(f).free));
    if (st.cfg.indexed.has(f.name)) detail.push("  " + fieldCostLine(f.name, e, st.cfg));
  }

  return twoCol(left, right, 46) + "\n" + detail.join("\n");
}

function twoCol(l: string[], r: string[], leftWidth: number): string {
  const n = Math.max(l.length, r.length);
  const out: string[] = [];
  for (let i = 0; i < n; i++) {
    const lc = l[i] ?? "";
    const rc = r[i] ?? "";
    out.push(pad(lc, leftWidth) + col(c.gry, "│ ") + rc);
  }
  return out.join("\n");
}

// =====================================================================
// SHAPE C — REVIEW & OVERRIDE
// =====================================================================
interface Override { label: string; active: () => boolean; apply: () => void; delta: string; }

// The overrides are curated from THIS dataset's shape — the handful of ways a
// real user would most likely want to deviate from the recommendation. Each one
// is phrased as a decision and shows the consequence of flipping it.
const PK = profile.pkGuess ?? profile.fields[0].name;      // "id"
const REC = recommendSortField(profile);                    // "released_at"
const dropCandidate = "artist";
const containsField = "oracle_text";                        // the free-text trap
const endsField = "name";

function overridesC(): Override[] {
  return [
    {
      label: `Sort by ${b(PK)} instead of ${b(REC)} ${dim("(makes get(id) free)")}`,
      active: () => st.cfg.sortField === PK,
      apply: () => {
        st.cfg.sortField = st.cfg.sortField === PK ? REC : PK;
        st.cfg.indexed.add(st.cfg.sortField);
      },
      delta: `moves free range-pruning to a different field`,
    },
    {
      label: `Full-text ${b("contains")} on ${b(containsField)} ${dim("(force-indexes the free-text field)")}`,
      active: () => st.cfg.indexed.has(containsField) && st.cfg.operators[containsField].has("contains"),
      apply: () => {
        if (st.cfg.operators[containsField].has("contains")) {
          st.cfg.operators[containsField].delete("contains");
          st.cfg.indexed.delete(containsField);
        } else {
          st.cfg.indexed.add(containsField);
          st.cfg.operators[containsField].add("contains");
        }
      },
      delta: probeDelta(() => { st.cfg.indexed.add(containsField); toggleOp(containsField, "contains"); }, containsField),
    },
    {
      label: `Enable ${b("endsWith")} on ${b(endsField)}`,
      active: () => st.cfg.operators[endsField].has("endsWith"),
      apply: () => toggleOp(endsField, "endsWith"),
      delta: probeDelta(() => toggleOp(endsField, "endsWith"), endsField),
    },
    {
      label: `Drop the ${b(dropCandidate)} index ${dim("(make it non-queryable)")}`,
      active: () => !st.cfg.indexed.has(dropCandidate),
      apply: () => { st.cfg.indexed.has(dropCandidate) ? st.cfg.indexed.delete(dropCandidate) : st.cfg.indexed.add(dropCandidate); },
      delta: `removes a pay-on-use index + its zonemap`,
    },
    {
      label: `Use a smaller shard target ${dim("(finer pruning, more requests)")}`,
      active: () => st.cfg.chunkTargetBytes < defaultChunkTarget(profile),
      apply: () => { st.cfg.chunkTargetBytes = st.cfg.chunkTargetBytes <= 256 * KB ? nearestStep(defaultChunkTarget(profile)) : 256 * KB; },
      delta: `more shards, smaller each`,
    },
  ];
}

function nearestStep(x: number): number {
  return CHUNK_STEPS.reduce((best, s) => Math.abs(s - x) < Math.abs(best - x) ? s : best, CHUNK_STEPS[0]);
}
function toggleOp(field: string, op: Operator) {
  const s = st.cfg.operators[field];
  s.has(op) ? s.delete(op) : s.add(op);
}
// snapshot the mutable config, apply a hypothetical change, read the cost for
// one field, then restore exactly — so overrides can preview their delta.
function probeDelta(mut: () => void, field: string): string {
  const savedSort = st.cfg.sortField;
  const savedChunk = st.cfg.chunkTargetBytes;
  const savedIndexed = new Set(st.cfg.indexed);
  const savedOps: Record<string, Set<Operator>> = {};
  for (const k in st.cfg.operators) savedOps[k] = new Set(st.cfg.operators[k]);

  mut();
  const fc = estimate(profile, st.cfg).perField.find((x) => x.name === field);

  st.cfg.sortField = savedSort;
  st.cfg.chunkTargetBytes = savedChunk;
  st.cfg.indexed = savedIndexed;
  st.cfg.operators = savedOps;

  if (!fc) return "";
  const added = fc.endsWithBytes + fc.containsBytes || fc.baseIndexBytes;
  return `+${fmtBytes(added)} index${fc.containsExceedsColumn ? col(c.red, " — larger than the column!") : ""}`;
}

function renderC(): string {
  const e = estimate(profile, st.cfg);
  const L: string[] = [];
  L.push(b("static-shard analysed ") + b(profile.label));
  L.push(dim("Here's the config it recommends. Accept it, or override by exception."), "");

  // the recommendation as prose
  L.push(col(c.grn, "  ✓ ") + `Sort by ${b(st.cfg.sortField)} ${dim("— best free range-pruning; get(" + profile.pkGuess + ") is " + (st.cfg.sortField === profile.pkGuess ? "free" : "1+1 fetch"))}`);
  L.push(col(c.grn, "  ✓ ") + `Index ${b(fmtInt(st.cfg.indexed.size) + " fields")} ${dim("(" + [...st.cfg.indexed].join(", ") + ")")}`);
  L.push(col(c.grn, "  ✓ ") + `Shard target ${b(fmtBytes(st.cfg.chunkTargetBytes))} ${dim("→ " + fmtInt(e.shardCount) + " shards")}`);
  const enabledOps = Object.entries(st.cfg.operators).filter(([, s]) => s.size).map(([f, s]) => `${f}:${[...s].join("/")}`);
  L.push(col(c.grn, "  ✓ ") + `Extra operators ${enabledOps.length ? b(enabledOps.join(", ")) : dim("none (free set only)")}`);
  L.push("");

  // compact estimate strip
  L.push(dim("  ─── consequences ───────────────────────────────────────────"));
  L.push(`  ${fmtInt(e.shardCount)} shards · manifest ${col(e.manifestOverBudget ? c.yel : c.grn, fmtBytes(e.manifestBytesGzip))}/${fmtBytes(e.manifestBudgetBytes)} · equality query ${fmtBytes(e.equalityQuery.bytes)}/${e.equalityQuery.requests}req`);
  if (e.warnings.length) { L.push(""); L.push(...warningLines(e.warnings)); }
  L.push("");

  if (!st.overrideOpen) {
    L.push(col(c.cyn, "  [Enter] accept & persist    [o] override something"));
  } else {
    L.push(b("  Overrides") + dim("  — flip any of these; the report above updates live"));
    const ovs = overridesC();
    ovs.forEach((o, i) => {
      const cur = i === st.cursorC;
      const mark = o.active() ? col(c.grn, "[✓]") : dim("[ ]");
      const line = `  ${mark} ${o.label}   ${dim(o.delta)}`;
      L.push(cur ? col(c.inv, pad(stripToInv(line), 92)) : line);
    });
    L.push("", dim("  [↑/↓] move  [space] flip  [o] close overrides  [Enter] persist"));
  }
  if (st.persisted) L.push("", col(c.grn, "  ✓ wrote static-shard.config.json  (prototype: see JSON below)"), "", ...configJson(st.cfg).split("\n").map((l) => "  " + col(c.gry, l)));
  return L.join("\n");
}
// inv highlighting mangles nested codes; strip for measuring only
function stripToInv(s: string): string { return s; }

// ---- config serialization (what "persist" would write) --------------------
function configJson(cfg: Config): string {
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

// =====================================================================
// FOOTER + FRAME
// =====================================================================
function footer(): string {
  const names = { A: "Linear wizard", B: "Live cockpit", C: "Review & override" };
  const tabs = (["A", "B", "C"] as const).map((v) =>
    v === st.variant ? col(c.bgBlu, b(` ${v} · ${names[v]} `)) : dim(` ${v} · ${names[v]} `)).join("  ");
  return "\n" + dim("─".repeat(94)) + "\n" + tabs + "   " + dim("[Tab] cycle shape   [1/2/3] jump   [q] quit");
}

function render() {
  clear();
  const title = b(col(c.mag, "static-shard init")) + dim("  — config wizard prototype (T7 #10) — pick a shape, react, steal the best bits");
  let body = "";
  if (st.variant === "A") body = renderA();
  else if (st.variant === "B") body = renderB();
  else body = renderC();
  process.stdout.write(title + "\n\n" + body + footer() + "\n");
}

// =====================================================================
// INPUT
// =====================================================================
function onKey(str: string, key: readline.Key) {
  const name = key?.name ?? str;
  if (name === "q" || (key?.ctrl && name === "c")) { clear(); process.exit(0); }
  if (name === "tab") { st.variant = st.variant === "A" ? "B" : st.variant === "B" ? "C" : "A"; st.persisted = false; render(); return; }
  if (str === "1") { st.variant = "A"; render(); return; }
  if (str === "2") { st.variant = "B"; render(); return; }
  if (str === "3") { st.variant = "C"; render(); return; }

  if (st.variant === "A") handleA(name);
  else if (st.variant === "B") handleB(name);
  else handleC(name);
  render();
}

function handleA(name: string) {
  const step = st.stepA;
  const next = () => { st.stepA = Math.min(5, st.stepA + 1); st.cursorA = 0; };
  const back = () => { st.stepA = Math.max(0, st.stepA - 1); st.cursorA = 0; };
  // nav is n/b only, so ←/→ stay free for in-step controls (the chunk slider)
  if (name === "n") return step < 5 ? next() : undefined;
  if (name === "b") return back();
  if (step === 0 && (name === "return" || name === "space")) return next();
  if (step === 1) {
    if (name === "up") st.cursorA = (st.cursorA - 1 + sortCandidates.length) % sortCandidates.length;
    if (name === "down") st.cursorA = (st.cursorA + 1) % sortCandidates.length;
    if (name === "space" || name === "return") st.cfg.sortField = sortCandidates[st.cursorA].name;
  } else if (step === 2) {
    if (name === "up") st.cursorA = (st.cursorA - 1 + indexable.length) % indexable.length;
    if (name === "down") st.cursorA = (st.cursorA + 1) % indexable.length;
    if (name === "space") {
      const f = indexable[st.cursorA];
      if (f.name === st.cfg.sortField) return; // sort field must stay indexed
      st.cfg.indexed.has(f.name) ? st.cfg.indexed.delete(f.name) : st.cfg.indexed.add(f.name);
    }
  } else if (step === 3) {
    const eligible = opEligibleFields();
    if (eligible.length === 0) return;
    if (st.cursorA >= eligible.length) st.cursorA = 0;
    if (name === "up") st.cursorA = (st.cursorA - 1 + eligible.length) % eligible.length;
    if (name === "down") st.cursorA = (st.cursorA + 1) % eligible.length;
    const fname = eligible[st.cursorA].name;
    if (name === "e") toggleOp(fname, "endsWith");
    if (name === "c") toggleOp(fname, "contains");
  } else if (step === 4) {
    const i = CHUNK_STEPS.indexOf(st.cfg.chunkTargetBytes);
    if (name === "left") st.cfg.chunkTargetBytes = CHUNK_STEPS[Math.max(0, i - 1)];
    if (name === "right") st.cfg.chunkTargetBytes = CHUNK_STEPS[Math.min(CHUNK_STEPS.length - 1, i + 1)];
  } else if (step === 5) {
    if (name === "return") st.persisted = true;
  }
}

function handleB(name: string) {
  const idxFields = indexable;
  const maxRow = 1 + idxFields.length;
  if (name === "up") st.rowB = (st.rowB - 1 + (maxRow + 1)) % (maxRow + 1);
  if (name === "down") st.rowB = (st.rowB + 1) % (maxRow + 1);
  if (st.rowB === 0) {
    const i = sortCandidates.findIndex((f) => f.name === st.cfg.sortField);
    if (name === "left") st.cfg.sortField = sortCandidates[(i - 1 + sortCandidates.length) % sortCandidates.length].name;
    if (name === "right") st.cfg.sortField = sortCandidates[(i + 1) % sortCandidates.length].name;
    // ensure sort field is indexed
    st.cfg.indexed.add(st.cfg.sortField);
  } else if (st.rowB === 1) {
    const i = CHUNK_STEPS.indexOf(st.cfg.chunkTargetBytes);
    if (name === "left") st.cfg.chunkTargetBytes = CHUNK_STEPS[Math.max(0, i - 1)];
    if (name === "right") st.cfg.chunkTargetBytes = CHUNK_STEPS[Math.min(CHUNK_STEPS.length - 1, i + 1)];
  } else {
    const f = idxFields[st.rowB - 2];
    if (name === "left") st.colB = Math.max(0, st.colB - 1);
    if (name === "right") st.colB = Math.min(2, st.colB + 1);
    if (name === "space") {
      if (st.colB === 0) {
        if (f.name === st.cfg.sortField) return;
        st.cfg.indexed.has(f.name) ? st.cfg.indexed.delete(f.name) : st.cfg.indexed.add(f.name);
      } else if (st.cfg.indexed.has(f.name) && f.type === "string" && !f.multiValued) {
        toggleOp(f.name, st.colB === 1 ? "endsWith" : "contains");
      }
    }
  }
}

function handleC(name: string) {
  if (name === "o") { st.overrideOpen = !st.overrideOpen; st.cursorC = 0; return; }
  if (name === "return") { st.persisted = true; return; }
  if (!st.overrideOpen) return;
  const ovs = overridesC();
  if (name === "up") st.cursorC = (st.cursorC - 1 + ovs.length) % ovs.length;
  if (name === "down") st.cursorC = (st.cursorC + 1) % ovs.length;
  if (name === "space") ovs[st.cursorC].apply();
}

// ---- boot ------------------------------------------------------------------
readline.emitKeypressEvents(process.stdin);
if (process.stdin.isTTY) process.stdin.setRawMode(true);
process.stdin.on("keypress", onKey);
render();
