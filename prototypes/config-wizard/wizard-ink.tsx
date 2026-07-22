// PROTOTYPE — Shape A (linear wizard) rebuilt with Ink (React for the terminal).
//
// This is the VALIDATED UX REFERENCE, not the shipping choice: it pins down the
// flow and interaction model (←/→ staging, ↑/↓ within a step, the flat operator
// checklist, the file-size list, the live consequences panel, the collapsed
// review). Ink's flexbox lets the consequence axes sit in a bordered panel
// BESIDE the controls and update live as you move.
//
// The real CLI implements this DEP-LIGHT (no React/Ink footprint) — see tui.ts
// for the zero-dep rendering approach; live estimates simply move inline (above
// the controls) instead of in a side panel.
//   run:  pnpm ink   (from prototypes/config-wizard)

import React, { useRef, useState } from "react";
import { render, Box, Text, useInput, useApp, useStdout } from "ink";
import {
  profile, estimate, recommendSortField, CHUNK_STEPS, freshConfig,
  sortCandidates, indexable, eligibleOpFields, containsProbe, endsWithProbe,
  plainAxes, summaryLines, configJson, warningsFor, fmtBytes, fmtInt,
  type Config, type Operator,
} from "./wizard-shared.ts";

const STEPS = ["Detect", "Sort field", "Filter fields", "Text search", "File size", "Review"];

const filesAt = (s: number) => Math.ceil(profile.datasetBytesCompressed / s);
// the text-search step is a flat checklist: one row per (field × operator)
const opRows = (c: Config) =>
  eligibleOpFields(c).flatMap((f) => [
    { field: f.name, op: "endsWith" as Operator },
    { field: f.name, op: "contains" as Operator },
  ]);

function EstimatePanel({ cfg }: { cfg: Config }) {
  const e = estimate(profile, cfg);
  const extraIndex = e.perField.reduce((n, f) => n + f.endsWithBytes + f.containsBytes, 0);
  const anyBig = e.perField.some((f) => f.containsBytes > 0 && f.containsExceedsColumn);
  return (
    <Box flexDirection="column" borderStyle="round" borderColor="gray" paddingX={1} width={40}>
      <Text bold color="cyan">Consequences</Text>
      {plainAxes(e).map((a) => (
        <Box key={a.title} flexDirection="column" marginTop={1}>
          <Text bold color={a.warn ? "yellow" : "white"}>{a.title}</Text>
          <Text dimColor>{a.hint}</Text>
          {a.body.map((line, i) => <Text key={i} color={a.warn && i === 0 ? "yellow" : "green"}>{"  " + line}</Text>)}
        </Box>
      ))}
      <Box flexDirection="column" marginTop={1}>
        <Text bold color={anyBig ? "red" : "white"}>Extra search indexes</Text>
        <Text dimColor>built only for what you turn on</Text>
        <Text color={anyBig ? "red" : "green"}>{"  " + (extraIndex > 0 ? fmtBytes(extraIndex) + (anyBig ? "  ⚠" : "") : "none yet")}</Text>
      </Box>
    </Box>
  );
}

function Warnings({ cfg }: { cfg: Config }) {
  const ws = warningsFor(cfg);
  if (!ws.length) return null;
  return (
    <Box flexDirection="column" marginTop={1}>
      {ws.map((w, i) => (
        <Text key={i} color={w.level === "danger" ? "red" : "yellow"} wrap="wrap">
          {(w.level === "danger" ? "▲ " : "△ ") + (w.field ? w.field + " — " : "") + w.text}
        </Text>
      ))}
    </Box>
  );
}

function App() {
  const { exit } = useApp();
  const { stdout } = useStdout();
  const cfg = useRef<Config>(freshConfig());
  const cursor = useRef(0);
  const [step, setStep] = useState(0);
  const [persisted, setPersisted] = useState(false);
  const [showJson, setShowJson] = useState(false);
  const [, force] = useState(0);
  const rerender = () => force((x) => x + 1);
  const rec = recommendSortField(profile);

  const go = (d: number) => { cursor.current = 0; setStep((s) => Math.max(0, Math.min(STEPS.length - 1, s + d))); };

  useInput((input, key) => {
    if (input === "q") return exit();
    // ←/→ move between stages
    if (key.rightArrow && step < STEPS.length - 1) { go(1); return; }
    if (key.leftArrow) { go(-1); return; }
    const c = cfg.current;

    if (step === 0 && key.return) return go(1);

    if (step === 1) {
      if (key.upArrow) cursor.current = (cursor.current - 1 + sortCandidates.length) % sortCandidates.length;
      if (key.downArrow) cursor.current = (cursor.current + 1) % sortCandidates.length;
      if (input === " ") { c.sortField = sortCandidates[cursor.current].name; c.indexed.add(c.sortField); }
    } else if (step === 2) {
      if (key.upArrow) cursor.current = (cursor.current - 1 + indexable.length) % indexable.length;
      if (key.downArrow) cursor.current = (cursor.current + 1) % indexable.length;
      if (input === " ") {
        const f = indexable[cursor.current];
        if (f.name !== c.sortField) c.indexed.has(f.name) ? c.indexed.delete(f.name) : c.indexed.add(f.name);
      }
    } else if (step === 3) {
      const rows = opRows(c);
      if (rows.length) {
        if (cursor.current >= rows.length) cursor.current = 0;
        if (key.upArrow) cursor.current = (cursor.current - 1 + rows.length) % rows.length;
        if (key.downArrow) cursor.current = (cursor.current + 1) % rows.length;
        if (input === " ") {
          const { field, op } = rows[cursor.current];
          c.operators[field].has(op) ? c.operators[field].delete(op) : c.operators[field].add(op);
        }
      }
    } else if (step === 4) {
      // ↑/↓ pick a file-size (←/→ are stage nav now)
      const i = CHUNK_STEPS.indexOf(c.chunkTargetBytes);
      if (key.upArrow) c.chunkTargetBytes = CHUNK_STEPS[Math.max(0, i - 1)];
      if (key.downArrow) c.chunkTargetBytes = CHUNK_STEPS[Math.min(CHUNK_STEPS.length - 1, i + 1)];
    } else if (step === 5) {
      if (input === "v") setShowJson((s) => !s);
      if (key.return) setPersisted(true);
    }
    rerender();
  });

  const c = cfg.current;
  const wide = (stdout?.columns ?? 80) >= 92;
  const showPanel = step >= 1 && step <= 4;

  return (
    <Box flexDirection="column" paddingX={1}>
      <Box marginBottom={1}>
        <Text backgroundColor="magenta" color="black"> static-shard init </Text>
        <Text dimColor>  config wizard — Ink</Text>
      </Box>
      {/* breadcrumb */}
      <Box marginBottom={1}>
        {STEPS.map((s, i) => (
          <Text key={s} color={i === step ? "cyanBright" : "gray"} bold={i === step}>
            {(i === step ? `▶ ${i + 1} ${s}` : ` ${i + 1} ${s}`) + (i < STEPS.length - 1 ? "  " : "")}
          </Text>
        ))}
      </Box>

      <Box flexDirection={wide ? "row" : "column"} gap={wide ? 2 : 0}>
        <Box flexDirection="column" width={wide && showPanel ? 54 : undefined}>{stepBody()}</Box>
        {showPanel && <EstimatePanel cfg={c} />}
      </Box>

      <Box marginTop={1}><Text dimColor>{helpText()}</Text></Box>
    </Box>
  );

  function stepBody(): React.ReactNode {
    if (step === 0) {
      return (
        <Box flexDirection="column">
          <Text bold>Detected  {profile.label}</Text>
          <Box marginTop={1} flexDirection="column">
            {profile.fields.map((f) => {
              const role = f.name === profile.pkGuess ? "id / primary key"
                : f.multiValued ? "multi-value" : f.discourageIndex ? "long free text" : "";
              return (
                <Text key={f.name}>
                  {f.name.padEnd(13)}
                  <Text dimColor>{f.type.padEnd(8) + " " + (fmtInt(f.cardinality) + " values").padEnd(16)}</Text>
                  <Text color={f.name === profile.pkGuess ? "green" : f.discourageIndex ? "gray" : "magenta"}>{role}</Text>
                </Text>
              );
            })}
          </Box>
          <Box marginTop={1}><Text color="cyan">Press Enter to continue →</Text></Box>
        </Box>
      );
    }
    if (step === 1) {
      return (
        <Box flexDirection="column">
          <Text bold>Sort by which field?</Text>
          <Text dimColor>Fast filtering + ranges, free — pick what you filter on most.</Text>
          <Box marginTop={1} flexDirection="column">
            {sortCandidates.map((f, i) => {
              const sel = c.sortField === f.name;
              return (
                <Text key={f.name} color={i === cursor.current ? "cyan" : undefined} inverse={i === cursor.current}>
                  {(sel ? "◉ " : "○ ") + f.name.padEnd(13)}
                  <Text dimColor>{`${f.type} · ${fmtInt(f.cardinality)} values`}</Text>
                  {f.name === rec ? <Text color="green"> ★ recommended</Text> : null}
                </Text>
              );
            })}
          </Box>
          <Box marginTop={1}>
            <Text color={c.sortField === profile.pkGuess ? "green" : "gray"}>
              {c.sortField === profile.pkGuess
                ? `Looking up by ${profile.pkGuess}: instant`
                : `Looking up by ${profile.pkGuess}: one extra fetch`}
            </Text>
          </Box>
        </Box>
      );
    }
    if (step === 2) {
      const e = estimate(profile, c);
      return (
        <Box flexDirection="column">
          <Text bold>Which fields do you want to filter on?</Text>
          <Text dimColor>Only indexed fields are filterable.</Text>
          <Box marginTop={1} flexDirection="column">
            {indexable.map((f, i) => {
              const on = c.indexed.has(f.name);
              const isSort = f.name === c.sortField;
              const fc = e.perField.find((x) => x.name === f.name);
              const cost = !on ? "" : isSort ? " sorted — free ranges"
                : ` +${fmtBytes((fc?.baseIndexBytes ?? 0))} when used`;
              return (
                <Text key={f.name} color={i === cursor.current ? "cyan" : undefined} inverse={i === cursor.current}>
                  {(on ? "[✓] " : "[ ] ") + f.name.padEnd(12)}
                  {isSort ? <Text color="cyan">(sort)</Text> : null}
                  <Text dimColor>{cost}</Text>
                </Text>
              );
            })}
          </Box>
        </Box>
      );
    }
    if (step === 3) {
      const rows = opRows(c);
      if (!rows.length) return <Text dimColor>No text fields indexed — nothing to add. Press → to continue.</Text>;
      if (cursor.current >= rows.length) cursor.current = 0;
      return (
        <Box flexDirection="column">
          <Text bold>Extra ways to search text</Text>
          <Text dimColor>Already on: exact match · is-one-of · starts-with</Text>
          <Text dimColor>ends with = match the end · contains = match anywhere</Text>
          <Box marginTop={1} flexDirection="column">
            {rows.map((r, i) => {
              const on = c.operators[r.field].has(r.op);
              const focused = i === cursor.current;
              const label = r.op === "endsWith" ? "ends with" : "contains ";
              let cost = ""; let big = false;
              if (r.op === "endsWith") cost = `+${fmtBytes(endsWithProbe(c, r.field))}`;
              else { const cp = containsProbe(c, r.field); cost = `+${fmtBytes(cp.bytes)}`; big = cp.big; }
              return (
                <Text key={r.field + r.op} inverse={focused}>
                  <Text color={on ? (big ? "red" : "green") : "gray"}>{on ? "[✓] " : "[ ] "}</Text>
                  <Text bold>{r.field.padEnd(12)}</Text>
                  <Text>{label + "   "}</Text>
                  <Text color={big ? "red" : "gray"}>{cost + (big ? "  ⚠ bigger than data" : "")}</Text>
                </Text>
              );
            })}
          </Box>
        </Box>
      );
    }
    if (step === 4) {
      return (
        <Box flexDirection="column">
          <Text bold>How big should each data file be?</Text>
          <Text dimColor>Smaller = each query grabs less it doesn't need, but takes more requests.</Text>
          <Box marginTop={1} flexDirection="column">
            {CHUNK_STEPS.map((s) => {
              const on = s === c.chunkTargetBytes;
              return (
                <Text key={s} color={on ? "cyanBright" : undefined} bold={on}>
                  {(on ? "◉ " : "○ ") + fmtBytes(s).padEnd(9)}
                  <Text dimColor>{`${fmtInt(filesAt(s))} files`}</Text>
                </Text>
              );
            })}
          </Box>
        </Box>
      );
    }
    // step 5 — review
    const json = configJson(c);
    const lineCount = json.split("\n").length;
    return (
      <Box flexDirection="column" width={wide ? 74 : undefined}>
        <Text bold>Review</Text>
        <Box flexDirection="column" marginTop={1}>
          {summaryLines(c).map((l, i) => <Text key={i} color="green">{"✓ " + l}</Text>)}
        </Box>
        <Warnings cfg={c} />
        <Box marginTop={1} flexDirection="column">
          {showJson ? (
            <>
              <Text color="cyan">▾ static-shard.config.json  ·  press v to collapse</Text>
              <Box borderStyle="round" borderColor="gray" flexDirection="column" paddingX={1}>
                {json.split("\n").map((l, i) => <Text key={i} color="gray">{l}</Text>)}
              </Box>
            </>
          ) : (
            <Text dimColor>{`▸ static-shard.config.json  (${lineCount} lines)  ·  press v to preview`}</Text>
          )}
        </Box>
        <Box marginTop={1}>
          {persisted
            ? <Text color="green">✓ wrote static-shard.config.json  (prototype: nothing on disk)</Text>
            : <Text color="cyan">Press Enter to write the config</Text>}
        </Box>
      </Box>
    );
  }

  function helpText(): string {
    const nav = "←/→ change stage   ·   q quit";
    if (step === 0) return "→ or Enter to continue   ·   q quit";
    if (step === 1) return "↑/↓ move   space choose   ·   " + nav;
    if (step === 2) return "↑/↓ move   space add/remove   ·   " + nav;
    if (step === 3) return "↑/↓ move   space turn on/off   ·   " + nav;
    if (step === 4) return "↑/↓ pick size   ·   " + nav;
    return "Enter write   ·   v preview file   ·   ← back   ·   q quit";
  }
}

if (!process.stdin.isTTY) {
  console.log("wizard-ink needs an interactive terminal. Run:  pnpm ink");
  process.exit(0);
}
render(<App />);
