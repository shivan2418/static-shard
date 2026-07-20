# R2 — Schema → TypeScript Type Inference Tooling for `static-shard`

## TL;DR

For `static-shard`, **hand-roll a minimal streaming schema inferencer and emit TypeScript directly from it** — do not adopt quicktype or json_typegen as the type engine. The reason is that static-shard's differentiator is not a plain `interface Record {...}`; it needs an **internal schema descriptor** (per field: observed primitive types, null/absent flags, numeric-ness, cardinality/enum candidacy) that drives *which query operators are valid per field* (numeric → range ops, string → contains/startsWith, low-cardinality → literal-union). Off-the-shelf tools give you TS text, not the structured field-metadata you need to generate a constrained query API, and their heavy dependency trees (quicktype-core is ~500 KB with lodash, pako, urijs, etc.) work against the "lightweight" goal. The hand-rolled core is small (F#-Data-style shape merging is famously ~200 lines) and you get exact control over sampling, CSV coercion, date/big-number policy, and the operator-constraint typing. Optionally keep `quicktype` around only as an *escape hatch* for users who just want a raw interface. Use **reservoir + head-biased sampling** on large inputs so rare fields are not missed.

## Options table

| Tool / approach | Input | Optional / nullable / union handling | Dep weight | Library-usable | Fit for static-shard |
|---|---|---|---|---|---|
| **quicktype-core** | JSON samples, JSON Schema, TS, GraphQL | Strong: missing→`?`, null→`\| null`, heterogeneous→union, integer vs number, enums, dates/UUIDs, map-vs-object via Markov heuristic | Heavy (~500 KB; lodash, pako, pluralize, readable-stream, unicode-properties, urijs, wordwrap, yaml) | Yes — `quicktype`, `InputData`, `jsonInputForTargetLanguage` | Escape hatch only; produces TS text, not field-metadata for operator typing |
| **json_typegen** (Rust) | JSON samples (multi via `---`), SQL | Multi-sample merge → optional + unions; F#-Data shape inference; nullable auto | Rust binary / WASM; not a JS lib | CLI + WASM web only (no npm library API) | Poor: shelling to a Rust binary or WASM in a JS build tool adds friction; still only emits TS text |
| **json-schema-to-typescript** | JSON **Schema** (not raw JSON) | Faithful to schema: `required`→optional, `type` arrays→union; needs a schema first | Medium | Yes (`compile`) | Only useful paired with a schema-inference step (see genson-js) |
| **genson-js** (+ json-schema-to-typescript) | Raw JSON objects | `required[]`→optional, `type:[...]`→union, merges N samples | **Zero runtime deps** (archived Oct 2024) | Yes (`createSchema`, `mergeSchemas`, `extendSchema`) | Good building block if you want JSON Schema as an intermediate; but archived and still no operator-metadata |
| **json-to-ts** | Single raw JSON object | Basic; weak multi-sample merging | Light | Yes | Too weak for messy multi-record data |
| **Hand-rolled inferencer** | JSON array / NDJSON / CSV rows | You define it exactly: per-field type-set + null/absent + numeric/cardinality | Near-zero (optional: papaparse for CSV) | N/A (it's yours) | **Best** — emits both the TS types and the operator-constraint metadata the query API needs |

## Deep dives

### quicktype (`quicktype-core`)

- **How inference works.** You build an `InputData` from one or more sources; each source is a named type with an array of JSON sample strings (`jsonInputForTargetLanguage(...).addSource({ name, samples: [...] })`). quicktype merges samples: a field present in some but not all samples becomes optional (`name?: string`); a `null` value becomes a union with `null`; values of differing types across samples become a real union (`IRUnion` internally) rather than being widened to `any`/`object`. It distinguishes integers from floats, infers enums (`inferEnums`), dates/UUIDs (`inferDateTimes`), and uses a Markov-chain heuristic to decide "typed object" vs "string-keyed map" (`inferMaps`). Mixed-type arrays become element unions.
- **Library API.** Fully usable as a library, not just CLI:
  ```js
  import { quicktype, InputData, jsonInputForTargetLanguage } from "quicktype-core";
  const jsonInput = jsonInputForTargetLanguage("typescript");
  await jsonInput.addSource({ name: "Record", samples: [jsonString1, jsonString2] });
  const inputData = new InputData();
  inputData.addInput(jsonInput);
  const { lines } = await quicktype({ inputData, lang: "typescript", inferEnums: true, inferDateTimes: true });
  ```
- **Dependency weight.** This is the main strike against it. `quicktype-core` is ~506 KB unpacked and pulls lodash, pako, pluralize, readable-stream, unicode-properties, urijs, wordwrap, yaml. For a "lightweight build-time tool" that is a lot of transitive surface.
- **Large inputs.** quicktype infers from the samples you hand it; it does not stream a 500 MB file for you. You must do sampling yourself and feed it a subset — which means you are already writing the sampling layer regardless.
- **Ergonomics/output.** Output quality is high and battle-tested. But the output is *TypeScript source text*. To drive per-field operator constraints you would have to re-parse that text or run its samples through your own analyzer anyway — defeating the point.

### json_typegen (Rust)

- **Algorithm.** Same lineage as F# Data type providers — a "shape" is inferred per value and shapes are merged across samples; the stripped-down algorithm is ~200 lines, which is a useful reference if hand-rolling. Multi-sample input uses a `---` separator; fields absent in some samples become optional, differing types become unions, nulls handled automatically.
- **Outputs.** Rust / Kotlin / TypeScript / Python, plus `json_schema` and `shape` output modes.
- **Interfaces.** Rust proc-macro, `json_typegen_cli`, and a WASM-powered web UI (typegen.vestera.as). **There is no npm JS-library API.** Integrating into a Node/TS build means shelling out to a compiled binary or loading the WASM bundle — extra build complexity and a non-JS toolchain dependency. Enum/date/big-number handling is not documented as a first-class feature.
- **Verdict.** Great algorithm to *learn from*, poor fit to *depend on* from a JS build tool.

### Hand-rolled inferencer (recommended core)

What static-shard actually needs per field is small and well-defined:

- the set of observed JSON types (`string | number | boolean | object | array | null`),
- whether the field was ever **absent** (→ optional `?`) vs ever explicitly **null** (→ `| null`) — these are distinct and off-the-shelf JSON tools often conflate them,
- for numbers: integer-vs-float, and min/max/precision if you want big-number safety,
- **cardinality**: a running set of distinct string values, capped (e.g. stop collecting past N≈50) so you can decide enum/literal-union candidacy,
- for CSV: the raw-string column + a coercion verdict.

The algorithm is a single pass over sampled records maintaining a `Map<fieldPath, FieldStats>` and a `merge(shape, record)` fold — conceptually the F#-Data / json_typegen shape merge, but you keep the *stats object*, not just the collapsed type. From that one structure you emit **two artifacts**: (1) the `interface Record` and (2) a field-descriptor table the query-API codegen consumes. No heavy deps; ~a few hundred lines. This is the only option that natively produces the operator-constraint metadata (see below).

## Sampling strategy for large datasets

You cannot type a 500 MB dataset from record 0, and a naive "first N records" both misses rare tail fields and mistypes fields that are homogeneous early but heterogeneous later.

Recommended hybrid, streaming (NDJSON line-by-line, JSON array via a streaming parser like `stream-json` or `clarinet`, CSV via papaparse's step/stream callback):

1. **Always include the head** (first ~1–2 k records) — cheap and catches the common shape immediately.
2. **Reservoir-sample** (Algorithm R) across the *entire* stream into a fixed buffer (e.g. 10–20 k records) so late/rare records get representation without holding the whole file in memory.
3. **Union the two** for inference. Head-bias gives fast common-case correctness; the reservoir guards against rare-field / late-type-drift blind spots.
4. **Track field-presence counts** during the full scan even for un-sampled rows if cheap (NDJSON/CSV make per-row key enumeration trivial) so "field appears in 0.1 % of rows" is still detected as optional rather than missed. Cheap counters over a full scan + deep type inference over a sample is the best accuracy/speed trade.
5. Expose knobs: `sampleSize`, `fullScanPresence: boolean`, and a `--sample all` opt-out for small datasets where full inference is affordable.

Note: whatever tool you pick, *you* own this layer — none of quicktype/json_typegen stream the source file for you.

## The operator-constraint angle (why this favors hand-rolling)

static-shard's typed client is the differentiator: the query API must be typed so that only *valid operators for a field's type* are offered. Concretely the codegen wants, per field:

- **numeric** → `gt/gte/lt/lte/between/eq` (range operators),
- **string** → `contains/startsWith/endsWith/eq`,
- **boolean** → `eq`,
- **date** → range operators (once date-detected),
- **low-cardinality string** → a **string-literal union** (`"A" | "B" | "C"`) and `eq/in` — and these are typically exactly the *indexed* fields, so cardinality info is doubly valuable.

This requires a *structured, machine-readable field descriptor*, not TypeScript text. quicktype and json_typegen both terminate at "emit TS source" — to recover per-field type + cardinality from their output you would re-parse generated code or re-run your own analysis on the samples. The hand-rolled inferencer produces the descriptor as its *primary* output, and the `interface` and the operator-typed query surface (e.g. a generated `WhereClause<T>` mapped type, or per-field method builders) both fall out of the same structure. Cardinality-based literal-union promotion, in particular, is a policy you want to own (threshold, max distinct values, opt-out) — an external tool's enum heuristic won't align with "these are the indexed fields."

This is the decisive argument: the type engine and the operator-constraint engine want the same intermediate representation, so build one IR and own it.

## Recommended design

- **Core:** a hand-rolled streaming inferencer producing an internal `SchemaDescriptor` = `{ fields: Record<string, FieldStats> }`, `FieldStats = { types: Set, everAbsent, everNull, isInteger, numericRange, distinctStringValues (capped), sampleCount }`.
- **Dependency footprint:** effectively zero for JSON/NDJSON (use a small streaming JSON parser only if inputs are giant single arrays — `stream-json` is light). For **CSV**, use **papaparse** with `dynamicTyping` as a *first pass*, but re-verify per column with your own coercion rules (see below) — papaparse's per-cell typing is not column-consistent and keeps `>2^53` numbers as strings.
- **Emit two artifacts from one IR:** (1) `interface Record`, (2) the operator-typed query API.
- **Optional / null:** keep `everAbsent` and `everNull` separate → `field?: T` vs `field: T | null` vs `field?: T | null`. This is a correctness edge most tools blur.
- **Unions:** if a field shows >1 primitive type across samples, emit the union; if it explodes (e.g. object-or-string with many shapes), fall back to a widened type and warn.
- **Dates:** detect ISO-8601 / configurable formats on string fields; **default to typing as `string`** (dates survive JSON round-trips as strings) but record a `isDate` flag so the query API can offer range operators. Do *not* silently emit `Date` — that implies a parse step the sharded client may not do. Make `Date` opt-in.
- **Big numbers:** if an integer field exceeds `Number.MAX_SAFE_INTEGER` (2^53−1), type it as `string` (or `bigint` behind a flag) and warn — mirrors papaparse's own guard and avoids silent precision loss.
- **CSV typing:** per column, scan sampled cells: all parse as int → `number` (integer), all numeric → `number`, all in `{true,false,yes,no,0,1}` → `boolean`, all ISO-date → date-flagged string, low distinct count → literal union, else `string`. Track an empty-cell → optional/`""` policy. Confidence is inherently lower than JSON; surface a report and let the user override per column.
- **Enums / low-cardinality:** promote to string-literal union when `distinctStringValues.size <= threshold` (default ~12–20, configurable) **and** the field is a candidate index. Cap distinct-value collection to bound memory.
- **Escape hatch (optional):** expose `--emit-plain-interface` backed by `quicktype-core` for users who only want a vanilla interface and don't care about the query API. Lazy-load it so the heavy deps aren't in the hot path.

## Open questions for the Codegen & typed-client decision

1. **Query-API shape:** mapped-type `WhereClause<Record>` (one generic type, operators gated by field type via conditional types) vs. generated per-field builder methods? This determines how much the inferencer must emit vs. how much lives in a static generic runtime type. *(Most important — see below.)*
2. **Literal-union threshold & indexing coupling:** is enum promotion driven purely by cardinality, or only for fields the user marks as indexed? Where does that config live (data-driven auto vs. explicit config)?
3. **Date policy default:** `string`-with-flag vs. `Date` — depends on whether the generated client deserializes shards or hands back raw JSON.
4. **Sampling defaults:** what sample size / reservoir size balances build speed vs. rare-field capture for the target dataset sizes, and should presence-counting always be a full scan?
5. **Union/heterogeneous fallback:** when a field is too messy to type usefully, fail the build, warn + widen to `unknown`, or require user annotation?
6. **CSV override mechanism:** how do users correct a mis-inferred column type — a sidecar schema file, inline config, or annotations?

## Sources

- quicktype FAQ (inference: optionals, nullability, dates/UUIDs/enums, integer detection, unions, map-vs-object Markov heuristic): https://github.com/glideapps/quicktype/blob/master/FAQ.md
- quicktype "under the hood" (IR, union inference): http://blog.quicktype.io/under-the-hood/
- quicktype transformed string types (dates/UUIDs): http://blog.quicktype.io/transformed-string-types/
- quicktype npm (CLI/library): https://www.npmjs.com/package/quicktype
- quicktype-core dependencies (lodash, pako, pluralize, readable-stream, unicode-properties, urijs, wordwrap, yaml; ~506 KB): https://www.npmjs.com/package/quicktype-core?activeTab=dependencies and https://socket.dev/npm/package/quicktype-core/dependencies
- quicktype-core programmatic API examples (`InputData`, `jsonInputForTargetLanguage`, `inferEnums`): https://snyk.io/advisor/npm-package/quicktype-core/functions/quicktype-core.quicktype
- quicktype customizing (options like inferMaps/inferDateTimes/inferEnums): http://blog.quicktype.io/customizing-quicktype/
- json_typegen repo (F#-Data shape inference, ~200-line algorithm, `---` multi-sample, output modes, WASM/CLI, no JS lib API): https://github.com/evestera/json_typegen
- json_typegen CLI README: https://github.com/evestera/json_typegen/blob/master/json_typegen_cli/README.md
- json_typegen crate docs: https://docs.rs/json_typegen
- json_typegen web (WASM): https://typegen.vestera.as/
- json-schema-to-typescript npm (compile JSON Schema → TS, required/optional, additionalProperties): https://www.npmjs.com/package/json-schema-to-typescript
- genson-js (zero-dep JSON-Schema inference, createSchema/mergeSchemas/extendSchema, required[]→optional, type[]→union; archived Oct 2024): https://github.com/aspecto-io/genson-js
- GenSON (Python original, schema merging): https://github.com/wolverdude/GenSON
- Papa Parse docs (dynamicTyping: number/boolean/null detection, per-column config, >2^53 stays string, European number caveat): https://www.papaparse.com/docs
- Papa Parse dynamicTyping setup: https://app.studyraid.com/en/read/11463/359348/setting-up-dynamic-typing
- Comparison of JSON→TS approaches: https://dev.to/helloashish99/from-json-to-typescript-five-ways-to-stop-hand-writing-interfaces-3bm5
