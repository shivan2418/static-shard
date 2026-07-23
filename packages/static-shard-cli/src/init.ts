import { existsSync, mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import { loadConfigFile, resolveConfig } from "./config.js";
import { inferSchema } from "./infer.js";
import { readInputRecords } from "./input.js";
import type { FieldConfig, InputFormat, StaticShardConfig } from "./types.js";
import { getFormatVersion } from "./version.js";

const DEFAULT_SAMPLE_SIZE = 1000;

/** Convention for editor JSON-schema resolution: `config.schema.json` ships inside the installed devDependency. */
const CONFIG_SCHEMA_REF = "node_modules/static-shard-cli/config.schema.json";

export interface InitOptions {
  /** Directory input/config-relative paths resolve against. */
  cwd: string;
  /** Absolute path to read/write `static-shard.config.json`. */
  configPath: string;
  /** `init` has no interactive wizard yet (T12) — must be true, or this throws. */
  yes: boolean;
  /** Re-run inference even if a config already exists, refreshing the baked schema block. */
  reinfer?: boolean;
  /** Infer from every record instead of a sample (rare fields / true cardinalities). */
  fullScan?: boolean;
  sampleSize?: number;
  collection?: string;
  /** Positional input path/glob — required the first time `init` runs for a given config. */
  inputPath?: string;
  format?: InputFormat;
  delimiter?: string;
  records?: string;
  sortField?: string;
  pk?: string;
  /** Explicit opt-in indexed-field set, overriding the inferred/existing recommendation. */
  indexedFields?: string[];
  /** Fields to opt into the reversed-value index (ADR-0003 §7) — forces them indexed too. */
  endsWithFields?: string[];
  /** Fields to opt into the trigram index (ADR-0003 §7) — forces them indexed too. */
  containsFields?: string[];
  output?: string;
  clientOut?: string;
  basePath?: string;
  shardBytes?: number;
  indexChunkBytes?: number;
}

type FieldFlagOverrides = Pick<InitOptions, "indexedFields" | "endsWithFields" | "containsFields">;

/**
 * Layers `--indexed`/`--ends-with`/`--contains` on top of a fields record — used identically
 * whether `fields` just came from inference or is being reused from an existing config, so the
 * flag-equivalence contract (ADR-0005 §3) is honored the same way either way. `--indexed`, when
 * passed, is the *complete* indexed set (flags > file precedence) rather than merged with
 * whatever was already indexed. Multi-valued fields and a non-sort-field pk are always forced
 * indexed regardless — omitting them isn't a real choice, it produces a structurally broken config.
 */
function applyFieldFlagOverrides(
  fields: Record<string, FieldConfig>,
  sortField: string,
  pk: string | undefined,
  overrides: FieldFlagOverrides,
): Record<string, FieldConfig> {
  const flagGroups: [string, string[] | undefined][] = [
    ["indexed", overrides.indexedFields],
    ["ends-with", overrides.endsWithFields],
    ["contains", overrides.containsFields],
  ];
  for (const [flagName, names] of flagGroups) {
    for (const name of names ?? []) {
      if (!fields[name]) {
        throw new Error(
          `static-shard: --${flagName} "${name}" is not declared in the baked schema — pass --reinfer to rediscover fields`,
        );
      }
    }
  }

  const indexedWanted = overrides.indexedFields ? new Set(overrides.indexedFields) : undefined;
  const endsWithWanted = new Set(overrides.endsWithFields ?? []);
  const containsWanted = new Set(overrides.containsFields ?? []);
  if (!indexedWanted && endsWithWanted.size === 0 && containsWanted.size === 0) return fields;

  const next: Record<string, FieldConfig> = {};
  for (const [name, f] of Object.entries(fields)) {
    if (name === sortField) {
      next[name] = f;
      continue;
    }
    const cfg: FieldConfig = { ...f };
    const mustIndex = cfg.multi === true || name === pk;

    if (indexedWanted) {
      if (indexedWanted.has(name) || mustIndex) cfg.indexed = true;
      else delete cfg.indexed;
    }
    if (endsWithWanted.has(name)) {
      cfg.indexed = true;
      cfg.endsWith = true;
    }
    if (containsWanted.has(name)) {
      cfg.indexed = true;
      cfg.contains = true;
    }
    if (mustIndex) cfg.indexed = true;
    next[name] = cfg;
  }
  return next;
}

export interface InitResult {
  configPath: string;
  config: StaticShardConfig;
  /** True when this run actually (re)inferred the schema, false when it reused an existing baked one. */
  reinferred: boolean;
}

/**
 * The non-interactive half of `init` (ADR-0005 §4 / ADR-0006 §1): infer → recommend → persist
 * `static-shard.config.json`. `init --yes` + flags is fully scriptable; the interactive wizard
 * (T12) is a UX layer over this exact same core. Precedence is flags > existing file > inferred
 * defaults (ADR-0005 §3).
 */
export function init(opts: InitOptions): InitResult {
  if (!opts.yes) {
    throw new Error(
      'static-shard: "init" requires --yes — the interactive wizard is not implemented yet; run with --yes plus flags',
    );
  }

  const existing = existsSync(opts.configPath) ? loadConfigFile(opts.configPath) : undefined;

  const inputPath = opts.inputPath ?? existing?.input.path;
  if (!inputPath) {
    throw new Error("static-shard: init needs an input path/glob — pass it as the positional argument");
  }
  const format: InputFormat = opts.format ?? existing?.input.format ?? "ndjson";
  const delimiter = opts.delimiter ?? existing?.input.delimiter;
  const recordsPath = opts.records ?? existing?.input.records;
  const collection = opts.collection ?? existing?.collection ?? path.basename(inputPath).replace(/\.[^.]+$/, "");

  const reinferred = existing === undefined || opts.reinfer === true;

  let fields: Record<string, FieldConfig>;
  let sortField: string;
  let pk: string | undefined;

  if (reinferred) {
    const readDelimiter = delimiter ?? (format === "tsv" ? "\t" : ",");
    const allRecords = readInputRecords(path.resolve(opts.cwd, inputPath), {
      format,
      delimiter: readDelimiter,
      recordsPath,
      fields: {},
    });
    if (allRecords.length === 0) {
      throw new Error(`static-shard: init found no records in "${inputPath}" to infer a schema from`);
    }
    const sample = opts.fullScan ? allRecords : allRecords.slice(0, opts.sampleSize ?? DEFAULT_SAMPLE_SIZE);
    const inferred = inferSchema(sample);

    sortField = opts.sortField ?? inferred.sortField;
    pk = opts.pk ?? inferred.pk;

    const defaultIndexed = new Set(inferred.indexedFields);
    if (pk !== undefined) defaultIndexed.add(pk);

    fields = {};
    for (const [name, f] of Object.entries(inferred.fields)) {
      const cfg: FieldConfig = { kind: f.kind };
      const isIndexed = name !== sortField && (defaultIndexed.has(name) || f.multi);
      if (isIndexed) cfg.indexed = true;
      if (f.multi) cfg.multi = true;
      if (f.absent && isIndexed) cfg.absent = true;
      fields[name] = cfg;
    }
  } else {
    fields = existing!.schema.fields;
    sortField = opts.sortField ?? existing!.schema.sortField;
    pk = opts.pk ?? existing!.schema.pk;
  }

  fields = applyFieldFlagOverrides(fields, sortField, pk, opts);

  const output = opts.output ?? existing?.output;
  const clientOut = opts.clientOut ?? existing?.clientOut;
  const basePath = opts.basePath ?? existing?.basePath;
  const shardBytes = opts.shardBytes ?? existing?.shardBytes;
  const indexChunkBytes = opts.indexChunkBytes ?? existing?.indexChunkBytes;

  const config: StaticShardConfig = {
    $schema: CONFIG_SCHEMA_REF,
    formatVersion: getFormatVersion(),
    collection,
    input: {
      path: inputPath,
      ...(format !== "ndjson" ? { format } : {}),
      ...(delimiter !== undefined ? { delimiter } : {}),
      ...(recordsPath !== undefined ? { records: recordsPath } : {}),
    },
    ...(output !== undefined ? { output } : {}),
    ...(clientOut !== undefined ? { clientOut } : {}),
    ...(basePath !== undefined ? { basePath } : {}),
    ...(shardBytes !== undefined ? { shardBytes } : {}),
    ...(indexChunkBytes !== undefined ? { indexChunkBytes } : {}),
    schema: { sortField, ...(pk !== undefined ? { pk } : {}), fields },
  };

  // Fail loud on any invalid combination before writing anything — reuses build's own invariants.
  resolveConfig(config, path.dirname(opts.configPath));

  mkdirSync(path.dirname(opts.configPath), { recursive: true });
  writeFileSync(opts.configPath, JSON.stringify(config, null, 2) + "\n");

  return { configPath: opts.configPath, config, reinferred };
}
