import { readFileSync, readdirSync, realpathSync, statSync } from "node:fs";
import path from "node:path";
import type { FieldConfig, FieldKind, InputFormat } from "./types.js";

export interface InputReadOptions {
  format: InputFormat;
  /** Column delimiter for csv/tsv; ignored for ndjson/json. */
  delimiter: string;
  /** JSON only: dot-path to the nested array/map of records (record selector). */
  recordsPath?: string;
  fields: Record<string, FieldConfig>;
}

const GLOB_MAGIC = /[*?]/;

function hasGlobMagic(segment: string): boolean {
  return GLOB_MAGIC.test(segment);
}

function segmentToRegExp(segment: string): RegExp {
  let pattern = "^";
  for (const ch of segment) {
    if (ch === "*") pattern += ".*";
    else if (ch === "?") pattern += ".";
    else pattern += ch.replace(/[.+^${}()|[\]\\]/g, "\\$&");
  }
  return new RegExp(pattern + "$");
}

function walkGlob(dir: string, segments: string[], results: string[], visitedRealDirs: Set<string>): void {
  if (segments.length === 0) return;
  const [segment, ...rest] = segments as [string, ...string[]];

  if (segment === "**") {
    walkGlob(dir, rest, results, visitedRealDirs);
    let entries: string[];
    try {
      entries = readdirSync(dir);
    } catch {
      return;
    }
    for (const entry of entries) {
      const full = path.join(dir, entry);
      if (!statSync(full).isDirectory()) continue;
      // Guard `**`'s unbounded recursion against symlink cycles (a fixed-segment pattern can't loop).
      let real: string;
      try {
        real = realpathSync(full);
      } catch {
        continue;
      }
      if (visitedRealDirs.has(real)) continue;
      visitedRealDirs.add(real);
      walkGlob(full, segments, results, visitedRealDirs);
    }
    return;
  }

  let entries: string[];
  try {
    entries = readdirSync(dir);
  } catch {
    return;
  }
  const regex = segmentToRegExp(segment);
  for (const entry of entries) {
    if (!regex.test(entry)) continue;
    const full = path.join(dir, entry);
    if (rest.length === 0) {
      if (statSync(full).isFile()) results.push(full);
    } else if (statSync(full).isDirectory()) {
      walkGlob(full, rest, results, visitedRealDirs);
    }
  }
}

/** Expands a single path or glob pattern (`*`/`?`/`**`) to a sorted list of matching absolute file paths (T9). */
export function expandInputFiles(absPathOrPattern: string): string[] {
  const segments = absPathOrPattern.split(path.sep);
  const firstGlobIdx = segments.findIndex(hasGlobMagic);
  if (firstGlobIdx === -1) return [absPathOrPattern];

  const startDir = segments.slice(0, firstGlobIdx).join(path.sep) || path.sep;
  const patternSegments = segments.slice(firstGlobIdx);
  const results: string[] = [];
  walkGlob(startDir, patternSegments, results, new Set());
  return results.sort();
}

function readNdjsonRecords(filePath: string): Record<string, unknown>[] {
  const raw = readFileSync(filePath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}

function navigateRecordsPath(doc: unknown, recordsPath: string): unknown {
  let node: unknown = doc;
  for (const key of recordsPath.split(".")) {
    if (node === null || typeof node !== "object" || Array.isArray(node)) {
      throw new Error(
        `static-shard: input.records path "${recordsPath}" — "${key}" cannot be navigated into (not an object)`,
      );
    }
    node = (node as Record<string, unknown>)[key];
  }
  return node;
}

function selectRecordsFromNode(node: unknown, recordsPath: string | undefined): Record<string, unknown>[] {
  if (Array.isArray(node)) return node as Record<string, unknown>[];
  if (node !== null && typeof node === "object") {
    return Object.values(node as Record<string, unknown>) as Record<string, unknown>[];
  }
  throw new Error(
    `static-shard: input.records${recordsPath ? ` path "${recordsPath}"` : ""} did not land on an array or object of records`,
  );
}

function readJsonRecords(filePath: string, recordsPath: string | undefined): Record<string, unknown>[] {
  const doc: unknown = JSON.parse(readFileSync(filePath, "utf8"));
  const node = recordsPath === undefined ? doc : navigateRecordsPath(doc, recordsPath);
  return selectRecordsFromNode(node, recordsPath);
}

/** RFC4180-ish: quoted fields may contain the delimiter, newlines, and `""`-escaped quotes. */
function parseDelimitedRows(content: string, delimiter: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let field = "";
  let inQuotes = false;

  for (let i = 0; i < content.length; i++) {
    const ch = content[i]!;
    if (inQuotes) {
      if (ch === '"') {
        if (content[i + 1] === '"') {
          field += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
      continue;
    }
    if (ch === delimiter) {
      row.push(field);
      field = "";
      continue;
    }
    if (ch === "\r") continue;
    if (ch === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    field += ch;
  }
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }
  return rows;
}

/**
 * Empty cell ⇒ absent (the key is omitted, not coerced to `""`/`NaN`) — CSV/TSV has no null literal.
 * A cell that can't be coerced to its declared kind fails loud rather than silently admitting a
 * wrong-typed value (a `NaN` or stray string) into the record.
 */
function coerceCsvValue(raw: string, fieldName: string, kind: FieldKind | undefined): unknown {
  if (raw === "") return undefined;
  if (kind === "number") {
    const value = Number(raw);
    if (Number.isNaN(value)) {
      throw new Error(
        `static-shard: input field "${fieldName}" is declared kind "number" but CSV/TSV cell "${raw}" isn't a valid number`,
      );
    }
    return value;
  }
  if (kind === "boolean") {
    if (raw === "true") return true;
    if (raw === "false") return false;
    throw new Error(
      `static-shard: input field "${fieldName}" is declared kind "boolean" but CSV/TSV cell "${raw}" is neither "true" nor "false"`,
    );
  }
  return raw;
}

function readDelimitedRecords(
  filePath: string,
  delimiter: string,
  fields: Record<string, FieldConfig>,
): Record<string, unknown>[] {
  const rows = parseDelimitedRows(readFileSync(filePath, "utf8"), delimiter);
  if (rows.length === 0) return [];
  const header = rows[0]!;
  return rows.slice(1).map((cells) => {
    const record: Record<string, unknown> = {};
    header.forEach((name, idx) => {
      const value = coerceCsvValue(cells[idx] ?? "", name, fields[name]?.kind);
      if (value !== undefined) record[name] = value;
    });
    return record;
  });
}

/**
 * Reads and merges records from a single path or glob pattern, per the configured
 * input format and record selector (T9). Same-format files matched by a glob are
 * concatenated in filename order as one dataset.
 */
export function readInputRecords(inputPathOrGlob: string, opts: InputReadOptions): Record<string, unknown>[] {
  const files = expandInputFiles(inputPathOrGlob);
  if (files.length === 0) {
    throw new Error(`static-shard: no input files matched "${inputPathOrGlob}"`);
  }

  const records: Record<string, unknown>[] = [];
  for (const file of files) {
    switch (opts.format) {
      case "ndjson":
        records.push(...readNdjsonRecords(file));
        break;
      case "json":
        records.push(...readJsonRecords(file, opts.recordsPath));
        break;
      case "csv":
      case "tsv":
        records.push(...readDelimitedRecords(file, opts.delimiter, opts.fields));
        break;
    }
  }
  return records;
}
