import type { FieldKind, PairZonemapEntry } from "./types.js";

const DEFAULT_TRUNCATE_LEN = 12;

/** Parquet-style lower truncation: a proper prefix always compares ≤ the true value. */
export function truncateStringLower(value: string, maxLen = DEFAULT_TRUNCATE_LEN): string {
  return value.length <= maxLen ? value : value.slice(0, maxLen);
}

/** Increments the last code point of `prefix` so the result strictly exceeds every string sharing it. */
function incrementString(prefix: string): string {
  const chars = [...prefix];
  for (let i = chars.length - 1; i >= 0; i--) {
    const code = chars[i]!.codePointAt(0)!;
    if (code < 0x10ffff) {
      chars[i] = String.fromCodePoint(code + 1);
      return chars.slice(0, i + 1).join("");
    }
  }
  return prefix + "￿";
}

/** Parquet-style "next string after" upper truncation — a strict upper bound a few bytes long. */
export function truncateStringUpper(value: string, maxLen = DEFAULT_TRUNCATE_LEN): string {
  return value.length <= maxLen ? value : incrementString(value.slice(0, maxLen));
}

function canonicalKey(value: unknown, kind: FieldKind): string {
  if (kind === "number") return String(value as number);
  if (kind === "boolean") return String(value as boolean);
  return value as string; // string | date — dates are already ISO strings
}

function compareByKind(a: unknown, b: unknown, kind: FieldKind): number {
  if (kind === "number") return (a as number) - (b as number);
  if (kind === "boolean") return a === b ? 0 : a ? 1 : -1;
  return (a as string) < (b as string) ? -1 : (a as string) > (b as string) ? 1 : 0;
}

/** Per-shard [min,max] over `field`, ordinal-aligned with `groups` (ADR-0002 §7 / ADR-0003 §2, §9). */
export function computeSecondaryZonemap(
  groups: Record<string, unknown>[][],
  field: string,
  kind: FieldKind,
): PairZonemapEntry {
  const pairs: [unknown, unknown][] = groups.map((group) => {
    let min: unknown;
    let max: unknown;
    for (const record of group) {
      const value = record[field];
      if (value === null || value === undefined) continue;
      if (min === undefined || compareByKind(value, min, kind) < 0) min = value;
      if (max === undefined || compareByKind(value, max, kind) > 0) max = value;
    }
    if (kind === "string") {
      return [truncateStringLower(min as string), truncateStringUpper(max as string)];
    }
    return [min, max];
  });

  return kind === "string" ? { pairs, truncated: true } : { pairs };
}

export interface IndexChunkEntry {
  /** Chars shared with the PREVIOUS entry's canonical key; 0 for a chunk's first entry (front-coding, ADR-0003 §4). */
  prefixLen: number;
  suffix: string;
  /** Delta-encoded ascending shard ordinals — cumulative-sum from 0 reconstructs them (ADR-0003 §5). */
  postings: number[];
}

export interface IndexChunkFile {
  entries: IndexChunkEntry[];
}

export interface BuiltIndexChunk {
  from: unknown;
  to: unknown;
  content: string;
}

interface DictEntry {
  value: unknown;
  key: string;
  shardIndices: number[];
}

function collectDistinctValues(groups: Record<string, unknown>[][], field: string, kind: FieldKind): DictEntry[] {
  const byKey = new Map<string, DictEntry>();
  groups.forEach((group, shardIndex) => {
    for (const record of group) {
      const value = record[field];
      if (value === null || value === undefined) continue;
      const key = canonicalKey(value, kind);
      let entry = byKey.get(key);
      if (!entry) {
        entry = { value, key, shardIndices: [] };
        byKey.set(key, entry);
      }
      if (entry.shardIndices[entry.shardIndices.length - 1] !== shardIndex) entry.shardIndices.push(shardIndex);
    }
  });
  return [...byKey.values()].sort((a, b) => compareByKind(a.value, b.value, kind));
}

function encodeEntry(entry: DictEntry, prevKey: string): IndexChunkEntry {
  const maxShared = Math.min(prevKey.length, entry.key.length);
  let prefixLen = 0;
  while (prefixLen < maxShared && prevKey[prefixLen] === entry.key[prefixLen]) prefixLen++;

  const postings: number[] = [];
  let prev = 0;
  for (const shardIndex of entry.shardIndices) {
    postings.push(shardIndex - prev);
    prev = shardIndex;
  }

  return { prefixLen, suffix: entry.key.slice(prefixLen), postings };
}

/**
 * Builds the chunked inverted index for one non-sort indexed field (ADR-0003):
 * distinct values sorted, front-coded within each chunk (so a chunk decodes
 * standalone), delta-encoded postings (value → shard ordinals), cut into
 * ~chunkBytes-sized groups.
 */
export function buildInvertedIndex(
  groups: Record<string, unknown>[][],
  field: string,
  kind: FieldKind,
  chunkBytes: number,
): BuiltIndexChunk[] {
  const distinct = collectDistinctValues(groups, field, kind);
  if (distinct.length === 0) return [];

  const chunks: BuiltIndexChunk[] = [];
  let currentEntries: IndexChunkEntry[] = [];
  let currentBytes = 0;
  let chunkFirstValue: unknown;

  const flush = (lastValue: unknown): void => {
    if (currentEntries.length === 0) return;
    chunks.push({
      from: chunkFirstValue,
      to: lastValue,
      content: JSON.stringify({ entries: currentEntries } satisfies IndexChunkFile),
    });
    currentEntries = [];
    currentBytes = 0;
  };

  for (let i = 0; i < distinct.length; i++) {
    const dictEntry = distinct[i]!;
    const isChunkStart = currentEntries.length === 0;
    const prevKey = isChunkStart ? "" : distinct[i - 1]!.key;
    const encoded = encodeEntry(dictEntry, prevKey);
    const entryBytes = JSON.stringify(encoded).length + 1;

    if (!isChunkStart && currentBytes + entryBytes > chunkBytes) {
      flush(distinct[i - 1]!.value);
      const restart = encodeEntry(dictEntry, "");
      currentEntries.push(restart);
      currentBytes = JSON.stringify(restart).length + 1;
      chunkFirstValue = dictEntry.value;
    } else {
      if (isChunkStart) chunkFirstValue = dictEntry.value;
      currentEntries.push(encoded);
      currentBytes += entryBytes;
    }
  }
  flush(distinct[distinct.length - 1]!.value);

  return chunks;
}
