import { gzipSync } from "node:zlib";
import { MANIFEST_BUDGET_BYTES } from "./estimator.js";
import { contentHash } from "./hash.js";
import type { Manifest, PairZonemapEntry, ZonemapEntry } from "./types.js";

export interface ZonemapSpillResult {
  manifest: Manifest;
  /** Sidecar files to write alongside shards/index chunks, content-hashed and referenced from the (now-lighter) root manifest. */
  sidecarFiles: { relPath: string; content: string }[];
}

function manifestGzipBytes(manifest: Manifest): number {
  return gzipSync(JSON.stringify(manifest)).length;
}

function isPairEntry(entry: ZonemapEntry): entry is PairZonemapEntry {
  return "pairs" in entry;
}

/**
 * Spills the largest secondary-field zonemap to a sidecar file, one field at a time, until the
 * root manifest's gzipped size is back under `MANIFEST_BUDGET_BYTES` or every secondary zonemap
 * has been spilled (ADR-0003 §3) — the `O(shards × fields)` root-manifest failure mode. The sort
 * field's own zonemap (split-points) is never spilled: it routes every query and must stay in root.
 */
export function spillOversizedZonemaps(manifest: Manifest): ZonemapSpillResult {
  if (manifestGzipBytes(manifest) <= MANIFEST_BUDGET_BYTES) {
    return { manifest, sidecarFiles: [] };
  }

  let current = manifest;
  const sidecarFiles: { relPath: string; content: string }[] = [];

  for (;;) {
    const candidates = Object.entries(current.zonemap)
      .filter((entry): entry is [string, PairZonemapEntry] => entry[0] !== current.dataset.sortField && isPairEntry(entry[1]))
      .map(([field, entry]) => ({ field, entry, size: JSON.stringify(entry).length }))
      .sort((a, b) => b.size - a.size);

    if (candidates.length === 0) break;

    const { field, entry } = candidates[0]!;
    const content = JSON.stringify(entry);
    const relPath = `zonemap/${field}-${contentHash(content)}.json`;
    sidecarFiles.push({ relPath, content });

    current = { ...current, zonemap: { ...current.zonemap, [field]: { sidecar: relPath } } };

    if (manifestGzipBytes(current) <= MANIFEST_BUDGET_BYTES) break;
  }

  return { manifest: current, sidecarFiles };
}
