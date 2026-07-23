import { mkdirSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { resolveConfig } from "./config.js";
import { generateClientTs, generateSchemaTs } from "./codegen.js";
import { contentHash } from "./hash.js";
import { readInputRecords } from "./input.js";
import { buildManifest, computeSplitPoints } from "./manifest.js";
import {
  buildInvertedIndex,
  buildReversedIndex,
  buildTrigramIndex,
  computeColumnBytes,
  computeSecondaryZonemap,
} from "./secondary-index.js";
import { cutIntoShards, materializeShards } from "./shard.js";
import { compareSortValues, type SortKind } from "./sort.js";
import type { BuiltIndexChunk } from "./secondary-index.js";
import type { IndexChunkDirEntry, Manifest, PairZonemapEntry, StaticShardConfig } from "./types.js";
import { getFormatVersion, getGeneratorVersion } from "./version.js";

export interface BuildOptions {
  /** Directory config-relative paths (input/output/clientOut) are resolved against. */
  baseDir: string;
  generatorVersion?: string;
  formatVersion?: number;
}

export interface BuildResult {
  manifest: Manifest;
  outputDir: string;
  clientOutDir: string;
  /** Loud, non-fatal build-time warnings (e.g. a `contains` trigram index bigger than its column, ADR-0003 §7). */
  warnings: string[];
}

/**
 * The walking skeleton: read config's baked schema → read NDJSON → global sort
 * by the sort field → cut into byte-target shards → write the served data
 * tree (manifest + content-hash-named shards) and the generated client
 * (schema.ts + client.ts) in one pass.
 */
export function build(config: StaticShardConfig, opts: BuildOptions): BuildResult {
  const resolved = resolveConfig(config, opts.baseDir);
  const generatorVersion = opts.generatorVersion ?? getGeneratorVersion();
  const formatVersion = opts.formatVersion ?? getFormatVersion();
  const sortKind = resolved.fields[resolved.sortField]!.kind as SortKind;

  const records = readInputRecords(resolved.inputPath, {
    format: resolved.inputFormat,
    delimiter: resolved.inputDelimiter,
    recordsPath: resolved.inputRecordsPath,
    fields: resolved.fields,
  });
  const sorted = [...records].sort((a, b) =>
    compareSortValues(a[resolved.sortField], b[resolved.sortField], sortKind),
  );

  const groups = cutIntoShards(sorted, resolved.sortField, resolved.shardBytes);
  const shardFiles = materializeShards(groups);
  const splitPoints = computeSplitPoints(groups, resolved.sortField);

  const indexedSecondaryFields = Object.entries(resolved.fields).filter(
    ([name, field]) => name !== resolved.sortField && field.indexed === true,
  );

  const secondaryZonemaps: Record<string, PairZonemapEntry> = {};
  const indexChunkDirs: Record<string, IndexChunkDirEntry[]> = {};
  const reversedChunkDirs: Record<string, IndexChunkDirEntry[]> = {};
  const trigramChunkDirs: Record<string, IndexChunkDirEntry[]> = {};
  const indexFiles: { relPath: string; content: string }[] = [];
  const warnings: string[] = [];

  const addIndexChunks = (field: string, subdir: string | null, builtChunks: BuiltIndexChunk[]): IndexChunkDirEntry[] =>
    builtChunks.map(({ from, to, content }) => {
      const hash = contentHash(content);
      const relPath = subdir ? `index/${field}/${subdir}/${hash}.json` : `index/${field}/${hash}.json`;
      indexFiles.push({ relPath, content });
      return { from, to, file: relPath };
    });

  for (const [name, field] of indexedSecondaryFields) {
    const multi = field.multi === true;
    secondaryZonemaps[name] = computeSecondaryZonemap(groups, name, field.kind, multi);
    indexChunkDirs[name] = addIndexChunks(
      name,
      null,
      buildInvertedIndex(groups, name, field.kind, resolved.indexChunkBytes, multi),
    );

    if (field.endsWith) {
      reversedChunkDirs[name] = addIndexChunks(
        name,
        "reversed",
        buildReversedIndex(groups, name, resolved.indexChunkBytes, multi),
      );
    }

    if (field.contains) {
      const trigramChunks = buildTrigramIndex(groups, name, resolved.indexChunkBytes, multi);
      trigramChunkDirs[name] = addIndexChunks(name, "trigram", trigramChunks);

      const trigramBytes = trigramChunks.reduce((sum, c) => sum + Buffer.byteLength(c.content, "utf8"), 0);
      const columnBytes = computeColumnBytes(groups, name, multi);
      if (trigramBytes > columnBytes) {
        warnings.push(
          `static-shard: contains(${name}): trigram index (${trigramBytes} bytes) is bigger than the data — ` +
            `the raw "${name}" column is only ${columnBytes} bytes. This is the single biggest build-output cost; ` +
            `consider disabling contains for this field.`,
        );
      }
    }
  }

  const manifest = buildManifest({
    config: resolved,
    shardFiles,
    splitPoints,
    secondaryZonemaps,
    indexChunkDirs,
    reversedChunkDirs,
    trigramChunkDirs,
    formatVersion,
    generatorVersion,
  });

  rmSync(resolved.output, { recursive: true, force: true });
  const shardsDir = path.join(resolved.output, "shards");
  mkdirSync(shardsDir, { recursive: true });
  for (const file of shardFiles) {
    writeFileSync(path.join(shardsDir, `${file.hash}.ndjson`), file.content);
  }
  for (const { relPath, content } of indexFiles) {
    const filePath = path.join(resolved.output, relPath);
    mkdirSync(path.dirname(filePath), { recursive: true });
    writeFileSync(filePath, content);
  }
  writeFileSync(path.join(resolved.output, "manifest.json"), JSON.stringify(manifest, null, 2));

  mkdirSync(resolved.clientOut, { recursive: true });
  writeFileSync(path.join(resolved.clientOut, "schema.ts"), generateSchemaTs(manifest, generatorVersion));
  writeFileSync(
    path.join(resolved.clientOut, "client.ts"),
    generateClientTs(manifest, { basePath: resolved.basePath, generatorVersion }),
  );

  return { manifest, outputDir: resolved.output, clientOutDir: resolved.clientOut, warnings };
}
