import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import path from "node:path";
import { resolveConfig } from "./config.js";
import { generateClientTs, generateSchemaTs } from "./codegen.js";
import { contentHash } from "./hash.js";
import { buildManifest, computeSplitPoints } from "./manifest.js";
import { buildInvertedIndex, computeSecondaryZonemap } from "./secondary-index.js";
import { cutIntoShards, materializeShards } from "./shard.js";
import { compareSortValues, type SortKind } from "./sort.js";
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
}

function readNdjson(inputPath: string): Record<string, unknown>[] {
  const raw = readFileSync(inputPath, "utf8");
  return raw
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
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

  const records = readNdjson(resolved.inputPath);
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
  const indexChunkFiles: { field: string; hash: string; content: string }[] = [];
  for (const [name, field] of indexedSecondaryFields) {
    secondaryZonemaps[name] = computeSecondaryZonemap(groups, name, field.kind);

    const builtChunks = buildInvertedIndex(groups, name, field.kind, resolved.indexChunkBytes);
    indexChunkDirs[name] = builtChunks.map(({ from, to, content }) => {
      const hash = contentHash(content);
      indexChunkFiles.push({ field: name, hash, content });
      return { from, to, file: `index/${name}/${hash}.json` };
    });
  }

  const manifest = buildManifest({
    config: resolved,
    shardFiles,
    splitPoints,
    secondaryZonemaps,
    indexChunkDirs,
    formatVersion,
    generatorVersion,
  });

  rmSync(resolved.output, { recursive: true, force: true });
  const shardsDir = path.join(resolved.output, "shards");
  mkdirSync(shardsDir, { recursive: true });
  for (const file of shardFiles) {
    writeFileSync(path.join(shardsDir, `${file.hash}.ndjson`), file.content);
  }
  for (const { field, hash, content } of indexChunkFiles) {
    const fieldDir = path.join(resolved.output, "index", field);
    mkdirSync(fieldDir, { recursive: true });
    writeFileSync(path.join(fieldDir, `${hash}.json`), content);
  }
  writeFileSync(path.join(resolved.output, "manifest.json"), JSON.stringify(manifest, null, 2));

  mkdirSync(resolved.clientOut, { recursive: true });
  writeFileSync(path.join(resolved.clientOut, "schema.ts"), generateSchemaTs(manifest, generatorVersion));
  writeFileSync(
    path.join(resolved.clientOut, "client.ts"),
    generateClientTs(manifest, { basePath: resolved.basePath, generatorVersion }),
  );

  return { manifest, outputDir: resolved.output, clientOutDir: resolved.clientOut };
}
