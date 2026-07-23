import { readFileSync } from "node:fs";
import path from "node:path";
import type { ResolvedConfig, StaticShardConfig } from "./types.js";

const DEFAULT_OUTPUT = "public/shard-data";
const DEFAULT_CLIENT_OUT = "src/shard-db";
const DEFAULT_SHARD_BYTES = 2_097_152; // 2 MiB
const DEFAULT_INDEX_CHUNK_BYTES = 45_000; // ~45 KB gzipped anchor (ADR-0003 §5)

function defaultBasePath(output: string): string {
  const normalized = output.replace(/\\/g, "/").replace(/^\/+/, "");
  return normalized.startsWith("public/") ? `/${normalized.slice("public/".length)}` : `/${normalized}`;
}

export function resolveConfig(config: StaticShardConfig, baseDir: string): ResolvedConfig {
  const format = config.input.format;
  if (format !== undefined && format !== "ndjson") {
    throw new Error(`static-shard: unsupported input format "${format}" — T2 supports "ndjson" only`);
  }

  const sortField = config.schema.sortField;
  const sortFieldConfig = config.schema.fields[sortField];
  if (!sortFieldConfig) {
    throw new Error(`static-shard: config.schema.sortField "${sortField}" is not declared in config.schema.fields`);
  }
  if (sortFieldConfig.kind !== "number" && sortFieldConfig.kind !== "date") {
    throw new Error(
      `static-shard: sortField "${sortField}" must be "number" or "date", got "${sortFieldConfig.kind}"`,
    );
  }

  const output = config.output ?? DEFAULT_OUTPUT;

  return {
    collection: config.collection,
    inputPath: path.resolve(baseDir, config.input.path),
    output: path.resolve(baseDir, output),
    clientOut: path.resolve(baseDir, config.clientOut ?? DEFAULT_CLIENT_OUT),
    basePath: config.basePath ?? defaultBasePath(output),
    shardBytes: config.shardBytes ?? DEFAULT_SHARD_BYTES,
    indexChunkBytes: config.indexChunkBytes ?? DEFAULT_INDEX_CHUNK_BYTES,
    sortField,
    fields: config.schema.fields,
  };
}

export function loadConfigFile(configPath: string): StaticShardConfig {
  return JSON.parse(readFileSync(configPath, "utf8")) as StaticShardConfig;
}
