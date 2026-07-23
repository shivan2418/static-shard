#!/usr/bin/env node
import path from "node:path";
import { build } from "./build.js";
import { loadConfigFile } from "./config.js";
import { init } from "./init.js";
import type { InitOptions } from "./init.js";
import type { InputFormat } from "./types.js";

function runBuild(rest: string[]): void {
  let configPath = "static-shard.config.json";
  for (let i = 0; i < rest.length; i++) {
    if (rest[i] === "--config") {
      configPath = rest[++i] ?? configPath;
    }
  }

  const resolvedConfigPath = path.resolve(process.cwd(), configPath);
  const config = loadConfigFile(resolvedConfigPath);
  const result = build(config, { baseDir: path.dirname(resolvedConfigPath) });

  console.log(
    `static-shard: built ${result.manifest.dataset.shardCount} shard(s), ` +
      `${result.manifest.dataset.recordCount} record(s) → ${result.outputDir}`,
  );
  console.log(`static-shard: generated client → ${result.clientOutDir}`);
  for (const warning of result.warnings) console.warn(warning);
}

function parseInitArgs(rest: string[]): { configPath: string; options: Omit<InitOptions, "cwd" | "configPath"> } {
  let configPath = "static-shard.config.json";
  let inputPath: string | undefined;
  let format: InputFormat | undefined;
  let delimiter: string | undefined;
  let records: string | undefined;
  let collection: string | undefined;
  let sortField: string | undefined;
  let pk: string | undefined;
  let indexedFields: string[] | undefined;
  let endsWithFields: string[] | undefined;
  let containsFields: string[] | undefined;
  let fullScan = false;
  let sampleSize: number | undefined;
  let reinfer = false;
  let yes = false;
  let output: string | undefined;
  let clientOut: string | undefined;
  let basePath: string | undefined;
  let shardBytes: number | undefined;
  let indexChunkBytes: number | undefined;

  for (let i = 0; i < rest.length; i++) {
    const arg = rest[i];
    switch (arg) {
      case "--config":
        configPath = rest[++i] ?? configPath;
        break;
      case "--format":
        format = rest[++i] as InputFormat;
        break;
      case "--delimiter":
        delimiter = rest[++i];
        break;
      case "--records":
        records = rest[++i];
        break;
      case "--collection":
        collection = rest[++i];
        break;
      case "--sort-field":
        sortField = rest[++i];
        break;
      case "--pk":
        pk = rest[++i];
        break;
      case "--indexed":
        indexedFields = (rest[++i] ?? "").split(",").filter((s) => s.length > 0);
        break;
      case "--ends-with":
        endsWithFields = (rest[++i] ?? "").split(",").filter((s) => s.length > 0);
        break;
      case "--contains":
        containsFields = (rest[++i] ?? "").split(",").filter((s) => s.length > 0);
        break;
      case "--full-scan":
        fullScan = true;
        break;
      case "--sample-size":
        sampleSize = Number(rest[++i]);
        break;
      case "--reinfer":
        reinfer = true;
        break;
      case "--yes":
        yes = true;
        break;
      case "--output":
        output = rest[++i];
        break;
      case "--client-out":
        clientOut = rest[++i];
        break;
      case "--base-path":
        basePath = rest[++i];
        break;
      case "--shard-bytes":
        shardBytes = Number(rest[++i]);
        break;
      case "--index-chunk-bytes":
        indexChunkBytes = Number(rest[++i]);
        break;
      default:
        if (arg !== undefined && !arg.startsWith("--")) inputPath = arg;
    }
  }

  return {
    configPath,
    options: {
      yes,
      reinfer,
      fullScan,
      sampleSize,
      collection,
      inputPath,
      format,
      delimiter,
      records,
      sortField,
      pk,
      indexedFields,
      endsWithFields,
      containsFields,
      output,
      clientOut,
      basePath,
      shardBytes,
      indexChunkBytes,
    },
  };
}

function runInit(rest: string[]): void {
  const { configPath, options } = parseInitArgs(rest);
  const resolvedConfigPath = path.resolve(process.cwd(), configPath);
  const result = init({ cwd: process.cwd(), configPath: resolvedConfigPath, ...options });

  console.log(
    `static-shard: wrote ${result.configPath}` +
      (result.reinferred ? " (schema inferred)" : " (schema unchanged — pass --reinfer to refresh)"),
  );
}

function main(argv: string[]): void {
  const [command, ...rest] = argv;
  if (command === "build") {
    runBuild(rest);
    return;
  }
  if (command === "init") {
    runInit(rest);
    return;
  }

  console.error(`static-shard: unknown command "${command ?? ""}" — usage: static-shard <init|build> [--config <path>]`);
  process.exitCode = 1;
}

main(process.argv.slice(2));
