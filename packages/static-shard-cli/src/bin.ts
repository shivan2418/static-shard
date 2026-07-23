#!/usr/bin/env node
import path from "node:path";
import { build } from "./build.js";
import { loadConfigFile } from "./config.js";

function main(argv: string[]): void {
  const [command, ...rest] = argv;
  if (command !== "build") {
    console.error(`static-shard: unknown command "${command ?? ""}" — usage: static-shard build [--config <path>]`);
    process.exitCode = 1;
    return;
  }

  let configPath = "static-shard.config.json";
  for (let i = 0; i < rest.length; i++) {
    if (rest[i] === "--config") {
      configPath = rest[i + 1] ?? configPath;
      i++;
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
}

main(process.argv.slice(2));
