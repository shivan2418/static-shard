#!/usr/bin/env node

/**
 * static-shard CLI
 */

import { Command } from "commander";
import { build } from "./commands/build.js";
import { inspect } from "./commands/inspect.js";
import { types } from "./commands/types.js";

const program = new Command();

program
  .name("static-shard")
  .description("Query large static datasets efficiently by splitting them into chunks")
  .version("0.1.0");

program
  .command("build")
  .description("Build chunks and client from a data file")
  .argument("<input>", "Input data file (JSON, NDJSON, or CSV)")
  .requiredOption("-o, --output <dir>", "Output directory")
  .option("-s, --chunk-size <size>", "Target chunk size (e.g., 5mb)", "5mb")
  .option("-c, --chunk-by <field>", "Field to sort and chunk by")
  .option("-i, --index <fields>", "Comma-separated fields to index")
  .option("-f, --format <format>", "Input format (json, ndjson, csv)")
  .action(async (input, options) => {
    try {
      await build(input, {
        output: options.output,
        chunkSize: options.chunkSize,
        chunkBy: options.chunkBy,
        index: options.index,
        format: options.format,
      });
    } catch (error) {
      console.error("Error:", (error as Error).message);
      process.exit(1);
    }
  });

program
  .command("inspect")
  .description("Analyze a data file and suggest chunking strategy")
  .argument("<input>", "Input data file")
  .option("-n, --sample <count>", "Number of records to sample", "1000")
  .option("-f, --format <format>", "Input format (json, ndjson, csv)")
  .option("--fast", "Fast mode: estimate record count instead of reading entire file")
  .action(async (input, options) => {
    try {
      await inspect(input, {
        sample: parseInt(options.sample, 10),
        format: options.format,
        fast: options.fast,
      });
    } catch (error) {
      console.error("Error:", (error as Error).message);
      process.exit(1);
    }
  });

program
  .command("types")
  .description("Generate TypeScript types from a data file")
  .argument("<input>", "Input data file (JSON, NDJSON, or CSV)")
  .option("-n, --sample <count>", "Number of records to sample", "1000")
  .option("-f, --format <format>", "Input format (json, ndjson, csv)")
  .option("-o, --output <file>", "Output file (default: stdout)")
  .action(async (input, options) => {
    try {
      await types(input, {
        sample: parseInt(options.sample, 10),
        format: options.format,
        output: options.output,
      });
    } catch (error) {
      console.error("Error:", (error as Error).message);
      process.exit(1);
    }
  });

program.parse();
