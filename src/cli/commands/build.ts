/**
 * Build command - process data file and generate output
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { BuildConfig, BuildOptions, Manifest } from "../../types/index.js";
import { parseFile, parseSize } from "../utils/parsers.js";
import { inferSchema, suggestChunkField } from "../utils/schema.js";
import { chunkRecords, generateChunkMeta, serializeChunk } from "../utils/chunker.js";
import { buildInvertedIndices } from "../utils/indexer.js";
import { generateClient } from "../utils/codegen.js";

const VERSION = "1.0.0";

export interface BuildResult {
  manifest: Manifest;
  outputDir: string;
  chunkCount: number;
  totalRecords: number;
}

export async function build(
  inputFile: string,
  options: BuildOptions
): Promise<BuildResult> {
  const startTime = Date.now();

  // Validate input file exists
  if (!fs.existsSync(inputFile)) {
    throw new Error(`Input file not found: ${inputFile}`);
  }

  console.log(`Reading ${inputFile}...`);

  // Parse input file
  const { records, format } = await parseFile(inputFile, options.format);
  console.log(`Parsed ${records.length} records (format: ${format})`);

  if (records.length === 0) {
    throw new Error("No records found in input file");
  }

  // Infer schema
  console.log("Inferring schema...");
  const schema = inferSchema(records);
  console.log(`Found ${schema.fields.length} fields`);

  if (schema.primaryField) {
    console.log(`Detected primary field: ${schema.primaryField}`);
  }

  // Determine chunk field
  const chunkBy = options.chunkBy || suggestChunkField(schema, records);
  if (chunkBy) {
    console.log(`Chunking by field: ${chunkBy}`);
  }

  // Parse chunk size
  const targetChunkSize = parseSize(options.chunkSize);
  console.log(`Target chunk size: ${formatBytes(targetChunkSize)}`);

  // Parse indexed fields
  const indexedFields = options.index
    ? options.index.split(",").map((f) => f.trim())
    : schema.fields.filter((f) => f.indexed).map((f) => f.name);

  if (indexedFields.length > 0) {
    console.log(`Indexing fields: ${indexedFields.join(", ")}`);
  }

  // Update schema with indexed fields
  for (const field of schema.fields) {
    field.indexed = indexedFields.includes(field.name);
  }

  // Chunk the data
  console.log("Chunking data...");
  const chunks = chunkRecords(records, schema, {
    targetSize: targetChunkSize,
    chunkBy,
  });
  console.log(`Created ${chunks.length} chunks`);

  // Build indices
  console.log("Building indices...");
  const indices = buildInvertedIndices(chunks, schema, indexedFields);
  console.log(`Built indices for ${Object.keys(indices).length} fields`);

  // Create output directory
  const outputDir = path.resolve(options.output);
  const chunksDir = path.join(outputDir, "chunks");
  await fs.promises.mkdir(chunksDir, { recursive: true });

  // Write chunks
  console.log("Writing chunks...");
  const chunkMetas = [];
  for (const chunk of chunks) {
    const chunkPath = path.join(chunksDir, `${chunk.id}.json`);
    await fs.promises.writeFile(chunkPath, serializeChunk(chunk));
    chunkMetas.push(generateChunkMeta(chunk, schema, "chunks"));
  }

  // Build config
  const config: BuildConfig = {
    chunkSize: targetChunkSize,
    chunkBy: chunkBy || null,
    indexedFields,
  };

  // Build manifest
  const manifest: Manifest = {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    schema,
    chunks: chunkMetas,
    indices,
    totalRecords: records.length,
    config,
  };

  // Write manifest
  const manifestPath = path.join(outputDir, "manifest.json");
  await fs.promises.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`Wrote manifest to ${manifestPath}`);

  // Generate client
  console.log("Generating client...");
  const clientCode = generateClient(schema, manifest);
  const clientPath = path.join(outputDir, "client.ts");
  await fs.promises.writeFile(clientPath, clientCode);
  console.log(`Wrote client to ${clientPath}`);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`\nBuild completed in ${elapsed}s`);
  console.log(`Output: ${outputDir}`);
  console.log(`  - ${chunks.length} chunks`);
  console.log(`  - ${records.length} total records`);
  console.log(`  - manifest.json`);
  console.log(`  - client.ts`);

  return {
    manifest,
    outputDir,
    chunkCount: chunks.length,
    totalRecords: records.length,
  };
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}
