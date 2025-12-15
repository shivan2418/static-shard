/**
 * Build command - process data file and generate output
 * Supports streaming for large files (2GB+)
 */

import * as fs from "node:fs";
import * as path from "node:path";
import type { BuildConfig, BuildOptions, Manifest, DataRecord, ChunkMeta, Schema } from "../../types/index.js";
import { parseFile, parseSize, streamFile, getFileSize } from "../utils/parsers.js";
import { inferSchema, suggestChunkField } from "../utils/schema.js";
import { chunkRecords, generateChunkMeta, serializeChunk, calculateFieldRanges, type Chunk } from "../utils/chunker.js";
import { buildInvertedIndices } from "../utils/indexer.js";
import { generateClient } from "../utils/codegen.js";

const VERSION = "1.0.0";

// Use streaming for files larger than 100MB
const STREAMING_THRESHOLD = 100 * 1024 * 1024;

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

  const fileSize = getFileSize(inputFile);
  console.log(`Input: ${inputFile} (${formatBytes(fileSize)})`);

  // Use streaming for large files
  if (fileSize > STREAMING_THRESHOLD) {
    console.log("Using streaming mode for large file...");
    return buildStreaming(inputFile, options, startTime);
  }

  return buildInMemory(inputFile, options, startTime);
}

/**
 * Standard in-memory build for smaller files
 */
async function buildInMemory(
  inputFile: string,
  options: BuildOptions,
  startTime: number
): Promise<BuildResult> {
  console.log("Reading file...");

  // Parse input file
  const { records, format } = await parseFile(inputFile, options.format);
  console.log(`Parsed ${records.length.toLocaleString()} records (format: ${format})`);

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

  // Parse indexed fields (limit auto-detected to 10 to prevent memory issues)
  const MAX_AUTO_INDEX_FIELDS = 10;
  let indexedFields: string[];
  if (options.index) {
    indexedFields = options.index.split(",").map((f) => f.trim());
  } else {
    const autoIndexed = schema.fields.filter((f) => f.indexed).map((f) => f.name);
    if (autoIndexed.length > MAX_AUTO_INDEX_FIELDS) {
      console.log(`Warning: ${autoIndexed.length} indexable fields detected, limiting to ${MAX_AUTO_INDEX_FIELDS}`);
      console.log(`Use --index to specify which fields to index`);
      indexedFields = autoIndexed.slice(0, MAX_AUTO_INDEX_FIELDS);
    } else {
      indexedFields = autoIndexed;
    }
  }

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

  // Generate client (use small sample for type inference - 100 is enough)
  console.log("Generating client...");
  const sampleForTypes = records.slice(0, 100);
  const clientCode = generateClient(schema, manifest, sampleForTypes);
  const clientPath = path.join(outputDir, "client.ts");
  await fs.promises.writeFile(clientPath, clientCode);
  console.log(`Wrote client to ${clientPath}`);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`\nBuild completed in ${elapsed}s`);
  console.log(`Output: ${outputDir}`);
  console.log(`  - ${chunks.length} chunks`);
  console.log(`  - ${records.length.toLocaleString()} total records`);
  console.log(`  - manifest.json`);
  console.log(`  - client.ts`);

  return {
    manifest,
    outputDir,
    chunkCount: chunks.length,
    totalRecords: records.length,
  };
}

/**
 * Streaming build for large files - processes records incrementally
 * without loading entire file into memory
 */
async function buildStreaming(
  inputFile: string,
  options: BuildOptions,
  startTime: number
): Promise<BuildResult> {
  // Parse chunk size
  const targetChunkSize = parseSize(options.chunkSize);
  console.log(`Target chunk size: ${formatBytes(targetChunkSize)}`);

  // Create output directory
  const outputDir = path.resolve(options.output);
  const chunksDir = path.join(outputDir, "chunks");
  await fs.promises.mkdir(chunksDir, { recursive: true });

  // Streaming state
  let currentChunk: DataRecord[] = [];
  let currentChunkSize = 0;
  let chunkId = 0;
  const chunkMetas: ChunkMeta[] = [];
  const indices: Record<string, Record<string, string[]>> = {};
  let schema: Schema | null = null;
  let indexedFields: string[] = [];
  let chunkBy: string | null = null;

  // Estimate record size without full serialization (rough heuristic)
  const estimateRecordSize = (record: DataRecord): number => {
    let size = 2; // {}
    for (const key in record) {
      const value = record[key];
      size += key.length + 3; // "key":
      if (value === null) size += 4;
      else if (typeof value === "string") size += value.length + 2;
      else if (typeof value === "number") size += String(value).length;
      else if (typeof value === "boolean") size += value ? 4 : 5;
      else size += 20; // rough estimate for complex values
    }
    return size;
  };

  // Process each record as it streams in
  const processRecord = (record: DataRecord) => {
    currentChunk.push(record);
    currentChunkSize += estimateRecordSize(record);

    // When chunk reaches target size, flush it
    if (currentChunkSize >= targetChunkSize) {
      flushChunk();
    }
  };

  // Flush current chunk to disk
  const flushChunk = () => {
    if (currentChunk.length === 0) return;

    // Sort by chunkBy field if specified
    if (chunkBy) {
      currentChunk.sort((a, b) => {
        const aVal = a[chunkBy];
        const bVal = b[chunkBy];
        if (aVal === bVal) return 0;
        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;
        return aVal < bVal ? -1 : 1;
      });
    }

    const chunk: Chunk = {
      id: String(chunkId),
      records: currentChunk,
      byteSize: currentChunkSize,
    };

    // Write chunk to disk
    const chunkPath = path.join(chunksDir, `${chunkId}.json`);
    fs.writeFileSync(chunkPath, JSON.stringify(currentChunk));

    // Generate chunk metadata
    const fieldRanges = calculateFieldRanges(currentChunk, schema!);
    chunkMetas.push({
      id: String(chunkId),
      path: `chunks/${chunkId}.json`,
      count: currentChunk.length,
      byteSize: currentChunkSize,
      fieldRanges,
    });

    // Update indices
    for (const fieldName of indexedFields) {
      if (!indices[fieldName]) {
        indices[fieldName] = {};
      }
      for (const record of currentChunk) {
        const value = record[fieldName];
        if (value === null || value === undefined) continue;
        const key = String(value);
        if (!indices[fieldName][key]) {
          indices[fieldName][key] = [];
        }
        if (!indices[fieldName][key].includes(String(chunkId))) {
          indices[fieldName][key].push(String(chunkId));
        }
      }
    }

    console.log(`  Wrote chunk ${chunkId} (${currentChunk.length.toLocaleString()} records, ${formatBytes(currentChunkSize)})`);

    // Reset for next chunk
    chunkId++;
    currentChunk = [];
    currentChunkSize = 0;
  };

  // First pass: sample records to infer schema
  console.log("Sampling records for schema inference...");
  const sampleResult = await streamFile(
    inputFile,
    options.format,
    () => {}, // No processing in first pass
    1000,
    { sampleOnly: true, sampleSize: 1000 }
  );

  const format = sampleResult.format;
  const sample = sampleResult.sample;

  if (sample.length === 0) {
    throw new Error("No records found in input file");
  }

  // Infer schema from sample
  console.log("Inferring schema from sample...");
  schema = inferSchema(sample);
  console.log(`Found ${schema.fields.length} fields`);

  if (schema.primaryField) {
    console.log(`Detected primary field: ${schema.primaryField}`);
  }

  // Determine chunk field
  chunkBy = options.chunkBy || suggestChunkField(schema, sample);
  if (chunkBy) {
    console.log(`Chunking by field: ${chunkBy}`);
  }

  // Parse indexed fields (limit auto-detected to 10 to prevent memory issues)
  const MAX_AUTO_INDEX_FIELDS = 10;
  if (options.index) {
    indexedFields = options.index.split(",").map((f) => f.trim());
  } else {
    const autoIndexed = schema.fields.filter((f) => f.indexed).map((f) => f.name);
    if (autoIndexed.length > MAX_AUTO_INDEX_FIELDS) {
      console.log(`Warning: ${autoIndexed.length} indexable fields detected, limiting to ${MAX_AUTO_INDEX_FIELDS}`);
      console.log(`Use --index to specify which fields to index`);
      indexedFields = autoIndexed.slice(0, MAX_AUTO_INDEX_FIELDS);
    } else {
      indexedFields = autoIndexed;
    }
  }

  if (indexedFields.length > 0) {
    console.log(`Indexing fields: ${indexedFields.join(", ")}`);
  }

  // Update schema with indexed fields
  for (const field of schema.fields) {
    field.indexed = indexedFields.includes(field.name);
  }

  // Second pass: process all records
  console.log("Processing all records...");
  const { count } = await streamFile(
    inputFile,
    options.format,
    processRecord,
    1000,
    { showProgress: true }
  );

  // Flush any remaining records
  flushChunk();

  console.log(`\nProcessed ${count.toLocaleString()} records (format: ${format})`);

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
    schema: schema!,
    chunks: chunkMetas,
    indices,
    totalRecords: count,
    config,
  };

  // Write manifest
  const manifestPath = path.join(outputDir, "manifest.json");
  await fs.promises.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`Wrote manifest to ${manifestPath}`);

  // Generate client (use small sample for type inference - 100 is enough)
  console.log("Generating client...");
  const sampleForTypes = sample.slice(0, 100);
  const clientCode = generateClient(schema!, manifest, sampleForTypes);
  const clientPath = path.join(outputDir, "client.ts");
  await fs.promises.writeFile(clientPath, clientCode);
  console.log(`Wrote client to ${clientPath}`);

  const elapsed = ((Date.now() - startTime) / 1000).toFixed(2);
  console.log(`\nBuild completed in ${elapsed}s`);
  console.log(`Output: ${outputDir}`);
  console.log(`  - ${chunkMetas.length} chunks`);
  console.log(`  - ${count.toLocaleString()} total records`);
  console.log(`  - manifest.json`);
  console.log(`  - client.ts`);

  return {
    manifest,
    outputDir,
    chunkCount: chunkMetas.length,
    totalRecords: count,
  };
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}
