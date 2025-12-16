/**
 * Build command - process data file and generate output
 * Supports streaming for large files (2GB+)
 */

import * as fs from "node:fs";
import * as path from "node:path";
import { checkbox, confirm, select } from "@inquirer/prompts";
import type { BuildConfig, BuildOptions, Manifest, DataRecord, ChunkMeta, Schema, FieldSchema } from "../../types/index.js";
import { parseFile, parseSize, streamFile, getFileSize } from "../utils/parsers.js";
import { inferSchema, suggestChunkField, getIndexableFields } from "../utils/schema.js";
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

  // Parse chunk size
  const targetChunkSize = parseSize(options.chunkSize);
  console.log(`Target chunk size: ${formatBytes(targetChunkSize)}`);

  // Determine chunk field - prompt if not specified
  let chunkBy: string | null;
  if (options.chunkBy !== undefined) {
    chunkBy = options.chunkBy || null;
    if (chunkBy) {
      console.log(`Chunking by field: ${chunkBy}`);
    }
  } else {
    const suggestedField = suggestChunkField(schema, records);
    chunkBy = await promptChunkField(schema, suggestedField, records);
    if (chunkBy) {
      console.log(`Selected chunk field: ${chunkBy}`);
    }
  }

  // Determine indexed fields - prompt if not specified
  let indexedFields: string[];
  if (options.index !== undefined) {
    indexedFields = options.index ? options.index.split(",").map((f) => f.trim()) : [];
    if (indexedFields.length > 0) {
      console.log(`Indexing fields: ${indexedFields.join(", ")}`);
    }
  } else {
    const recommendedFields = getIndexableFields(schema);
    indexedFields = await promptIndexSelection(schema, recommendedFields);
    if (indexedFields.length > 0) {
      console.log(`Selected indexes: ${indexedFields.join(", ")}`);
    }
  }

  // Update schema with indexed fields
  for (const field of schema.fields) {
    field.indexed = indexedFields.includes(field.name);
  }

  // Chunk the data
  console.log("Chunking data...");
  const chunks = chunkRecords(records, schema, {
    targetSize: targetChunkSize,
    chunkBy: chunkBy || undefined,
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

  // Build manifest with stripped schema (remove build-time stats)
  const manifest: Manifest = {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    schema: stripSchemaForManifest(schema),
    chunks: chunkMetas,
    indices,
    totalRecords: records.length,
    config,
  };

  // Write manifest (compact JSON for smaller download)
  const manifestPath = path.join(outputDir, "manifest.json");
  await fs.promises.writeFile(manifestPath, JSON.stringify(manifest));
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
      const sortField = chunkBy;
      currentChunk.sort((a, b) => {
        const aVal = a[sortField];
        const bVal = b[sortField];
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

  // Determine chunk field - prompt if not specified
  if (options.chunkBy !== undefined) {
    chunkBy = options.chunkBy || null;
    if (chunkBy) {
      console.log(`Chunking by field: ${chunkBy}`);
    }
  } else {
    const suggestedField = suggestChunkField(schema, sample);
    chunkBy = await promptChunkField(schema, suggestedField, sample);
    if (chunkBy) {
      console.log(`Selected chunk field: ${chunkBy}`);
    }
  }

  // Determine indexed fields - prompt if not specified
  if (options.index !== undefined) {
    indexedFields = options.index ? options.index.split(",").map((f) => f.trim()) : [];
    if (indexedFields.length > 0) {
      console.log(`Indexing fields: ${indexedFields.join(", ")}`);
    }
  } else {
    const recommendedFields = getIndexableFields(schema);
    indexedFields = await promptIndexSelection(schema, recommendedFields);
    if (indexedFields.length > 0) {
      console.log(`Selected indexes: ${indexedFields.join(", ")}`);
    }
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

  // Build manifest with stripped schema (remove build-time stats)
  const manifest: Manifest = {
    version: VERSION,
    generatedAt: new Date().toISOString(),
    schema: stripSchemaForManifest(schema!),
    chunks: chunkMetas,
    indices,
    totalRecords: count,
    config,
  };

  // Write manifest (compact JSON for smaller download)
  const manifestPath = path.join(outputDir, "manifest.json");
  await fs.promises.writeFile(manifestPath, JSON.stringify(manifest));
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

/**
 * Strip build-time stats from schema for lean manifest
 * Removes sampleValues, cardinality, nullCount - only keeps min/max for range queries
 */
function stripSchemaForManifest(schema: Schema): Schema {
  return {
    ...schema,
    fields: schema.fields.map((field) => ({
      name: field.name,
      type: field.type,
      nullable: field.nullable,
      indexed: field.indexed,
      stats: {
        min: field.stats.min,
        max: field.stats.max,
        cardinality: 0, // placeholder, not needed at runtime
        nullCount: 0,   // placeholder, not needed at runtime
      },
    })),
  };
}

/**
 * Interactive prompt for selecting chunk field
 */
async function promptChunkField(
  schema: Schema,
  suggestedField: string | null,
  records: DataRecord[]
): Promise<string | null> {
  const chunkableFields = schema.fields
    .filter((f) => f.type === "number" || f.type === "string" || f.type === "date")
    .map((f) => f.name);

  if (chunkableFields.length === 0) {
    return null;
  }

  const choices = [
    { name: "(none - chunk by record order)", value: "" },
    ...chunkableFields.map((f) => ({
      name: f === suggestedField ? `${f} (recommended)` : f,
      value: f,
    })),
  ];

  // Sort to put recommended first
  choices.sort((a, b) => {
    if (a.value === suggestedField) return -1;
    if (b.value === suggestedField) return 1;
    if (a.value === "") return 1;
    if (b.value === "") return -1;
    return 0;
  });

  const selected = await select({
    message: "Sort and chunk records by which field?",
    choices,
    default: suggestedField || "",
  });

  return selected === "" ? null : selected;
}

/**
 * Interactive prompt for selecting indexed fields
 */
async function promptIndexSelection(
  schema: Schema,
  recommendedFields: string[]
): Promise<string[]> {
  const choices = schema.fields
    .filter((f) => {
      // Only show fields that could reasonably be indexed
      if (f.stats.cardinality < 2) return false;
      if (f.type === "boolean") return true;
      if (f.stats.cardinality <= 500) return true;
      return false;
    })
    .map((f) => {
      const isRecommended = recommendedFields.includes(f.name);
      const cardinalityInfo = f.stats.cardinality ? ` (${f.stats.cardinality} values)` : "";

      return {
        name: `${f.name}${cardinalityInfo}${isRecommended ? " *" : ""}`,
        value: f.name,
        checked: isRecommended,
      };
    })
    .sort((a, b) => {
      const aRec = recommendedFields.includes(a.value) ? 0 : 1;
      const bRec = recommendedFields.includes(b.value) ? 0 : 1;
      return aRec - bRec;
    });

  if (choices.length === 0) {
    return [];
  }

  console.log("\n  * = recommended for indexing");
  console.log("  Controls: <space> toggle, <a> all, <i> invert, <enter> confirm\n");

  const selected = await checkbox({
    message: "Select fields to index:",
    choices,
    pageSize: 15,
  });

  return selected;
}
