/**
 * Inspect command - analyze data file and suggest chunking strategy
 * Supports streaming for large files
 */

import * as fs from "node:fs";
import type { InspectOptions } from "../../types/index.js";
import { getFileSize, parseFile, streamFile } from "../utils/parsers.js";
import { inferSchema, suggestChunkField, getIndexableFields } from "../utils/schema.js";

// Use streaming for files larger than 100MB
const STREAMING_THRESHOLD = 100 * 1024 * 1024;

export async function inspect(
  inputFile: string,
  options: InspectOptions
): Promise<void> {
  // Validate input file exists
  if (!fs.existsSync(inputFile)) {
    throw new Error(`Input file not found: ${inputFile}`);
  }

  const fileSize = getFileSize(inputFile);
  console.log(`\nFile: ${inputFile}`);
  console.log(`Size: ${formatBytes(fileSize)}`);

  let totalRecords: number;
  let sampleRecords: unknown[];
  let format: string;
  let isEstimated = false;

  // Use streaming for large files
  if (fileSize > STREAMING_THRESHOLD) {
    console.log("\nUsing streaming mode for large file...");

    const sampleSize = options.sample || 1000;

    // Fast mode: only sample, don't read entire file
    if (options.fast) {
      console.log("Fast mode: sampling records (count will be estimated)...");

      const result = await streamFile(
        inputFile,
        options.format,
        () => {}, // No-op
        sampleSize,
        { sampleOnly: true, sampleSize }
      );

      totalRecords = result.estimatedCount || result.count;
      sampleRecords = result.sample;
      format = result.format;
      isEstimated = true;

      console.log(`Format: ${format}`);
      console.log(`Estimated records: ~${totalRecords.toLocaleString()}`);
    } else {
      console.log("Processing all records (use --fast to estimate count instead)...");

      const result = await streamFile(
        inputFile,
        options.format,
        () => {}, // No-op, we just want the count and sample
        sampleSize,
        { showProgress: true }
      );

      totalRecords = result.count;
      sampleRecords = result.sample;
      format = result.format;

      console.log(`Format: ${format}`);
      console.log(`Total records: ${totalRecords.toLocaleString()}`);
    }
  } else {
    console.log("\nParsing file...");

    // Parse the file
    const result = await parseFile(inputFile, options.format);
    totalRecords = result.records.length;
    format = result.format;

    console.log(`Format: ${format}`);
    console.log(`Total records: ${totalRecords.toLocaleString()}`);

    if (totalRecords === 0) {
      console.log("\nNo records found.");
      return;
    }

    // Sample records for schema inference if needed
    const sampleSize = Math.min(options.sample || 1000, totalRecords);
    sampleRecords = result.records.slice(0, sampleSize);
  }

  if (sampleRecords.length === 0) {
    console.log("\nNo records found.");
    return;
  }

  const sampleSize = sampleRecords.length;
  console.log(`\nAnalyzing ${sampleSize.toLocaleString()} records...`);

  // Infer schema
  const schema = inferSchema(sampleRecords);

  console.log("\n" + "=".repeat(60));
  console.log("SCHEMA");
  console.log("=".repeat(60));

  console.log(`\nFields (${schema.fields.length}):\n`);

  for (const field of schema.fields) {
    const isPrimary = field.name === schema.primaryField ? " [PRIMARY]" : "";
    const isIndexed = field.indexed ? " [INDEXED]" : "";
    const nullable = field.nullable ? " (nullable)" : "";

    console.log(`  ${field.name}: ${field.type}${nullable}${isPrimary}${isIndexed}`);

    // Show stats
    const stats = field.stats;
    const statParts = [];

    if (stats.cardinality !== undefined) {
      const cardinalityPct = ((stats.cardinality / totalRecords) * 100).toFixed(1);
      statParts.push(`cardinality: ${stats.cardinality} (${cardinalityPct}%)`);
    }

    if (stats.min !== undefined && stats.max !== undefined) {
      statParts.push(`range: ${formatValue(stats.min)} - ${formatValue(stats.max)}`);
    }

    if (stats.nullCount > 0) {
      statParts.push(`nulls: ${stats.nullCount}`);
    }

    if (statParts.length > 0) {
      console.log(`    ${statParts.join(", ")}`);
    }

    if (stats.sampleValues && stats.sampleValues.length > 0) {
      const samples = stats.sampleValues.slice(0, 3).map(formatValue).join(", ");
      console.log(`    examples: ${samples}`);
    }
  }

  console.log("\n" + "=".repeat(60));
  console.log("RECOMMENDATIONS");
  console.log("=".repeat(60));

  // Suggest chunk field
  const suggestedChunkField = suggestChunkField(schema, sampleRecords);
  if (suggestedChunkField) {
    console.log(`\nRecommended --chunk-by: ${suggestedChunkField}`);
  } else {
    console.log("\nNo specific chunk field recommended (will chunk by record order)");
  }

  // Suggest indexed fields
  const indexableFields = getIndexableFields(schema);
  if (indexableFields.length > 0) {
    console.log(`Recommended --index: ${indexableFields.join(",")}`);
  } else {
    console.log("No fields recommended for indexing");
  }

  // Estimate chunks
  const avgRecordSize = fileSize / totalRecords;
  const targetChunkSize = 5 * 1024 * 1024; // 5MB
  const estimatedChunks = Math.ceil(fileSize / targetChunkSize);

  console.log("\n" + "=".repeat(60));
  console.log("SIZE ESTIMATES");
  console.log("=".repeat(60));

  console.log(`\nAverage record size: ${formatBytes(avgRecordSize)}`);
  console.log(`\nWith default 5MB chunks:`);
  console.log(`  Estimated chunks: ${estimatedChunks}`);
  console.log(`  Records per chunk: ~${Math.ceil(totalRecords / estimatedChunks).toLocaleString()}`);

  // Show example command
  console.log("\n" + "=".repeat(60));
  console.log("EXAMPLE COMMAND");
  console.log("=".repeat(60));

  let cmd = `npx static-shard build ${inputFile} --output ./dist`;
  if (suggestedChunkField) {
    cmd += ` --chunk-by ${suggestedChunkField}`;
  }
  if (indexableFields.length > 0) {
    cmd += ` --index "${indexableFields.join(",")}"`;
  }

  console.log(`\n${cmd}\n`);
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

function formatValue(value: unknown): string {
  if (value === null) return "null";
  if (value === undefined) return "undefined";
  if (typeof value === "string") {
    if (value.length > 30) {
      return `"${value.slice(0, 27)}..."`;
    }
    return `"${value}"`;
  }
  return String(value);
}
