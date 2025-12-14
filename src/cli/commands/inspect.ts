/**
 * Inspect command - analyze data file and suggest chunking strategy
 */

import * as fs from "node:fs";
import type { InspectOptions } from "../../types/index.js";
import { getFileSize, parseFile } from "../utils/parsers.js";
import { inferSchema, suggestChunkField, getIndexableFields } from "../utils/schema.js";

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

  console.log("\nParsing file...");

  // Parse the file
  const { records, format } = await parseFile(inputFile, options.format);

  console.log(`Format: ${format}`);
  console.log(`Total records: ${records.length}`);

  if (records.length === 0) {
    console.log("\nNo records found.");
    return;
  }

  // Sample records for schema inference if needed
  const sampleSize = Math.min(options.sample || 1000, records.length);
  const sampleRecords = records.slice(0, sampleSize);

  console.log(`\nAnalyzing ${sampleSize} records...`);

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
      const cardinalityPct = ((stats.cardinality / records.length) * 100).toFixed(1);
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
  const suggestedChunkField = suggestChunkField(schema, records);
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
  const avgRecordSize = fileSize / records.length;
  const targetChunkSize = 5 * 1024 * 1024; // 5MB
  const estimatedChunks = Math.ceil(fileSize / targetChunkSize);

  console.log("\n" + "=".repeat(60));
  console.log("SIZE ESTIMATES");
  console.log("=".repeat(60));

  console.log(`\nAverage record size: ${formatBytes(avgRecordSize)}`);
  console.log(`\nWith default 5MB chunks:`);
  console.log(`  Estimated chunks: ${estimatedChunks}`);
  console.log(`  Records per chunk: ~${Math.ceil(records.length / estimatedChunks)}`);

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
