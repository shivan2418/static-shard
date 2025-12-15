/**
 * Inspect command - analyze data file and guide through build setup
 * Interactive wizard for selecting indexes and build options
 */

import * as fs from "node:fs";
import { checkbox, confirm, input, select } from "@inquirer/prompts";
import type { InspectOptions, FieldSchema } from "../../types/index.js";
import { getFileSize, parseFile, streamFile } from "../utils/parsers.js";
import { inferSchema, suggestChunkField, getIndexableFields } from "../utils/schema.js";
import { build } from "./build.js";

// Use streaming for files larger than 100MB
const STREAMING_THRESHOLD = 100 * 1024 * 1024;

// Threshold for prompting index selection
const INDEX_PROMPT_THRESHOLD = 5;

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

  // Show schema summary
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

  // Size estimates
  const avgRecordSize = fileSize / totalRecords;
  const targetChunkSize = 5 * 1024 * 1024; // 5MB
  const estimatedChunks = Math.ceil(fileSize / targetChunkSize);

  console.log("\n" + "=".repeat(60));
  console.log("SIZE ESTIMATES");
  console.log("=".repeat(60));

  console.log(`\nAverage record size: ${formatBytes(avgRecordSize)}`);
  console.log(`With default 5MB chunks: ~${estimatedChunks} chunks`);

  // Interactive wizard
  console.log("\n" + "=".repeat(60));
  console.log("BUILD CONFIGURATION");
  console.log("=".repeat(60));

  // 1. Chunk field selection
  const suggestedChunkField = suggestChunkField(schema, sampleRecords);
  const chunkableFields = schema.fields
    .filter(f => f.type === "number" || f.type === "string" || f.type === "date")
    .map(f => f.name);

  let selectedChunkField: string | null = null;

  if (chunkableFields.length > 0) {
    const chunkFieldChoices = [
      { name: "(none - chunk by record order)", value: "" },
      ...chunkableFields.map(f => ({
        name: f === suggestedChunkField ? `${f} (recommended)` : f,
        value: f
      }))
    ];

    // Sort to put recommended first
    chunkFieldChoices.sort((a, b) => {
      if (a.value === suggestedChunkField) return -1;
      if (b.value === suggestedChunkField) return 1;
      if (a.value === "") return 1;
      if (b.value === "") return -1;
      return 0;
    });

    selectedChunkField = await select({
      message: "Sort and chunk records by which field?",
      choices: chunkFieldChoices,
      default: suggestedChunkField || ""
    });

    if (selectedChunkField === "") {
      selectedChunkField = null;
    }
  }

  // 2. Index selection
  const indexableFields = getIndexableFields(schema);
  let selectedIndexes: string[] = [];

  if (indexableFields.length > 0) {
    if (indexableFields.length <= INDEX_PROMPT_THRESHOLD) {
      // Few fields - show simple yes/no for using recommended indexes
      const useRecommended = await confirm({
        message: `Use recommended indexes? (${indexableFields.join(", ")})`,
        default: true
      });

      if (useRecommended) {
        selectedIndexes = indexableFields;
      } else {
        // Let them pick manually
        selectedIndexes = await promptIndexSelection(schema.fields, indexableFields);
      }
    } else {
      // Many fields - always show selection UI
      console.log(`\nFound ${indexableFields.length} indexable fields.`);
      selectedIndexes = await promptIndexSelection(schema.fields, indexableFields);
    }
  }

  // 3. Output directory
  const outputDir = await input({
    message: "Output directory:",
    default: "./output"
  });

  // 4. Chunk size
  const chunkSizeAnswer = await select({
    message: "Target chunk size:",
    choices: [
      { name: "1 MB (more chunks, faster queries)", value: "1mb" },
      { name: "5 MB (balanced) - recommended", value: "5mb" },
      { name: "10 MB (fewer chunks, larger downloads)", value: "10mb" },
      { name: "Custom", value: "custom" }
    ],
    default: "5mb"
  });

  let chunkSize = chunkSizeAnswer;
  if (chunkSizeAnswer === "custom") {
    chunkSize = await input({
      message: "Enter chunk size (e.g., 2mb, 500kb):",
      default: "5mb"
    });
  }

  // Summary
  console.log("\n" + "=".repeat(60));
  console.log("CONFIGURATION SUMMARY");
  console.log("=".repeat(60));

  console.log(`\nInput: ${inputFile}`);
  console.log(`Output: ${outputDir}`);
  console.log(`Chunk size: ${chunkSize}`);
  console.log(`Chunk by: ${selectedChunkField || "(record order)"}`);
  console.log(`Indexes: ${selectedIndexes.length > 0 ? selectedIndexes.join(", ") : "(none)"}`);

  // Build command for reference
  let cmd = `npx static-shard build "${inputFile}" -o "${outputDir}" -s ${chunkSize}`;
  if (selectedChunkField) {
    cmd += ` -c ${selectedChunkField}`;
  }
  if (selectedIndexes.length > 0) {
    cmd += ` -i "${selectedIndexes.join(",")}"`;
  }

  console.log(`\nEquivalent command:\n  ${cmd}`);

  // 5. Run build?
  const runBuild = await confirm({
    message: "Run build now?",
    default: true
  });

  if (runBuild) {
    console.log("\n");
    await build(inputFile, {
      output: outputDir,
      chunkSize,
      chunkBy: selectedChunkField || undefined,
      index: selectedIndexes.length > 0 ? selectedIndexes.join(",") : undefined,
      format: options.format
    });
  } else {
    console.log("\nBuild skipped. Run the command above when ready.\n");
  }
}

/**
 * Interactive prompt for selecting which fields to index
 */
async function promptIndexSelection(
  allFields: FieldSchema[],
  recommendedFields: string[]
): Promise<string[]> {
  // Group fields by category for better UX
  const booleanFields = allFields.filter(f => f.type === "boolean").map(f => f.name);
  const enumLikeFields = allFields.filter(f =>
    f.type === "string" &&
    f.stats.cardinality <= 100 &&
    !booleanFields.includes(f.name)
  ).map(f => f.name);
  const numericFields = allFields.filter(f => f.type === "number").map(f => f.name);

  const choices = allFields
    .filter(f => {
      // Only show fields that could reasonably be indexed
      // Must have cardinality >= 2 (cardinality 1 = all same value, useless for filtering)
      if (f.stats.cardinality < 2) return false;
      if (f.type === "boolean") return true;
      if (f.stats.cardinality <= 500) return true;
      return false;
    })
    .map(f => {
      const isRecommended = recommendedFields.includes(f.name);
      const cardinalityInfo = f.stats.cardinality ? ` (${f.stats.cardinality} values)` : "";

      return {
        name: `${f.name}${cardinalityInfo}${isRecommended ? " *" : ""}`,
        value: f.name,
        checked: isRecommended
      };
    })
    .sort((a, b) => {
      // Sort: recommended first, then by cardinality
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
    message: "Select fields to index (or none):",
    choices,
    pageSize: 15,
    instructions: false // We provide our own instructions above
  });

  return selected;
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
