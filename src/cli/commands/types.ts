/**
 * Types command - quickly generate TypeScript types from a data file
 * Uses json-ts for accurate type inference from JSON samples
 */

import * as fs from "node:fs";
import { json2ts } from "json-ts";
import type { DataRecord } from "../../types/index.js";
import { parseFile, streamFile, getFileSize } from "../utils/parsers.js";

const STREAMING_THRESHOLD = 100 * 1024 * 1024;

export interface TypesOptions {
  sample?: number;
  format?: string;
  output?: string;
}

/**
 * Post-process json-ts output to clean up interface names
 */
function cleanupTypes(types: string): string {
  return types
    // Remove the array type wrapper (we want the item type)
    .replace(/^type IItem = IItemItem\[\];\n/m, "")
    // Rename IItemItem to Item (both references and declarations)
    .replace(/IItemItem/g, "Item")
    // Remove I prefix from all interface names (both references and declarations)
    // This handles IImage_uris -> Image_uris, ILegalities -> Legalities, etc.
    .replace(/\bI([A-Z][a-z_]+)/g, "$1")
    // Export all interfaces
    .replace(/^interface /gm, "export interface ");
}

/**
 * Generate field names union type from samples
 */
function generateFieldNamesType(samples: DataRecord[]): string {
  const fieldNames = new Set<string>();
  for (const record of samples) {
    for (const key of Object.keys(record)) {
      fieldNames.add(key);
    }
  }
  const names = Array.from(fieldNames).sort().map((f) => `"${f}"`).join(" | ");
  return `export type FieldName = ${names};`;
}

export async function types(
  inputFile: string,
  options: TypesOptions
): Promise<void> {
  // Validate input file exists
  if (!fs.existsSync(inputFile)) {
    throw new Error(`Input file not found: ${inputFile}`);
  }

  const fileSize = getFileSize(inputFile);
  const sampleSize = options.sample || 1000;

  console.error(`Analyzing: ${inputFile}`);
  console.error(`Sampling ${sampleSize} records...`);

  let samples: DataRecord[];
  let format: string;

  if (fileSize > STREAMING_THRESHOLD) {
    // Use streaming for large files
    const result = await streamFile(
      inputFile,
      options.format,
      () => {},
      1000,
      { sampleOnly: true, sampleSize }
    );
    samples = result.sample;
    format = result.format;
  } else {
    // Small file - read directly
    const parseResult = await parseFile(inputFile, options.format);
    samples = parseResult.records.slice(0, sampleSize);
    format = parseResult.format;
  }

  console.error(`Format: ${format}`);
  console.error(`Sampled ${samples.length} records\n`);

  // Generate types using json-ts
  const rawTypes = json2ts(JSON.stringify(samples), { rootName: "Item" });
  const types = cleanupTypes(rawTypes);
  const fieldNames = generateFieldNamesType(samples);

  // Generate output
  const output = `/**
 * Auto-generated TypeScript types
 * Source: ${inputFile}
 * Generated: ${new Date().toISOString()}
 */

${types}

${fieldNames}
`;

  if (options.output) {
    await fs.promises.writeFile(options.output, output);
    console.error(`Types written to: ${options.output}`);
  } else {
    // Output to stdout
    console.log(output);
  }
}
