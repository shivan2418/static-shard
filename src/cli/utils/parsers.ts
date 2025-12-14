/**
 * Data file parsers for JSON, NDJSON, and CSV formats
 */

import * as fs from "node:fs";
import * as readline from "node:readline";
import Papa from "papaparse";
import type { DataFormat, DataRecord } from "../../types/index.js";

/**
 * Detect file format from extension or content
 */
export function detectFormat(filePath: string): DataFormat {
  const ext = filePath.toLowerCase().split(".").pop();

  if (ext === "csv") return "csv";
  if (ext === "ndjson" || ext === "jsonl") return "ndjson";
  if (ext === "json") {
    // Peek at file to distinguish JSON array from NDJSON
    const fd = fs.openSync(filePath, "r");
    const buffer = Buffer.alloc(1024);
    fs.readSync(fd, buffer, 0, 1024, 0);
    fs.closeSync(fd);

    const content = buffer.toString("utf-8").trim();
    if (content.startsWith("[")) return "json";
    if (content.startsWith("{")) return "ndjson";
  }

  return "json"; // default
}

/**
 * Parse a JSON array file
 * For large files, reads entirely into memory (JSON parsing requires this)
 */
export async function parseJsonArray(filePath: string): Promise<DataRecord[]> {
  const content = await fs.promises.readFile(filePath, "utf-8");
  const data = JSON.parse(content);

  if (!Array.isArray(data)) {
    throw new Error("JSON file must contain an array of objects");
  }

  return data as DataRecord[];
}

/**
 * Parse an NDJSON file (newline-delimited JSON)
 * Streams line-by-line for memory efficiency
 */
export async function parseNdjson(
  filePath: string,
  onRecord?: (record: DataRecord, index: number) => void
): Promise<DataRecord[]> {
  const records: DataRecord[] = [];
  let index = 0;

  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    try {
      const record = JSON.parse(trimmed) as DataRecord;
      records.push(record);
      onRecord?.(record, index);
      index++;
    } catch (e) {
      throw new Error(`Invalid JSON at line ${index + 1}: ${trimmed.slice(0, 50)}...`);
    }
  }

  return records;
}

/**
 * Parse a CSV file
 * Uses papaparse for robust CSV handling
 */
export async function parseCsv(
  filePath: string,
  onRecord?: (record: DataRecord, index: number) => void
): Promise<DataRecord[]> {
  return new Promise((resolve, reject) => {
    const records: DataRecord[] = [];
    let index = 0;

    const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });

    Papa.parse(fileStream, {
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      step: (result) => {
        if (result.errors.length > 0) {
          reject(new Error(`CSV parse error: ${result.errors[0].message}`));
          return;
        }
        const record = result.data as DataRecord;
        records.push(record);
        onRecord?.(record, index);
        index++;
      },
      complete: () => resolve(records),
      error: (error) => reject(error),
    });
  });
}

/**
 * Parse a data file in any supported format
 */
export async function parseFile(
  filePath: string,
  format?: DataFormat,
  onRecord?: (record: DataRecord, index: number) => void
): Promise<{ records: DataRecord[]; format: DataFormat }> {
  const detectedFormat = format || detectFormat(filePath);

  let records: DataRecord[];

  switch (detectedFormat) {
    case "json":
      records = await parseJsonArray(filePath);
      // Call onRecord for each if provided
      if (onRecord) {
        records.forEach((r, i) => onRecord(r, i));
      }
      break;
    case "ndjson":
      records = await parseNdjson(filePath, onRecord);
      break;
    case "csv":
      records = await parseCsv(filePath, onRecord);
      break;
    default:
      throw new Error(`Unsupported format: ${detectedFormat}`);
  }

  return { records, format: detectedFormat };
}

/**
 * Get file size in bytes
 */
export function getFileSize(filePath: string): number {
  const stats = fs.statSync(filePath);
  return stats.size;
}

/**
 * Parse size string (e.g., "5mb", "1gb") to bytes
 */
export function parseSize(sizeStr: string): number {
  const match = sizeStr.toLowerCase().match(/^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb)?$/);
  if (!match) {
    throw new Error(`Invalid size format: ${sizeStr}. Use format like "5mb" or "1gb"`);
  }

  const value = parseFloat(match[1]);
  const unit = match[2] || "b";

  const multipliers: Record<string, number> = {
    b: 1,
    kb: 1024,
    mb: 1024 * 1024,
    gb: 1024 * 1024 * 1024,
  };

  return Math.floor(value * multipliers[unit]);
}
