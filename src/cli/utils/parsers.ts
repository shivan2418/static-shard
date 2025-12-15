/**
 * Data file parsers for JSON, NDJSON, and CSV formats
 * Supports streaming for large files (2GB+)
 */

import * as fs from "node:fs";
import * as readline from "node:readline";
import Papa from "papaparse";
import streamJson from "stream-json";
import StreamArrayModule from "stream-json/streamers/StreamArray.js";
import streamChain from "stream-chain";
import cliProgress from "cli-progress";
import type { DataFormat, DataRecord } from "../../types/index.js";

const { parser: jsonParser } = streamJson;
const { chain } = streamChain;
const { streamArray } = StreamArrayModule;

// Threshold for using streaming parser (100MB)
const STREAMING_THRESHOLD = 100 * 1024 * 1024;

export interface StreamOptions {
  /** Show progress bar */
  showProgress?: boolean;
  /** Sample size for schema inference */
  sampleSize?: number;
  /** Stop after sampling (don't read entire file) */
  sampleOnly?: boolean;
}

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
 * Parse a JSON array file using streaming for large files
 */
export async function parseJsonArray(
  filePath: string,
  onRecord?: (record: DataRecord, index: number) => void
): Promise<DataRecord[]> {
  const stats = fs.statSync(filePath);

  // Use streaming for large files
  if (stats.size > STREAMING_THRESHOLD) {
    return parseJsonArrayStreaming(filePath, onRecord);
  }

  // For smaller files, use standard parsing (faster)
  const content = await fs.promises.readFile(filePath, "utf-8");
  const data = JSON.parse(content);

  if (!Array.isArray(data)) {
    throw new Error("JSON file must contain an array of objects");
  }

  if (onRecord) {
    data.forEach((record, index) => onRecord(record as DataRecord, index));
  }

  return data as DataRecord[];
}

/**
 * Stream-parse a large JSON array file
 * Memory efficient - processes records one at a time
 */
export async function parseJsonArrayStreaming(
  filePath: string,
  onRecord?: (record: DataRecord, index: number) => void
): Promise<DataRecord[]> {
  return new Promise((resolve, reject) => {
    const records: DataRecord[] = [];
    let index = 0;
    let lastLogTime = Date.now();

    const pipeline = chain([
      fs.createReadStream(filePath),
      jsonParser(),
      streamArray(),
    ]);

    pipeline.on("data", (data: { key: number; value: DataRecord }) => {
      const record = data.value;
      records.push(record);
      onRecord?.(record, index);
      index++;

      // Log progress every 5 seconds for large files
      if (Date.now() - lastLogTime > 5000) {
        console.log(`  Parsed ${index.toLocaleString()} records...`);
        lastLogTime = Date.now();
      }
    });

    pipeline.on("end", () => {
      resolve(records);
    });

    pipeline.on("error", (err: Error) => {
      reject(new Error(`JSON parse error: ${err.message}`));
    });
  });
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
      records = await parseJsonArray(filePath, onRecord);
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
 * Stream-only parsing - doesn't accumulate records in memory
 * Returns count and sample records for schema inference
 */
export async function streamFile(
  filePath: string,
  format: DataFormat | undefined,
  onRecord: (record: DataRecord, index: number) => void,
  sampleSize: number = 1000,
  options: StreamOptions = {}
): Promise<{ count: number; sample: DataRecord[]; format: DataFormat; estimatedCount?: number }> {
  const detectedFormat = format || detectFormat(filePath);
  const sample: DataRecord[] = [];
  let count = 0;
  let bytesProcessed = 0;
  const fileSize = getFileSize(filePath);

  // Setup progress bar if requested
  let progressBar: cliProgress.SingleBar | null = null;
  if (options.showProgress && !options.sampleOnly) {
    progressBar = new cliProgress.SingleBar({
      format: '  Processing |{bar}| {percentage}% | {value}/{total} MB | {records} records | ETA: {eta}s',
      barCompleteChar: '\u2588',
      barIncompleteChar: '\u2591',
      hideCursor: true,
    }, cliProgress.Presets.shades_classic);
    progressBar.start(Math.round(fileSize / (1024 * 1024)), 0, { records: 0 });
  }

  const collector = (record: DataRecord, index: number, bytes?: number) => {
    if (sample.length < sampleSize) {
      sample.push(record);
    }
    count++;
    if (bytes) bytesProcessed = bytes;

    // Update progress bar every 1000 records
    if (progressBar && count % 1000 === 0) {
      progressBar.update(Math.round(bytesProcessed / (1024 * 1024)), { records: count.toLocaleString() });
    }

    onRecord(record, index);
  };

  // For sample-only mode, just read enough to get the sample
  if (options.sampleOnly) {
    switch (detectedFormat) {
      case "json":
        await streamJsonArraySample(filePath, collector, sampleSize);
        break;
      case "ndjson":
        await streamNdjsonSample(filePath, collector, sampleSize);
        break;
      case "csv":
        await streamCsvSample(filePath, collector, sampleSize);
        break;
      default:
        throw new Error(`Unsupported format: ${detectedFormat}`);
    }

    // Estimate total count from sample
    const avgRecordSize = bytesProcessed / count;
    const estimatedCount = Math.round(fileSize / avgRecordSize);

    return { count, sample, format: detectedFormat, estimatedCount };
  }

  switch (detectedFormat) {
    case "json":
      await streamJsonArray(filePath, collector, options.showProgress);
      break;
    case "ndjson":
      await streamNdjson(filePath, collector, options.showProgress);
      break;
    case "csv":
      await streamCsv(filePath, collector, options.showProgress);
      break;
    default:
      throw new Error(`Unsupported format: ${detectedFormat}`);
  }

  if (progressBar) {
    progressBar.update(Math.round(fileSize / (1024 * 1024)), { records: count.toLocaleString() });
    progressBar.stop();
  }

  return { count, sample, format: detectedFormat };
}

/**
 * Stream JSON array without accumulating in memory
 */
async function streamJsonArray(
  filePath: string,
  onRecord: (record: DataRecord, index: number, bytes?: number) => void,
  showProgress?: boolean
): Promise<void> {
  return new Promise((resolve, reject) => {
    let index = 0;
    let bytesRead = 0;

    const readStream = fs.createReadStream(filePath);
    readStream.on("data", (chunk: Buffer) => {
      bytesRead += chunk.length;
    });

    const pipeline = chain([
      readStream,
      jsonParser(),
      streamArray(),
    ]);

    pipeline.on("data", (data: { key: number; value: DataRecord }) => {
      onRecord(data.value, index, bytesRead);
      index++;
    });

    pipeline.on("end", () => resolve());
    pipeline.on("error", (err: Error) => {
      reject(new Error(`JSON parse error: ${err.message}`));
    });
  });
}

/**
 * Stream JSON array but stop after N records (for sampling)
 */
async function streamJsonArraySample(
  filePath: string,
  onRecord: (record: DataRecord, index: number, bytes?: number) => void,
  maxRecords: number
): Promise<void> {
  return new Promise((resolve, reject) => {
    let index = 0;
    let bytesRead = 0;

    const readStream = fs.createReadStream(filePath);
    readStream.on("data", (chunk: Buffer) => {
      bytesRead += chunk.length;
    });

    const pipeline = chain([
      readStream,
      jsonParser(),
      streamArray(),
    ]);

    pipeline.on("data", (data: { key: number; value: DataRecord }) => {
      onRecord(data.value, index, bytesRead);
      index++;

      if (index >= maxRecords) {
        readStream.destroy();
        resolve();
      }
    });

    pipeline.on("end", () => resolve());
    pipeline.on("error", (err: Error) => {
      // Ignore errors from destroying the stream early
      if (err.message.includes("aborted") || err.message.includes("destroyed")) {
        resolve();
      } else {
        reject(new Error(`JSON parse error: ${err.message}`));
      }
    });
  });
}

/**
 * Stream NDJSON without accumulating in memory
 */
async function streamNdjson(
  filePath: string,
  onRecord: (record: DataRecord, index: number, bytes?: number) => void,
  showProgress?: boolean
): Promise<void> {
  let index = 0;
  let bytesRead = 0;

  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  fileStream.on("data", (chunk: string) => {
    bytesRead += Buffer.byteLength(chunk);
  });

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    try {
      const record = JSON.parse(trimmed) as DataRecord;
      onRecord(record, index, bytesRead);
      index++;
    } catch (e) {
      throw new Error(`Invalid JSON at line ${index + 1}: ${trimmed.slice(0, 50)}...`);
    }
  }
}

/**
 * Stream NDJSON but stop after N records (for sampling)
 */
async function streamNdjsonSample(
  filePath: string,
  onRecord: (record: DataRecord, index: number, bytes?: number) => void,
  maxRecords: number
): Promise<void> {
  let index = 0;
  let bytesRead = 0;

  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  fileStream.on("data", (chunk: string) => {
    bytesRead += Buffer.byteLength(chunk);
  });

  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;

    try {
      const record = JSON.parse(trimmed) as DataRecord;
      onRecord(record, index, bytesRead);
      index++;

      if (index >= maxRecords) {
        rl.close();
        fileStream.destroy();
        return;
      }
    } catch (e) {
      throw new Error(`Invalid JSON at line ${index + 1}: ${trimmed.slice(0, 50)}...`);
    }
  }
}

/**
 * Stream CSV without accumulating in memory
 */
async function streamCsv(
  filePath: string,
  onRecord: (record: DataRecord, index: number, bytes?: number) => void,
  showProgress?: boolean
): Promise<void> {
  return new Promise((resolve, reject) => {
    let index = 0;
    let bytesRead = 0;

    const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
    fileStream.on("data", (chunk: string) => {
      bytesRead += Buffer.byteLength(chunk);
    });

    Papa.parse(fileStream, {
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      step: (result) => {
        if (result.errors.length > 0) {
          reject(new Error(`CSV parse error: ${result.errors[0].message}`));
          return;
        }
        onRecord(result.data as DataRecord, index, bytesRead);
        index++;
      },
      complete: () => resolve(),
      error: (error) => reject(error),
    });
  });
}

/**
 * Stream CSV but stop after N records (for sampling)
 */
async function streamCsvSample(
  filePath: string,
  onRecord: (record: DataRecord, index: number, bytes?: number) => void,
  maxRecords: number
): Promise<void> {
  return new Promise((resolve, reject) => {
    let index = 0;
    let bytesRead = 0;
    let resolved = false;

    const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
    fileStream.on("data", (chunk: string) => {
      bytesRead += Buffer.byteLength(chunk);
    });

    Papa.parse(fileStream, {
      header: true,
      dynamicTyping: true,
      skipEmptyLines: true,
      step: (result, parser) => {
        if (resolved) return;

        if (result.errors.length > 0) {
          reject(new Error(`CSV parse error: ${result.errors[0].message}`));
          return;
        }
        onRecord(result.data as DataRecord, index, bytesRead);
        index++;

        if (index >= maxRecords) {
          resolved = true;
          parser.abort();
          fileStream.destroy();
          resolve();
        }
      },
      complete: () => {
        if (!resolved) resolve();
      },
      error: (error) => reject(error),
    });
  });
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
