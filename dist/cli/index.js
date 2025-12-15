#!/usr/bin/env node

// src/cli/index.ts
import { Command } from "commander";

// src/cli/commands/build.ts
import * as fs2 from "fs";
import * as path from "path";

// src/cli/utils/parsers.ts
import * as fs from "fs";
import * as readline from "readline";
import Papa from "papaparse";
import streamJson from "stream-json";
import StreamArrayModule from "stream-json/streamers/StreamArray.js";
import streamChain from "stream-chain";
import cliProgress from "cli-progress";
var { parser: jsonParser } = streamJson;
var { chain } = streamChain;
var { streamArray } = StreamArrayModule;
var STREAMING_THRESHOLD = 100 * 1024 * 1024;
function detectFormat(filePath) {
  const ext = filePath.toLowerCase().split(".").pop();
  if (ext === "csv") return "csv";
  if (ext === "ndjson" || ext === "jsonl") return "ndjson";
  if (ext === "json") {
    const fd = fs.openSync(filePath, "r");
    const buffer = Buffer.alloc(1024);
    fs.readSync(fd, buffer, 0, 1024, 0);
    fs.closeSync(fd);
    const content = buffer.toString("utf-8").trim();
    if (content.startsWith("[")) return "json";
    if (content.startsWith("{")) return "ndjson";
  }
  return "json";
}
async function parseJsonArray(filePath, onRecord) {
  const stats = fs.statSync(filePath);
  if (stats.size > STREAMING_THRESHOLD) {
    return parseJsonArrayStreaming(filePath, onRecord);
  }
  const content = await fs.promises.readFile(filePath, "utf-8");
  const data = JSON.parse(content);
  if (!Array.isArray(data)) {
    throw new Error("JSON file must contain an array of objects");
  }
  if (onRecord) {
    data.forEach((record, index) => onRecord(record, index));
  }
  return data;
}
async function parseJsonArrayStreaming(filePath, onRecord) {
  return new Promise((resolve2, reject) => {
    const records = [];
    let index = 0;
    let lastLogTime = Date.now();
    const pipeline = chain([
      fs.createReadStream(filePath),
      jsonParser(),
      streamArray()
    ]);
    pipeline.on("data", (data) => {
      const record = data.value;
      records.push(record);
      onRecord?.(record, index);
      index++;
      if (Date.now() - lastLogTime > 5e3) {
        console.log(`  Parsed ${index.toLocaleString()} records...`);
        lastLogTime = Date.now();
      }
    });
    pipeline.on("end", () => {
      resolve2(records);
    });
    pipeline.on("error", (err) => {
      reject(new Error(`JSON parse error: ${err.message}`));
    });
  });
}
async function parseNdjson(filePath, onRecord) {
  const records = [];
  let index = 0;
  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const record = JSON.parse(trimmed);
      records.push(record);
      onRecord?.(record, index);
      index++;
    } catch (e) {
      throw new Error(`Invalid JSON at line ${index + 1}: ${trimmed.slice(0, 50)}...`);
    }
  }
  return records;
}
async function parseCsv(filePath, onRecord) {
  return new Promise((resolve2, reject) => {
    const records = [];
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
        const record = result.data;
        records.push(record);
        onRecord?.(record, index);
        index++;
      },
      complete: () => resolve2(records),
      error: (error) => reject(error)
    });
  });
}
async function parseFile(filePath, format, onRecord) {
  const detectedFormat = format || detectFormat(filePath);
  let records;
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
async function streamFile(filePath, format, onRecord, sampleSize = 1e3, options = {}) {
  const detectedFormat = format || detectFormat(filePath);
  const sample = [];
  let count = 0;
  let bytesProcessed = 0;
  const fileSize = getFileSize(filePath);
  let progressBar = null;
  if (options.showProgress && !options.sampleOnly) {
    progressBar = new cliProgress.SingleBar({
      format: "  Processing |{bar}| {percentage}% | {value}/{total} MB | {records} records | ETA: {eta}s",
      barCompleteChar: "\u2588",
      barIncompleteChar: "\u2591",
      hideCursor: true
    }, cliProgress.Presets.shades_classic);
    progressBar.start(Math.round(fileSize / (1024 * 1024)), 0, { records: 0 });
  }
  const collector = (record, index, bytes) => {
    if (sample.length < sampleSize) {
      sample.push(record);
    }
    count++;
    if (bytes) bytesProcessed = bytes;
    if (progressBar && count % 1e3 === 0) {
      progressBar.update(Math.round(bytesProcessed / (1024 * 1024)), { records: count.toLocaleString() });
    }
    onRecord(record, index);
  };
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
async function streamJsonArray(filePath, onRecord, showProgress) {
  return new Promise((resolve2, reject) => {
    let index = 0;
    let bytesRead = 0;
    const readStream = fs.createReadStream(filePath);
    readStream.on("data", (chunk) => {
      bytesRead += chunk.length;
    });
    const pipeline = chain([
      readStream,
      jsonParser(),
      streamArray()
    ]);
    pipeline.on("data", (data) => {
      onRecord(data.value, index, bytesRead);
      index++;
    });
    pipeline.on("end", () => resolve2());
    pipeline.on("error", (err) => {
      reject(new Error(`JSON parse error: ${err.message}`));
    });
  });
}
async function streamJsonArraySample(filePath, onRecord, maxRecords) {
  return new Promise((resolve2, reject) => {
    let index = 0;
    let bytesRead = 0;
    const readStream = fs.createReadStream(filePath);
    readStream.on("data", (chunk) => {
      bytesRead += chunk.length;
    });
    const pipeline = chain([
      readStream,
      jsonParser(),
      streamArray()
    ]);
    pipeline.on("data", (data) => {
      onRecord(data.value, index, bytesRead);
      index++;
      if (index >= maxRecords) {
        readStream.destroy();
        resolve2();
      }
    });
    pipeline.on("end", () => resolve2());
    pipeline.on("error", (err) => {
      if (err.message.includes("aborted") || err.message.includes("destroyed")) {
        resolve2();
      } else {
        reject(new Error(`JSON parse error: ${err.message}`));
      }
    });
  });
}
async function streamNdjson(filePath, onRecord, showProgress) {
  let index = 0;
  let bytesRead = 0;
  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  fileStream.on("data", (chunk) => {
    bytesRead += Buffer.byteLength(chunk);
  });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const record = JSON.parse(trimmed);
      onRecord(record, index, bytesRead);
      index++;
    } catch (e) {
      throw new Error(`Invalid JSON at line ${index + 1}: ${trimmed.slice(0, 50)}...`);
    }
  }
}
async function streamNdjsonSample(filePath, onRecord, maxRecords) {
  let index = 0;
  let bytesRead = 0;
  const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
  fileStream.on("data", (chunk) => {
    bytesRead += Buffer.byteLength(chunk);
  });
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity
  });
  for await (const line of rl) {
    const trimmed = line.trim();
    if (!trimmed) continue;
    try {
      const record = JSON.parse(trimmed);
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
async function streamCsv(filePath, onRecord, showProgress) {
  return new Promise((resolve2, reject) => {
    let index = 0;
    let bytesRead = 0;
    const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
    fileStream.on("data", (chunk) => {
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
        onRecord(result.data, index, bytesRead);
        index++;
      },
      complete: () => resolve2(),
      error: (error) => reject(error)
    });
  });
}
async function streamCsvSample(filePath, onRecord, maxRecords) {
  return new Promise((resolve2, reject) => {
    let index = 0;
    let bytesRead = 0;
    let resolved = false;
    const fileStream = fs.createReadStream(filePath, { encoding: "utf-8" });
    fileStream.on("data", (chunk) => {
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
        onRecord(result.data, index, bytesRead);
        index++;
        if (index >= maxRecords) {
          resolved = true;
          parser.abort();
          fileStream.destroy();
          resolve2();
        }
      },
      complete: () => {
        if (!resolved) resolve2();
      },
      error: (error) => reject(error)
    });
  });
}
function getFileSize(filePath) {
  const stats = fs.statSync(filePath);
  return stats.size;
}
function parseSize(sizeStr) {
  const match = sizeStr.toLowerCase().match(/^(\d+(?:\.\d+)?)\s*(b|kb|mb|gb)?$/);
  if (!match) {
    throw new Error(`Invalid size format: ${sizeStr}. Use format like "5mb" or "1gb"`);
  }
  const value = parseFloat(match[1]);
  const unit = match[2] || "b";
  const multipliers = {
    b: 1,
    kb: 1024,
    mb: 1024 * 1024,
    gb: 1024 * 1024 * 1024
  };
  return Math.floor(value * multipliers[unit]);
}

// src/cli/utils/schema.ts
function inferType(value) {
  if (value === null || value === void 0) {
    return "null";
  }
  const type = typeof value;
  if (type === "string") {
    if (isDateString(value)) {
      return "date";
    }
    return "string";
  }
  if (type === "number") {
    return "number";
  }
  if (type === "boolean") {
    return "boolean";
  }
  return "string";
}
function isDateString(value) {
  const isoPattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{3})?(Z|[+-]\d{2}:?\d{2})?)?$/;
  if (isoPattern.test(value)) {
    const date = new Date(value);
    return !isNaN(date.getTime());
  }
  return false;
}
function mergeTypes(type1, type2) {
  if (type1 === type2) return type1;
  if (type1 === "null") return type2;
  if (type2 === "null") return type1;
  return "string";
}
var FieldStatsCollector = class {
  values = /* @__PURE__ */ new Set();
  min;
  max;
  nullCount = 0;
  count = 0;
  type = "null";
  sampleValues = [];
  add(value) {
    this.count++;
    if (value === null || value === void 0) {
      this.nullCount++;
      return;
    }
    const valueType = inferType(value);
    this.type = mergeTypes(this.type, valueType);
    if (this.values.size < 1e4) {
      this.values.add(String(value));
    }
    if (this.sampleValues.length < 5 && !this.sampleValues.includes(value)) {
      this.sampleValues.push(value);
    }
    if (typeof value === "number") {
      if (this.min === void 0 || value < this.min) {
        this.min = value;
      }
      if (this.max === void 0 || value > this.max) {
        this.max = value;
      }
    } else if (typeof value === "string" && (this.type === "date" || this.type === "string")) {
      if (this.min === void 0 || value < this.min) {
        this.min = value;
      }
      if (this.max === void 0 || value > this.max) {
        this.max = value;
      }
    }
  }
  getStats() {
    return {
      min: this.min,
      max: this.max,
      cardinality: this.values.size,
      nullCount: this.nullCount,
      sampleValues: this.sampleValues
    };
  }
  getType() {
    return this.type;
  }
  isNullable() {
    return this.nullCount > 0;
  }
};
function shouldIndex(name, type, cardinality, totalRecords, stats) {
  if (cardinality <= 1) {
    return false;
  }
  if (cardinality > totalRecords * 0.5) {
    return false;
  }
  if (cardinality > 1e3) {
    return false;
  }
  const samples = stats.sampleValues || [];
  for (const sample of samples) {
    if (sample === null || sample === void 0) continue;
    const str = String(sample);
    if (str.startsWith("[object ") || str === "[object Object]") {
      return false;
    }
    if (str.includes("://")) {
      return false;
    }
    if (str.length > 100) {
      return false;
    }
  }
  const nameLower = name.toLowerCase();
  const skipPatterns = [
    "_uri",
    "_url",
    "_id",
    // ID/URL suffixes (unless it's a category ID)
    "uri",
    "url",
    "href",
    "link",
    // URL fields
    "hash",
    "token",
    "secret",
    "key",
    // Security/hash fields
    "description",
    "text",
    "content",
    "body"
    // Long text fields
  ];
  const allowPatterns = [
    "category",
    "type",
    "status",
    "state",
    "level",
    "color",
    "lang",
    "rarity",
    "set",
    "frame"
  ];
  const hasSkipPattern = skipPatterns.some((p) => nameLower.includes(p));
  const hasAllowPattern = allowPatterns.some((p) => nameLower.includes(p));
  if (hasSkipPattern && !hasAllowPattern) {
    return false;
  }
  if (type === "boolean") {
    return cardinality >= 2;
  }
  return cardinality >= 2 && cardinality <= 500;
}
function inferSchema(records) {
  if (records.length === 0) {
    return { fields: [], primaryField: null };
  }
  const fieldNames = /* @__PURE__ */ new Set();
  for (const record of records) {
    for (const key of Object.keys(record)) {
      fieldNames.add(key);
    }
  }
  const collectors = /* @__PURE__ */ new Map();
  for (const name of fieldNames) {
    collectors.set(name, new FieldStatsCollector());
  }
  for (const record of records) {
    for (const name of fieldNames) {
      const collector = collectors.get(name);
      collector.add(record[name]);
    }
  }
  const fields = [];
  let primaryField = null;
  for (const name of fieldNames) {
    const collector = collectors.get(name);
    const stats = collector.getStats();
    const type = collector.getType();
    const cardinality = stats.cardinality;
    const indexed = shouldIndex(name, type, cardinality, records.length, stats);
    if (primaryField === null && stats.cardinality === records.length && !collector.isNullable() && (name.toLowerCase().includes("id") || name.toLowerCase() === "key")) {
      primaryField = name;
    }
    fields.push({
      name,
      type,
      nullable: collector.isNullable(),
      indexed,
      stats
    });
  }
  fields.sort((a, b) => {
    if (a.name === primaryField) return -1;
    if (b.name === primaryField) return 1;
    return a.name.localeCompare(b.name);
  });
  return { fields, primaryField };
}
function suggestChunkField(schema, records) {
  if (schema.primaryField) {
    return schema.primaryField;
  }
  let bestField = null;
  let bestScore = 0;
  for (const field of schema.fields) {
    if (field.nullable) continue;
    let score = 0;
    if (field.type === "number" || field.type === "date") {
      score += 50;
    }
    const cardinalityRatio = field.stats.cardinality / records.length;
    if (cardinalityRatio > 0.1 && cardinalityRatio <= 1) {
      score += cardinalityRatio * 30;
    }
    const nameLower = field.name.toLowerCase();
    if (nameLower.includes("id")) score += 20;
    if (nameLower.includes("date") || nameLower.includes("time")) score += 15;
    if (nameLower.includes("created") || nameLower.includes("updated")) score += 10;
    if (score > bestScore) {
      bestScore = score;
      bestField = field.name;
    }
  }
  return bestField;
}
function getIndexableFields(schema) {
  return schema.fields.filter((f) => f.indexed).map((f) => f.name);
}

// src/cli/utils/chunker.ts
function estimateRecordSize(record) {
  return JSON.stringify(record).length;
}
function compareValues(a, b) {
  if (a === b) return 0;
  if (a === null || a === void 0) return -1;
  if (b === null || b === void 0) return 1;
  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }
  return String(a).localeCompare(String(b));
}
function chunkRecords(records, schema, options) {
  const { targetSize, chunkBy } = options;
  let sortedRecords = records;
  if (chunkBy) {
    sortedRecords = [...records].sort(
      (a, b) => compareValues(a[chunkBy], b[chunkBy])
    );
  }
  const chunks = [];
  let currentChunk = [];
  let currentSize = 0;
  let chunkIndex = 0;
  const arrayOverhead = 2;
  const commaOverhead = 1;
  for (const record of sortedRecords) {
    const recordSize = estimateRecordSize(record) + commaOverhead;
    if (currentSize + recordSize > targetSize && currentChunk.length > 0) {
      chunks.push({
        id: String(chunkIndex),
        records: currentChunk,
        byteSize: currentSize + arrayOverhead
      });
      chunkIndex++;
      currentChunk = [];
      currentSize = 0;
    }
    currentChunk.push(record);
    currentSize += recordSize;
  }
  if (currentChunk.length > 0) {
    chunks.push({
      id: String(chunkIndex),
      records: currentChunk,
      byteSize: currentSize + arrayOverhead
    });
  }
  return chunks;
}
function calculateFieldRanges(records, schema) {
  const ranges = {};
  for (const field of schema.fields) {
    const values = records.map((r) => r[field.name]).filter((v) => v !== null && v !== void 0);
    if (values.length === 0) {
      continue;
    }
    if (field.type === "number" || field.type === "date" || field.type === "string") {
      const sorted = [...values].sort(compareValues);
      ranges[field.name] = {
        min: sorted[0],
        max: sorted[sorted.length - 1]
      };
    }
  }
  return ranges;
}
function generateChunkMeta(chunk, schema, basePath) {
  return {
    id: chunk.id,
    path: `${basePath}/${chunk.id}.json`,
    count: chunk.records.length,
    byteSize: chunk.byteSize,
    fieldRanges: calculateFieldRanges(chunk.records, schema)
  };
}
function serializeChunk(chunk) {
  return JSON.stringify(chunk.records);
}

// src/cli/utils/indexer.ts
function buildInvertedIndices(chunks, schema, indexedFields) {
  const indices = {};
  const fieldsToIndex = indexedFields ? schema.fields.filter((f) => indexedFields.includes(f.name)) : schema.fields.filter((f) => f.indexed);
  for (const field of fieldsToIndex) {
    const fieldIndex = {};
    for (const chunk of chunks) {
      const valuesInChunk = /* @__PURE__ */ new Set();
      for (const record of chunk.records) {
        const value = record[field.name];
        if (value !== null && value !== void 0) {
          valuesInChunk.add(String(value));
        }
      }
      for (const value of valuesInChunk) {
        if (!fieldIndex[value]) {
          fieldIndex[value] = [];
        }
        fieldIndex[value].push(chunk.id);
      }
    }
    const uniqueValues = Object.keys(fieldIndex).length;
    if (uniqueValues <= 1e4) {
      indices[field.name] = fieldIndex;
    }
  }
  return indices;
}

// src/cli/utils/codegen.ts
import { json2ts } from "json-ts";
function cleanupTypes(types2) {
  return types2.replace(/^type IItem = IItemItem\[\];\n/m, "").replace(/IItemItem/g, "Item").replace(/\bI([A-Z][a-z_]+)/g, "$1").replace(/^interface /gm, "export interface ");
}
function generateFieldNamesType(schema) {
  const names = schema.fields.map((f) => `"${f.name}"`).join(" | ");
  return `export type FieldName = ${names};`;
}
function generateWhereTypes(schema) {
  return `
export type StringOperators = {
  eq?: string;
  neq?: string;
  contains?: string;
  startsWith?: string;
  endsWith?: string;
  in?: string[];
};

export type NumericOperators = {
  eq?: number;
  neq?: number;
  gt?: number;
  gte?: number;
  lt?: number;
  lte?: number;
  in?: number[];
};

export type WhereClause = {
${schema.fields.map((f) => {
    if (f.type === "number") {
      return `  ${f.name}?: number | NumericOperators;`;
    } else if (f.type === "boolean") {
      return `  ${f.name}?: boolean;`;
    } else {
      return `  ${f.name}?: string | StringOperators;`;
    }
  }).join("\n")}
};`;
}
function generateClient(schema, manifest, samples) {
  const rawTypes = json2ts(JSON.stringify(samples), { rootName: "Item" });
  const itemInterface = cleanupTypes(rawTypes);
  const fieldNamesType = generateFieldNamesType(schema);
  const whereTypes = generateWhereTypes(schema);
  const sortableFields = schema.fields.filter((f) => f.type === "number" || f.type === "string" || f.type === "date").map((f) => `"${f.name}"`).join(" | ");
  return `/**
 * Auto-generated client for static-shard
 * Generated at: ${manifest.generatedAt}
 * Total records: ${manifest.totalRecords}
 * Chunks: ${manifest.chunks.length}
 */

// ============================================================================
// Types
// ============================================================================

${itemInterface}

${fieldNamesType}

${whereTypes}

export type SortableField = ${sortableFields || "string"};

export interface QueryOptions {
  where?: WhereClause;
  orderBy?: SortableField | { field: SortableField; direction: "asc" | "desc" };
  limit?: number;
  offset?: number;
}

export interface Manifest {
  version: string;
  schema: {
    fields: Array<{
      name: string;
      type: string;
      nullable: boolean;
      indexed: boolean;
    }>;
    primaryField: string | null;
  };
  chunks: Array<{
    id: string;
    path: string;
    count: number;
    byteSize: number;
    fieldRanges: { [field: string]: { min: unknown; max: unknown } };
  }>;
  indices: { [field: string]: { [value: string]: string[] } };
  totalRecords: number;
}

// ============================================================================
// Runtime
// ============================================================================

interface ClientOptions {
  basePath: string;
}

class StaticShardClient {
  private basePath: string;
  private manifest: Manifest | null = null;
  private chunkCache: Map<string, Item[]> = new Map();

  constructor(options: ClientOptions) {
    this.basePath = options.basePath.replace(/\\/$/, "");
  }

  /**
   * Load the manifest file
   */
  private async loadManifest(): Promise<Manifest> {
    if (this.manifest) return this.manifest;

    const response = await fetch(\`\${this.basePath}/manifest.json\`);
    if (!response.ok) {
      throw new Error(\`Failed to load manifest: \${response.statusText}\`);
    }

    this.manifest = await response.json();
    return this.manifest!;
  }

  /**
   * Load a chunk by ID
   */
  private async loadChunk(chunkId: string): Promise<Item[]> {
    const cached = this.chunkCache.get(chunkId);
    if (cached) return cached;

    const manifest = await this.loadManifest();
    const chunkMeta = manifest.chunks.find((c) => c.id === chunkId);
    if (!chunkMeta) {
      throw new Error(\`Chunk not found: \${chunkId}\`);
    }

    const response = await fetch(\`\${this.basePath}/\${chunkMeta.path}\`);
    if (!response.ok) {
      throw new Error(\`Failed to load chunk \${chunkId}: \${response.statusText}\`);
    }

    const records = await response.json();
    this.chunkCache.set(chunkId, records);
    return records;
  }

  /**
   * Find chunk IDs that might contain matching records
   */
  private findCandidateChunks(manifest: Manifest, where?: WhereClause): string[] {
    if (!where) {
      return manifest.chunks.map((c) => c.id);
    }

    let candidateChunks: Set<string> | null = null;

    for (const [field, condition] of Object.entries(where)) {
      // Check if we can use the index
      const index = manifest.indices[field];

      if (index && (typeof condition === "string" || typeof condition === "number" || typeof condition === "boolean")) {
        const value = String(condition);
        const chunks = index[value] || [];

        if (candidateChunks === null) {
          candidateChunks = new Set(chunks);
        } else {
          candidateChunks = new Set(Array.from(candidateChunks).filter((c) => chunks.includes(c)));
        }
      } else if (typeof condition === "object" && condition !== null && "eq" in condition) {
        const value = String(condition.eq);
        const chunks = index?.[value] || [];

        if (index) {
          if (candidateChunks === null) {
            candidateChunks = new Set(chunks);
          } else {
            candidateChunks = new Set(Array.from(candidateChunks).filter((c) => chunks.includes(c)));
          }
        }
      }

      // Range pruning using fieldRanges
      if (typeof condition === "object" && condition !== null) {
        const rangeCondition = condition as { gt?: number; gte?: number; lt?: number; lte?: number };
        const hasRangeOp = "gt" in rangeCondition || "gte" in rangeCondition || "lt" in rangeCondition || "lte" in rangeCondition;

        if (hasRangeOp) {
          const matchingChunks = manifest.chunks
            .filter((chunk) => {
              const range = chunk.fieldRanges[field];
              if (!range) return true; // Can't prune, include chunk

              const min = range.min as number;
              const max = range.max as number;

              if (rangeCondition.gt !== undefined && max <= rangeCondition.gt) return false;
              if (rangeCondition.gte !== undefined && max < rangeCondition.gte) return false;
              if (rangeCondition.lt !== undefined && min >= rangeCondition.lt) return false;
              if (rangeCondition.lte !== undefined && min > rangeCondition.lte) return false;

              return true;
            })
            .map((c) => c.id);

          if (candidateChunks === null) {
            candidateChunks = new Set(matchingChunks);
          } else {
            candidateChunks = new Set(Array.from(candidateChunks).filter((c) => matchingChunks.includes(c)));
          }
        }
      }
    }

    return candidateChunks ? Array.from(candidateChunks) : manifest.chunks.map((c) => c.id);
  }

  /**
   * Check if a record matches the where clause
   */
  private matchesWhere(record: Item, where?: WhereClause): boolean {
    if (!where) return true;

    for (const [field, condition] of Object.entries(where)) {
      const value = record[field as keyof Item];

      // Direct value comparison
      if (typeof condition !== "object" || condition === null) {
        if (value !== condition) return false;
        continue;
      }

      const ops = condition as StringOperators & NumericOperators;

      if ("eq" in ops && value !== ops.eq) return false;
      if ("neq" in ops && value === ops.neq) return false;
      if ("gt" in ops && (typeof value !== "number" || value <= ops.gt!)) return false;
      if ("gte" in ops && (typeof value !== "number" || value < ops.gte!)) return false;
      if ("lt" in ops && (typeof value !== "number" || value >= ops.lt!)) return false;
      if ("lte" in ops && (typeof value !== "number" || value > ops.lte!)) return false;
      if ("contains" in ops && (typeof value !== "string" || !value.includes(ops.contains!))) return false;
      if ("startsWith" in ops && (typeof value !== "string" || !value.startsWith(ops.startsWith!))) return false;
      if ("endsWith" in ops && (typeof value !== "string" || !value.endsWith(ops.endsWith!))) return false;
      if ("in" in ops && !(ops.in as unknown[])!.includes(value)) return false;
    }

    return true;
  }

  /**
   * Query records
   */
  async query(options: QueryOptions = {}): Promise<Item[]> {
    const manifest = await this.loadManifest();

    // Find candidate chunks
    const candidateChunkIds = this.findCandidateChunks(manifest, options.where);

    // Load chunks in parallel
    const chunkPromises = candidateChunkIds.map((id) => this.loadChunk(id));
    const chunks = await Promise.all(chunkPromises);

    // Flatten and filter
    let results: Item[] = [];
    for (const chunk of chunks) {
      for (const record of chunk) {
        if (this.matchesWhere(record, options.where)) {
          results.push(record);
        }
      }
    }

    // Sort
    if (options.orderBy) {
      const field = typeof options.orderBy === "string" ? options.orderBy : options.orderBy.field;
      const direction = typeof options.orderBy === "string" ? "asc" : options.orderBy.direction;

      results.sort((a, b) => {
        const aVal = a[field as keyof Item];
        const bVal = b[field as keyof Item];

        if (aVal === bVal) return 0;
        if (aVal === null || aVal === undefined) return 1;
        if (bVal === null || bVal === undefined) return -1;

        const cmp = aVal < bVal ? -1 : 1;
        return direction === "asc" ? cmp : -cmp;
      });
    }

    // Pagination
    const offset = options.offset || 0;
    const limit = options.limit;

    if (offset > 0 || limit !== undefined) {
      results = results.slice(offset, limit !== undefined ? offset + limit : undefined);
    }

    return results;
  }

  /**
   * Get a single record by primary key
   */
  async get(id: string | number): Promise<Item | null> {
    const manifest = await this.loadManifest();
    const primaryField = manifest.schema.primaryField;

    if (!primaryField) {
      throw new Error("No primary field defined in schema");
    }

    const results = await this.query({
      where: { [primaryField]: id } as WhereClause,
      limit: 1,
    });

    return results[0] || null;
  }

  /**
   * Count records matching a query
   */
  async count(options: { where?: WhereClause } = {}): Promise<number> {
    const manifest = await this.loadManifest();

    if (!options.where) {
      return manifest.totalRecords;
    }

    // For complex queries, we need to load and count
    const results = await this.query({ where: options.where });
    return results.length;
  }

  /**
   * Get schema information
   */
  async getSchema(): Promise<Manifest["schema"]> {
    const manifest = await this.loadManifest();
    return manifest.schema;
  }

  /**
   * Clear the chunk cache
   */
  clearCache(): void {
    this.chunkCache.clear();
  }
}

// ============================================================================
// Export
// ============================================================================

export function createClient(options: ClientOptions): StaticShardClient {
  return new StaticShardClient(options);
}

// Default export for convenience
export const db = createClient({ basePath: "." });
`;
}

// src/cli/commands/build.ts
var VERSION = "1.0.0";
var STREAMING_THRESHOLD2 = 100 * 1024 * 1024;
async function build(inputFile, options) {
  const startTime = Date.now();
  if (!fs2.existsSync(inputFile)) {
    throw new Error(`Input file not found: ${inputFile}`);
  }
  const fileSize = getFileSize(inputFile);
  console.log(`Input: ${inputFile} (${formatBytes(fileSize)})`);
  if (fileSize > STREAMING_THRESHOLD2) {
    console.log("Using streaming mode for large file...");
    return buildStreaming(inputFile, options, startTime);
  }
  return buildInMemory(inputFile, options, startTime);
}
async function buildInMemory(inputFile, options, startTime) {
  console.log("Reading file...");
  const { records, format } = await parseFile(inputFile, options.format);
  console.log(`Parsed ${records.length.toLocaleString()} records (format: ${format})`);
  if (records.length === 0) {
    throw new Error("No records found in input file");
  }
  console.log("Inferring schema...");
  const schema = inferSchema(records);
  console.log(`Found ${schema.fields.length} fields`);
  if (schema.primaryField) {
    console.log(`Detected primary field: ${schema.primaryField}`);
  }
  const chunkBy = options.chunkBy || suggestChunkField(schema, records);
  if (chunkBy) {
    console.log(`Chunking by field: ${chunkBy}`);
  }
  const targetChunkSize = parseSize(options.chunkSize);
  console.log(`Target chunk size: ${formatBytes(targetChunkSize)}`);
  const MAX_AUTO_INDEX_FIELDS = 10;
  let indexedFields;
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
  for (const field of schema.fields) {
    field.indexed = indexedFields.includes(field.name);
  }
  console.log("Chunking data...");
  const chunks = chunkRecords(records, schema, {
    targetSize: targetChunkSize,
    chunkBy
  });
  console.log(`Created ${chunks.length} chunks`);
  console.log("Building indices...");
  const indices = buildInvertedIndices(chunks, schema, indexedFields);
  console.log(`Built indices for ${Object.keys(indices).length} fields`);
  const outputDir = path.resolve(options.output);
  const chunksDir = path.join(outputDir, "chunks");
  await fs2.promises.mkdir(chunksDir, { recursive: true });
  console.log("Writing chunks...");
  const chunkMetas = [];
  for (const chunk of chunks) {
    const chunkPath = path.join(chunksDir, `${chunk.id}.json`);
    await fs2.promises.writeFile(chunkPath, serializeChunk(chunk));
    chunkMetas.push(generateChunkMeta(chunk, schema, "chunks"));
  }
  const config = {
    chunkSize: targetChunkSize,
    chunkBy: chunkBy || null,
    indexedFields
  };
  const manifest = {
    version: VERSION,
    generatedAt: (/* @__PURE__ */ new Date()).toISOString(),
    schema,
    chunks: chunkMetas,
    indices,
    totalRecords: records.length,
    config
  };
  const manifestPath = path.join(outputDir, "manifest.json");
  await fs2.promises.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`Wrote manifest to ${manifestPath}`);
  console.log("Generating client...");
  const sampleForTypes = records.slice(0, 100);
  const clientCode = generateClient(schema, manifest, sampleForTypes);
  const clientPath = path.join(outputDir, "client.ts");
  await fs2.promises.writeFile(clientPath, clientCode);
  console.log(`Wrote client to ${clientPath}`);
  const elapsed = ((Date.now() - startTime) / 1e3).toFixed(2);
  console.log(`
Build completed in ${elapsed}s`);
  console.log(`Output: ${outputDir}`);
  console.log(`  - ${chunks.length} chunks`);
  console.log(`  - ${records.length.toLocaleString()} total records`);
  console.log(`  - manifest.json`);
  console.log(`  - client.ts`);
  return {
    manifest,
    outputDir,
    chunkCount: chunks.length,
    totalRecords: records.length
  };
}
async function buildStreaming(inputFile, options, startTime) {
  const targetChunkSize = parseSize(options.chunkSize);
  console.log(`Target chunk size: ${formatBytes(targetChunkSize)}`);
  const outputDir = path.resolve(options.output);
  const chunksDir = path.join(outputDir, "chunks");
  await fs2.promises.mkdir(chunksDir, { recursive: true });
  let currentChunk = [];
  let currentChunkSize = 0;
  let chunkId = 0;
  const chunkMetas = [];
  const indices = {};
  let schema = null;
  let indexedFields = [];
  let chunkBy = null;
  const estimateRecordSize2 = (record) => {
    let size = 2;
    for (const key in record) {
      const value = record[key];
      size += key.length + 3;
      if (value === null) size += 4;
      else if (typeof value === "string") size += value.length + 2;
      else if (typeof value === "number") size += String(value).length;
      else if (typeof value === "boolean") size += value ? 4 : 5;
      else size += 20;
    }
    return size;
  };
  const processRecord = (record) => {
    currentChunk.push(record);
    currentChunkSize += estimateRecordSize2(record);
    if (currentChunkSize >= targetChunkSize) {
      flushChunk();
    }
  };
  const flushChunk = () => {
    if (currentChunk.length === 0) return;
    if (chunkBy) {
      currentChunk.sort((a, b) => {
        const aVal = a[chunkBy];
        const bVal = b[chunkBy];
        if (aVal === bVal) return 0;
        if (aVal === null || aVal === void 0) return 1;
        if (bVal === null || bVal === void 0) return -1;
        return aVal < bVal ? -1 : 1;
      });
    }
    const chunk = {
      id: String(chunkId),
      records: currentChunk,
      byteSize: currentChunkSize
    };
    const chunkPath = path.join(chunksDir, `${chunkId}.json`);
    fs2.writeFileSync(chunkPath, JSON.stringify(currentChunk));
    const fieldRanges = calculateFieldRanges(currentChunk, schema);
    chunkMetas.push({
      id: String(chunkId),
      path: `chunks/${chunkId}.json`,
      count: currentChunk.length,
      byteSize: currentChunkSize,
      fieldRanges
    });
    for (const fieldName of indexedFields) {
      if (!indices[fieldName]) {
        indices[fieldName] = {};
      }
      for (const record of currentChunk) {
        const value = record[fieldName];
        if (value === null || value === void 0) continue;
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
    chunkId++;
    currentChunk = [];
    currentChunkSize = 0;
  };
  console.log("Sampling records for schema inference...");
  const sampleResult = await streamFile(
    inputFile,
    options.format,
    () => {
    },
    // No processing in first pass
    1e3,
    { sampleOnly: true, sampleSize: 1e3 }
  );
  const format = sampleResult.format;
  const sample = sampleResult.sample;
  if (sample.length === 0) {
    throw new Error("No records found in input file");
  }
  console.log("Inferring schema from sample...");
  schema = inferSchema(sample);
  console.log(`Found ${schema.fields.length} fields`);
  if (schema.primaryField) {
    console.log(`Detected primary field: ${schema.primaryField}`);
  }
  chunkBy = options.chunkBy || suggestChunkField(schema, sample);
  if (chunkBy) {
    console.log(`Chunking by field: ${chunkBy}`);
  }
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
  for (const field of schema.fields) {
    field.indexed = indexedFields.includes(field.name);
  }
  console.log("Processing all records...");
  const { count } = await streamFile(
    inputFile,
    options.format,
    processRecord,
    1e3,
    { showProgress: true }
  );
  flushChunk();
  console.log(`
Processed ${count.toLocaleString()} records (format: ${format})`);
  const config = {
    chunkSize: targetChunkSize,
    chunkBy: chunkBy || null,
    indexedFields
  };
  const manifest = {
    version: VERSION,
    generatedAt: (/* @__PURE__ */ new Date()).toISOString(),
    schema,
    chunks: chunkMetas,
    indices,
    totalRecords: count,
    config
  };
  const manifestPath = path.join(outputDir, "manifest.json");
  await fs2.promises.writeFile(manifestPath, JSON.stringify(manifest, null, 2));
  console.log(`Wrote manifest to ${manifestPath}`);
  console.log("Generating client...");
  const sampleForTypes = sample.slice(0, 100);
  const clientCode = generateClient(schema, manifest, sampleForTypes);
  const clientPath = path.join(outputDir, "client.ts");
  await fs2.promises.writeFile(clientPath, clientCode);
  console.log(`Wrote client to ${clientPath}`);
  const elapsed = ((Date.now() - startTime) / 1e3).toFixed(2);
  console.log(`
Build completed in ${elapsed}s`);
  console.log(`Output: ${outputDir}`);
  console.log(`  - ${chunkMetas.length} chunks`);
  console.log(`  - ${count.toLocaleString()} total records`);
  console.log(`  - manifest.json`);
  console.log(`  - client.ts`);
  return {
    manifest,
    outputDir,
    chunkCount: chunkMetas.length,
    totalRecords: count
  };
}
function formatBytes(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}

// src/cli/commands/inspect.ts
import * as fs3 from "fs";
import { checkbox, confirm, input, select } from "@inquirer/prompts";
var STREAMING_THRESHOLD3 = 100 * 1024 * 1024;
var INDEX_PROMPT_THRESHOLD = 5;
async function inspect(inputFile, options) {
  if (!fs3.existsSync(inputFile)) {
    throw new Error(`Input file not found: ${inputFile}`);
  }
  const fileSize = getFileSize(inputFile);
  console.log(`
File: ${inputFile}`);
  console.log(`Size: ${formatBytes2(fileSize)}`);
  let totalRecords;
  let sampleRecords;
  let format;
  let isEstimated = false;
  if (fileSize > STREAMING_THRESHOLD3) {
    console.log("\nUsing streaming mode for large file...");
    const sampleSize2 = options.sample || 1e3;
    if (options.fast) {
      console.log("Fast mode: sampling records (count will be estimated)...");
      const result = await streamFile(
        inputFile,
        options.format,
        () => {
        },
        // No-op
        sampleSize2,
        { sampleOnly: true, sampleSize: sampleSize2 }
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
        () => {
        },
        // No-op, we just want the count and sample
        sampleSize2,
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
    const result = await parseFile(inputFile, options.format);
    totalRecords = result.records.length;
    format = result.format;
    console.log(`Format: ${format}`);
    console.log(`Total records: ${totalRecords.toLocaleString()}`);
    if (totalRecords === 0) {
      console.log("\nNo records found.");
      return;
    }
    const sampleSize2 = Math.min(options.sample || 1e3, totalRecords);
    sampleRecords = result.records.slice(0, sampleSize2);
  }
  if (sampleRecords.length === 0) {
    console.log("\nNo records found.");
    return;
  }
  const sampleSize = sampleRecords.length;
  console.log(`
Analyzing ${sampleSize.toLocaleString()} records...`);
  const schema = inferSchema(sampleRecords);
  console.log("\n" + "=".repeat(60));
  console.log("SCHEMA");
  console.log("=".repeat(60));
  console.log(`
Fields (${schema.fields.length}):
`);
  for (const field of schema.fields) {
    const isPrimary = field.name === schema.primaryField ? " [PRIMARY]" : "";
    const isIndexed = field.indexed ? " [INDEXED]" : "";
    const nullable = field.nullable ? " (nullable)" : "";
    console.log(`  ${field.name}: ${field.type}${nullable}${isPrimary}${isIndexed}`);
    const stats = field.stats;
    const statParts = [];
    if (stats.cardinality !== void 0) {
      const cardinalityPct = (stats.cardinality / totalRecords * 100).toFixed(1);
      statParts.push(`cardinality: ${stats.cardinality} (${cardinalityPct}%)`);
    }
    if (stats.min !== void 0 && stats.max !== void 0) {
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
  const avgRecordSize = fileSize / totalRecords;
  const targetChunkSize = 5 * 1024 * 1024;
  const estimatedChunks = Math.ceil(fileSize / targetChunkSize);
  console.log("\n" + "=".repeat(60));
  console.log("SIZE ESTIMATES");
  console.log("=".repeat(60));
  console.log(`
Average record size: ${formatBytes2(avgRecordSize)}`);
  console.log(`With default 5MB chunks: ~${estimatedChunks} chunks`);
  console.log("\n" + "=".repeat(60));
  console.log("BUILD CONFIGURATION");
  console.log("=".repeat(60));
  const suggestedChunkField = suggestChunkField(schema, sampleRecords);
  const chunkableFields = schema.fields.filter((f) => f.type === "number" || f.type === "string" || f.type === "date").map((f) => f.name);
  let selectedChunkField = null;
  if (chunkableFields.length > 0) {
    const chunkFieldChoices = [
      { name: "(none - chunk by record order)", value: "" },
      ...chunkableFields.map((f) => ({
        name: f === suggestedChunkField ? `${f} (recommended)` : f,
        value: f
      }))
    ];
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
  const indexableFields = getIndexableFields(schema);
  let selectedIndexes = [];
  if (indexableFields.length > 0) {
    if (indexableFields.length <= INDEX_PROMPT_THRESHOLD) {
      const useRecommended = await confirm({
        message: `Use recommended indexes? (${indexableFields.join(", ")})`,
        default: true
      });
      if (useRecommended) {
        selectedIndexes = indexableFields;
      } else {
        selectedIndexes = await promptIndexSelection(schema.fields, indexableFields);
      }
    } else {
      console.log(`
Found ${indexableFields.length} indexable fields.`);
      selectedIndexes = await promptIndexSelection(schema.fields, indexableFields);
    }
  }
  const outputDir = await input({
    message: "Output directory:",
    default: "./output"
  });
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
  console.log("\n" + "=".repeat(60));
  console.log("CONFIGURATION SUMMARY");
  console.log("=".repeat(60));
  console.log(`
Input: ${inputFile}`);
  console.log(`Output: ${outputDir}`);
  console.log(`Chunk size: ${chunkSize}`);
  console.log(`Chunk by: ${selectedChunkField || "(record order)"}`);
  console.log(`Indexes: ${selectedIndexes.length > 0 ? selectedIndexes.join(", ") : "(none)"}`);
  let cmd = `npx static-shard build "${inputFile}" -o "${outputDir}" -s ${chunkSize}`;
  if (selectedChunkField) {
    cmd += ` -c ${selectedChunkField}`;
  }
  if (selectedIndexes.length > 0) {
    cmd += ` -i "${selectedIndexes.join(",")}"`;
  }
  console.log(`
Equivalent command:
  ${cmd}`);
  const runBuild = await confirm({
    message: "Run build now?",
    default: true
  });
  if (runBuild) {
    console.log("\n");
    await build(inputFile, {
      output: outputDir,
      chunkSize,
      chunkBy: selectedChunkField || void 0,
      index: selectedIndexes.length > 0 ? selectedIndexes.join(",") : void 0,
      format: options.format
    });
  } else {
    console.log("\nBuild skipped. Run the command above when ready.\n");
  }
}
async function promptIndexSelection(allFields, recommendedFields) {
  const booleanFields = allFields.filter((f) => f.type === "boolean").map((f) => f.name);
  const enumLikeFields = allFields.filter(
    (f) => f.type === "string" && f.stats.cardinality <= 100 && !booleanFields.includes(f.name)
  ).map((f) => f.name);
  const numericFields = allFields.filter((f) => f.type === "number").map((f) => f.name);
  const choices = allFields.filter((f) => {
    if (f.stats.cardinality < 2) return false;
    if (f.type === "boolean") return true;
    if (f.stats.cardinality <= 500) return true;
    return false;
  }).map((f) => {
    const isRecommended = recommendedFields.includes(f.name);
    const cardinalityInfo = f.stats.cardinality ? ` (${f.stats.cardinality} values)` : "";
    return {
      name: `${f.name}${cardinalityInfo}${isRecommended ? " *" : ""}`,
      value: f.name,
      checked: isRecommended
    };
  }).sort((a, b) => {
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
    instructions: false
    // We provide our own instructions above
  });
  return selected;
}
function formatBytes2(bytes) {
  if (bytes < 1024) return `${bytes} B`;
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
  if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  return `${(bytes / (1024 * 1024 * 1024)).toFixed(1)} GB`;
}
function formatValue(value) {
  if (value === null) return "null";
  if (value === void 0) return "undefined";
  if (typeof value === "string") {
    if (value.length > 30) {
      return `"${value.slice(0, 27)}..."`;
    }
    return `"${value}"`;
  }
  return String(value);
}

// src/cli/commands/types.ts
import * as fs4 from "fs";
import { json2ts as json2ts2 } from "json-ts";
var STREAMING_THRESHOLD4 = 100 * 1024 * 1024;
function cleanupTypes2(types2) {
  return types2.replace(/^type IItem = IItemItem\[\];\n/m, "").replace(/IItemItem/g, "Item").replace(/\bI([A-Z][a-z_]+)/g, "$1").replace(/^interface /gm, "export interface ");
}
function generateFieldNamesType2(samples) {
  const fieldNames = /* @__PURE__ */ new Set();
  for (const record of samples) {
    for (const key of Object.keys(record)) {
      fieldNames.add(key);
    }
  }
  const names = Array.from(fieldNames).sort().map((f) => `"${f}"`).join(" | ");
  return `export type FieldName = ${names};`;
}
async function types(inputFile, options) {
  if (!fs4.existsSync(inputFile)) {
    throw new Error(`Input file not found: ${inputFile}`);
  }
  const fileSize = getFileSize(inputFile);
  const sampleSize = options.sample || 1e3;
  console.error(`Analyzing: ${inputFile}`);
  console.error(`Sampling ${sampleSize} records...`);
  let samples;
  let format;
  if (fileSize > STREAMING_THRESHOLD4) {
    const result = await streamFile(
      inputFile,
      options.format,
      () => {
      },
      1e3,
      { sampleOnly: true, sampleSize }
    );
    samples = result.sample;
    format = result.format;
  } else {
    const parseResult = await parseFile(inputFile, options.format);
    samples = parseResult.records.slice(0, sampleSize);
    format = parseResult.format;
  }
  console.error(`Format: ${format}`);
  console.error(`Sampled ${samples.length} records
`);
  const typeSamples = samples.slice(0, 100);
  const rawTypes = json2ts2(JSON.stringify(typeSamples), { rootName: "Item" });
  const types2 = cleanupTypes2(rawTypes);
  const fieldNames = generateFieldNamesType2(samples);
  const output = `/**
 * Auto-generated TypeScript types
 * Source: ${inputFile}
 * Generated: ${(/* @__PURE__ */ new Date()).toISOString()}
 */

${types2}

${fieldNames}
`;
  if (options.output) {
    await fs4.promises.writeFile(options.output, output);
    console.error(`Types written to: ${options.output}`);
  } else {
    console.log(output);
  }
}

// src/cli/index.ts
var program = new Command();
program.name("static-shard").description("Query large static datasets efficiently by splitting them into chunks").version("0.1.0");
program.command("build").description("Build chunks and client from a data file").argument("<input>", "Input data file (JSON, NDJSON, or CSV)").requiredOption("-o, --output <dir>", "Output directory").option("-s, --chunk-size <size>", "Target chunk size (e.g., 5mb)", "5mb").option("-c, --chunk-by <field>", "Field to sort and chunk by").option("-i, --index <fields>", "Comma-separated fields to index").option("-f, --format <format>", "Input format (json, ndjson, csv)").action(async (input2, options) => {
  try {
    await build(input2, {
      output: options.output,
      chunkSize: options.chunkSize,
      chunkBy: options.chunkBy,
      index: options.index,
      format: options.format
    });
  } catch (error) {
    console.error("Error:", error.message);
    process.exit(1);
  }
});
program.command("inspect").description("Analyze a data file and suggest chunking strategy").argument("<input>", "Input data file").option("-n, --sample <count>", "Number of records to sample", "1000").option("-f, --format <format>", "Input format (json, ndjson, csv)").option("--fast", "Fast mode: estimate record count instead of reading entire file").action(async (input2, options) => {
  try {
    await inspect(input2, {
      sample: parseInt(options.sample, 10),
      format: options.format,
      fast: options.fast
    });
  } catch (error) {
    console.error("Error:", error.message);
    process.exit(1);
  }
});
program.command("types").description("Generate TypeScript types from a data file").argument("<input>", "Input data file (JSON, NDJSON, or CSV)").option("-n, --sample <count>", "Number of records to sample", "1000").option("-f, --format <format>", "Input format (json, ndjson, csv)").option("-o, --output <file>", "Output file (default: stdout)").action(async (input2, options) => {
  try {
    await types(input2, {
      sample: parseInt(options.sample, 10),
      format: options.format,
      output: options.output
    });
  } catch (error) {
    console.error("Error:", error.message);
    process.exit(1);
  }
});
program.parse();
//# sourceMappingURL=index.js.map