/**
 * Data chunking logic for splitting records into smaller files
 */

import type { ChunkMeta, DataRecord, Schema } from "../../types/index.js";

export interface Chunk {
  id: string;
  records: DataRecord[];
  byteSize: number;
}

export interface ChunkOptions {
  targetSize: number; // target bytes per chunk
  chunkBy?: string; // field to sort/chunk by
}

/**
 * Estimate JSON byte size of a record
 */
function estimateRecordSize(record: DataRecord): number {
  return JSON.stringify(record).length;
}

/**
 * Compare two values for sorting
 */
function compareValues(a: unknown, b: unknown): number {
  if (a === b) return 0;
  if (a === null || a === undefined) return -1;
  if (b === null || b === undefined) return 1;

  if (typeof a === "number" && typeof b === "number") {
    return a - b;
  }

  return String(a).localeCompare(String(b));
}

/**
 * Split records into chunks based on target size
 */
export function chunkRecords(
  records: DataRecord[],
  schema: Schema,
  options: ChunkOptions
): Chunk[] {
  const { targetSize, chunkBy } = options;

  // Sort records if chunkBy field is specified
  let sortedRecords = records;
  if (chunkBy) {
    sortedRecords = [...records].sort((a, b) =>
      compareValues(a[chunkBy], b[chunkBy])
    );
  }

  const chunks: Chunk[] = [];
  let currentChunk: DataRecord[] = [];
  let currentSize = 0;
  let chunkIndex = 0;

  // Account for JSON array overhead: [ and ] plus commas
  const arrayOverhead = 2; // []
  const commaOverhead = 1; // ,

  for (const record of sortedRecords) {
    const recordSize = estimateRecordSize(record) + commaOverhead;

    // If adding this record would exceed target size and we have records,
    // start a new chunk
    if (currentSize + recordSize > targetSize && currentChunk.length > 0) {
      chunks.push({
        id: String(chunkIndex),
        records: currentChunk,
        byteSize: currentSize + arrayOverhead,
      });
      chunkIndex++;
      currentChunk = [];
      currentSize = 0;
    }

    currentChunk.push(record);
    currentSize += recordSize;
  }

  // Don't forget the last chunk
  if (currentChunk.length > 0) {
    chunks.push({
      id: String(chunkIndex),
      records: currentChunk,
      byteSize: currentSize + arrayOverhead,
    });
  }

  return chunks;
}

/**
 * Calculate field ranges for a chunk (min/max per field)
 */
export function calculateFieldRanges(
  records: DataRecord[],
  schema: Schema
): ChunkMeta["fieldRanges"] {
  const ranges: ChunkMeta["fieldRanges"] = {};

  for (const field of schema.fields) {
    const values = records
      .map((r) => r[field.name])
      .filter((v) => v !== null && v !== undefined);

    if (values.length === 0) {
      continue;
    }

    // For numbers and dates, calculate actual min/max
    if (field.type === "number" || field.type === "date" || field.type === "string") {
      const sorted = [...values].sort(compareValues);
      ranges[field.name] = {
        min: sorted[0],
        max: sorted[sorted.length - 1],
      };
    }
  }

  return ranges;
}

/**
 * Generate chunk metadata
 */
export function generateChunkMeta(
  chunk: Chunk,
  schema: Schema,
  basePath: string
): ChunkMeta {
  return {
    id: chunk.id,
    path: `${basePath}/${chunk.id}.json`,
    count: chunk.records.length,
    byteSize: chunk.byteSize,
    fieldRanges: calculateFieldRanges(chunk.records, schema),
  };
}

/**
 * Serialize a chunk to JSON string
 */
export function serializeChunk(chunk: Chunk): string {
  return JSON.stringify(chunk.records);
}

/**
 * Calculate optimal chunk count based on data size and target chunk size
 */
export function calculateChunkCount(
  totalRecords: number,
  totalSize: number,
  targetChunkSize: number
): number {
  const estimatedChunks = Math.ceil(totalSize / targetChunkSize);
  // At least 1 chunk, and reasonable upper bound
  return Math.max(1, Math.min(estimatedChunks, totalRecords));
}
