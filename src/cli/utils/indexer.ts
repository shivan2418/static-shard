/**
 * Index generation for efficient query pruning
 */

import type { Chunk } from "./chunker.js";
import type { Schema } from "../../types/index.js";

/**
 * Inverted index: field -> value -> chunk IDs
 */
export type InvertedIndex = Record<string, Record<string, string[]>>;

/**
 * Build inverted indices for indexed fields
 * Maps each unique value to the chunk IDs that contain it
 */
export function buildInvertedIndices(
  chunks: Chunk[],
  schema: Schema,
  indexedFields?: string[]
): InvertedIndex {
  const indices: InvertedIndex = {};

  // Get fields to index
  const fieldsToIndex = indexedFields
    ? schema.fields.filter((f) => indexedFields.includes(f.name))
    : schema.fields.filter((f) => f.indexed);

  for (const field of fieldsToIndex) {
    const fieldIndex: Record<string, string[]> = {};

    for (const chunk of chunks) {
      const valuesInChunk = new Set<string>();

      for (const record of chunk.records) {
        const value = record[field.name];
        if (value !== null && value !== undefined) {
          valuesInChunk.add(String(value));
        }
      }

      // Add chunk ID to each value's list
      for (const value of valuesInChunk) {
        if (!fieldIndex[value]) {
          fieldIndex[value] = [];
        }
        fieldIndex[value].push(chunk.id);
      }
    }

    // Only include index if it would be useful
    // (not too many unique values, which would make the index huge)
    const uniqueValues = Object.keys(fieldIndex).length;
    if (uniqueValues <= 10000) {
      indices[field.name] = fieldIndex;
    }
  }

  return indices;
}

/**
 * Find chunk IDs that might contain records matching a field value
 */
export function findChunksForValue(
  indices: InvertedIndex,
  fieldName: string,
  value: unknown
): string[] | null {
  const fieldIndex = indices[fieldName];
  if (!fieldIndex) {
    // Field not indexed, return null to indicate all chunks must be searched
    return null;
  }

  const stringValue = String(value);
  return fieldIndex[stringValue] || [];
}

/**
 * Find chunk IDs that match multiple field conditions (AND logic)
 */
export function findChunksForConditions(
  indices: InvertedIndex,
  conditions: Record<string, unknown>
): string[] | null {
  let resultChunks: Set<string> | null = null;

  for (const [field, value] of Object.entries(conditions)) {
    const chunks = findChunksForValue(indices, field, value);

    if (chunks === null) {
      // This field isn't indexed, can't narrow down
      continue;
    }

    if (resultChunks === null) {
      resultChunks = new Set(chunks);
    } else {
      // Intersect with existing results
      resultChunks = new Set([...resultChunks].filter((c) => chunks.includes(c)));
    }

    // Early exit if no chunks match
    if (resultChunks.size === 0) {
      return [];
    }
  }

  return resultChunks ? [...resultChunks] : null;
}

/**
 * Estimate index size in bytes
 */
export function estimateIndexSize(indices: InvertedIndex): number {
  return JSON.stringify(indices).length;
}
