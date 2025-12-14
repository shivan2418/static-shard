/**
 * Schema inference from data records
 */

import type { DataRecord, FieldSchema, FieldStats, FieldType, Schema } from "../../types/index.js";

/**
 * Infer the type of a value
 */
function inferType(value: unknown): FieldType {
  if (value === null || value === undefined) {
    return "null";
  }

  const type = typeof value;

  if (type === "string") {
    // Check if it's a date string
    if (isDateString(value as string)) {
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

  // Arrays and objects treated as string (JSON serialized)
  return "string";
}

/**
 * Check if a string looks like a date
 */
function isDateString(value: string): boolean {
  // ISO 8601 date patterns
  const isoPattern = /^\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(\.\d{3})?(Z|[+-]\d{2}:?\d{2})?)?$/;
  if (isoPattern.test(value)) {
    const date = new Date(value);
    return !isNaN(date.getTime());
  }
  return false;
}

/**
 * Merge two field types, returning the more general type
 */
function mergeTypes(type1: FieldType, type2: FieldType): FieldType {
  if (type1 === type2) return type1;
  if (type1 === "null") return type2;
  if (type2 === "null") return type1;

  // If types differ, fall back to string
  return "string";
}

/**
 * Collect statistics for a field across all records
 */
class FieldStatsCollector {
  private values = new Set<string>();
  private min: string | number | undefined;
  private max: string | number | undefined;
  private nullCount = 0;
  private count = 0;
  private type: FieldType = "null";
  private sampleValues: (string | number | boolean | null)[] = [];

  add(value: unknown): void {
    this.count++;

    if (value === null || value === undefined) {
      this.nullCount++;
      return;
    }

    const valueType = inferType(value);
    this.type = mergeTypes(this.type, valueType);

    // Track unique values for cardinality (limit to avoid memory issues)
    if (this.values.size < 10000) {
      this.values.add(String(value));
    }

    // Collect sample values
    if (this.sampleValues.length < 5 && !this.sampleValues.includes(value as string | number | boolean | null)) {
      this.sampleValues.push(value as string | number | boolean | null);
    }

    // Track min/max for numbers and dates
    if (typeof value === "number") {
      if (this.min === undefined || value < (this.min as number)) {
        this.min = value;
      }
      if (this.max === undefined || value > (this.max as number)) {
        this.max = value;
      }
    } else if (typeof value === "string" && (this.type === "date" || this.type === "string")) {
      if (this.min === undefined || value < this.min) {
        this.min = value;
      }
      if (this.max === undefined || value > this.max) {
        this.max = value;
      }
    }
  }

  getStats(): FieldStats {
    return {
      min: this.min,
      max: this.max,
      cardinality: this.values.size,
      nullCount: this.nullCount,
      sampleValues: this.sampleValues,
    };
  }

  getType(): FieldType {
    return this.type;
  }

  isNullable(): boolean {
    return this.nullCount > 0;
  }
}

/**
 * Infer schema from a set of records
 */
export function inferSchema(records: DataRecord[]): Schema {
  if (records.length === 0) {
    return { fields: [], primaryField: null };
  }

  // Collect all field names
  const fieldNames = new Set<string>();
  for (const record of records) {
    for (const key of Object.keys(record)) {
      fieldNames.add(key);
    }
  }

  // Collect stats for each field
  const collectors = new Map<string, FieldStatsCollector>();
  for (const name of fieldNames) {
    collectors.set(name, new FieldStatsCollector());
  }

  for (const record of records) {
    for (const name of fieldNames) {
      const collector = collectors.get(name)!;
      collector.add(record[name]);
    }
  }

  // Build field schemas
  const fields: FieldSchema[] = [];
  let primaryField: string | null = null;

  for (const name of fieldNames) {
    const collector = collectors.get(name)!;
    const stats = collector.getStats();
    const type = collector.getType();

    // Determine if field should be indexed
    // Index low-cardinality fields (good for filtering)
    const cardinality = stats.cardinality;
    const isLowCardinality = cardinality <= 1000 && cardinality < records.length * 0.5;
    const indexed = isLowCardinality;

    // Detect potential primary key
    // High cardinality, not nullable, unique values
    if (
      primaryField === null &&
      stats.cardinality === records.length &&
      !collector.isNullable() &&
      (name.toLowerCase().includes("id") || name.toLowerCase() === "key")
    ) {
      primaryField = name;
    }

    fields.push({
      name,
      type,
      nullable: collector.isNullable(),
      indexed,
      stats,
    });
  }

  // Sort fields: primary key first, then alphabetically
  fields.sort((a, b) => {
    if (a.name === primaryField) return -1;
    if (b.name === primaryField) return 1;
    return a.name.localeCompare(b.name);
  });

  return { fields, primaryField };
}

/**
 * Suggest the best field to chunk by
 */
export function suggestChunkField(schema: Schema, records: DataRecord[]): string | null {
  // If there's a primary field, chunk by it
  if (schema.primaryField) {
    return schema.primaryField;
  }

  // Look for a good chunking candidate:
  // - High cardinality (spreads data across chunks)
  // - Numeric or date (supports range queries)
  // - Not nullable
  let bestField: string | null = null;
  let bestScore = 0;

  for (const field of schema.fields) {
    if (field.nullable) continue;

    let score = 0;

    // Prefer numeric/date fields
    if (field.type === "number" || field.type === "date") {
      score += 50;
    }

    // Prefer higher cardinality (but not too high)
    const cardinalityRatio = field.stats.cardinality / records.length;
    if (cardinalityRatio > 0.1 && cardinalityRatio <= 1) {
      score += cardinalityRatio * 30;
    }

    // Prefer fields with "id", "key", "date", "time" in name
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

/**
 * Get fields that should be indexed based on cardinality
 */
export function getIndexableFields(schema: Schema): string[] {
  return schema.fields
    .filter((f) => f.indexed)
    .map((f) => f.name);
}
