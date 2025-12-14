import { describe, it, expect } from "vitest";
import {
  chunkRecords,
  calculateFieldRanges,
  generateChunkMeta,
  serializeChunk,
  calculateChunkCount,
} from "../src/cli/utils/chunker.js";
import { inferSchema } from "../src/cli/utils/schema.js";

describe("chunker", () => {
  describe("chunkRecords", () => {
    it("creates a single chunk for small data", () => {
      const records = [
        { id: "1", name: "Alice" },
        { id: "2", name: "Bob" },
      ];
      const schema = inferSchema(records);

      const chunks = chunkRecords(records, schema, {
        targetSize: 10000, // 10KB - much larger than our data
      });

      expect(chunks).toHaveLength(1);
      expect(chunks[0].records).toHaveLength(2);
    });

    it("splits data into multiple chunks", () => {
      const records = [];
      for (let i = 0; i < 100; i++) {
        records.push({ id: String(i), name: `User ${i}`, data: "x".repeat(100) });
      }
      const schema = inferSchema(records);

      const chunks = chunkRecords(records, schema, {
        targetSize: 500, // Very small chunks
      });

      expect(chunks.length).toBeGreaterThan(1);

      // Verify all records are present
      const totalRecords = chunks.reduce((sum, c) => sum + c.records.length, 0);
      expect(totalRecords).toBe(100);
    });

    it("sorts by chunkBy field", () => {
      const records = [
        { id: "3", value: 30 },
        { id: "1", value: 10 },
        { id: "2", value: 20 },
      ];
      const schema = inferSchema(records);

      const chunks = chunkRecords(records, schema, {
        targetSize: 10000,
        chunkBy: "id",
      });

      expect(chunks[0].records[0].id).toBe("1");
      expect(chunks[0].records[1].id).toBe("2");
      expect(chunks[0].records[2].id).toBe("3");
    });

    it("sorts numerically when chunkBy is numeric", () => {
      const records = [
        { id: 10, name: "Ten" },
        { id: 2, name: "Two" },
        { id: 1, name: "One" },
      ];
      const schema = inferSchema(records);

      const chunks = chunkRecords(records, schema, {
        targetSize: 10000,
        chunkBy: "id",
      });

      expect(chunks[0].records[0].id).toBe(1);
      expect(chunks[0].records[1].id).toBe(2);
      expect(chunks[0].records[2].id).toBe(10);
    });

    it("handles null values in chunkBy field", () => {
      const records = [
        { id: "2", name: "Bob" },
        { id: null, name: "Unknown" },
        { id: "1", name: "Alice" },
      ];
      const schema = inferSchema(records);

      const chunks = chunkRecords(records, schema, {
        targetSize: 10000,
        chunkBy: "id",
      });

      // Null should sort first
      expect(chunks[0].records[0].id).toBeNull();
    });

    it("assigns sequential chunk IDs", () => {
      const records = [];
      for (let i = 0; i < 50; i++) {
        records.push({ id: String(i), data: "x".repeat(100) });
      }
      const schema = inferSchema(records);

      const chunks = chunkRecords(records, schema, {
        targetSize: 500,
      });

      chunks.forEach((chunk, index) => {
        expect(chunk.id).toBe(String(index));
      });
    });

    it("estimates byte size", () => {
      const records = [{ id: "1", name: "Alice" }];
      const schema = inferSchema(records);

      const chunks = chunkRecords(records, schema, {
        targetSize: 10000,
      });

      expect(chunks[0].byteSize).toBeGreaterThan(0);
      // Should be close to JSON.stringify([record]).length
      const expectedSize = JSON.stringify(records).length;
      expect(Math.abs(chunks[0].byteSize - expectedSize)).toBeLessThan(10);
    });
  });

  describe("calculateFieldRanges", () => {
    it("calculates min/max for numeric fields", () => {
      const records = [
        { age: 25 },
        { age: 35 },
        { age: 30 },
      ];
      const schema = inferSchema(records);

      const ranges = calculateFieldRanges(records, schema);

      expect(ranges.age.min).toBe(25);
      expect(ranges.age.max).toBe(35);
    });

    it("calculates min/max for string fields", () => {
      const records = [
        { name: "Charlie" },
        { name: "Alice" },
        { name: "Bob" },
      ];
      const schema = inferSchema(records);

      const ranges = calculateFieldRanges(records, schema);

      expect(ranges.name.min).toBe("Alice");
      expect(ranges.name.max).toBe("Charlie");
    });

    it("calculates min/max for date fields", () => {
      const records = [
        { date: "2024-01-15" },
        { date: "2024-01-10" },
        { date: "2024-01-20" },
      ];
      const schema = inferSchema(records);

      const ranges = calculateFieldRanges(records, schema);

      expect(ranges.date.min).toBe("2024-01-10");
      expect(ranges.date.max).toBe("2024-01-20");
    });

    it("skips null values", () => {
      const records = [
        { value: null },
        { value: 10 },
        { value: 20 },
      ];
      const schema = inferSchema(records);

      const ranges = calculateFieldRanges(records, schema);

      expect(ranges.value.min).toBe(10);
      expect(ranges.value.max).toBe(20);
    });

    it("handles all-null fields", () => {
      const records = [
        { value: null },
        { value: null },
      ];
      const schema = inferSchema(records);

      const ranges = calculateFieldRanges(records, schema);

      expect(ranges.value).toBeUndefined();
    });
  });

  describe("generateChunkMeta", () => {
    it("generates correct metadata", () => {
      const records = [
        { id: "1", age: 25 },
        { id: "2", age: 35 },
      ];
      const schema = inferSchema(records);
      const chunk = {
        id: "0",
        records,
        byteSize: 100,
      };

      const meta = generateChunkMeta(chunk, schema, "chunks");

      expect(meta.id).toBe("0");
      expect(meta.path).toBe("chunks/0.json");
      expect(meta.count).toBe(2);
      expect(meta.byteSize).toBe(100);
      expect(meta.fieldRanges.id.min).toBe("1");
      expect(meta.fieldRanges.id.max).toBe("2");
      expect(meta.fieldRanges.age.min).toBe(25);
      expect(meta.fieldRanges.age.max).toBe(35);
    });
  });

  describe("serializeChunk", () => {
    it("serializes chunk records to JSON", () => {
      const chunk = {
        id: "0",
        records: [{ id: "1" }, { id: "2" }],
        byteSize: 100,
      };

      const json = serializeChunk(chunk);
      const parsed = JSON.parse(json);

      expect(parsed).toHaveLength(2);
      expect(parsed[0].id).toBe("1");
      expect(parsed[1].id).toBe("2");
    });
  });

  describe("calculateChunkCount", () => {
    it("calculates chunk count based on size", () => {
      const count = calculateChunkCount(
        1000, // records
        1000000, // 1MB total
        100000 // 100KB per chunk
      );

      expect(count).toBe(10);
    });

    it("returns at least 1 chunk", () => {
      const count = calculateChunkCount(10, 100, 10000);
      expect(count).toBe(1);
    });

    it("does not exceed record count", () => {
      const count = calculateChunkCount(
        5, // only 5 records
        1000000, // 1MB
        100 // tiny chunks
      );

      expect(count).toBeLessThanOrEqual(5);
    });
  });
});
