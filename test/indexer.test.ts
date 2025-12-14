import { describe, it, expect } from "vitest";
import {
  buildInvertedIndices,
  findChunksForValue,
  findChunksForConditions,
  estimateIndexSize,
} from "../src/cli/utils/indexer.js";
import { inferSchema } from "../src/cli/utils/schema.js";
import { chunkRecords } from "../src/cli/utils/chunker.js";
import type { Chunk } from "../src/cli/utils/chunker.js";

describe("indexer", () => {
  // Helper to create test chunks
  function createTestChunks(): Chunk[] {
    return [
      {
        id: "0",
        records: [
          { id: "1", category: "A", status: "active" },
          { id: "2", category: "B", status: "active" },
        ],
        byteSize: 100,
      },
      {
        id: "1",
        records: [
          { id: "3", category: "A", status: "inactive" },
          { id: "4", category: "C", status: "active" },
        ],
        byteSize: 100,
      },
      {
        id: "2",
        records: [
          { id: "5", category: "B", status: "inactive" },
          { id: "6", category: "A", status: "active" },
        ],
        byteSize: 100,
      },
    ];
  }

  describe("buildInvertedIndices", () => {
    it("builds indices for indexed fields", () => {
      const chunks = createTestChunks();
      const allRecords = chunks.flatMap((c) => c.records);
      const schema = inferSchema(allRecords);

      // Mark fields as indexed
      schema.fields.forEach((f) => {
        if (f.name === "category" || f.name === "status") {
          f.indexed = true;
        }
      });

      const indices = buildInvertedIndices(chunks, schema);

      expect(indices).toHaveProperty("category");
      expect(indices).toHaveProperty("status");
      expect(indices).not.toHaveProperty("id"); // Not indexed
    });

    it("maps values to chunk IDs", () => {
      const chunks = createTestChunks();
      const allRecords = chunks.flatMap((c) => c.records);
      const schema = inferSchema(allRecords);
      schema.fields.forEach((f) => (f.indexed = true));

      const indices = buildInvertedIndices(chunks, schema);

      // Category A appears in chunks 0, 1, 2
      expect(indices.category["A"]).toContain("0");
      expect(indices.category["A"]).toContain("1");
      expect(indices.category["A"]).toContain("2");

      // Category B appears in chunks 0, 2
      expect(indices.category["B"]).toContain("0");
      expect(indices.category["B"]).toContain("2");
      expect(indices.category["B"]).not.toContain("1");

      // Category C appears only in chunk 1
      expect(indices.category["C"]).toEqual(["1"]);
    });

    it("handles custom indexed fields list", () => {
      const chunks = createTestChunks();
      const allRecords = chunks.flatMap((c) => c.records);
      const schema = inferSchema(allRecords);

      const indices = buildInvertedIndices(chunks, schema, ["category"]);

      expect(indices).toHaveProperty("category");
      expect(indices).not.toHaveProperty("status");
    });

    it("ignores null values in index", () => {
      const chunks: Chunk[] = [
        {
          id: "0",
          records: [
            { id: "1", category: "A" },
            { id: "2", category: null },
          ],
          byteSize: 100,
        },
      ];
      const allRecords = chunks.flatMap((c) => c.records);
      const schema = inferSchema(allRecords);
      schema.fields.forEach((f) => (f.indexed = true));

      const indices = buildInvertedIndices(chunks, schema);

      expect(indices.category["A"]).toEqual(["0"]);
      expect(indices.category["null"]).toBeUndefined();
    });

    it("skips high-cardinality fields", () => {
      // Create chunks with many unique values
      const chunks: Chunk[] = [];
      for (let i = 0; i < 100; i++) {
        chunks.push({
          id: String(i),
          records: Array.from({ length: 200 }, (_, j) => ({
            id: String(i * 200 + j),
            uniqueValue: `value-${i * 200 + j}`,
          })),
          byteSize: 1000,
        });
      }

      const allRecords = chunks.flatMap((c) => c.records);
      const schema = inferSchema(allRecords.slice(0, 1000)); // Sample
      schema.fields.forEach((f) => (f.indexed = true));

      const indices = buildInvertedIndices(chunks, schema);

      // uniqueValue has too many values, should be skipped
      expect(indices.uniqueValue).toBeUndefined();
    });
  });

  describe("findChunksForValue", () => {
    it("returns chunk IDs for indexed value", () => {
      const indices = {
        category: {
          A: ["0", "1"],
          B: ["0", "2"],
        },
      };

      const chunks = findChunksForValue(indices, "category", "A");
      expect(chunks).toEqual(["0", "1"]);
    });

    it("returns empty array for non-existent value", () => {
      const indices = {
        category: {
          A: ["0"],
        },
      };

      const chunks = findChunksForValue(indices, "category", "Z");
      expect(chunks).toEqual([]);
    });

    it("returns null for non-indexed field", () => {
      const indices = {
        category: {
          A: ["0"],
        },
      };

      const chunks = findChunksForValue(indices, "status", "active");
      expect(chunks).toBeNull();
    });

    it("handles numeric values", () => {
      const indices = {
        count: {
          "5": ["0"],
          "10": ["1"],
        },
      };

      const chunks = findChunksForValue(indices, "count", 5);
      expect(chunks).toEqual(["0"]);
    });
  });

  describe("findChunksForConditions", () => {
    const indices = {
      category: {
        A: ["0", "1", "2"],
        B: ["0", "2"],
        C: ["1"],
      },
      status: {
        active: ["0", "1"],
        inactive: ["1", "2"],
      },
    };

    it("returns intersection for multiple conditions", () => {
      const chunks = findChunksForConditions(indices, {
        category: "A",
        status: "active",
      });

      // A is in [0,1,2], active is in [0,1] → intersection is [0,1]
      expect(chunks).toContain("0");
      expect(chunks).toContain("1");
      expect(chunks).not.toContain("2");
    });

    it("returns empty array when no intersection", () => {
      const chunks = findChunksForConditions(indices, {
        category: "C", // Only in chunk 1
        status: "inactive", // In chunks 1, 2 → intersection is [1]
      });

      expect(chunks).toEqual(["1"]);
    });

    it("returns null when no indexed fields in conditions", () => {
      const chunks = findChunksForConditions(indices, {
        unknownField: "value",
      });

      expect(chunks).toBeNull();
    });

    it("ignores non-indexed fields in conditions", () => {
      const chunks = findChunksForConditions(indices, {
        category: "B",
        unknownField: "value", // Should be ignored
      });

      expect(chunks).toEqual(["0", "2"]);
    });

    it("returns empty array for non-existent values", () => {
      const chunks = findChunksForConditions(indices, {
        category: "Z", // Doesn't exist
      });

      expect(chunks).toEqual([]);
    });
  });

  describe("estimateIndexSize", () => {
    it("estimates index size in bytes", () => {
      const indices = {
        category: {
          A: ["0", "1"],
          B: ["2"],
        },
      };

      const size = estimateIndexSize(indices);
      expect(size).toBeGreaterThan(0);
      expect(size).toBe(JSON.stringify(indices).length);
    });

    it("returns 2 for empty indices", () => {
      const size = estimateIndexSize({});
      expect(size).toBe(2); // "{}"
    });
  });

  describe("integration", () => {
    it("works with real chunked data", () => {
      // Create realistic test data
      const records = [];
      for (let i = 0; i < 100; i++) {
        records.push({
          id: String(i),
          category: ["electronics", "clothing", "food"][i % 3],
          status: i % 4 === 0 ? "inactive" : "active",
          price: Math.floor(Math.random() * 1000),
        });
      }

      const schema = inferSchema(records);
      schema.fields.forEach((f) => {
        if (f.name === "category" || f.name === "status") {
          f.indexed = true;
        }
      });

      // Chunk the data
      const chunks = chunkRecords(records, schema, {
        targetSize: 500,
        chunkBy: "id",
      });

      // Build indices
      const indices = buildInvertedIndices(chunks, schema);

      // Verify we can find electronics
      const electronicsChunks = findChunksForValue(indices, "category", "electronics");
      expect(electronicsChunks).not.toBeNull();
      expect(electronicsChunks!.length).toBeGreaterThan(0);

      // Verify intersection works - returns candidate chunks
      const activeElectronics = findChunksForConditions(indices, {
        category: "electronics",
        status: "active",
      });
      expect(activeElectronics).not.toBeNull();

      // Verify that candidate chunks are a subset of both individual conditions
      const electronicsOnly = findChunksForValue(indices, "category", "electronics")!;
      const activeOnly = findChunksForValue(indices, "status", "active")!;

      for (const chunkId of activeElectronics!) {
        // Each chunk should be in both individual sets
        expect(electronicsOnly).toContain(chunkId);
        expect(activeOnly).toContain(chunkId);
      }

      // Verify the chunks exist
      for (const chunkId of activeElectronics!) {
        const chunk = chunks.find((c) => c.id === chunkId);
        expect(chunk).toBeDefined();
      }
    });
  });
});
