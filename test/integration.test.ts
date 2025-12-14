import { describe, it, expect, beforeAll, afterAll } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import { build } from "../src/cli/commands/build.js";

const testDir = path.join(__dirname, "integration-output");
const fixturesDir = path.join(__dirname, "fixtures");

describe("integration", () => {
  beforeAll(async () => {
    await fs.promises.mkdir(testDir, { recursive: true });
    await fs.promises.mkdir(fixturesDir, { recursive: true });
  });

  afterAll(async () => {
    // Clean up
    await fs.promises.rm(testDir, { recursive: true, force: true });
  });

  describe("build command", () => {
    it("builds from JSON file", async () => {
      // Create test data
      const testData = [];
      for (let i = 0; i < 50; i++) {
        testData.push({
          id: `user-${i}`,
          name: `User ${i}`,
          age: 20 + (i % 50),
          city: ["New York", "Los Angeles", "Chicago"][i % 3],
          active: i % 3 !== 0,
          createdAt: `2024-01-${String(1 + (i % 28)).padStart(2, "0")}T10:00:00Z`,
        });
      }

      const inputPath = path.join(fixturesDir, "users.json");
      await fs.promises.writeFile(inputPath, JSON.stringify(testData));

      const outputDir = path.join(testDir, "json-output");

      const result = await build(inputPath, {
        output: outputDir,
        chunkSize: "1kb", // Small chunks for testing
        chunkBy: "id",
        index: "city,active",
      });

      // Verify result
      expect(result.totalRecords).toBe(50);
      expect(result.chunkCount).toBeGreaterThan(1);

      // Verify output files exist
      expect(fs.existsSync(path.join(outputDir, "manifest.json"))).toBe(true);
      expect(fs.existsSync(path.join(outputDir, "client.ts"))).toBe(true);
      expect(fs.existsSync(path.join(outputDir, "chunks"))).toBe(true);

      // Verify manifest structure
      const manifest = JSON.parse(
        await fs.promises.readFile(path.join(outputDir, "manifest.json"), "utf-8")
      );

      expect(manifest.version).toBe("1.0.0");
      expect(manifest.totalRecords).toBe(50);
      expect(manifest.schema.fields).toHaveLength(6);
      expect(manifest.schema.primaryField).toBe("id");
      expect(manifest.chunks.length).toBe(result.chunkCount);

      // Verify indices
      expect(manifest.indices).toHaveProperty("city");
      expect(manifest.indices).toHaveProperty("active");
      expect(manifest.indices.city["New York"]).toBeDefined();
      expect(manifest.indices.active["true"]).toBeDefined();
      expect(manifest.indices.active["false"]).toBeDefined();

      // Verify chunks exist and contain valid JSON
      for (const chunkMeta of manifest.chunks) {
        const chunkPath = path.join(outputDir, chunkMeta.path);
        expect(fs.existsSync(chunkPath)).toBe(true);

        const chunkData = JSON.parse(
          await fs.promises.readFile(chunkPath, "utf-8")
        );
        expect(Array.isArray(chunkData)).toBe(true);
        expect(chunkData.length).toBe(chunkMeta.count);
      }

      // Verify all records are accounted for
      let totalRecordsInChunks = 0;
      for (const chunkMeta of manifest.chunks) {
        totalRecordsInChunks += chunkMeta.count;
      }
      expect(totalRecordsInChunks).toBe(50);

      // Verify client.ts is valid TypeScript (basic check)
      const clientCode = await fs.promises.readFile(
        path.join(outputDir, "client.ts"),
        "utf-8"
      );
      expect(clientCode).toContain("interface Record");
      expect(clientCode).toContain("class StaticShardClient");
      expect(clientCode).toContain("export function createClient");
    });

    it("builds from CSV file", async () => {
      const csvContent = `id,name,category,price
1,Product A,electronics,99.99
2,Product B,clothing,49.99
3,Product C,electronics,149.99
4,Product D,food,9.99
5,Product E,clothing,79.99`;

      const inputPath = path.join(fixturesDir, "products.csv");
      await fs.promises.writeFile(inputPath, csvContent);

      const outputDir = path.join(testDir, "csv-output");

      const result = await build(inputPath, {
        output: outputDir,
        chunkSize: "5mb",
      });

      expect(result.totalRecords).toBe(5);

      // Verify schema detected types correctly
      const manifest = JSON.parse(
        await fs.promises.readFile(path.join(outputDir, "manifest.json"), "utf-8")
      );

      const priceField = manifest.schema.fields.find(
        (f: { name: string }) => f.name === "price"
      );
      expect(priceField.type).toBe("number");

      const categoryField = manifest.schema.fields.find(
        (f: { name: string }) => f.name === "category"
      );
      expect(categoryField.type).toBe("string");
    });

    it("builds from NDJSON file", async () => {
      const ndjsonContent = `{"id":"1","event":"click","timestamp":"2024-01-01T10:00:00Z"}
{"id":"2","event":"view","timestamp":"2024-01-01T10:01:00Z"}
{"id":"3","event":"click","timestamp":"2024-01-01T10:02:00Z"}
{"id":"4","event":"purchase","timestamp":"2024-01-01T10:03:00Z"}`;

      const inputPath = path.join(fixturesDir, "events.ndjson");
      await fs.promises.writeFile(inputPath, ndjsonContent);

      const outputDir = path.join(testDir, "ndjson-output");

      const result = await build(inputPath, {
        output: outputDir,
        chunkSize: "5mb",
        format: "ndjson",
      });

      expect(result.totalRecords).toBe(4);

      const manifest = JSON.parse(
        await fs.promises.readFile(path.join(outputDir, "manifest.json"), "utf-8")
      );

      const timestampField = manifest.schema.fields.find(
        (f: { name: string }) => f.name === "timestamp"
      );
      expect(timestampField.type).toBe("date");
    });

    it("generates correct field ranges in chunks", async () => {
      const testData = [
        { id: 1, value: 100 },
        { id: 2, value: 200 },
        { id: 3, value: 300 },
        { id: 4, value: 400 },
        { id: 5, value: 500 },
        { id: 6, value: 600 },
      ];

      const inputPath = path.join(fixturesDir, "values.json");
      await fs.promises.writeFile(inputPath, JSON.stringify(testData));

      const outputDir = path.join(testDir, "ranges-output");

      await build(inputPath, {
        output: outputDir,
        chunkSize: "100b", // Force multiple chunks
        chunkBy: "id",
      });

      const manifest = JSON.parse(
        await fs.promises.readFile(path.join(outputDir, "manifest.json"), "utf-8")
      );

      // Each chunk should have accurate min/max for the value field
      for (const chunkMeta of manifest.chunks) {
        const chunkPath = path.join(outputDir, chunkMeta.path);
        const chunkData = JSON.parse(
          await fs.promises.readFile(chunkPath, "utf-8")
        );

        const values = chunkData.map((r: { value: number }) => r.value);
        const actualMin = Math.min(...values);
        const actualMax = Math.max(...values);

        expect(chunkMeta.fieldRanges.value.min).toBe(actualMin);
        expect(chunkMeta.fieldRanges.value.max).toBe(actualMax);
      }
    });

    it("handles nullable fields", async () => {
      const testData = [
        { id: "1", name: "Alice", nickname: "Ali" },
        { id: "2", name: "Bob", nickname: null },
        { id: "3", name: "Charlie" }, // nickname is missing
      ];

      const inputPath = path.join(fixturesDir, "nullable.json");
      await fs.promises.writeFile(inputPath, JSON.stringify(testData));

      const outputDir = path.join(testDir, "nullable-output");

      await build(inputPath, {
        output: outputDir,
        chunkSize: "5mb",
      });

      const manifest = JSON.parse(
        await fs.promises.readFile(path.join(outputDir, "manifest.json"), "utf-8")
      );

      const nicknameField = manifest.schema.fields.find(
        (f: { name: string }) => f.name === "nickname"
      );
      expect(nicknameField.nullable).toBe(true);
    });

    it("auto-detects primary field", async () => {
      const testData = [
        { userId: "u1", email: "a@test.com" },
        { userId: "u2", email: "b@test.com" },
        { userId: "u3", email: "c@test.com" },
      ];

      const inputPath = path.join(fixturesDir, "users-id.json");
      await fs.promises.writeFile(inputPath, JSON.stringify(testData));

      const outputDir = path.join(testDir, "primary-output");

      await build(inputPath, {
        output: outputDir,
        chunkSize: "5mb",
      });

      const manifest = JSON.parse(
        await fs.promises.readFile(path.join(outputDir, "manifest.json"), "utf-8")
      );

      expect(manifest.schema.primaryField).toBe("userId");
    });

    it("creates indices only for specified fields", async () => {
      const testData = [];
      for (let i = 0; i < 20; i++) {
        testData.push({
          id: String(i),
          category: ["A", "B", "C"][i % 3],
          status: ["active", "inactive"][i % 2],
          priority: ["low", "medium", "high"][i % 3],
        });
      }

      const inputPath = path.join(fixturesDir, "selective-index.json");
      await fs.promises.writeFile(inputPath, JSON.stringify(testData));

      const outputDir = path.join(testDir, "selective-output");

      await build(inputPath, {
        output: outputDir,
        chunkSize: "5mb",
        index: "category", // Only index category
      });

      const manifest = JSON.parse(
        await fs.promises.readFile(path.join(outputDir, "manifest.json"), "utf-8")
      );

      expect(manifest.indices).toHaveProperty("category");
      expect(manifest.indices).not.toHaveProperty("status");
      expect(manifest.indices).not.toHaveProperty("priority");
    });

    it("handles large datasets", async () => {
      // Create a moderately large dataset
      const testData = [];
      for (let i = 0; i < 1000; i++) {
        testData.push({
          id: `item-${i}`,
          name: `Item ${i}`,
          description: `This is a description for item ${i}. `.repeat(5),
          category: ["electronics", "clothing", "food", "home", "sports"][i % 5],
          price: Math.floor(Math.random() * 10000) / 100,
          inStock: i % 3 !== 0,
        });
      }

      const inputPath = path.join(fixturesDir, "large.json");
      await fs.promises.writeFile(inputPath, JSON.stringify(testData));

      const outputDir = path.join(testDir, "large-output");

      const result = await build(inputPath, {
        output: outputDir,
        chunkSize: "10kb",
        chunkBy: "id",
        index: "category,inStock",
      });

      expect(result.totalRecords).toBe(1000);
      expect(result.chunkCount).toBeGreaterThan(10); // Should have many chunks

      // Verify data integrity
      const manifest = JSON.parse(
        await fs.promises.readFile(path.join(outputDir, "manifest.json"), "utf-8")
      );

      let totalRecords = 0;
      const allIds = new Set<string>();

      for (const chunkMeta of manifest.chunks) {
        const chunkPath = path.join(outputDir, chunkMeta.path);
        const chunkData = JSON.parse(
          await fs.promises.readFile(chunkPath, "utf-8")
        );

        totalRecords += chunkData.length;
        for (const record of chunkData) {
          allIds.add(record.id);
        }
      }

      expect(totalRecords).toBe(1000);
      expect(allIds.size).toBe(1000); // All unique IDs preserved
    });
  });
});
