import { describe, it, expect, beforeAll, afterAll } from "vitest";
import * as fs from "node:fs";
import * as path from "node:path";
import {
  detectFormat,
  parseJsonArray,
  parseNdjson,
  parseCsv,
  parseFile,
  parseSize,
} from "../src/cli/utils/parsers.js";

const fixturesDir = path.join(__dirname, "fixtures");

describe("parsers", () => {
  // Create test fixtures
  beforeAll(async () => {
    await fs.promises.mkdir(fixturesDir, { recursive: true });

    // JSON array
    await fs.promises.writeFile(
      path.join(fixturesDir, "test.json"),
      JSON.stringify([
        { id: 1, name: "Alice" },
        { id: 2, name: "Bob" },
      ])
    );

    // NDJSON
    await fs.promises.writeFile(
      path.join(fixturesDir, "test.ndjson"),
      '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'
    );

    // CSV
    await fs.promises.writeFile(
      path.join(fixturesDir, "test.csv"),
      "id,name,age\n1,Alice,30\n2,Bob,25\n"
    );
  });

  describe("detectFormat", () => {
    it("detects NDJSON from extension", () => {
      expect(detectFormat("data.ndjson")).toBe("ndjson");
      expect(detectFormat("data.jsonl")).toBe("ndjson");
    });

    it("detects CSV from extension", () => {
      expect(detectFormat("data.csv")).toBe("csv");
    });

    it("detects JSON array from content", () => {
      const result = detectFormat(path.join(fixturesDir, "test.json"));
      expect(result).toBe("json");
    });

    it("detects NDJSON from content", () => {
      const result = detectFormat(path.join(fixturesDir, "test.ndjson"));
      expect(result).toBe("ndjson");
    });
  });

  describe("parseJsonArray", () => {
    it("parses a JSON array file", async () => {
      const records = await parseJsonArray(path.join(fixturesDir, "test.json"));
      expect(records).toHaveLength(2);
      expect(records[0]).toEqual({ id: 1, name: "Alice" });
      expect(records[1]).toEqual({ id: 2, name: "Bob" });
    });

    it("throws for non-array JSON", async () => {
      const filePath = path.join(fixturesDir, "object.json");
      await fs.promises.writeFile(filePath, '{"key": "value"}');

      await expect(parseJsonArray(filePath)).rejects.toThrow(
        "JSON file must contain an array"
      );
    });
  });

  describe("parseNdjson", () => {
    it("parses an NDJSON file", async () => {
      const records = await parseNdjson(path.join(fixturesDir, "test.ndjson"));
      expect(records).toHaveLength(2);
      expect(records[0]).toEqual({ id: 1, name: "Alice" });
      expect(records[1]).toEqual({ id: 2, name: "Bob" });
    });

    it("calls onRecord callback for each record", async () => {
      const seen: Array<{ record: unknown; index: number }> = [];
      await parseNdjson(path.join(fixturesDir, "test.ndjson"), (record, index) => {
        seen.push({ record, index });
      });

      expect(seen).toHaveLength(2);
      expect(seen[0].index).toBe(0);
      expect(seen[1].index).toBe(1);
    });

    it("skips empty lines", async () => {
      const filePath = path.join(fixturesDir, "with-blanks.ndjson");
      await fs.promises.writeFile(
        filePath,
        '{"id": 1}\n\n{"id": 2}\n\n'
      );

      const records = await parseNdjson(filePath);
      expect(records).toHaveLength(2);
    });

    it("throws for invalid JSON lines", async () => {
      const filePath = path.join(fixturesDir, "invalid.ndjson");
      await fs.promises.writeFile(filePath, '{"id": 1}\n{invalid}\n');

      await expect(parseNdjson(filePath)).rejects.toThrow("Invalid JSON at line");
    });
  });

  describe("parseCsv", () => {
    it("parses a CSV file with headers", async () => {
      const records = await parseCsv(path.join(fixturesDir, "test.csv"));
      expect(records).toHaveLength(2);
      expect(records[0]).toEqual({ id: 1, name: "Alice", age: 30 });
      expect(records[1]).toEqual({ id: 2, name: "Bob", age: 25 });
    });

    it("performs dynamic typing", async () => {
      const records = await parseCsv(path.join(fixturesDir, "test.csv"));
      expect(typeof records[0].id).toBe("number");
      expect(typeof records[0].name).toBe("string");
      expect(typeof records[0].age).toBe("number");
    });
  });

  describe("parseFile", () => {
    it("auto-detects and parses JSON", async () => {
      const { records, format } = await parseFile(
        path.join(fixturesDir, "test.json")
      );
      expect(format).toBe("json");
      expect(records).toHaveLength(2);
    });

    it("auto-detects and parses NDJSON", async () => {
      const { records, format } = await parseFile(
        path.join(fixturesDir, "test.ndjson")
      );
      expect(format).toBe("ndjson");
      expect(records).toHaveLength(2);
    });

    it("auto-detects and parses CSV", async () => {
      const { records, format } = await parseFile(
        path.join(fixturesDir, "test.csv")
      );
      expect(format).toBe("csv");
      expect(records).toHaveLength(2);
    });

    it("uses explicit format when provided", async () => {
      const { format } = await parseFile(
        path.join(fixturesDir, "test.json"),
        "json"
      );
      expect(format).toBe("json");
    });
  });

  describe("parseSize", () => {
    it("parses bytes", () => {
      expect(parseSize("100")).toBe(100);
      expect(parseSize("100b")).toBe(100);
    });

    it("parses kilobytes", () => {
      expect(parseSize("1kb")).toBe(1024);
      expect(parseSize("2.5kb")).toBe(2560);
    });

    it("parses megabytes", () => {
      expect(parseSize("1mb")).toBe(1024 * 1024);
      expect(parseSize("5mb")).toBe(5 * 1024 * 1024);
    });

    it("parses gigabytes", () => {
      expect(parseSize("1gb")).toBe(1024 * 1024 * 1024);
    });

    it("is case insensitive", () => {
      expect(parseSize("5MB")).toBe(5 * 1024 * 1024);
      expect(parseSize("5Mb")).toBe(5 * 1024 * 1024);
    });

    it("throws for invalid format", () => {
      expect(() => parseSize("invalid")).toThrow("Invalid size format");
      expect(() => parseSize("5tb")).toThrow("Invalid size format");
    });
  });

  // Cleanup
  afterAll(async () => {
    const files = await fs.promises.readdir(fixturesDir);
    for (const file of files) {
      if (file !== "sample.json") {
        await fs.promises.unlink(path.join(fixturesDir, file));
      }
    }
  });
});
