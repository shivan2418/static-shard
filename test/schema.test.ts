import { describe, it, expect } from "vitest";
import {
  inferSchema,
  suggestChunkField,
  getIndexableFields,
} from "../src/cli/utils/schema.js";

describe("schema", () => {
  describe("inferSchema", () => {
    it("returns empty schema for empty records", () => {
      const schema = inferSchema([]);
      expect(schema.fields).toHaveLength(0);
      expect(schema.primaryField).toBeNull();
    });

    it("infers string fields", () => {
      const schema = inferSchema([
        { name: "Alice" },
        { name: "Bob" },
      ]);

      expect(schema.fields).toHaveLength(1);
      expect(schema.fields[0].name).toBe("name");
      expect(schema.fields[0].type).toBe("string");
    });

    it("infers number fields", () => {
      const schema = inferSchema([
        { age: 30 },
        { age: 25 },
      ]);

      expect(schema.fields[0].type).toBe("number");
      expect(schema.fields[0].stats.min).toBe(25);
      expect(schema.fields[0].stats.max).toBe(30);
    });

    it("infers boolean fields", () => {
      const schema = inferSchema([
        { active: true },
        { active: false },
      ]);

      expect(schema.fields[0].type).toBe("boolean");
    });

    it("infers date fields from ISO strings", () => {
      const schema = inferSchema([
        { createdAt: "2024-01-15T10:00:00Z" },
        { createdAt: "2024-01-16T11:00:00Z" },
      ]);

      expect(schema.fields[0].type).toBe("date");
    });

    it("handles nullable fields", () => {
      const schema = inferSchema([
        { name: "Alice" },
        { name: null },
        { name: "Charlie" },
      ]);

      expect(schema.fields[0].nullable).toBe(true);
      expect(schema.fields[0].stats.nullCount).toBe(1);
    });

    it("handles missing fields as nullable", () => {
      const schema = inferSchema([
        { name: "Alice", age: 30 },
        { name: "Bob" }, // missing age
      ]);

      const ageField = schema.fields.find((f) => f.name === "age");
      expect(ageField?.nullable).toBe(true);
    });

    it("calculates cardinality", () => {
      const schema = inferSchema([
        { status: "active" },
        { status: "active" },
        { status: "inactive" },
        { status: "pending" },
      ]);

      expect(schema.fields[0].stats.cardinality).toBe(3);
    });

    it("detects primary field from 'id' column", () => {
      const schema = inferSchema([
        { id: "1", name: "Alice" },
        { id: "2", name: "Bob" },
        { id: "3", name: "Charlie" },
      ]);

      expect(schema.primaryField).toBe("id");
    });

    it("detects primary field from 'userId' column", () => {
      const schema = inferSchema([
        { userId: "u1", name: "Alice" },
        { userId: "u2", name: "Bob" },
      ]);

      expect(schema.primaryField).toBe("userId");
    });

    it("marks low-cardinality fields as indexed", () => {
      const records = [];
      for (let i = 0; i < 100; i++) {
        records.push({
          id: String(i),
          category: i % 5 === 0 ? "A" : i % 5 === 1 ? "B" : i % 5 === 2 ? "C" : i % 5 === 3 ? "D" : "E",
        });
      }

      const schema = inferSchema(records);
      const categoryField = schema.fields.find((f) => f.name === "category");
      expect(categoryField?.indexed).toBe(true);
    });

    it("does not mark high-cardinality fields as indexed", () => {
      const records = [];
      for (let i = 0; i < 100; i++) {
        records.push({ id: String(i), name: `User ${i}` });
      }

      const schema = inferSchema(records);
      const nameField = schema.fields.find((f) => f.name === "name");
      expect(nameField?.indexed).toBe(false);
    });

    it("collects sample values", () => {
      const schema = inferSchema([
        { name: "Alice" },
        { name: "Bob" },
        { name: "Charlie" },
      ]);

      expect(schema.fields[0].stats.sampleValues).toContain("Alice");
      expect(schema.fields[0].stats.sampleValues).toContain("Bob");
      expect(schema.fields[0].stats.sampleValues).toContain("Charlie");
    });

    it("handles mixed types by falling back to string", () => {
      const schema = inferSchema([
        { value: "text" },
        { value: 123 },
      ]);

      expect(schema.fields[0].type).toBe("string");
    });
  });

  describe("suggestChunkField", () => {
    it("returns primary field if present", () => {
      const schema = inferSchema([
        { id: "1", name: "Alice" },
        { id: "2", name: "Bob" },
      ]);

      const suggestion = suggestChunkField(schema, []);
      expect(suggestion).toBe("id");
    });

    it("prefers numeric fields", () => {
      const records = [
        { name: "Alice", timestamp: 1000 },
        { name: "Bob", timestamp: 2000 },
      ];
      const schema = inferSchema(records);
      schema.primaryField = null; // Remove primary field

      const suggestion = suggestChunkField(schema, records);
      expect(suggestion).toBe("timestamp");
    });

    it("prefers fields with 'date' in name", () => {
      const records = [
        { name: "Alice", createdDate: "2024-01-01" },
        { name: "Bob", createdDate: "2024-01-02" },
      ];
      const schema = inferSchema(records);
      schema.primaryField = null;

      const suggestion = suggestChunkField(schema, records);
      expect(suggestion).toBe("createdDate");
    });

    it("returns null when no good candidate", () => {
      const records = [{ value: null }, { value: null }];
      const schema = inferSchema(records);

      const suggestion = suggestChunkField(schema, records);
      expect(suggestion).toBeNull();
    });
  });

  describe("getIndexableFields", () => {
    it("returns fields marked as indexed", () => {
      const records = [];
      for (let i = 0; i < 100; i++) {
        records.push({
          id: String(i),
          category: i % 3 === 0 ? "A" : i % 3 === 1 ? "B" : "C",
          status: i % 2 === 0 ? "active" : "inactive",
        });
      }

      const schema = inferSchema(records);
      const indexable = getIndexableFields(schema);

      expect(indexable).toContain("category");
      expect(indexable).toContain("status");
      expect(indexable).not.toContain("id"); // High cardinality
    });

    it("returns empty array when no indexable fields", () => {
      const records = [
        { id: "1", name: "Alice" },
        { id: "2", name: "Bob" },
      ];
      const schema = inferSchema(records);

      const indexable = getIndexableFields(schema);
      expect(indexable).toHaveLength(0);
    });
  });
});
