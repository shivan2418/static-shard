/**
 * Unit tests for QueryBuilder chainable API
 */
import { describe, it, expect, vi, beforeEach } from "vitest";
import { StaticShardClient, QueryBuilder } from "../src/client/index.js";
import type { Manifest } from "../src/types/index.js";

// Test types
interface Product {
  id: string;
  name: string;
  category: string;
  price: number;
  inStock: boolean;
}

type WhereClause = {
  id?: string | { eq?: string };
  category?: string | { eq?: string; in?: string[] };
  name?: string | { contains?: string };
  price?: number | { gt?: number; gte?: number; lt?: number; lte?: number };
  inStock?: boolean;
};

type SortableField = "id" | "category" | "name" | "price";

// Test data
const testProducts: Product[] = [
  { id: "1", name: "Laptop", category: "electronics", price: 999.99, inStock: true },
  { id: "2", name: "Phone", category: "electronics", price: 599.99, inStock: true },
  { id: "3", name: "Tablet", category: "electronics", price: 399.99, inStock: false },
  { id: "4", name: "Desk", category: "furniture", price: 249.99, inStock: true },
  { id: "5", name: "Chair", category: "furniture", price: 149.99, inStock: true },
  { id: "6", name: "Lamp", category: "furniture", price: 49.99, inStock: false },
];

const mockManifest: Manifest = {
  version: "1.0.0",
  generatedAt: new Date().toISOString(),
  totalRecords: testProducts.length,
  schema: {
    fields: [
      { name: "id", type: "string", nullable: false, indexed: true, stats: { uniqueValues: 6, nullCount: 0 } },
      { name: "name", type: "string", nullable: false, indexed: false, stats: { uniqueValues: 6, nullCount: 0 } },
      { name: "category", type: "string", nullable: false, indexed: true, stats: { uniqueValues: 2, nullCount: 0 } },
      { name: "price", type: "number", nullable: false, indexed: false, stats: { uniqueValues: 6, nullCount: 0, min: 49.99, max: 999.99 } },
      { name: "inStock", type: "boolean", nullable: false, indexed: true, stats: { uniqueValues: 2, nullCount: 0 } },
    ],
    primaryField: "id",
  },
  chunks: [
    {
      id: "chunk-1",
      path: "chunks/chunk-1.json",
      count: 6,
      byteSize: 500,
      fieldRanges: {
        price: { min: 49.99, max: 999.99 },
      },
    },
  ],
  indices: {
    category: {
      electronics: ["chunk-1"],
      furniture: ["chunk-1"],
    },
    inStock: {
      true: ["chunk-1"],
      false: ["chunk-1"],
    },
  },
  config: {
    chunkSize: 5000000,
    chunkBy: "id",
    indexFields: ["category", "inStock"],
  },
};

describe("QueryBuilder", () => {
  let client: StaticShardClient<Product, WhereClause, SortableField>;

  beforeEach(() => {
    client = new StaticShardClient<Product, WhereClause, SortableField>({
      basePath: "http://test.local",
    });

    // Mock fetch
    vi.spyOn(global, "fetch").mockImplementation(async (url) => {
      const urlStr = url.toString();
      if (urlStr.includes("manifest.json")) {
        return new Response(JSON.stringify(mockManifest));
      }
      if (urlStr.includes("chunk-1.json")) {
        return new Response(JSON.stringify(testProducts));
      }
      return new Response("Not found", { status: 404 });
    });
  });

  describe("query()", () => {
    it("returns a QueryBuilder instance", () => {
      const qb = client.query();
      expect(qb).toBeInstanceOf(QueryBuilder);
    });
  });

  describe("method chaining", () => {
    it("where() returns this for chaining", () => {
      const qb = client.query();
      const result = qb.where({ category: "electronics" });
      expect(result).toBe(qb);
    });

    it("orderBy() returns this for chaining", () => {
      const qb = client.query();
      const result = qb.orderBy("price");
      expect(result).toBe(qb);
    });

    it("limit() returns this for chaining", () => {
      const qb = client.query();
      const result = qb.limit(10);
      expect(result).toBe(qb);
    });

    it("offset() returns this for chaining", () => {
      const qb = client.query();
      const result = qb.offset(5);
      expect(result).toBe(qb);
    });

    it("supports full chain", () => {
      const qb = client.query()
        .where({ category: "electronics" })
        .where({ inStock: true })
        .orderBy("price", "desc")
        .limit(10)
        .offset(0);

      expect(qb).toBeInstanceOf(QueryBuilder);
    });
  });

  describe("execute()", () => {
    it("returns all items when no filters", async () => {
      const results = await client.query().execute();
      expect(results).toHaveLength(6);
    });

    it("filters by category", async () => {
      const results = await client.query()
        .where({ category: "electronics" })
        .execute();

      expect(results).toHaveLength(3);
      expect(results.every(p => p.category === "electronics")).toBe(true);
    });

    it("filters with multiple where() calls (AND logic)", async () => {
      const results = await client.query()
        .where({ category: "electronics" })
        .where({ inStock: true })
        .execute();

      expect(results).toHaveLength(2);
      expect(results.every(p => p.category === "electronics" && p.inStock)).toBe(true);
    });

    it("sorts by field ascending", async () => {
      const results = await client.query()
        .orderBy("price", "asc")
        .execute();

      expect(results[0].price).toBe(49.99);
      expect(results[results.length - 1].price).toBe(999.99);
    });

    it("sorts by field descending", async () => {
      const results = await client.query()
        .orderBy("price", "desc")
        .execute();

      expect(results[0].price).toBe(999.99);
      expect(results[results.length - 1].price).toBe(49.99);
    });

    it("limits results", async () => {
      const results = await client.query()
        .limit(3)
        .execute();

      expect(results).toHaveLength(3);
    });

    it("skips results with offset", async () => {
      const all = await client.query().execute();
      const offset = await client.query()
        .offset(2)
        .execute();

      expect(offset).toHaveLength(4);
      expect(offset[0]).toEqual(all[2]);
    });

    it("combines limit and offset for pagination", async () => {
      const page2 = await client.query()
        .orderBy("price", "asc")
        .offset(2)
        .limit(2)
        .execute();

      expect(page2).toHaveLength(2);
      expect(page2[0].price).toBe(249.99); // 3rd cheapest
      expect(page2[1].price).toBe(399.99); // 4th cheapest
    });
  });

  describe("first()", () => {
    it("returns first matching item", async () => {
      const result = await client.query()
        .where({ category: "furniture" })
        .first();

      expect(result).not.toBeNull();
      expect(result?.category).toBe("furniture");
    });

    it("returns null when no matches", async () => {
      const result = await client.query()
        .where({ category: "nonexistent" as string })
        .first();

      expect(result).toBeNull();
    });

    it("respects orderBy when getting first", async () => {
      const cheapest = await client.query()
        .orderBy("price", "asc")
        .first();

      expect(cheapest?.price).toBe(49.99);
    });
  });

  describe("count()", () => {
    it("returns total count with no filter", async () => {
      const count = await client.query().count();
      expect(count).toBe(6);
    });

    it("returns filtered count", async () => {
      const count = await client.query()
        .where({ category: "electronics" })
        .count();

      expect(count).toBe(3);
    });

    it("returns 0 when no matches", async () => {
      const count = await client.query()
        .where({ category: "nonexistent" as string })
        .count();

      expect(count).toBe(0);
    });
  });

  describe("complex queries", () => {
    it("filters with numeric operators", async () => {
      const results = await client.query()
        .where({ price: { gte: 200 } })
        .execute();

      expect(results.every(p => p.price >= 200)).toBe(true);
    });

    it("combines category filter with price range", async () => {
      const results = await client.query()
        .where({ category: "electronics" })
        .where({ price: { lte: 500 } })
        .execute();

      expect(results).toHaveLength(1); // Only Tablet ($399.99)
      expect(results.every(p => p.category === "electronics" && p.price <= 500)).toBe(true);
    });

    it("full query with filter, sort, and pagination", async () => {
      const results = await client.query()
        .where({ inStock: true })
        .orderBy("price", "desc")
        .limit(2)
        .execute();

      expect(results).toHaveLength(2);
      expect(results[0].name).toBe("Laptop");
      expect(results[1].name).toBe("Phone");
    });
  });
});
