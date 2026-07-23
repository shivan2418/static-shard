import { describe, expect, test } from "vitest";
import { resolveConfig } from "../src/config.js";
import type { StaticShardConfig } from "../src/types.js";

const base: StaticShardConfig = {
  collection: "movies",
  input: { path: "data/movies.ndjson" },
  schema: {
    sortField: "year",
    fields: {
      year: { kind: "number" },
      title: { kind: "string" },
    },
  },
};

describe("resolveConfig", () => {
  test("fills in defaults for output, clientOut, basePath, shardBytes", () => {
    const resolved = resolveConfig(base, "/repo");
    expect(resolved.output).toBe("/repo/public/shard-data");
    expect(resolved.clientOut).toBe("/repo/src/shard-db");
    expect(resolved.basePath).toBe("/shard-data");
    expect(resolved.shardBytes).toBeGreaterThan(0);
    expect(resolved.inputPath).toBe("/repo/data/movies.ndjson");
  });

  test("honors explicit output/clientOut/basePath/shardBytes overrides", () => {
    const resolved = resolveConfig(
      { ...base, output: "dist/data", clientOut: "dist/client", basePath: "https://cdn.example.com/data", shardBytes: 1024 },
      "/repo",
    );
    expect(resolved.output).toBe("/repo/dist/data");
    expect(resolved.clientOut).toBe("/repo/dist/client");
    expect(resolved.basePath).toBe("https://cdn.example.com/data");
    expect(resolved.shardBytes).toBe(1024);
  });

  test("rejects a sortField not declared in schema.fields", () => {
    const bad: StaticShardConfig = { ...base, schema: { sortField: "missing", fields: base.schema.fields } };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/sortField/);
  });

  test("rejects a sortField whose kind is not number or date", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "title", fields: base.schema.fields },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/number.*date|date.*number/i);
  });

  test("rejects an input format other than ndjson", () => {
    const bad: StaticShardConfig = { ...base, input: { path: "x.csv", format: "csv" as never } };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/ndjson/);
  });
});
