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

describe("resolveConfig — endsWith/contains opt-ins (T6)", () => {
  const indexedString: StaticShardConfig = {
    ...base,
    schema: {
      sortField: "year",
      fields: { ...base.schema.fields, title: { kind: "string", indexed: true } },
    },
  };

  test("accepts endsWith/contains on an indexed string field", () => {
    const resolved = resolveConfig(
      { ...indexedString, schema: { ...indexedString.schema, fields: { ...indexedString.schema.fields, title: { kind: "string", indexed: true, endsWith: true, contains: true } } } },
      "/repo",
    );
    expect(resolved.fields.title).toEqual({ kind: "string", indexed: true, endsWith: true, contains: true });
  });

  test("rejects endsWith on a non-indexed field", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "year", fields: { ...base.schema.fields, title: { kind: "string", endsWith: true } } },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/title.*indexed|indexed.*title/i);
  });

  test("rejects contains on a non-indexed field", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "year", fields: { ...base.schema.fields, title: { kind: "string", contains: true } } },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/title.*indexed|indexed.*title/i);
  });

  test("rejects endsWith on a non-string indexed field", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: {
        sortField: "year",
        fields: { ...base.schema.fields, rating: { kind: "number", indexed: true, endsWith: true } },
      },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/rating.*string|string.*rating/i);
  });

  test("rejects contains on a non-string indexed field", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: {
        sortField: "year",
        fields: { ...base.schema.fields, rating: { kind: "number", indexed: true, contains: true } },
      },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/rating.*string|string.*rating/i);
  });

  test("rejects endsWith/contains on the sort field itself (always number/date, never string)", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "year", fields: { ...base.schema.fields, year: { kind: "number", endsWith: true } } },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/year.*string|string.*year/i);
  });
});

describe("resolveConfig — multi/absent opt-ins (T7)", () => {
  test("accepts multi on an indexed string field", () => {
    const resolved = resolveConfig(
      { ...base, schema: { sortField: "year", fields: { ...base.schema.fields, title: { kind: "string", indexed: true, multi: true } } } },
      "/repo",
    );
    expect(resolved.fields.title).toEqual({ kind: "string", indexed: true, multi: true });
  });

  test("accepts absent on an indexed field of any kind", () => {
    const resolved = resolveConfig(
      { ...base, schema: { sortField: "year", fields: { ...base.schema.fields, title: { kind: "string", indexed: true, absent: true } } } },
      "/repo",
    );
    expect(resolved.fields.title).toEqual({ kind: "string", indexed: true, absent: true });
  });

  test("rejects multi on a non-indexed field", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "year", fields: { ...base.schema.fields, title: { kind: "string", multi: true } } },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/title.*indexed|indexed.*title/i);
  });

  test("rejects multi on a non-string indexed field", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "year", fields: { ...base.schema.fields, rating: { kind: "number", indexed: true, multi: true } } },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/rating.*string|string.*rating/i);
  });

  test("rejects multi on the sort field itself", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "year", fields: { ...base.schema.fields, year: { kind: "number", multi: true } } },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/year.*sort field/i);
  });

  test("rejects absent on a non-indexed field", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "year", fields: { ...base.schema.fields, title: { kind: "string", absent: true } } },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/title.*indexed|indexed.*title/i);
  });

  test("rejects absent on the sort field itself", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: { sortField: "year", fields: { ...base.schema.fields, year: { kind: "number", absent: true } } },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/year.*sort field/i);
  });

  test("rejects a field opting into both multi and absent (element-presence semantics are unsupported)", () => {
    const bad: StaticShardConfig = {
      ...base,
      schema: {
        sortField: "year",
        fields: { ...base.schema.fields, title: { kind: "string", indexed: true, multi: true, absent: true } },
      },
    };
    expect(() => resolveConfig(bad, "/repo")).toThrow(/title.*multi.*absent|absent.*multi/i);
  });
});
