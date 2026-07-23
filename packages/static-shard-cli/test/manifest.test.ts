import { describe, expect, test } from "vitest";
import { computeSplitPoints, buildManifest } from "../src/manifest.js";
import type { ResolvedConfig, ShardDescriptor } from "../src/types.js";

const config: ResolvedConfig = {
  collection: "movies",
  inputPath: "data/movies.ndjson",
  output: "public/shard-data",
  clientOut: "src/shard-db",
  basePath: "/shard-data",
  shardBytes: 2_097_152,
  indexChunkBytes: 45_000,
  sortField: "year",
  fields: {
    year: { kind: "number" },
    title: { kind: "string" },
  },
};

describe("computeSplitPoints", () => {
  test("returns N+1 monotonic boundaries for N shards", () => {
    const groups = [
      [{ year: 2000 }, { year: 2001 }],
      [{ year: 2002 }, { year: 2002 }],
      [{ year: 2005 }],
    ];
    const points = computeSplitPoints(groups, "year");
    // 3 shards → 4 boundaries: each shard's min, plus the last shard's max.
    expect(points).toEqual([2000, 2002, 2005, 2005]);
  });

  test("returns an empty array for no shards", () => {
    expect(computeSplitPoints([], "year")).toEqual([]);
  });
});

describe("buildManifest", () => {
  const shardFiles: ShardDescriptor[] = [
    { hash: "aaaa000000000000", bytes: 100, count: 2 },
    { hash: "bbbb000000000000", bytes: 120, count: 3 },
  ];
  const splitPoints = [2000, 2005];

  test("matches the spec's manifest shape for the sort-field-only case", () => {
    const manifest = buildManifest({
      config,
      shardFiles,
      splitPoints,
      formatVersion: 0,
      generatorVersion: "0.0.0",
    });

    expect(manifest.formatVersion).toBe(0);
    expect(manifest.generatorVersion).toBe("0.0.0");
    expect(manifest.dataset).toEqual({
      collection: "movies",
      recordCount: 5,
      shardCount: 2,
      sortField: "year",
    });
    expect(manifest.shards).toEqual(shardFiles);
    expect(manifest.zonemap).toEqual({ year: { splitPoints: [2000, 2005] } });
    expect(manifest.schema.sortField).toBe("year");
    expect(manifest.schema.fields.year).toEqual({
      kind: "number",
      isDate: false,
      indexed: true,
      operators: ["equals", "in", "gt", "gte", "lt", "lte"],
    });
    expect(manifest.schema.fields.title).toEqual({
      kind: "string",
      isDate: false,
      indexed: false,
      operators: [],
    });
  });

  test("per-shard counts sum to recordCount", () => {
    const manifest = buildManifest({
      config,
      shardFiles,
      splitPoints,
      formatVersion: 0,
      generatorVersion: "0.0.0",
    });
    const sum = manifest.shards.reduce((acc, s) => acc + s.count, 0);
    expect(sum).toBe(manifest.dataset.recordCount);
  });

  test("marks a date sort field with isDate: true", () => {
    const dateConfig: ResolvedConfig = {
      ...config,
      sortField: "releaseDate",
      fields: { releaseDate: { kind: "date" } },
    };
    const manifest = buildManifest({
      config: dateConfig,
      shardFiles: [],
      splitPoints: [],
      formatVersion: 0,
      generatorVersion: "0.0.0",
    });
    expect(manifest.schema.fields.releaseDate).toEqual({
      kind: "date",
      isDate: true,
      indexed: true,
      operators: ["equals", "in", "gt", "gte", "lt", "lte"],
    });
  });

  test("a non-sort field with no `indexed` opt-in is not indexed and has no operators", () => {
    const manifest = buildManifest({
      config,
      shardFiles,
      splitPoints,
      formatVersion: 0,
      generatorVersion: "0.0.0",
    });
    expect(manifest.schema.fields.title).toEqual({ kind: "string", isDate: false, indexed: false, operators: [] });
    expect(manifest.indexes).toEqual({});
  });

  test("an opted-in secondary string field gets equals/in/startsWith and merges its zonemap + index directory", () => {
    const indexedConfig: ResolvedConfig = {
      ...config,
      fields: { ...config.fields, title: { kind: "string", indexed: true } },
    };
    const manifest = buildManifest({
      config: indexedConfig,
      shardFiles,
      splitPoints,
      secondaryZonemaps: { title: { pairs: [["Alpha", "Zeta"]], truncated: true } },
      indexChunkDirs: { title: [{ from: "Alpha", to: "Zeta", file: "index/title/abc123.json" }] },
      formatVersion: 0,
      generatorVersion: "0.0.0",
    });

    expect(manifest.schema.fields.title).toEqual({
      kind: "string",
      isDate: false,
      indexed: true,
      operators: ["equals", "in", "startsWith"],
    });
    expect(manifest.zonemap.title).toEqual({ pairs: [["Alpha", "Zeta"]], truncated: true });
    expect(manifest.indexes.title).toEqual({
      operators: ["equals", "in", "startsWith"],
      chunks: [{ from: "Alpha", to: "Zeta", file: "index/title/abc123.json" }],
    });
    // the sort field's own zonemap entry is untouched
    expect(manifest.zonemap.year).toEqual({ splitPoints: [2000, 2005] });
  });

  test("an opted-in secondary number field gets only equals/in (no gt/lt — that's zonemap territory, out of T3 scope)", () => {
    const indexedConfig: ResolvedConfig = {
      ...config,
      fields: { ...config.fields, rating: { kind: "number", indexed: true } },
    };
    const manifest = buildManifest({
      config: indexedConfig,
      shardFiles,
      splitPoints,
      formatVersion: 0,
      generatorVersion: "0.0.0",
    });
    expect(manifest.schema.fields.rating).toEqual({
      kind: "number",
      isDate: false,
      indexed: true,
      operators: ["equals", "in"],
    });
  });

  test("an opted-in secondary boolean field gets only equals", () => {
    const indexedConfig: ResolvedConfig = {
      ...config,
      fields: { ...config.fields, isClassic: { kind: "boolean", indexed: true } },
    };
    const manifest = buildManifest({
      config: indexedConfig,
      shardFiles,
      splitPoints,
      formatVersion: 0,
      generatorVersion: "0.0.0",
    });
    expect(manifest.schema.fields.isClassic).toEqual({
      kind: "boolean",
      isDate: false,
      indexed: true,
      operators: ["equals"],
    });
  });
});
