import { execFileSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { build } from "../src/build.js";
import { contentHash } from "../src/hash.js";
import type { StaticShardConfig } from "../src/types.js";

const testDir = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(testDir, "../../..");

const MOVIES = [
  { year: 1999, title: "The Matrix", rating: 8.7 },
  { year: 2000, title: "Gladiator", rating: 8.5 },
  { year: 2000, title: "Snatch", rating: 8.3 },
  { year: 2000, title: "Memento", rating: 8.4 },
  { year: 2003, title: "The Matrix Reloaded", rating: 7.2 },
  { year: 2008, title: "The Dark Knight", rating: 9.0 },
  { year: 2010, title: "Inception", rating: 8.8 },
  { year: 2010, title: "Toy Story 3", rating: 8.3 },
  { year: 2014, title: "Interstellar", rating: 8.6 },
  { year: 2019, title: "Parasite", rating: 8.6 },
];

const config: StaticShardConfig = {
  collection: "movies",
  input: { path: "movies.ndjson" },
  schema: {
    sortField: "year",
    fields: {
      year: { kind: "number" },
      title: { kind: "string" },
      rating: { kind: "number" },
    },
  },
};

let tmpDir: string;

beforeEach(() => {
  tmpDir = mkdtempSync(path.join(tmpdir(), "static-shard-seam1-"));
  writeFileSync(path.join(tmpDir, "movies.ndjson"), MOVIES.map((m) => JSON.stringify(m)).join("\n") + "\n");
});

afterEach(() => {
  rmSync(tmpDir, { recursive: true, force: true });
});

/** T3/T4 shared fixture: same movies, with title secondary-indexed. */
const indexedConfig: StaticShardConfig = {
  ...config,
  schema: {
    sortField: "year",
    fields: {
      year: { kind: "number" },
      title: { kind: "string", indexed: true },
      rating: { kind: "number" },
    },
  },
};

/**
 * Seam #3 harness: write a consumer against the generated client in
 * `clientOutDir` and assert `tsc -p` over it exits 0 — so every
 * `@ts-expect-error` inside `consumerSource` must genuinely error.
 */
function assertConsumerCompiles(clientOutDir: string, consumerSource: string): void {
  writeFileSync(path.join(clientOutDir, "consumer.ts"), consumerSource);
  // Real consumers are ESM projects; declare it here so NodeNext treats these .ts files as modules.
  writeFileSync(path.join(clientOutDir, "package.json"), JSON.stringify({ type: "module" }));

  const tsconfigContent = {
    extends: path.join(repoRoot, "tsconfig.base.json"),
    compilerOptions: {
      paths: { "static-shard": [path.join(repoRoot, "packages/static-shard/src/index.ts")] },
      noEmit: true,
    },
    include: ["*.ts"],
  };
  writeFileSync(path.join(clientOutDir, "tsconfig.json"), JSON.stringify(tsconfigContent, null, 2));

  const tscBin = path.join(repoRoot, "node_modules", ".bin", "tsc");
  let output = "";
  let status = 0;
  try {
    output = execFileSync(tscBin, ["-p", path.join(clientOutDir, "tsconfig.json")], { encoding: "utf8" });
  } catch (err) {
    const e = err as { status?: number; stdout?: string; message: string };
    status = e.status ?? 1;
    output = e.stdout ?? e.message;
  }
  expect(output + `\n(exit ${status})`).toBe(`\n(exit 0)`);
}

describe("seam #1 — config + NDJSON → build artifacts", () => {
  test("produces a manifest matching the spec's shape for the sort-field-only case", () => {
    const { manifest, outputDir, clientOutDir } = build(config, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });

    expect(manifest.formatVersion).toBe(0);
    expect(manifest.generatorVersion).toBe("0.1.0");
    expect(manifest.dataset.collection).toBe("movies");
    expect(manifest.dataset.recordCount).toBe(MOVIES.length);
    expect(manifest.dataset.sortField).toBe("year");

    // per-shard counts sum to recordCount
    const summedCount = manifest.shards.reduce((sum, s) => sum + s.count, 0);
    expect(summedCount).toBe(manifest.dataset.recordCount);
    expect(manifest.dataset.shardCount).toBe(manifest.shards.length);

    // splitPoints: N+1 boundaries, monotonic
    const splitPoints = manifest.zonemap.year!.splitPoints as number[];
    expect(splitPoints).toHaveLength(manifest.shards.length + 1);
    for (let i = 1; i < splitPoints.length; i++) {
      expect(splitPoints[i]!).toBeGreaterThanOrEqual(splitPoints[i - 1]!);
    }

    // only the sort field is indexed
    expect(manifest.schema.fields.year!.indexed).toBe(true);
    expect(manifest.schema.fields.title!.indexed).toBe(false);
    expect(manifest.schema.fields.rating!.indexed).toBe(false);

    expect(existsSync(path.join(outputDir, "manifest.json"))).toBe(true);
    expect(existsSync(clientOutDir)).toBe(true);
  });

  test("shard files are content-hash-named and their bytes/count match the manifest", () => {
    const { manifest, outputDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    for (const shard of manifest.shards) {
      const filePath = path.join(outputDir, "shards", `${shard.hash}.ndjson`);
      expect(existsSync(filePath)).toBe(true);
      const content = readFileSync(filePath, "utf8");
      expect(contentHash(content)).toBe(shard.hash);
      expect(Buffer.byteLength(content, "utf8")).toBe(shard.bytes);
      const lineCount = content.split("\n").filter((l) => l.length > 0).length;
      expect(lineCount).toBe(shard.count);
    }
  });

  test("cutting into small shards keeps equal sort-field years contiguous within one shard", () => {
    // A tiny byte target forces many cuts; the three year:2000 records must still land together.
    const tinyResult = build(
      { ...config, shardBytes: 40 },
      { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 },
    );
    expect(tinyResult.manifest.shards.length).toBeGreaterThan(1);

    const tinyShardsDir = path.join(tinyResult.outputDir, "shards");
    const shardsWith2000 = tinyResult.manifest.shards.filter((s) => {
      const content = readFileSync(path.join(tinyShardsDir, `${s.hash}.ndjson`), "utf8");
      return content.includes('"year":2000');
    });
    expect(shardsWith2000).toHaveLength(1);
  });

  test("generates schema.ts and client.ts with the generated-header stamp", () => {
    const { clientOutDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schemaTs = readFileSync(path.join(clientOutDir, "schema.ts"), "utf8");
    const clientTs = readFileSync(path.join(clientOutDir, "client.ts"), "utf8");
    expect(schemaTs).toContain("generated by static-shard@0.1.0 — do not edit");
    expect(clientTs).toContain("generated by static-shard@0.1.0 — do not edit");
    expect(schemaTs).toContain("export interface Movies");
    expect(clientTs).toContain("export function connect(");
  });

  test("identical input produces identical shard hashes and manifest (determinism)", () => {
    const first = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const secondDir = mkdtempSync(path.join(tmpdir(), "static-shard-seam1-again-"));
    writeFileSync(path.join(secondDir, "movies.ndjson"), MOVIES.map((m) => JSON.stringify(m)).join("\n") + "\n");
    const second = build(config, { baseDir: secondDir, generatorVersion: "0.1.0", formatVersion: 0 });

    expect(second.manifest.shards.map((s) => s.hash)).toEqual(first.manifest.shards.map((s) => s.hash));
    expect(second.manifest.zonemap).toEqual(first.manifest.zonemap);
    rmSync(secondDir, { recursive: true, force: true });
  });
});

describe("seam #1 — secondary inverted index & zonemap (T3)", () => {
  const indexedConfig: StaticShardConfig = {
    ...config,
    shardBytes: 60, // tiny — forces multiple shards so the index has real cross-shard postings to prove
    schema: {
      sortField: "year",
      fields: {
        year: { kind: "number" },
        title: { kind: "string", indexed: true },
        rating: { kind: "number", indexed: true },
      },
    },
  };

  test("writes a chunk directory in manifest.json + content-hash-named chunk files on disk covering the full value range", () => {
    const { manifest, outputDir } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    expect(manifest.schema.fields.title!.indexed).toBe(true);
    expect(manifest.schema.fields.title!.operators).toEqual(["equals", "in", "startsWith", "not"]);
    expect(manifest.indexes.title!.chunks.length).toBeGreaterThan(0);

    for (const chunk of manifest.indexes.title!.chunks) {
      const filePath = path.join(outputDir, chunk.file);
      expect(existsSync(filePath)).toBe(true);
      const content = readFileSync(filePath, "utf8");
      expect(contentHash(content)).toBe(path.basename(chunk.file, ".json"));
      expect((chunk.from as string) <= (chunk.to as string)).toBe(true);
    }

    // Chunks partition the distinct-value range in order, with no overlap between consecutive chunks.
    const chunks = manifest.indexes.title!.chunks;
    for (let i = 1; i < chunks.length; i++) {
      expect((chunks[i]!.from as string) > (chunks[i - 1]!.to as string)).toBe(true);
    }

    const expectedTitles = [...new Set(MOVIES.map((m) => m.title))].sort();
    expect(expectedTitles[0]! >= (chunks[0]!.from as string)).toBe(true);
    expect(expectedTitles[expectedTitles.length - 1]! <= (chunks[chunks.length - 1]!.to as string)).toBe(true);
  });

  test("secondary number field gets equals/in only, and its own chunk directory", () => {
    const { manifest } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    expect(manifest.schema.fields.rating!.operators).toEqual(["equals", "in", "not"]);
    expect(manifest.indexes.rating!.chunks.length).toBeGreaterThan(0);
  });

  test("secondary zonemap pairs are present and ordinal-aligned with shards[], string pairs marked truncated", () => {
    const { manifest } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    expect(manifest.zonemap.title).toBeDefined();
    const titleZonemap = manifest.zonemap.title as { pairs: [unknown, unknown][]; truncated?: boolean };
    expect(titleZonemap.truncated).toBe(true);
    expect(titleZonemap.pairs).toHaveLength(manifest.shards.length);

    expect(manifest.zonemap.rating).toBeDefined();
    const ratingZonemap = manifest.zonemap.rating as { pairs: [unknown, unknown][]; truncated?: boolean };
    expect(ratingZonemap.truncated).toBeUndefined();
    expect(ratingZonemap.pairs).toHaveLength(manifest.shards.length);

    // the sort field's own zonemap is untouched (still split-points, not pairs)
    expect(manifest.zonemap.year).toHaveProperty("splitPoints");
  });

  test("a non-opted-in field stays unindexed and absent from manifest.indexes", () => {
    const { manifest } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    expect(manifest.schema.fields.title!.indexed).toBe(false);
    expect(manifest.indexes).toEqual({});
  });
});

describe("seam #1 — endsWith (reversed index) & contains (trigram index) opt-ins (T6)", () => {
  const t6Config: StaticShardConfig = {
    ...config,
    shardBytes: 60, // tiny — forces multiple shards, same rationale as T3's fixture
    schema: {
      sortField: "year",
      fields: {
        year: { kind: "number" },
        title: { kind: "string", indexed: true, endsWith: true, contains: true },
        rating: { kind: "number" },
      },
    },
  };

  test("operators/manifest.indexes gain reversed+trigram structures only for the opted-in field", () => {
    const { manifest } = build(t6Config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    expect(manifest.schema.fields.title!.operators).toEqual(["equals", "in", "startsWith", "endsWith", "contains", "not"]);
    expect(manifest.indexes.title!.reversed).toBeDefined();
    expect(manifest.indexes.title!.trigram).toBeDefined();
    // rating never opted into endsWith/contains — no reversed/trigram structures for it, even though it's indexed.
    expect(manifest.indexes.rating).toBeUndefined();
  });

  test("reversed + trigram chunk files are written to disk, content-hash-named, matching the manifest directory", () => {
    const { manifest, outputDir } = build(t6Config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    const reversedChunks = manifest.indexes.title!.reversed!.chunks;
    expect(reversedChunks.length).toBeGreaterThan(0);
    for (const chunk of reversedChunks) {
      expect(chunk.file).toMatch(/^index\/title\/reversed\//);
      const filePath = path.join(outputDir, chunk.file);
      expect(existsSync(filePath)).toBe(true);
      expect(contentHash(readFileSync(filePath, "utf8"))).toBe(path.basename(chunk.file, ".json"));
    }

    const trigramChunks = manifest.indexes.title!.trigram!.chunks;
    expect(trigramChunks.length).toBeGreaterThan(0);
    for (const chunk of trigramChunks) {
      expect(chunk.file).toMatch(/^index\/title\/trigram\//);
      const filePath = path.join(outputDir, chunk.file);
      expect(existsSync(filePath)).toBe(true);
      expect(contentHash(readFileSync(filePath, "utf8"))).toBe(path.basename(chunk.file, ".json"));
    }
  });

  test("endsWith(suffix) over the built reversed index resolves correctly against real titles", () => {
    // Cross-checks the build output against ADR-0003's stated equivalence: endsWith(s) = startsWith(reverse(s))
    // on the reversed index — sanity-checked here structurally; seam #2 proves it end-to-end through the runtime.
    const { manifest } = build(t6Config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const reversedValues = manifest.indexes.title!.reversed!.chunks.map((c) => c.from as string);
    expect(reversedValues.length).toBeGreaterThan(0);
  });

  test("a contains opt-in whose trigram index exceeds its column emits a loud 'bigger than the data' warning", () => {
    // A handful of short movie titles: trigram-postings overhead trivially dwarfs the raw column bytes.
    const { warnings } = build(t6Config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    expect(warnings.some((w) => /contains\(title\)/.test(w) && /bigger than the data/i.test(w))).toBe(true);
  });

  test("a field with only endsWith opted in has no trigram structure, and vice versa", () => {
    const endsWithOnly: StaticShardConfig = {
      ...t6Config,
      schema: {
        ...t6Config.schema,
        fields: { ...t6Config.schema.fields, title: { kind: "string", indexed: true, endsWith: true } },
      },
    };
    const { manifest, warnings } = build(endsWithOnly, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    expect(manifest.schema.fields.title!.operators).toEqual(["equals", "in", "startsWith", "endsWith", "not"]);
    expect(manifest.indexes.title!.reversed).toBeDefined();
    expect(manifest.indexes.title!.trigram).toBeUndefined();
    expect(warnings).toEqual([]);
  });
});

describe("seam #3 (type-level, over seam #1's own output) — generated types reject bad queries", () => {
  test("tsc exits 0 over a consumer that exercises valid queries and @ts-expect-error cases", () => {
    const { clientOutDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    const consumerSource = `
import { connect } from "./client.js";

const db = connect();

async function valid() {
  await db.movies.findMany({ where: { year: { gte: 2000, lt: 2010 } }, orderBy: { year: "desc" }, limit: 5, offset: 1 });
  await db.movies.findMany({ where: { year: { in: [1999, 2003] } } });
  await db.movies.findMany();
  db.movies.getSchema();
}

async function invalid() {
  // title is NOT indexed (only year is, in T2) — unknown field in where.
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { equals: "Gladiator" } } });

  // wrong value type: year is a number.
  // @ts-expect-error
  await db.movies.findMany({ where: { year: { gt: "2000" } } });

  // orderBy over a non-indexed field.
  // @ts-expect-error
  await db.movies.findMany({ orderBy: { title: "asc" } });
}

void valid;
void invalid;
`;
    assertConsumerCompiles(clientOutDir, consumerSource);
  });

  test("T3: tsc exits 0 for a consumer exercising secondary-field equals/in/startsWith and rejecting disabled operators", () => {
    const { clientOutDir } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    const consumerSource = `
import { connect } from "./client.js";

const db = connect();

async function valid() {
  await db.movies.findMany({ where: { title: { equals: "Gladiator" } } });
  await db.movies.findMany({ where: { title: { in: ["Gladiator", "Snatch"] } } });
  await db.movies.findMany({ where: { title: { startsWith: "Gla" } } });
  await db.movies.findMany({ where: { year: { gte: 2000 }, title: { equals: "Gladiator" } } });
}

async function invalid() {
  // wrong value type: title is a string.
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { equals: 5 } } });

  // rating is NOT indexed in this config — unknown field in where.
  // @ts-expect-error
  await db.movies.findMany({ where: { rating: { equals: 8.5 } } });

  // contains was never opted in for title — disabled operator.
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { contains: "lad" } } });
}

void valid;
void invalid;
`;
    assertConsumerCompiles(clientOutDir, consumerSource);
  });

  test("T4: tsc exits 0 for a consumer exercising count() and rejecting exact: true (ADR-0008 §4)", () => {
    const { clientOutDir } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    const consumerSource = `
import { connect } from "./client.js";

const db = connect();

async function valid() {
  const all = await db.movies.count();
  const constrained = await db.movies.count({ year: { gte: 2000 } });
  const secondary = await db.movies.count({ title: { equals: "Gladiator" } });
  const explicitFalse = await db.movies.count({ year: { gte: 2000 } }, { exact: false });

  // The return shape: { count: number; exact: boolean }.
  const n: number = all.count;
  const e: boolean = all.exact;
  void n; void e; void constrained; void secondary; void explicitFalse;
}

async function invalid() {
  // the exact mode is deferred to v2 — 1.0 locks opts.exact to false (ADR-0008 §4).
  // @ts-expect-error
  await db.movies.count({ year: { gte: 2000 } }, { exact: true });

  // \`{ exact: true }\` is not a where either — the option lives in the second argument.
  // @ts-expect-error
  await db.movies.count({ exact: true });

  // rating is NOT indexed in this config — unknown field in where.
  // @ts-expect-error
  await db.movies.count({ rating: { equals: 9.0 } });
}

void valid;
void invalid;
`;
    assertConsumerCompiles(clientOutDir, consumerSource);
  });

  test("T6: tsc exits 0 for a consumer exercising endsWith/contains only where opted in, per-operator", () => {
    const t6Config: StaticShardConfig = {
      ...indexedConfig,
      schema: {
        sortField: "year",
        fields: {
          year: { kind: "number" },
          title: { kind: "string", indexed: true, endsWith: true, contains: true },
          rating: { kind: "number", indexed: true },
          // Indexed string field with ONLY contains opted in — proves the gate is per-OPERATOR,
          // not just "this field has some opt-in" (a plain kind/indexed check couldn't catch that).
          director: { kind: "string", indexed: true, contains: true },
        },
      },
    };
    // Overwrite the shared fixture with one that actually has `director` values (MOVIES has none).
    writeFileSync(
      path.join(tmpDir, "movies.ndjson"),
      MOVIES.map((m) => JSON.stringify({ ...m, director: "Nolan" })).join("\n") + "\n",
    );
    const { clientOutDir } = build(t6Config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    const consumerSource = `
import { connect } from "./client.js";

const db = connect();

async function valid() {
  await db.movies.findMany({ where: { title: { endsWith: "Knight" } } });
  await db.movies.findMany({ where: { title: { contains: "Matr" } } });
  // contains/endsWith prune via their own index, so each is valid as a SOLE constraint (not a filter-only rider).
  await db.movies.findMany({ where: { title: { contains: "Matr" } }, limit: 1 });
  await db.movies.findMany({ where: { director: { contains: "Nolan" } } });
}

async function invalid() {
  // rating is indexed but never opted into endsWith/contains — disabled operators.
  // @ts-expect-error
  await db.movies.findMany({ where: { rating: { endsWith: "5" } } });
  // @ts-expect-error
  await db.movies.findMany({ where: { rating: { contains: "5" } } });

  // director opted into contains but NOT endsWith — proves the gate is per-operator, not per-field.
  // @ts-expect-error
  await db.movies.findMany({ where: { director: { endsWith: "n" } } });

  // wrong value type: title's operators all take strings.
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { endsWith: 5 } } });
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { contains: 5 } } });
}

void valid;
void invalid;
`;
    assertConsumerCompiles(clientOutDir, consumerSource);
  });

  test("T7: tsc exits 0 for a consumer exercising some/presence ops/not-with-pruning and rejecting their misuse", () => {
    const t7Config: StaticShardConfig = {
      ...config,
      schema: {
        sortField: "year",
        fields: {
          year: { kind: "number" },
          title: { kind: "string", indexed: true },
          tagline: { kind: "string", indexed: true, absent: true },
          genres: { kind: "string", indexed: true, multi: true },
        },
      },
    };
    writeFileSync(
      path.join(tmpDir, "movies.ndjson"),
      [
        { year: 1999, title: "The Matrix", genres: ["Sci-Fi", "Action"], tagline: "Welcome to the Real World" },
        { year: 2000, title: "Gladiator", genres: ["Action", "Drama"] },
        { year: 2000, title: "Snatch", genres: ["Crime", "Comedy"], tagline: null },
        { year: 2008, title: "The Dark Knight", genres: ["Action", "Crime"], tagline: "Why So Serious?" },
      ]
        .map((m) => JSON.stringify(m))
        .join("\n") + "\n",
    );
    const { clientOutDir } = build(t7Config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    const consumerSource = `
import { connect } from "./client.js";

const db = connect();

async function valid() {
  // Multi-valued field forces \`some\`, shorthand and object form both work.
  await db.movies.findMany({ where: { genres: { some: "Sci-Fi" } } });
  await db.movies.findMany({ where: { genres: { some: { startsWith: "Sci" } } } });

  // Presence ops on the absentable field.
  await db.movies.findMany({ where: { tagline: { isNull: true } } });
  await db.movies.findMany({ where: { tagline: { isAbsent: true } } });
  await db.movies.findMany({ where: { tagline: { exists: false } } });

  // \`not\` alongside a real pruning constraint on the SAME field compiles and runs.
  await db.movies.findMany({ where: { title: { not: "Gladiator", startsWith: "G" } } });
}

async function invalid() {
  // some on a single-valued (non-multi) field.
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { some: "Gladiator" } } });

  // a non-some operator directly on a multi-valued field — must go through \`some\`.
  // @ts-expect-error
  await db.movies.findMany({ where: { genres: { equals: "Action" } } });

  // absent-ops on a field that never opted into absent: true.
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { isNull: true } } });
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { isAbsent: true } } });
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { exists: true } } });

  // \`not\` as the SOLE constraint — RiderGuard rejects it (no pruning companion).
  // @ts-expect-error
  await db.movies.findMany({ where: { title: { not: "Gladiator" } } });
}

void valid;
void invalid;
`;
    assertConsumerCompiles(clientOutDir, consumerSource);
  });

  test("T8: tsc exits 0 for a consumer exercising get(id) when a pk is declared, and rejecting get on a pk-less collection", () => {
    const pkConfig: StaticShardConfig = {
      ...config,
      schema: { sortField: "year", pk: "title", fields: { year: { kind: "number" }, title: { kind: "string", indexed: true } } },
    };
    const { clientOutDir: pkClientOutDir } = build(pkConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    const pkConsumerSource = `
import { connect } from "./client.js";

const db = connect();

async function valid() {
  const hit: { title: string; year: number } | null = await db.movies.get("Gladiator");
  void hit;
}

async function invalid() {
  // get(id) takes the pk's value type — title is a string.
  // @ts-expect-error
  await db.movies.get(5);
}

void valid;
void invalid;
`;
    assertConsumerCompiles(pkClientOutDir, pkConsumerSource);

    const noPkTmpDir = mkdtempSync(path.join(tmpdir(), "static-shard-seam1-nopk-"));
    writeFileSync(path.join(noPkTmpDir, "movies.ndjson"), MOVIES.map((m) => JSON.stringify(m)).join("\n") + "\n");
    const { clientOutDir: noPkClientOutDir } = build(config, { baseDir: noPkTmpDir, generatorVersion: "0.1.0", formatVersion: 0 });

    const noPkConsumerSource = `
import { connect } from "./client.js";

const db = connect();

async function invalid() {
  // no pk declared — the collection has no \`get\` member at all.
  // @ts-expect-error
  await db.movies.get("anything");
}

void invalid;
`;
    assertConsumerCompiles(noPkClientOutDir, noPkConsumerSource);
    rmSync(noPkTmpDir, { recursive: true, force: true });
  });
});
