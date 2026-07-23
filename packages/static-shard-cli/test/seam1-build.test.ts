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
  });
});
