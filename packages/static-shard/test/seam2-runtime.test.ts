import { readFile } from "node:fs/promises";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { build } from "static-shard-cli";
import type { StaticShardConfig } from "static-shard-cli";
import { createClient } from "../src/client.js";
import type { SchemaMeta } from "../src/types.js";

// Same fixture shape as static-shard-cli's seam #1 test — the point of seam #2
// is to serve a tree that seam #1 itself produced, keeping the two honest
// against each other.
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
  shardBytes: 60, // tiny — forces multiple shards so pruning is meaningfully exercised
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
  tmpDir = mkdtempSync(path.join(tmpdir(), "static-shard-seam2-"));
  writeFileSync(path.join(tmpDir, "movies.ndjson"), MOVIES.map((m) => JSON.stringify(m)).join("\n") + "\n");
});

afterEach(() => {
  rmSync(tmpDir, { recursive: true, force: true });
});

/** basePath = the real output dir's absolute fs path, so `${basePath}/x` IS a real file to read. */
function diskFetch(requests: string[]): typeof fetch {
  return (async (input: RequestInfo | URL) => {
    const filePath = String(input);
    requests.push(filePath);
    try {
      const content = await readFile(filePath, "utf8");
      return {
        ok: true,
        status: 200,
        json: async () => JSON.parse(content),
        text: async () => content,
      } as Response;
    } catch {
      return { ok: false, status: 404, json: async () => ({}), text: async () => "" } as Response;
    }
  }) as typeof fetch;
}

async function loadGeneratedSchema(clientOutDir: string): Promise<SchemaMeta> {
  const mod = (await import(pathToFileURL(path.join(clientOutDir, "schema.ts")).href)) as {
    schema: SchemaMeta;
  };
  return mod.schema;
}

describe("seam #2 — connect({ basePath, fetch }) → results, over a seam #1-built fixture tree", () => {
  test("findMany returns correct records for equals/in/range on the sort field", async () => {
    const { outputDir, clientOutDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });

    const equalsResult = await client.movies.findMany({ where: { year: { equals: 2000 } } });
    expect(equalsResult.records.map((r) => r.title).sort()).toEqual(["Gladiator", "Memento", "Snatch"].sort());

    const inResult = await client.movies.findMany({ where: { year: { in: [1999, 2019] } } });
    expect(inResult.records.map((r) => r.year).sort()).toEqual([1999, 2019]);

    const rangeResult = await client.movies.findMany({ where: { year: { gte: 2008, lt: 2014 } } });
    expect(rangeResult.records.map((r) => r.title).sort()).toEqual(["Inception", "The Dark Knight", "Toy Story 3"].sort());
  });

  test("only shards surviving zonemap pruning are fetched", async () => {
    const { outputDir, clientOutDir, manifest } = build(config, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    expect(manifest.shards.length).toBeGreaterThan(2); // tiny shardBytes should force multiple shards

    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });

    await client.movies.findMany({ where: { year: { equals: 1999 } } });
    const shardRequests = requests.filter((r) => r.includes(`${path.sep}shards${path.sep}`));
    // A point query for the earliest year should touch only the one shard that can contain it.
    expect(shardRequests).toHaveLength(1);
  });

  test("orderBy / limit / offset and exact hasMore over the built fixture", async () => {
    const { clientOutDir, outputDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch([]),
    });

    const desc = await client.movies.findMany({ orderBy: { year: "desc" }, limit: 3 });
    expect(desc.records.map((r) => r.year)).toEqual([2019, 2014, 2010]);
    expect(desc.hasMore).toBe(true);

    const all = await client.movies.findMany();
    expect(all.records).toHaveLength(MOVIES.length);
    expect(all.hasMore).toBe(false);

    const lastPage = await client.movies.findMany({ limit: 5, offset: MOVIES.length - 2 });
    expect(lastPage.records).toHaveLength(2);
    expect(lastPage.hasMore).toBe(false);
  });
});
