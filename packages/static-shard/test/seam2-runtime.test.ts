import { readFile } from "node:fs/promises";
import { mkdtempSync, rmSync, unlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { pathToFileURL } from "node:url";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import { build } from "static-shard-cli";
import type { StaticShardConfig } from "static-shard-cli";
import { createClient } from "../src/client.js";
import { ShardError } from "../src/errors.js";
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

/** T3/T4 shared fixture: same movies, with title + rating secondary-indexed. */
const indexedConfig: StaticShardConfig = {
  ...config,
  schema: {
    sortField: "year",
    fields: {
      year: { kind: "number" },
      title: { kind: "string", indexed: true },
      rating: { kind: "number", indexed: true },
    },
  },
};

describe("seam #2 — secondary inverted index & multi-field AND (T3), over a seam #1-built fixture tree", () => {

  test("equals/in/startsWith on the secondary field return correct records, fetching only surviving shards + constrained chunks", async () => {
    const { outputDir, clientOutDir } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });

    const equalsResult = await client.movies.findMany({ where: { title: { equals: "Inception" } } });
    expect(equalsResult.records.map((r) => r.title)).toEqual(["Inception"]);

    const inResult = await client.movies.findMany({ where: { title: { in: ["The Matrix", "Parasite"] } } });
    expect(inResult.records.map((r) => r.title).sort()).toEqual(["Parasite", "The Matrix"]);

    const startsWithResult = await client.movies.findMany({ where: { title: { startsWith: "The" } } });
    expect(startsWithResult.records.map((r) => r.title).sort()).toEqual(
      ["The Matrix", "The Matrix Reloaded", "The Dark Knight"].sort(),
    );
  });

  test("only chunks covering the queried value and only the matching shard are fetched for an equals query", async () => {
    const { outputDir, clientOutDir, manifest } = build(indexedConfig, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    expect(manifest.indexes.title!.chunks.length).toBeGreaterThan(0);

    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });

    await client.movies.findMany({ where: { title: { equals: "Parasite" } } });
    const indexRequests = requests.filter((r) => r.includes(`${path.sep}index${path.sep}`));
    const shardRequests = requests.filter((r) => r.includes(`${path.sep}shards${path.sep}`));
    expect(shardRequests).toHaveLength(1); // "Parasite" lives in exactly one shard
    // Never more chunks than the directory actually holds — proves we're not sweeping every chunk.
    expect(indexRequests.length).toBeGreaterThan(0);
    expect(indexRequests.length).toBeLessThanOrEqual(manifest.indexes.title!.chunks.length);
  });

  test("implicit AND across the sort field and the secondary index returns the exact intersection", async () => {
    const { outputDir, clientOutDir } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch([]),
    });

    // year:2000 → {Gladiator, Snatch, Memento}; title startsWith "S" → {Snatch, ...}. AND ⇒ exactly Snatch.
    const result = await client.movies.findMany({ where: { year: { equals: 2000 }, title: { startsWith: "S" } } });
    expect(result.records.map((r) => r.title)).toEqual(["Snatch"]);
  });

  test("implicit AND across TWO non-sort indexed fields intersects both fields' own index-derived shard sets", async () => {
    const { outputDir, clientOutDir } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch([]),
    });

    // title startsWith "The" → {The Matrix (8.7), The Matrix Reloaded (7.2), The Dark Knight (9.0)};
    // rating equals 9.0 → {The Dark Knight} alone. Neither constraint touches the sort field (year) at all.
    const result = await client.movies.findMany({ where: { title: { startsWith: "The" }, rating: { equals: 9.0 } } });
    expect(result.records.map((r) => r.title)).toEqual(["The Dark Knight"]);
  });

  test("a multi-field AND with no possible intersection returns an empty result and fetches no shards", async () => {
    const { outputDir, clientOutDir } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });

    // "Parasite" (year 2019) can never satisfy year:1999 — disjoint constraints.
    const result = await client.movies.findMany({ where: { year: { equals: 1999 }, title: { equals: "Parasite" } } });
    expect(result.records).toEqual([]);
    expect(requests.filter((r) => r.includes(`${path.sep}shards${path.sep}`))).toEqual([]);
  });
});

describe("seam #2 — count() approximate upper bound & pagination totals (T4), over a seam #1-built fixture tree", () => {
  test("count(where) is an upper bound ≥ the true match count, flagged exact: false, fetching no data shards", async () => {
    const { outputDir, clientOutDir } = build(indexedConfig, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });

    const wheres = [
      { year: { gte: 2000 } },
      { title: { startsWith: "The" } },
      { year: { gte: 2000 }, title: { startsWith: "The" } },
      { rating: { equals: 8.6 } },
    ] as const;
    for (const where of wheres) {
      const before = requests.length;
      const result = await client.movies.count(where);
      // Zero data-shard fetches per count call, whatever else it fetched.
      expect(requests.slice(before).filter((r) => r.includes(`${path.sep}shards${path.sep}`))).toEqual([]);
      // The truth, independently observed through the findMany seam.
      const truth = (await client.movies.findMany({ where })).records.length;
      expect(result.count).toBeGreaterThanOrEqual(truth);
      expect(result.exact).toBe(false);
    }
  });

  test("the two exact cases: empty where → recordCount, pruned-to-zero → 0", async () => {
    const { outputDir, clientOutDir, manifest } = build(indexedConfig, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });

    // Empty where → the free, exact recordCount — and literally nothing but the manifest is fetched.
    await expect(client.movies.count()).resolves.toEqual({ count: MOVIES.length, exact: true });
    expect(manifest.dataset.recordCount).toBe(MOVIES.length);
    expect(requests).toEqual([path.join(outputDir, "manifest.json")]);

    // Disjoint AND (year:1999 ∩ title:"Parasite") prunes to zero shards → an exact, trustworthy "none".
    await expect(client.movies.count({ year: { equals: 1999 }, title: { equals: "Parasite" } })).resolves.toEqual({
      count: 0,
      exact: true,
    });
    expect(requests.filter((r) => r.includes(`${path.sep}shards${path.sep}`))).toEqual([]);
  });

  test("a constrained secondary field costs only its index chunk(s) — manifest + chunks, never a shard body", async () => {
    const { outputDir, clientOutDir, manifest } = build(indexedConfig, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });

    const result = await client.movies.count({ rating: { equals: 9.0 } });
    expect(result.exact).toBe(false);
    expect(result.count).toBeGreaterThanOrEqual(1); // The Dark Knight's shard — maybe loose, never below the truth.

    const indexRequests = requests.filter((r) => r.includes(`${path.sep}index${path.sep}`));
    expect(indexRequests.length).toBeGreaterThan(0); // the constrained field's chunk(s) ARE the allowed cost
    expect(indexRequests.length).toBeLessThanOrEqual(manifest.indexes.rating!.chunks.length);
    expect(requests.filter((r) => r.includes(`${path.sep}shards${path.sep}`))).toEqual([]);
  });
});

describe("seam #2 — runtime failure contract & maxResults guardrail (T5), over a seam #1-built fixture tree", () => {
  /** Rejects with a ShardError; asserts the exact code + payload, then returns it for message checks. */
  async function expectFailure(
    promise: Promise<unknown>,
    expected: { code: string; url?: string; status?: number; remediation: RegExp },
  ): Promise<void> {
    const error = await promise.then(
      () => {
        throw new Error(`expected ${expected.code}, but the query resolved`);
      },
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ShardError);
    const shardError = error as ShardError;
    expect(shardError.code).toBe(expected.code);
    if (expected.url !== undefined) expect(shardError.url).toBe(expected.url);
    if (expected.status !== undefined) expect(shardError.status).toBe(expected.status);
    // Every message carries remediation (ADR-0007 §8), and the query object is never attached (PII).
    expect(shardError.message).toMatch(expected.remediation);
    expect((shardError as unknown as Record<string, unknown>).query).toBeUndefined();
    expect((shardError as unknown as Record<string, unknown>).where).toBeUndefined();
  }

  test("manifest.json missing (wrong basePath) → CONFIG with url, 404 status and a basePath remediation", async () => {
    const { clientOutDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const missingDir = path.join(tmpDir, "no-such-dataset");
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: missingDir,
      fetch: diskFetch([]),
    });
    await expectFailure(client.movies.findMany(), {
      code: "CONFIG",
      url: path.join(missingDir, "manifest.json"),
      status: 404,
      remediation: /basePath/,
    });
  });

  test("manifest major ≠ runtime major → FORMAT_VERSION at manifest load, before any query work", async () => {
    const { outputDir, clientOutDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 99 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
    });
    await expectFailure(client.movies.findMany({ where: { year: { equals: 2000 } } }), {
      code: "FORMAT_VERSION",
      url: path.join(outputDir, "manifest.json"),
      remediation: /static-shard build/,
    });
    // Nothing but the manifest was fetched — the mismatch halts everything downstream.
    expect(requests).toEqual([path.join(outputDir, "manifest.json")]);
  });

  test("a manifest-referenced shard missing from the deploy → DEPLOY_INTEGRITY naming the file, never a partial array", async () => {
    const { outputDir, clientOutDir, manifest } = build(config, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    const schema = await loadGeneratedSchema(clientOutDir);
    // Sabotage the deploy: delete one shard the manifest promises.
    const victim = manifest.shards[manifest.shards.length - 1]!;
    unlinkSync(path.join(outputDir, "shards", `${victim.hash}.ndjson`));

    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch([]),
    });
    await expectFailure(client.movies.findMany(), {
      code: "DEPLOY_INTEGRITY",
      url: path.join(outputDir, "shards", `${victim.hash}.ndjson`),
      status: 404,
      remediation: /redeploy/,
    });
  });

  test("a manifest-referenced index chunk missing → DEPLOY_INTEGRITY on the chunk url", async () => {
    const { outputDir, clientOutDir, manifest } = build(indexedConfig, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    const schema = await loadGeneratedSchema(clientOutDir);
    const victim = manifest.indexes.title!.chunks[0]!;
    unlinkSync(path.join(outputDir, victim.file));

    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch([]),
    });
    await expectFailure(client.movies.findMany({ where: { title: { equals: "Inception" } } }), {
      code: "DEPLOY_INTEGRITY",
      url: path.join(outputDir, victim.file),
      status: 404,
      remediation: /redeploy/,
    });
  });

  test("a 500 on any file → NETWORK with .status (the maybe-transient bucket), not a 404 code", async () => {
    const { outputDir, clientOutDir, manifest } = build(config, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    const schema = await loadGeneratedSchema(clientOutDir);
    const inner = diskFetch([]);
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes(`${path.sep}shards${path.sep}`)) {
        return { ok: false, status: 500, json: async () => ({}), text: async () => "" } as Response;
      }
      return inner(input, init);
    }) as typeof fetch;
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: fetchImpl,
    });
    // year:1999 is the earliest record — it lives in the first shard.
    await expectFailure(client.movies.findMany({ where: { year: { equals: 1999 } } }), {
      code: "NETWORK",
      url: path.join(outputDir, "shards", `${manifest.shards[0]!.hash}.ndjson`),
      status: 500,
      remediation: /transient|retry/i,
    });
  });

  test("a network-level rejection → NETWORK with NO .status and the original error chained as .cause", async () => {
    const { outputDir, clientOutDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const inner = diskFetch([]);
    const cause = new TypeError("socket hangup");
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.includes(`${path.sep}shards${path.sep}`)) throw cause;
      return inner(input, init);
    }) as typeof fetch;
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: fetchImpl,
    });
    const error = await client.movies.findMany({ where: { year: { equals: 1999 } } }).then(
      () => {
        throw new Error("expected NETWORK, but the query resolved");
      },
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ShardError);
    expect((error as ShardError).code).toBe("NETWORK");
    expect("status" in (error as ShardError)).toBe(false);
    expect((error as ShardError).cause).toBe(cause);
    expect((error as ShardError).message).toMatch(/socket hangup/);
  });

  test("a 2xx shard body that won't parse → CORRUPT_DATA with the parse error chained", async () => {
    const { outputDir, clientOutDir, manifest } = build(config, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    const schema = await loadGeneratedSchema(clientOutDir);
    // Sabotage: corrupt one shard's contents on disk (the file exists, the bytes are garbage).
    const victim = manifest.shards[manifest.shards.length - 1]!;
    writeFileSync(path.join(outputDir, "shards", `${victim.hash}.ndjson`), "<html>definitely not ndjson</html>\n");

    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch([]),
    });
    const error = await client.movies.findMany().then(
      () => {
        throw new Error("expected CORRUPT_DATA, but the query resolved");
      },
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ShardError);
    expect((error as ShardError).code).toBe("CORRUPT_DATA");
    expect((error as ShardError).url).toBe(path.join(outputDir, "shards", `${victim.hash}.ndjson`));
    expect((error as ShardError).cause).toBeInstanceOf(SyntaxError);
    expect((error as ShardError).message).toMatch(/redeploy/);
  });

  test("the first failure aborts the outstanding fetches through the injected fetch's signal", async () => {
    const { outputDir, clientOutDir, manifest } = build(config, {
      baseDir: tmpDir,
      generatorVersion: "0.1.0",
      formatVersion: 0,
    });
    const schema = await loadGeneratedSchema(clientOutDir);
    const inner = diskFetch([]);
    const abortUrl = path.join(outputDir, "shards", `${manifest.shards[0]!.hash}.ndjson`);
    const shardSignals: (AbortSignal | undefined)[] = [];
    // One shard 404s immediately; every OTHER shard hangs until its signal
    // fires — the only way the query settles is if the first failure's abort
    // reaches them (proving cancellation, not settle-all).
    const fetchImpl = (async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (!url.includes(`${path.sep}shards${path.sep}`)) return inner(input, init);
      shardSignals.push(init?.signal);
      if (url === abortUrl) {
        return { ok: false, status: 404, json: async () => ({}), text: async () => "" } as Response;
      }
      return new Promise<Response>((resolve) => {
        init?.signal?.addEventListener("abort", () =>
          resolve({ ok: true, status: 200, json: async () => [], text: async () => "" } as Response),
        );
      });
    }) as typeof fetch;

    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: fetchImpl,
    });
    await expectFailure(client.movies.findMany(), { code: "DEPLOY_INTEGRITY", status: 404, remediation: /redeploy/ });
    expect(shardSignals.length).toBeGreaterThan(1); // a real fan-out was in flight
    for (const signal of shardSignals) expect(signal?.aborted).toBe(true);
  });

  test("maxResults over the seam: unbounded query exceeding the ceiling throws LIMIT_EXCEEDED rather than truncating", async () => {
    const { outputDir, clientOutDir } = build(config, { baseDir: tmpDir, generatorVersion: "0.1.0", formatVersion: 0 });
    const schema = await loadGeneratedSchema(clientOutDir);
    const requests: string[] = [];
    const client = createClient<typeof schema, { movies: (typeof MOVIES)[number] }>(schema, {
      basePath: outputDir,
      fetch: diskFetch(requests),
      maxResults: 3,
    });
    // MOVIES holds 10 records > 3 — an unbounded findMany must fail loud…
    await expectFailure(client.movies.findMany(), { code: "LIMIT_EXCEEDED", remediation: /maxResults/ });
    // …and an explicit limit above the ceiling fails before fetching anything…
    requests.length = 0;
    await expectFailure(client.movies.findMany({ limit: 4 }), { code: "LIMIT_EXCEEDED", remediation: /maxResults/ });
    expect(requests).toEqual([]); // pure client-side validation — not even the manifest was fetched
    // …while paging within the ceiling works fine.
    const page = await client.movies.findMany({ limit: 3 });
    expect(page.records).toHaveLength(3);
    expect(page.hasMore).toBe(true);
  });
});
