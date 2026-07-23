import { describe, expect, test } from "vitest";
import { createClient } from "../src/client.js";
import type { Manifest } from "../src/manifest.js";
import type { SchemaMeta } from "../src/types.js";

// 3 shards, sorted by `year`: shard0 [1999], shard1 [2000,2000,2000], shard2 [2003,2008].
const shardContents: Record<string, string> = {
  s0: '{"year":1999,"title":"The Matrix"}\n',
  s1:
    '{"year":2000,"title":"Gladiator"}\n' +
    '{"year":2000,"title":"Snatch"}\n' +
    '{"year":2000,"title":"Memento"}\n',
  s2: '{"year":2003,"title":"Reloaded"}\n{"year":2008,"title":"Dark Knight"}\n',
};

// Titles sorted: "Dark Knight"(s2), "Gladiator"(s1), "Memento"(s1), "Reloaded"(s2), "Snatch"(s1), "The Matrix"(s0).
const indexChunks: Record<string, string> = {
  c1: JSON.stringify({
    entries: [
      { prefixLen: 0, suffix: "Dark Knight", postings: [2] },
      { prefixLen: 0, suffix: "Gladiator", postings: [1] },
      { prefixLen: 0, suffix: "Memento", postings: [1] },
    ],
  }),
  c2: JSON.stringify({
    entries: [
      { prefixLen: 0, suffix: "Reloaded", postings: [2] },
      { prefixLen: 0, suffix: "Snatch", postings: [1] },
      { prefixLen: 0, suffix: "The Matrix", postings: [0] },
    ],
  }),
};

const manifest: Manifest = {
  formatVersion: 0,
  generatorVersion: "0.1.0",
  dataset: { collection: "movies", recordCount: 6, shardCount: 3, sortField: "year" },
  schema: {
    collection: "movies",
    sortField: "year",
    fields: {
      year: { kind: "number", isDate: false, indexed: true, operators: ["equals", "in", "gt", "gte", "lt", "lte"] },
      title: { kind: "string", isDate: false, indexed: true, operators: ["equals", "in", "startsWith"] },
    },
  },
  shards: [
    { hash: "s0", bytes: shardContents.s0!.length, count: 1 },
    { hash: "s1", bytes: shardContents.s1!.length, count: 3 },
    { hash: "s2", bytes: shardContents.s2!.length, count: 2 },
  ],
  zonemap: { year: { splitPoints: [1999, 2000, 2003, 2008] } },
  indexes: {
    title: {
      operators: ["equals", "in", "startsWith"],
      chunks: [
        { from: "Dark Knight", to: "Memento", file: "index/title/c1.json" },
        { from: "Reloaded", to: "The Matrix", file: "index/title/c2.json" },
      ],
    },
  },
};

const schema: SchemaMeta = { movies: { fields: manifest.schema.fields } };

function fakeFetch(requests: string[]): typeof fetch {
  return (async (input: RequestInfo | URL) => {
    const url = String(input);
    requests.push(url);
    if (url.endsWith("manifest.json")) {
      return { ok: true, status: 200, json: async () => manifest, text: async () => JSON.stringify(manifest) } as Response;
    }
    const indexMatch = url.match(/index\/title\/(\w+)\.json$/);
    if (indexMatch) {
      const body = indexChunks[indexMatch[1]!];
      if (!body) return { ok: false, status: 404, json: async () => ({}), text: async () => "" } as Response;
      return { ok: true, status: 200, json: async () => JSON.parse(body), text: async () => body } as Response;
    }
    const hash = url.split("/").pop()!.replace(".ndjson", "");
    const body = shardContents[hash];
    if (!body) return { ok: false, status: 404, json: async () => ({}), text: async () => "" } as Response;
    return { ok: true, status: 200, json: async () => JSON.parse(body), text: async () => body } as Response;
  }) as typeof fetch;
}

interface Movie {
  year: number;
  title: string;
}
interface Records {
  movies: Movie;
}

describe("createClient / findMany", () => {
  test("equals prunes to the single containing shard", async () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    const { records, hasMore } = await client.movies.findMany({ where: { year: { equals: 2000 } } });
    expect(records.map((r) => r.title).sort()).toEqual(["Gladiator", "Memento", "Snatch"].sort());
    expect(hasMore).toBe(false);
    expect(requests.filter((u) => u.includes("/shards/"))).toEqual(["/data/shards/s1.ndjson"]);
  });

  test("range prunes to only the surviving shards", async () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    const { records } = await client.movies.findMany({ where: { year: { gte: 2003 } } });
    expect(records.map((r) => r.year)).toEqual([2003, 2008]);
    expect(requests.filter((u) => u.includes("/shards/")).sort()).toEqual(["/data/shards/s2.ndjson"]);
  });

  test("in unions each value's shard", async () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    const { records } = await client.movies.findMany({ where: { year: { in: [1999, 2008] } } });
    expect(records.map((r) => r.year).sort()).toEqual([1999, 2008]);
    expect(requests.filter((u) => u.includes("/shards/")).sort()).toEqual([
      "/data/shards/s0.ndjson",
      "/data/shards/s2.ndjson",
    ]);
  });

  test("results come back sorted ascending by the sort field by construction", async () => {
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch([]) });
    const { records } = await client.movies.findMany();
    expect(records.map((r) => r.year)).toEqual([1999, 2000, 2000, 2000, 2003, 2008]);
  });

  test("orderBy desc reverses the result", async () => {
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch([]) });
    const { records } = await client.movies.findMany({ orderBy: { year: "desc" } });
    expect(records.map((r) => r.year)).toEqual([2008, 2003, 2000, 2000, 2000, 1999]);
  });

  test("limit + offset paginate, and hasMore is exact at the limit+1 boundary", async () => {
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch([]) });

    const page1 = await client.movies.findMany({ limit: 2 });
    expect(page1.records.map((r) => r.year)).toEqual([1999, 2000]);
    expect(page1.hasMore).toBe(true);

    const page2 = await client.movies.findMany({ limit: 2, offset: 2 });
    expect(page2.records.map((r) => r.year)).toEqual([2000, 2000]);
    expect(page2.hasMore).toBe(true);

    const page3 = await client.movies.findMany({ limit: 2, offset: 4 });
    expect(page3.records.map((r) => r.year)).toEqual([2003, 2008]);
    expect(page3.hasMore).toBe(false);
  });

  test("an exact-fit limit reports hasMore: false", async () => {
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch([]) });
    const { records, hasMore } = await client.movies.findMany({ limit: 6 });
    expect(records).toHaveLength(6);
    expect(hasMore).toBe(false);
  });

  test("getSchema returns the collection meta with no network call", () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    expect(client.movies.getSchema()).toEqual(schema.movies);
    expect(requests).toEqual([]);
  });
});

describe("createClient / findMany — secondary inverted index (T3)", () => {
  test("equals on a secondary field fetches only the covering chunk and the matching shard", async () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    const { records } = await client.movies.findMany({ where: { title: { equals: "Gladiator" } } });

    expect(records.map((r) => r.title)).toEqual(["Gladiator"]);
    expect(requests.filter((u) => u.includes("/index/"))).toEqual(["/data/index/title/c1.json"]);
    expect(requests.filter((u) => u.includes("/shards/"))).toEqual(["/data/shards/s1.ndjson"]);
  });

  test("in on a secondary field unions shards across chunks, fetching only what's needed", async () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    const { records } = await client.movies.findMany({ where: { title: { in: ["Dark Knight", "The Matrix"] } } });

    expect(records.map((r) => r.title).sort()).toEqual(["Dark Knight", "The Matrix"]);
    expect(requests.filter((u) => u.includes("/index/")).sort()).toEqual([
      "/data/index/title/c1.json",
      "/data/index/title/c2.json",
    ]);
    expect(requests.filter((u) => u.includes("/shards/")).sort()).toEqual(["/data/shards/s0.ndjson", "/data/shards/s2.ndjson"]);
  });

  test("startsWith on a secondary field selects only overlapping chunks and post-filters exactly", async () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    const { records } = await client.movies.findMany({ where: { title: { startsWith: "S" } } });

    expect(records.map((r) => r.title)).toEqual(["Snatch"]);
    // "S" only overlaps chunk2 (Reloaded..The Matrix) — chunk1 (Dark Knight..Memento) is entirely below "S".
    expect(requests.filter((u) => u.includes("/index/"))).toEqual(["/data/index/title/c2.json"]);
    expect(requests.filter((u) => u.includes("/shards/"))).toEqual(["/data/shards/s1.ndjson"]);
  });

  test("multi-field implicit AND intersects the sort-field zonemap prune with the secondary index prune", async () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    // year >= 2000 zonemap-prunes to shards [1,2]; title equals "Gladiator" index-prunes to shard [1] — intersect to [1].
    const { records } = await client.movies.findMany({ where: { year: { gte: 2000 }, title: { equals: "Gladiator" } } });

    expect(records.map((r) => r.title)).toEqual(["Gladiator"]);
    expect(requests.filter((u) => u.includes("/shards/"))).toEqual(["/data/shards/s1.ndjson"]);
  });

  test("an AND across fields whose index prunes disjointly returns no results and fetches no shards", async () => {
    const requests: string[] = [];
    const client = createClient<typeof schema, Records>(schema, { basePath: "/data", fetch: fakeFetch(requests) });
    // year == 1999 → shard 0 only; title == "Gladiator" → shard 1 only. Disjoint ⇒ empty, no shard fetch needed.
    const { records } = await client.movies.findMany({ where: { year: { equals: 1999 }, title: { equals: "Gladiator" } } });

    expect(records).toEqual([]);
    expect(requests.filter((u) => u.includes("/shards/"))).toEqual([]);
  });
});
