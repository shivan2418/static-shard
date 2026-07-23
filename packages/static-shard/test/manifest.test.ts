import { describe, expect, test } from "vitest";
import { fetchManifest, type Manifest } from "../src/manifest.js";

const manifest: Manifest = {
  formatVersion: 0,
  generatorVersion: "0.1.0",
  dataset: { collection: "movies", recordCount: 1, shardCount: 1, sortField: "year" },
  schema: { collection: "movies", sortField: "year", fields: {} },
  shards: [{ hash: "abc", bytes: 1, count: 1 }],
  zonemap: { year: { splitPoints: [2000, 2000] } },
};

function fakeFetch(responses: Record<string, { status: number; body: string }>): typeof fetch {
  return (async (input: RequestInfo | URL) => {
    const url = String(input);
    const entry = responses[url];
    if (!entry) throw new Error(`fakeFetch: no response registered for ${url}`);
    return {
      ok: entry.status >= 200 && entry.status < 300,
      status: entry.status,
      json: async () => JSON.parse(entry.body),
      text: async () => entry.body,
    } as Response;
  }) as typeof fetch;
}

describe("fetchManifest", () => {
  test("fetches and parses manifest.json from basePath", async () => {
    const fetchImpl = fakeFetch({ "/data/manifest.json": { status: 200, body: JSON.stringify(manifest) } });
    const result = await fetchManifest("/data", fetchImpl);
    expect(result).toEqual(manifest);
  });

  test("throws when the manifest fetch is not ok", async () => {
    const fetchImpl = fakeFetch({ "/data/manifest.json": { status: 404, body: "" } });
    await expect(fetchManifest("/data", fetchImpl)).rejects.toThrow(/404/);
  });
});
