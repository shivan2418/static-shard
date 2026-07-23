import { describe, expect, test } from "vitest";
import { fetchShardRecords } from "../src/shard-fetch.js";

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

describe("fetchShardRecords", () => {
  test("parses NDJSON lines into records", async () => {
    const body = '{"year":2000,"title":"A"}\n{"year":2001,"title":"B"}\n';
    const fetchImpl = fakeFetch({ "/data/shards/abc123.ndjson": { status: 200, body } });
    const records = await fetchShardRecords("/data", "abc123", fetchImpl);
    expect(records).toEqual([
      { year: 2000, title: "A" },
      { year: 2001, title: "B" },
    ]);
  });

  test("throws when the shard fetch is not ok", async () => {
    const fetchImpl = fakeFetch({ "/data/shards/missing.ndjson": { status: 404, body: "" } });
    await expect(fetchShardRecords("/data", "missing", fetchImpl)).rejects.toThrow(/404/);
  });
});
