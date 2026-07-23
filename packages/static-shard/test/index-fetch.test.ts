import { describe, expect, test } from "vitest";
import { fetchIndexChunk } from "../src/index-fetch.js";

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

describe("fetchIndexChunk", () => {
  test("fetches and parses an index chunk file relative to basePath", async () => {
    const body = JSON.stringify({ entries: [{ prefixLen: 0, suffix: "Gladiator", postings: [1] }] });
    const fetchImpl = fakeFetch({ "/data/index/title/abc123.json": { status: 200, body } });
    const chunk = await fetchIndexChunk("/data", "index/title/abc123.json", fetchImpl);
    expect(chunk).toEqual({ entries: [{ prefixLen: 0, suffix: "Gladiator", postings: [1] }] });
  });

  test("throws when the chunk fetch is not ok", async () => {
    const fetchImpl = fakeFetch({ "/data/index/title/missing.json": { status: 404, body: "" } });
    await expect(fetchIndexChunk("/data", "index/title/missing.json", fetchImpl)).rejects.toThrow(/404/);
  });
});
