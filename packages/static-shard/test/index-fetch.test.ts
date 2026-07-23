import { describe, expect, test } from "vitest";
import { ShardError } from "../src/errors.js";
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

  test("a manifest-referenced chunk 404 → DEPLOY_INTEGRITY with url + status (ADR-0007 §6)", async () => {
    const fetchImpl = fakeFetch({ "/data/index/title/missing.json": { status: 404, body: "" } });
    const error = await fetchIndexChunk("/data", "index/title/missing.json", fetchImpl).then(
      () => {
        throw new Error("expected rejection");
      },
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ShardError);
    expect((error as ShardError).code).toBe("DEPLOY_INTEGRITY");
    expect((error as ShardError).url).toBe("/data/index/title/missing.json");
    expect((error as ShardError).status).toBe(404);
  });

  test("a 2xx chunk body that won't parse → CORRUPT_DATA", async () => {
    const fetchImpl = fakeFetch({ "/data/index/title/bad.json": { status: 200, body: "}{" } });
    const error = await fetchIndexChunk("/data", "index/title/bad.json", fetchImpl).then(
      () => {
        throw new Error("expected rejection");
      },
      (e: unknown) => e,
    );
    expect(error).toBeInstanceOf(ShardError);
    expect((error as ShardError).code).toBe("CORRUPT_DATA");
    expect((error as ShardError).cause).toBeInstanceOf(SyntaxError);
  });
});
