import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { describe, expect, test } from "vitest";
import { ShardError } from "../src/errors.js";
import { fetchManifest, type Manifest } from "../src/manifest.js";
import { FORMAT_VERSION } from "../src/version.js";

const manifest: Manifest = {
  formatVersion: 0,
  generatorVersion: "0.1.0",
  dataset: { collection: "movies", recordCount: 1, shardCount: 1, sortField: "year" },
  schema: { collection: "movies", sortField: "year", fields: {} },
  shards: [{ hash: "abc", bytes: 1, count: 1 }],
  zonemap: { year: { splitPoints: [2000, 2000] } },
  indexes: {},
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

async function expectShardError(
  promise: Promise<unknown>,
  expected: { code: string; url?: string; status?: number; message: RegExp },
): Promise<ShardError> {
  const error = await promise.then(
    () => {
      throw new Error("expected the promise to reject, but it resolved");
    },
    (e: unknown) => e,
  );
  expect(error).toBeInstanceOf(ShardError);
  const shardError = error as ShardError;
  expect(shardError.code).toBe(expected.code);
  expect(shardError.message).toMatch(expected.message);
  if (expected.url !== undefined) expect(shardError.url).toBe(expected.url);
  if (expected.status !== undefined) expect(shardError.status).toBe(expected.status);
  return shardError;
}

describe("fetchManifest", () => {
  test("fetches and parses manifest.json from basePath", async () => {
    const fetchImpl = fakeFetch({ "/data/manifest.json": { status: 200, body: JSON.stringify(manifest) } });
    const result = await fetchManifest("/data", fetchImpl);
    expect(result).toEqual(manifest);
  });

  test("manifest.json 404 → CONFIG with url, status and a basePath remediation (ADR-0007 §6)", async () => {
    const fetchImpl = fakeFetch({ "/data/manifest.json": { status: 404, body: "" } });
    await expectShardError(fetchManifest("/data", fetchImpl), {
      code: "CONFIG",
      url: "/data/manifest.json",
      status: 404,
      message: /basePath/,
    });
  });

  test("manifest.json 500 → NETWORK with url + status (the maybe-transient bucket)", async () => {
    const fetchImpl = fakeFetch({ "/data/manifest.json": { status: 500, body: "" } });
    await expectShardError(fetchManifest("/data", fetchImpl), {
      code: "NETWORK",
      url: "/data/manifest.json",
      status: 500,
      message: /500/,
    });
  });

  test("a rejected fetch → NETWORK with NO status and the original error as cause", async () => {
    const cause = new TypeError("fetch failed");
    const fetchImpl = (async () => {
      throw cause;
    }) as typeof fetch;
    const error = await expectShardError(fetchManifest("/data", fetchImpl), {
      code: "NETWORK",
      url: "/data/manifest.json",
      message: /fetch failed/,
    });
    expect("status" in error).toBe(false);
    expect(error.cause).toBe(cause);
  });

  test("a 2xx body that won't parse → CORRUPT_DATA with the parse error as cause", async () => {
    const fetchImpl = fakeFetch({ "/data/manifest.json": { status: 200, body: "<html>not json</html>" } });
    const error = await expectShardError(fetchManifest("/data", fetchImpl), {
      code: "CORRUPT_DATA",
      url: "/data/manifest.json",
      message: /parse|JSON/i,
    });
    expect(error.cause).toBeInstanceOf(SyntaxError);
  });

  test("a JSON-valid body that isn't a manifest (no numeric formatVersion) → CORRUPT_DATA, not a bogus FORMAT_VERSION", async () => {
    const fetchImpl = fakeFetch({ "/data/manifest.json": { status: 200, body: '{"hello":"world"}' } });
    await expectShardError(fetchManifest("/data", fetchImpl), {
      code: "CORRUPT_DATA",
      url: "/data/manifest.json",
      message: /formatVersion|manifest/i,
    });
  });

  test("manifest major ≠ runtime major → FORMAT_VERSION with an align-and-rebuild remediation (ADR-0005)", async () => {
    const mismatched = { ...manifest, formatVersion: FORMAT_VERSION + 1 };
    const fetchImpl = fakeFetch({ "/data/manifest.json": { status: 200, body: JSON.stringify(mismatched) } });
    await expectShardError(fetchManifest("/data", fetchImpl), {
      code: "FORMAT_VERSION",
      url: "/data/manifest.json",
      message: /static-shard build/,
    });
  });
});

describe("FORMAT_VERSION constant", () => {
  test("equals the runtime package's own major (ADR-0005: formatVersion = the package major)", () => {
    const packageJsonPath = fileURLToPath(new URL("../package.json", import.meta.url));
    const { version } = JSON.parse(readFileSync(packageJsonPath, "utf8")) as { version: string };
    expect(FORMAT_VERSION).toBe(parseInt(version.split(".")[0]!, 10));
  });
});
