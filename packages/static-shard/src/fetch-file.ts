// The WHATWG-fetch → ShardError mapping (ADR-0007 §3/§5/§6), shared by every
// file the runtime fetches. The injected fetch rejects on network-level failure
// but resolves on ANY HTTP status, so a fetch counts as failed when the promise
// rejects OR resolves !response.ok. No retry/backoff/timeout here — that is the
// injected fetch's / CDN's job (§2).

import { ShardError } from "./errors.js";

/**
 * 404 routes by WHICH file was being fetched — always known at the call site
 * (ADR-0007 §6): the manifest itself → `CONFIG` (wrong basePath); any
 * manifest-referenced content-hashed file → `DEPLOY_INTEGRITY` (corrupt deploy).
 */
export type FetchedFileKind = "manifest" | "referenced";

function messageFor404(kind: FetchedFileKind, url: string): string {
  return kind === "manifest"
    ? `static-shard: no manifest.json at "${url}" (HTTP 404) — check basePath: it must point at the deployed dataset root. If the dataset was never deployed there, re-run \`static-shard build\` and deploy the output.`
    : `static-shard: "${url}" referenced by the manifest returned HTTP 404 — the deploy is incomplete or corrupt. Re-run \`static-shard build\` and redeploy.`;
}

/** Fetches `url`, mapping rejection / !ok to the right ShardError code. Resolves only on 2xx. */
async function fetchOk(
  url: string,
  kind: FetchedFileKind,
  fetchImpl: typeof fetch,
  signal?: AbortSignal,
): Promise<Response> {
  let response: Response;
  try {
    response = await fetchImpl(url, { signal });
  } catch (cause) {
    throw new ShardError({
      code: "NETWORK",
      url,
      message: `static-shard: fetch for "${url}" failed at the network level (${cause instanceof Error ? cause.message : String(cause)}) — possibly transient. Retry by wrapping the injected fetch, or check connectivity/CORS.`,
      cause,
    });
  }
  if (response.ok) return response;
  if (response.status === 404) {
    throw new ShardError({
      code: kind === "manifest" ? "CONFIG" : "DEPLOY_INTEGRITY",
      url,
      status: 404,
      message: messageFor404(kind, url),
    });
  }
  throw new ShardError({
    code: "NETWORK",
    url,
    status: response.status,
    message: `static-shard: fetch for "${url}" returned HTTP ${response.status} — possibly transient. Retry by wrapping the injected fetch, or check the host/CDN.`,
  });
}

/** Fetch + JSON-parse; an unparseable 2xx body is CORRUPT_DATA (ADR-0007 §5). */
export async function fetchJson(
  url: string,
  kind: FetchedFileKind,
  fetchImpl: typeof fetch,
  signal?: AbortSignal,
): Promise<unknown> {
  const response = await fetchOk(url, kind, fetchImpl, signal);
  try {
    return await response.json();
  } catch (cause) {
    throw new ShardError({
      code: "CORRUPT_DATA",
      url,
      message: `static-shard: the body of "${url}" would not parse as JSON — the deploy is corrupt. Re-run \`static-shard build\` and redeploy.`,
      cause,
    });
  }
}

/** Fetch + text; a 2xx body that won't read is CORRUPT_DATA. */
export async function fetchText(
  url: string,
  kind: FetchedFileKind,
  fetchImpl: typeof fetch,
  signal?: AbortSignal,
): Promise<string> {
  const response = await fetchOk(url, kind, fetchImpl, signal);
  try {
    return await response.text();
  } catch (cause) {
    throw new ShardError({
      code: "CORRUPT_DATA",
      url,
      message: `static-shard: the body of "${url}" could not be read — the deploy is corrupt. Re-run \`static-shard build\` and redeploy.`,
      cause,
    });
  }
}

/** Parse/decode a 2xx body's CONTENT into a domain structure; failures are CORRUPT_DATA. */
export function parseCorruptible<T>(url: string, parse: () => T): T {
  try {
    return parse();
  } catch (cause) {
    throw new ShardError({
      code: "CORRUPT_DATA",
      url,
      message: `static-shard: the body of "${url}" would not parse — the deploy is corrupt. Re-run \`static-shard build\` and redeploy.`,
      cause,
    });
  }
}
