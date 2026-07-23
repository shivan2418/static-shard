import { fetchJson } from "./fetch-file.js";
import type { IndexChunkFile } from "./secondary-index.js";

export async function fetchIndexChunk(
  basePath: string,
  file: string,
  fetchImpl: typeof fetch,
  signal?: AbortSignal,
): Promise<IndexChunkFile> {
  return (await fetchJson(`${basePath}/${file}`, "referenced", fetchImpl, signal)) as IndexChunkFile;
}
