import type { IndexChunkFile } from "./secondary-index.js";

export async function fetchIndexChunk(basePath: string, file: string, fetchImpl: typeof fetch): Promise<IndexChunkFile> {
  const response = await fetchImpl(`${basePath}/${file}`);
  if (!response.ok) {
    throw new Error(`static-shard: failed to fetch index chunk "${file}" (status ${response.status})`);
  }
  return (await response.json()) as IndexChunkFile;
}
