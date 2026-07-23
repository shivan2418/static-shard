import { fetchText, parseCorruptible } from "./fetch-file.js";

export async function fetchShardRecords(
  basePath: string,
  hash: string,
  fetchImpl: typeof fetch,
  signal?: AbortSignal,
): Promise<Record<string, unknown>[]> {
  const url = `${basePath}/shards/${hash}.ndjson`;
  const text = await fetchText(url, "referenced", fetchImpl, signal);
  return parseCorruptible(url, () =>
    text
      .split("\n")
      .map((line) => line.trim())
      .filter((line) => line.length > 0)
      .map((line) => JSON.parse(line) as Record<string, unknown>),
  );
}
