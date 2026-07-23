export async function fetchShardRecords(
  basePath: string,
  hash: string,
  fetchImpl: typeof fetch,
): Promise<Record<string, unknown>[]> {
  const response = await fetchImpl(`${basePath}/shards/${hash}.ndjson`);
  if (!response.ok) {
    throw new Error(`static-shard: failed to fetch shard "${hash}" (status ${response.status})`);
  }
  const text = await response.text();
  return text
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .map((line) => JSON.parse(line) as Record<string, unknown>);
}
