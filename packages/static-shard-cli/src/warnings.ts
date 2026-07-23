import type { ShardDescriptor } from "./types.js";

/** Heuristic average sort-value run length past which a sort field counts as "low cardinality" (ADR-0002 §6) — scale-free, so it works identically whether cardinality came from raw records or shard-boundary split-points. Not a hard rule: `cutIntoShards` caps real `shardCount` at cardinality (equal-key runs never split), so this can't be phrased as "fewer distinct values than shards". */
const LOW_CARDINALITY_AVG_RUN_LENGTH = 20;

/** Distinct non-missing sort-field values across `records` — the raw input to `lowCardinalitySortFieldWarning`, shared by `build.ts` (materialize's own warnings) and `inspect --config`. */
export function sortFieldCardinalityOf(records: Record<string, unknown>[], sortField: string): number {
  const values = records.map((r) => r[sortField]).filter((v) => v !== null && v !== undefined);
  return new Set(values.map((v) => JSON.stringify(v))).size;
}

/** ADR-0002 §6: a low-cardinality sort field shards unevenly (every shard becomes a single-key pileup). */
export function lowCardinalitySortFieldWarning(recordCount: number, sortFieldCardinality: number): string | undefined {
  if (sortFieldCardinality === 0 || recordCount < LOW_CARDINALITY_AVG_RUN_LENGTH) return undefined;
  const avgRunLength = recordCount / sortFieldCardinality;
  if (avgRunLength <= LOW_CARDINALITY_AVG_RUN_LENGTH) return undefined;
  return `static-shard: the sort field has only ${sortFieldCardinality} distinct value(s) across ${recordCount} records (~${Math.round(avgRunLength)} per value) — low-cardinality sort fields shard unevenly (equal-key runs stay contiguous, ADR-0002 §6).`;
}

/** ADR-0002 §5: a record bigger than the shard-byte target gets its own oversized, flagged shard. */
export function oversizedRecordWarning(maxRecordBytes: number, shardBytes: number): string | undefined {
  if (maxRecordBytes <= shardBytes) return undefined;
  return `static-shard: the largest record is ${maxRecordBytes} bytes, over the ${shardBytes}-byte shard target — it will get its own oversized, flagged shard (ADR-0002 §5).`;
}

/** ADR-0002 §6: a single sort value's equal-key run pileup produces an oversized shard relative to the target. */
export function skewedShardsWarning(shards: ShardDescriptor[]): string | undefined {
  if (shards.length === 0) return undefined;
  const totalBytes = shards.reduce((sum, s) => sum + s.bytes, 0);
  const meanBytes = totalBytes / shards.length;
  if (meanBytes <= 0) return undefined;
  const oversized = shards.filter((s) => s.bytes > meanBytes * 2);
  if (oversized.length === 0) return undefined;
  return `static-shard: ${oversized.length} shard(s) are more than 2x the mean shard size (${Math.round(meanBytes)} bytes) — likely an equal-key pileup on the sort field or an oversized record (ADR-0002 §5/§6).`;
}
