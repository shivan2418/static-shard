// Zonemap pruning over the sort field's split-points (ADR-0002/0003): the
// split-points ARE the search structure — binary search finds the shards that
// could possibly satisfy a where-filter on the sort field, so only those need
// to be fetched.

export type SortValue = number | string;

export interface SortFieldFilter {
  equals?: SortValue;
  in?: SortValue[];
  gt?: SortValue;
  gte?: SortValue;
  lt?: SortValue;
  lte?: SortValue;
}

function pointShardIndex(splitPoints: readonly SortValue[], value: SortValue, shardCount: number): number {
  let lo = 0;
  let hi = shardCount - 1;
  let ans = 0;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if (splitPoints[mid]! <= value) {
      ans = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

function upperBoundShardIndex(
  splitPoints: readonly SortValue[],
  value: SortValue,
  shardCount: number,
  strict: boolean,
): number {
  let lo = 0;
  let hi = shardCount - 1;
  let ans = -1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    const boundOk = strict ? splitPoints[mid]! < value : splitPoints[mid]! <= value;
    if (boundOk) {
      ans = mid;
      lo = mid + 1;
    } else {
      hi = mid - 1;
    }
  }
  return ans;
}

function isWithinGlobalRange(splitPoints: readonly SortValue[], value: SortValue): boolean {
  return value >= splitPoints[0]! && value <= splitPoints[splitPoints.length - 1]!;
}

function range(start: number, end: number): number[] {
  const out: number[] = [];
  for (let i = start; i <= end; i++) out.push(i);
  return out;
}

/** Shard ordinals (into `manifest.shards`) that could satisfy `filter` on the sort field. */
export function candidateShardIndices(
  splitPoints: readonly SortValue[],
  filter: SortFieldFilter | undefined,
): number[] {
  const shardCount = splitPoints.length - 1;
  if (shardCount <= 0) return [];

  if (!filter || Object.keys(filter).length === 0) return range(0, shardCount - 1);

  if (filter.in !== undefined) {
    const indices = new Set<number>();
    for (const value of filter.in) {
      if (isWithinGlobalRange(splitPoints, value)) indices.add(pointShardIndex(splitPoints, value, shardCount));
    }
    return [...indices].sort((a, b) => a - b);
  }

  if (filter.equals !== undefined) {
    if (!isWithinGlobalRange(splitPoints, filter.equals)) return [];
    return [pointShardIndex(splitPoints, filter.equals, shardCount)];
  }

  const hasLower = filter.gt !== undefined || filter.gte !== undefined;
  const hasUpper = filter.lt !== undefined || filter.lte !== undefined;

  const startIdx = hasLower ? pointShardIndex(splitPoints, (filter.gte ?? filter.gt)!, shardCount) : 0;
  const endIdx = hasUpper
    ? upperBoundShardIndex(splitPoints, (filter.lte ?? filter.lt)!, shardCount, filter.lt !== undefined)
    : shardCount - 1;

  if (endIdx < startIdx) return [];
  return range(startIdx, endIdx);
}
