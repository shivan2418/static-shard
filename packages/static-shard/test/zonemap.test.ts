import { describe, expect, test } from "vitest";
import { candidateShardIndices, pairCandidateShardIndices } from "../src/zonemap.js";

// 3 shards: shard0 [1900,1950), shard1 [1950,2000), shard2 [2000,2020] (inclusive both ends, last shard).
const splitPoints = [1900, 1950, 2000, 2020];

describe("candidateShardIndices", () => {
  test("no filter (or empty filter) selects every shard", () => {
    expect(candidateShardIndices(splitPoints, undefined)).toEqual([0, 1, 2]);
    expect(candidateShardIndices(splitPoints, {})).toEqual([0, 1, 2]);
  });

  test("equals selects exactly the one containing shard", () => {
    expect(candidateShardIndices(splitPoints, { equals: 1925 })).toEqual([0]);
    expect(candidateShardIndices(splitPoints, { equals: 1950 })).toEqual([1]); // boundary → next shard
    expect(candidateShardIndices(splitPoints, { equals: 2020 })).toEqual([2]); // last shard is closed both ends
  });

  test("equals outside the global range selects nothing", () => {
    expect(candidateShardIndices(splitPoints, { equals: 1800 })).toEqual([]);
    expect(candidateShardIndices(splitPoints, { equals: 2025 })).toEqual([]);
  });

  test("in selects the union of each value's shard, deduplicated and sorted", () => {
    expect(candidateShardIndices(splitPoints, { in: [1925, 2010] })).toEqual([0, 2]);
    expect(candidateShardIndices(splitPoints, { in: [1925, 1930] })).toEqual([0]);
  });

  test("range (gte/lte) selects the contiguous covering shard span", () => {
    expect(candidateShardIndices(splitPoints, { gte: 1950, lte: 2000 })).toEqual([1, 2]);
  });

  test("an open-ended lower range selects from the start", () => {
    expect(candidateShardIndices(splitPoints, { lt: 1950 })).toEqual([0]);
  });

  test("an open-ended upper range selects through the end", () => {
    expect(candidateShardIndices(splitPoints, { gt: 1950 })).toEqual([1, 2]);
  });

  test("an empty manifest (no shards) selects nothing", () => {
    expect(candidateShardIndices([], { equals: 2000 })).toEqual([]);
  });
});

describe("pairCandidateShardIndices", () => {
  // shard0 [Alpha,Golf], shard1 [Hotel,Papa], shard2 [Quebec,Zulu]
  const pairs: [string, string][] = [
    ["Alpha", "Golf"],
    ["Hotel", "Papa"],
    ["Quebec", "Zulu"],
  ];

  test("equals selects every shard whose pair could contain the value", () => {
    expect(pairCandidateShardIndices(pairs, { equals: "Kilo" })).toEqual(new Set([1]));
  });

  test("equals outside every pair's range selects nothing", () => {
    expect(pairCandidateShardIndices(pairs, { equals: "0" })).toEqual(new Set());
  });

  test("in selects the union across values", () => {
    expect(pairCandidateShardIndices(pairs, { in: ["Bravo", "Romeo"] })).toEqual(new Set([0, 2]));
  });

  test("a filter shape with neither equals nor in returns undefined (no zonemap signal)", () => {
    expect(pairCandidateShardIndices(pairs, {})).toBeUndefined();
  });

  test("skips a shard with no bound (zero non-null values for the field)", () => {
    const withGap: [unknown, unknown][] = [
      ["Alpha", "Golf"],
      [undefined, undefined],
      ["Quebec", "Zulu"],
    ];
    expect(pairCandidateShardIndices(withGap, { equals: "Charlie" })).toEqual(new Set([0]));
  });
});
