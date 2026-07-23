import { describe, expect, test } from "vitest";
import { cutIntoShards } from "../src/shard.js";

function records(years: number[]): Record<string, unknown>[] {
  return years.map((year, i) => ({ id: i, year, title: `Movie ${i}` }));
}

describe("cutIntoShards", () => {
  test("puts everything in one shard when under the byte target", () => {
    const shards = cutIntoShards(records([2000, 2001, 2002]), "year", 1_000_000);
    expect(shards).toHaveLength(1);
    expect(shards[0]).toHaveLength(3);
  });

  test("cuts into multiple shards once the byte target is exceeded", () => {
    // Each record line is small; pick a tiny target to force multiple cuts.
    const shards = cutIntoShards(records([2000, 2001, 2002, 2003, 2004, 2005]), "year", 40);
    expect(shards.length).toBeGreaterThan(1);
    // Every record must appear exactly once, in original sorted order.
    const flatYears = shards.flat().map((r) => r.year);
    expect(flatYears).toEqual([2000, 2001, 2002, 2003, 2004, 2005]);
  });

  test("keeps equal-key runs contiguous even when they exceed the byte target", () => {
    const recs = records([2000, 2000, 2000, 2000, 2001]);
    const shards = cutIntoShards(recs, "year", 10); // tiny target — would split every record
    // All four 2000s must land in the same shard.
    const shardOf2000 = shards.find((s) => s.some((r) => r.year === 2000));
    expect(shardOf2000?.every((r) => r.year === 2000 || r.year === 2001)).toBe(true);
    const countOf2000 = shards.flatMap((s) => s).filter((r) => r.year === 2000).length;
    expect(countOf2000).toBe(4);
    // and no shard has both 2000 and 2001 split with another shard also holding 2000
    const shardsWith2000 = shards.filter((s) => s.some((r) => r.year === 2000));
    expect(shardsWith2000).toHaveLength(1);
  });

  test("never produces an empty shard", () => {
    const shards = cutIntoShards(records([2000, 2001, 2002]), "year", 1);
    for (const shard of shards) {
      expect(shard.length).toBeGreaterThan(0);
    }
  });
});
