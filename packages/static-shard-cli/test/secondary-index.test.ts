import { describe, expect, test } from "vitest";
import {
  buildInvertedIndex,
  computeSecondaryZonemap,
  truncateStringLower,
  truncateStringUpper,
} from "../src/secondary-index.js";
import type { IndexChunkFile } from "../src/secondary-index.js";

describe("truncateStringLower / truncateStringUpper", () => {
  test("short strings pass through untouched", () => {
    expect(truncateStringLower("Cameron")).toBe("Cameron");
    expect(truncateStringUpper("Cameron")).toBe("Cameron");
  });

  test("long strings are truncated to the max length on the lower bound", () => {
    expect(truncateStringLower("Zwigoff, Terry", 8)).toBe("Zwigoff,");
  });

  test("the upper bound is a next-string-after marker: every string sharing the prefix compares less than it", () => {
    const upper = truncateStringUpper("Zwigoff, Terry", 8);
    expect(upper).not.toBe("Zwigoff,");
    for (const v of ["Zwigoff, Terry", "Zwigoff, Terrylabel", "Zwigoff, Terry Jr", "Zwigoff, T"]) {
      expect(v < upper).toBe(true);
    }
  });
});

describe("computeSecondaryZonemap", () => {
  test("string field: per-shard truncated [min,max] pairs, marked truncated", () => {
    const groups = [
      [{ title: "Gladiator" }, { title: "Snatch" }],
      [{ title: "Interstellar" }, { title: "Parasite" }],
    ];
    const zonemap = computeSecondaryZonemap(groups, "title", "string");
    expect(zonemap.truncated).toBe(true);
    expect(zonemap.pairs).toEqual([
      ["Gladiator", "Snatch"],
      ["Interstellar", "Parasite"],
    ]);
  });

  test("number field: raw min/max pairs, no truncation flag", () => {
    const groups = [[{ rating: 8.7 }, { rating: 7.2 }], [{ rating: 9.0 }]];
    const zonemap = computeSecondaryZonemap(groups, "rating", "number");
    expect(zonemap.truncated).toBeUndefined();
    expect(zonemap.pairs).toEqual([
      [7.2, 8.7],
      [9.0, 9.0],
    ]);
  });

  test("ignores null/undefined values when computing min/max", () => {
    const groups = [[{ rating: null }, { rating: 5 }, { rating: 8 }, { rating: undefined }]];
    const zonemap = computeSecondaryZonemap(groups, "rating", "number");
    expect(zonemap.pairs).toEqual([[5, 8]]);
  });
});

/** Independently reconstructs the values a chunk's front-coded dictionary encodes. */
function decodeChunk(content: string): { value: string; shards: number[] }[] {
  const file = JSON.parse(content) as IndexChunkFile;
  let prevKey = "";
  return file.entries.map((entry) => {
    prevKey = prevKey.slice(0, entry.prefixLen) + entry.suffix;
    let acc = 0;
    const shards = entry.postings.map((delta) => (acc += delta));
    return { value: prevKey, shards };
  });
}

describe("buildInvertedIndex", () => {
  test("one chunk: sorted distinct values with correct per-shard postings, including a repeated value across shards", () => {
    const groups = [
      [{ title: "Snatch" }], // shard 0
      [{ title: "Gladiator" }, { title: "Memento" }], // shard 1
      [{ title: "Gladiator" }], // shard 2 — repeats shard 1's value
    ];
    const chunks = buildInvertedIndex(groups, "title", "string", 1_000_000);
    expect(chunks).toHaveLength(1);

    const decoded = decodeChunk(chunks[0]!.content);
    expect(decoded.map((d) => d.value)).toEqual(["Gladiator", "Memento", "Snatch"]);
    expect(decoded.find((d) => d.value === "Gladiator")?.shards).toEqual([1, 2]);
    expect(decoded.find((d) => d.value === "Memento")?.shards).toEqual([1]);
    expect(decoded.find((d) => d.value === "Snatch")?.shards).toEqual([0]);
    expect(chunks[0]!.from).toBe("Gladiator");
    expect(chunks[0]!.to).toBe("Snatch");
  });

  test("front-codes a shared prefix between adjacent sorted values", () => {
    const groups = [[{ name: "Cameron, James" }], [{ name: "Cameron, Jane" }]];
    const chunks = buildInvertedIndex(groups, "name", "string", 1_000_000);
    const file = JSON.parse(chunks[0]!.content) as IndexChunkFile;
    expect(file.entries[0]!.prefixLen).toBe(0);
    expect(file.entries[1]!.prefixLen).toBeGreaterThan(0);
    expect(file.entries[1]!.suffix.length).toBeLessThan("Cameron, Jane".length);
  });

  test("splits into multiple byte-target chunks, each self-decodable and covering a contiguous value range", () => {
    const titles = ["Alpha", "Bravo", "Charlie", "Delta", "Echo", "Foxtrot", "Golf", "Hotel"];
    const groups = titles.map((t) => [{ title: t }]);
    const chunks = buildInvertedIndex(groups, "title", "string", 40);
    expect(chunks.length).toBeGreaterThan(1);

    const allValues: string[] = [];
    for (const chunk of chunks) {
      const file = JSON.parse(chunk.content) as IndexChunkFile;
      expect(file.entries[0]!.prefixLen).toBe(0); // resets per chunk — independently fetchable/decodable

      const decoded = decodeChunk(chunk.content);
      expect(chunk.from).toBe(decoded[0]!.value);
      expect(chunk.to).toBe(decoded[decoded.length - 1]!.value);
      allValues.push(...decoded.map((d) => d.value));
    }
    expect(allValues).toEqual([...titles].sort());
  });

  test("number kind: dictionary values round-trip through the canonical-key encoding", () => {
    const groups = [[{ rating: 8.7 }], [{ rating: 7.2 }, { rating: 9.0 }]];
    const chunks = buildInvertedIndex(groups, "rating", "number", 1_000_000);
    expect(chunks).toHaveLength(1);
    expect(chunks[0]!.from).toBe(7.2);
    expect(chunks[0]!.to).toBe(9.0);
  });

  test("returns no chunks when the field has no non-null values", () => {
    expect(buildInvertedIndex([[{ title: null }]], "title", "string", 1_000_000)).toEqual([]);
  });
});
