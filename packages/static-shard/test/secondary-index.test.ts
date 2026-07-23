import { describe, expect, test } from "vitest";
import {
  chunksForFilter,
  decodeIndexChunk,
  shardIndicesForFilter,
  type IndexChunkFile,
  type SecondaryFieldFilter,
} from "../src/secondary-index.js";
import type { IndexChunkDirEntry } from "../src/manifest.js";

describe("decodeIndexChunk", () => {
  test("reconstructs front-coded string values and delta-decoded postings", () => {
    // "Gladiator" (full), "Memento" (shares nothing), "Snatch" (shares nothing) — a real front-coded chunk
    // would share prefixes when adjacent values overlap; this fixture exercises both shared and unshared cases.
    const file: IndexChunkFile = {
      entries: [
        { prefixLen: 0, suffix: "Gladiator", postings: [1] },
        { prefixLen: 0, suffix: "Memento", postings: [1] },
        { prefixLen: 0, suffix: "Snatch", postings: [0] },
      ],
    };
    const decoded = decodeIndexChunk(file, "string");
    expect(decoded).toEqual([
      { value: "Gladiator", shardIndices: [1] },
      { value: "Memento", shardIndices: [1] },
      { value: "Snatch", shardIndices: [0] },
    ]);
  });

  test("reconstructs a shared prefix via front-coding", () => {
    const file: IndexChunkFile = {
      entries: [
        { prefixLen: 0, suffix: "Cameron, James", postings: [0] },
        { prefixLen: 9, suffix: "Jane", postings: [1] },
      ],
    };
    const decoded = decodeIndexChunk(file, "string");
    expect(decoded[1]!.value).toBe("Cameron, Jane");
  });

  test("decodes multi-value postings via cumulative delta sum", () => {
    // [7, 35, 61] → shards 7, 42, 103 (ADR-0003 §5 worked example).
    const file: IndexChunkFile = { entries: [{ prefixLen: 0, suffix: "x", postings: [7, 35, 61] }] };
    expect(decodeIndexChunk(file, "string")[0]!.shardIndices).toEqual([7, 42, 103]);
  });

  test("coerces number-kind entries back to numbers", () => {
    const file: IndexChunkFile = { entries: [{ prefixLen: 0, suffix: "8.7", postings: [0] }] };
    expect(decodeIndexChunk(file, "number")[0]!.value).toBe(8.7);
  });

  test("coerces boolean-kind entries back to booleans", () => {
    const file: IndexChunkFile = {
      entries: [
        { prefixLen: 0, suffix: "false", postings: [0] },
        { prefixLen: 0, suffix: "true", postings: [1] },
      ],
    };
    const decoded = decodeIndexChunk(file, "boolean");
    expect(decoded[0]!.value).toBe(false);
    expect(decoded[1]!.value).toBe(true);
  });
});

describe("chunksForFilter", () => {
  const chunks: IndexChunkDirEntry[] = [
    { from: "Alpha", to: "Foxtrot", file: "index/title/1.json" },
    { from: "Golf", to: "November", file: "index/title/2.json" },
    { from: "Oscar", to: "Zulu", file: "index/title/3.json" },
  ];

  test("equals selects the one chunk whose range contains the value", () => {
    expect(chunksForFilter(chunks, { equals: "Hotel" })).toEqual([chunks[1]]);
  });

  test("in unions each value's chunk, deduplicated", () => {
    expect(chunksForFilter(chunks, { in: ["Alpha", "Bravo", "Papa"] })).toEqual([chunks[0], chunks[2]]);
  });

  test("startsWith selects every chunk whose range overlaps the prefix", () => {
    // "F" prefix could match "Foxtrot" (end of chunk 1) or "Fox..." only — chunk 1 alone.
    expect(chunksForFilter(chunks, { startsWith: "F" })).toEqual([chunks[0]]);
    // "N" prefix spans the boundary between chunk 2 ("November") and chunk 3 ("Oscar").
    expect(chunksForFilter(chunks, { startsWith: "N" })).toEqual([chunks[1]]);
  });

  test("a value outside every chunk's range selects nothing", () => {
    expect(chunksForFilter(chunks, { equals: "!!!" })).toEqual([]);
  });
});

describe("shardIndicesForFilter", () => {
  const decoded = [
    { value: "Gladiator", shardIndices: [1, 2] },
    { value: "Memento", shardIndices: [1] },
    { value: "Snatch", shardIndices: [0] },
  ];

  test("equals returns just that value's shards", () => {
    expect(shardIndicesForFilter(decoded, { equals: "Gladiator" })).toEqual(new Set([1, 2]));
  });

  test("in unions the shards of every matching value", () => {
    expect(shardIndicesForFilter(decoded, { in: ["Memento", "Snatch"] })).toEqual(new Set([1, 0]));
  });

  test("startsWith unions the shards of every value with the prefix", () => {
    const withShared = [
      { value: "The Matrix", shardIndices: [0] },
      { value: "The Matrix Reloaded", shardIndices: [1] },
      { value: "Snatch", shardIndices: [2] },
    ];
    expect(shardIndicesForFilter(withShared, { startsWith: "The Matrix" })).toEqual(new Set([0, 1]));
  });

  test("no matching value returns an empty set", () => {
    const filter: SecondaryFieldFilter = { equals: "nonexistent" };
    expect(shardIndicesForFilter(decoded, filter)).toEqual(new Set());
  });
});
