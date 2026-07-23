import { describe, expect, test } from "vitest";
import { matchesWhere } from "../src/filter.js";

describe("matchesWhere", () => {
  test("no where clause matches everything", () => {
    expect(matchesWhere({ year: 2000 }, undefined)).toBe(true);
  });

  test("equals", () => {
    expect(matchesWhere({ year: 2000 }, { year: { equals: 2000 } })).toBe(true);
    expect(matchesWhere({ year: 2001 }, { year: { equals: 2000 } })).toBe(false);
  });

  test("in", () => {
    expect(matchesWhere({ year: 2001 }, { year: { in: [1999, 2001] } })).toBe(true);
    expect(matchesWhere({ year: 2002 }, { year: { in: [1999, 2001] } })).toBe(false);
  });

  test("startsWith", () => {
    expect(matchesWhere({ title: "Gladiator" }, { title: { startsWith: "Glad" } })).toBe(true);
    expect(matchesWhere({ title: "Snatch" }, { title: { startsWith: "Glad" } })).toBe(false);
  });

  test("gt/gte/lt/lte compose as implicit AND", () => {
    expect(matchesWhere({ year: 2005 }, { year: { gte: 2000, lt: 2010 } })).toBe(true);
    expect(matchesWhere({ year: 2010 }, { year: { gte: 2000, lt: 2010 } })).toBe(false);
    expect(matchesWhere({ year: 1999 }, { year: { gte: 2000, lt: 2010 } })).toBe(false);
  });

  test("multiple fields compose as implicit AND", () => {
    const record = { year: 2005, rating: 8.5 };
    expect(matchesWhere(record, { year: { gte: 2000 }, rating: { gt: 8 } })).toBe(true);
    expect(matchesWhere(record, { year: { gte: 2000 }, rating: { gt: 9 } })).toBe(false);
  });

  test("a missing field value never matches", () => {
    expect(matchesWhere({}, { year: { gte: 2000 } })).toBe(false);
  });

  test("endsWith (T6)", () => {
    expect(matchesWhere({ title: "The Dark Knight" }, { title: { endsWith: "Knight" } })).toBe(true);
    expect(matchesWhere({ title: "Gladiator" }, { title: { endsWith: "Knight" } })).toBe(false);
  });

  test("contains (T6)", () => {
    expect(matchesWhere({ title: "The Dark Knight" }, { title: { contains: "Dark" } })).toBe(true);
    expect(matchesWhere({ title: "Gladiator" }, { title: { contains: "Dark" } })).toBe(false);
  });

  test("not (T7) — negated equals, valid alongside a pruning op", () => {
    expect(matchesWhere({ title: "Snatch" }, { title: { not: "Gladiator" } })).toBe(true);
    expect(matchesWhere({ title: "Gladiator" }, { title: { not: "Gladiator" } })).toBe(false);
    // not + startsWith compose as implicit AND, same as any two ops on one field.
    expect(matchesWhere({ title: "Gladiator" }, { title: { not: "Gladiator", startsWith: "G" } })).toBe(false);
    expect(matchesWhere({ title: "Gnome" }, { title: { not: "Gladiator", startsWith: "G" } })).toBe(true);
  });
});

describe("presence semantics — isNull/isAbsent/exists (T7)", () => {
  test("isNull matches an explicit null, never a missing key or a real value", () => {
    expect(matchesWhere({ tagline: null }, { tagline: { isNull: true } })).toBe(true);
    expect(matchesWhere({}, { tagline: { isNull: true } })).toBe(false);
    expect(matchesWhere({ tagline: "x" }, { tagline: { isNull: true } })).toBe(false);
  });

  test("isAbsent matches a missing key, never an explicit null or a real value", () => {
    expect(matchesWhere({}, { tagline: { isAbsent: true } })).toBe(true);
    expect(matchesWhere({ tagline: null }, { tagline: { isAbsent: true } })).toBe(false);
    expect(matchesWhere({ tagline: "x" }, { tagline: { isAbsent: true } })).toBe(false);
  });

  test("exists: true only for a present, non-null value; exists: false for null OR missing", () => {
    expect(matchesWhere({ tagline: "x" }, { tagline: { exists: true } })).toBe(true);
    expect(matchesWhere({ tagline: null }, { tagline: { exists: true } })).toBe(false);
    expect(matchesWhere({}, { tagline: { exists: true } })).toBe(false);

    expect(matchesWhere({ tagline: null }, { tagline: { exists: false } })).toBe(true);
    expect(matchesWhere({}, { tagline: { exists: false } })).toBe(true);
    expect(matchesWhere({ tagline: "x" }, { tagline: { exists: false } })).toBe(false);
  });
});

describe("some — existential match on multi-valued fields (T7)", () => {
  test("shorthand `{ some: value }` is equivalent to `{ some: { equals: value } }`", () => {
    expect(matchesWhere({ genres: ["Sci-Fi", "Action"] }, { genres: { some: "Sci-Fi" } })).toBe(true);
    expect(matchesWhere({ genres: ["Drama"] }, { genres: { some: "Sci-Fi" } })).toBe(false);
  });

  test("object form applies any operator existentially across elements", () => {
    expect(matchesWhere({ genres: ["Sci-Fi Epic"] }, { genres: { some: { startsWith: "Sci-Fi" } } })).toBe(true);
    expect(matchesWhere({ genres: ["Drama"] }, { genres: { some: { startsWith: "Sci-Fi" } } })).toBe(false);
  });

  test("an empty array or a missing/null field never matches", () => {
    expect(matchesWhere({ genres: [] }, { genres: { some: "Sci-Fi" } })).toBe(false);
    expect(matchesWhere({}, { genres: { some: "Sci-Fi" } })).toBe(false);
    expect(matchesWhere({ genres: null }, { genres: { some: "Sci-Fi" } })).toBe(false);
  });
});
