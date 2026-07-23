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
});
