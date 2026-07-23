import { describe, expect, test } from "vitest";
import { compareSortValues } from "../src/sort.js";

describe("compareSortValues", () => {
  test("orders numbers ascending", () => {
    expect(compareSortValues(1, 2, "number")).toBeLessThan(0);
    expect(compareSortValues(2, 1, "number")).toBeGreaterThan(0);
    expect(compareSortValues(2, 2, "number")).toBe(0);
  });

  test("orders ISO date strings ascending", () => {
    expect(compareSortValues("2000-01-01", "2010-01-01", "date")).toBeLessThan(0);
    expect(compareSortValues("2010-01-01", "2000-01-01", "date")).toBeGreaterThan(0);
    expect(compareSortValues("2000-01-01", "2000-01-01", "date")).toBe(0);
  });

  test("treats missing values (null/undefined) as sorting after any real value", () => {
    expect(compareSortValues(undefined, 1, "number")).toBeGreaterThan(0);
    expect(compareSortValues(1, undefined, "number")).toBeLessThan(0);
    expect(compareSortValues(null, 1, "number")).toBeGreaterThan(0);
    expect(compareSortValues(undefined, undefined, "number")).toBe(0);
  });
});
