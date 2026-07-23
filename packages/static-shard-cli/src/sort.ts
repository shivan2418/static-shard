export type SortKind = "number" | "date";

/**
 * Compares two sort-field values. Missing values (null/undefined) sort after
 * every real value (ADR-0002: missing sort values cluster at the high end).
 */
export function compareSortValues(a: unknown, b: unknown, kind: SortKind): number {
  const aMissing = a === null || a === undefined;
  const bMissing = b === null || b === undefined;
  if (aMissing && bMissing) return 0;
  if (aMissing) return 1;
  if (bMissing) return -1;

  if (kind === "number") {
    return (a as number) - (b as number);
  }
  // date values compare as ISO strings
  const av = a as string;
  const bv = b as string;
  return av < bv ? -1 : av > bv ? 1 : 0;
}
