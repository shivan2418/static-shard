/** Evaluates one non-null, present value against a single scalar operator (equals/in/gt/.../not). */
function matchesValueOp(value: unknown, op: string, opValue: unknown): boolean {
  switch (op) {
    case "equals":
      return value === opValue;
    case "not":
      return value !== opValue;
    case "in":
      return (opValue as unknown[]).includes(value);
    case "gt":
      return (value as number | string) > (opValue as number | string);
    case "gte":
      return (value as number | string) >= (opValue as number | string);
    case "lt":
      return (value as number | string) < (opValue as number | string);
    case "lte":
      return (value as number | string) <= (opValue as number | string);
    case "startsWith":
      return (value as string).startsWith(opValue as string);
    case "endsWith":
      return (value as string).endsWith(opValue as string);
    case "contains":
      return (value as string).includes(opValue as string);
    default:
      throw new Error(`static-shard: unsupported operator "${op}"`);
  }
}

/** `some` (T7): existential match over a multi-valued field's elements — shorthand value ≡ `{ equals: value }`. */
function matchesSome(values: unknown[], someFilter: unknown): boolean {
  if (typeof someFilter === "object" && someFilter !== null) {
    return values.some((element) =>
      Object.entries(someFilter as Record<string, unknown>).every(([op, opValue]) =>
        matchesValueOp(element, op, opValue),
      ),
    );
  }
  return values.includes(someFilter);
}

/**
 * Evaluates one field's operator filter against a record (T7): `isNull`/`isAbsent`/`exists`
 * distinguish an explicit `null` from a genuinely missing key; every other operator
 * (including `some` and the `not` rider) requires a present, non-null value.
 */
export function matchesFieldFilter(record: Record<string, unknown>, field: string, filter: Record<string, unknown>): boolean {
  const isAbsent = !(field in record);
  const value = isAbsent ? undefined : record[field];
  const isNull = !isAbsent && value === null;

  for (const [op, opValue] of Object.entries(filter)) {
    if (op === "isNull") {
      if (isNull !== opValue) return false;
      continue;
    }
    if (op === "isAbsent") {
      if (isAbsent !== opValue) return false;
      continue;
    }
    if (op === "exists") {
      const exists = !isAbsent && !isNull;
      if (exists !== opValue) return false;
      continue;
    }
    if (isAbsent || isNull) return false;
    if (op === "some") {
      if (!matchesSome(value as unknown[], opValue)) return false;
      continue;
    }
    if (!matchesValueOp(value, op, opValue)) return false;
  }
  return true;
}

/** Implicit-AND across every field in `where` (ADR-0001). */
export function matchesWhere(
  record: Record<string, unknown>,
  where: Record<string, Record<string, unknown>> | undefined,
): boolean {
  if (!where) return true;
  for (const [field, filter] of Object.entries(where)) {
    if (!matchesFieldFilter(record, field, filter)) return false;
  }
  return true;
}
