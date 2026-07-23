/** Evaluates one field's operator filter (equals/in/gt/gte/lt/lte) against a value. */
export function matchesFieldFilter(value: unknown, filter: Record<string, unknown>): boolean {
  if (value === null || value === undefined) return false;
  for (const [op, opValue] of Object.entries(filter)) {
    switch (op) {
      case "equals":
        if (value !== opValue) return false;
        break;
      case "in":
        if (!(opValue as unknown[]).includes(value)) return false;
        break;
      case "gt":
        if (!((value as number | string) > (opValue as number | string))) return false;
        break;
      case "gte":
        if (!((value as number | string) >= (opValue as number | string))) return false;
        break;
      case "lt":
        if (!((value as number | string) < (opValue as number | string))) return false;
        break;
      case "lte":
        if (!((value as number | string) <= (opValue as number | string))) return false;
        break;
      case "startsWith":
        if (!(value as string).startsWith(opValue as string)) return false;
        break;
      case "endsWith":
        if (!(value as string).endsWith(opValue as string)) return false;
        break;
      case "contains":
        if (!(value as string).includes(opValue as string)) return false;
        break;
      default:
        throw new Error(`static-shard: unsupported operator "${op}"`);
    }
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
    if (!matchesFieldFilter(record[field], filter)) return false;
  }
  return true;
}
