import type { FieldSchemaEntry, Manifest, ResolvedConfig, SchemaDescriptor, ShardDescriptor } from "./types.js";

/** The sort field prunes via zonemap alone, so every numeric/date operator is free. */
const SORT_FIELD_OPERATORS = ["equals", "in", "gt", "gte", "lt", "lte"] as const;

/** N+1 monotonic boundaries: splitPoints[i] = min value of shard i; the final entry is the last shard's max. */
export function computeSplitPoints(groups: Record<string, unknown>[][], sortField: string): unknown[] {
  if (groups.length === 0) return [];
  const points = groups.map((group) => group[0]![sortField]);
  const lastGroup = groups[groups.length - 1]!;
  points.push(lastGroup[lastGroup.length - 1]![sortField]);
  return points;
}

function buildSchemaDescriptor(config: ResolvedConfig): SchemaDescriptor {
  const fields: Record<string, FieldSchemaEntry> = {};
  for (const [name, field] of Object.entries(config.fields)) {
    const indexed = name === config.sortField;
    fields[name] = {
      kind: field.kind,
      isDate: field.kind === "date",
      indexed,
      operators: indexed ? SORT_FIELD_OPERATORS : [],
    };
  }
  return { collection: config.collection, sortField: config.sortField, fields };
}

export function buildManifest(opts: {
  config: ResolvedConfig;
  shardFiles: ShardDescriptor[];
  splitPoints: unknown[];
  formatVersion: number;
  generatorVersion: string;
}): Manifest {
  const { config, shardFiles, splitPoints, formatVersion, generatorVersion } = opts;
  const recordCount = shardFiles.reduce((sum, s) => sum + s.count, 0);

  return {
    formatVersion,
    generatorVersion,
    dataset: {
      collection: config.collection,
      recordCount,
      shardCount: shardFiles.length,
      sortField: config.sortField,
    },
    schema: buildSchemaDescriptor(config),
    shards: shardFiles.map(({ hash, bytes, count }) => ({ hash, bytes, count })),
    zonemap: { [config.sortField]: { splitPoints } },
  };
}
