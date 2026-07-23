export { build, type BuildOptions, type BuildResult } from "./build.js";
export { resolveConfig, loadConfigFile } from "./config.js";
export { getFormatVersion, getGeneratorVersion } from "./version.js";
export type {
  FieldConfig,
  FieldKind,
  Manifest,
  ResolvedConfig,
  SchemaDescriptor,
  ShardDescriptor,
  StaticShardConfig,
} from "./types.js";
