# static-shard-cli

Build tool for [static-shard](https://www.npmjs.com/package/static-shard): infer → shard → index → codegen, plus the config wizard. A Node-only devDependency — install it alongside the runtime `static-shard` package, never in production.

## Quickstart

```bash
pnpm add static-shard && pnpm add -D static-shard-cli
npx static-shard-cli init      # interactive wizard, or fully flag-driven with --yes
npx static-shard build         # → public/shard-data/  (deploy this)  +  src/shard-db/  (commit this)
```

`init` is the only place inference happens (sampled by default, `--full-scan` for exact cardinalities); it writes a single committed `static-shard.config.json`. `build` is headless (no TTY, safe in CI): it replays the config's baked schema — never re-infers — and fails loudly if your data has drifted from it.

### Commands

- **`init`** — interactive wizard or `--yes` + flags (fully non-interactive, scriptable). Detects the input format, infers a schema, recommends a sort field and default indexed set, and persists `static-shard.config.json`.
- **`build`** — reads the committed config, shards + indexes the data, writes the served tree (default `public/shard-data/`) and regenerates the typed client (default `src/shard-db/`). Flags: `--config`, `--out`, `--no-clean`.
- **`inspect`** — read-only report over a config or built directory: shard/index sizes, cost estimates, and warnings, without rebuilding. Flags: `--config`/`--dir`, `--json`.

Every wizard choice is also a CLI flag (nothing wizard-only), so `init --yes` with the right flags reproduces exactly what the wizard would have written — config generation is fully scriptable for CI.

See the [project README](https://github.com/shivan2418/static-shard#readme) for the full pitch and design, and [`examples/`](https://github.com/shivan2418/static-shard/tree/master/examples) for two complete example apps built with this CLI.

## License

MIT
