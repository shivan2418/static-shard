# static-shard

Query large static datasets from any static host — no database, no backend, no WASM, no HTTP Range requests.

`static-shard` splits a large dataset into many small files at build time, builds indexes over them, and generates a **typed client** that fetches only the files a query needs. Because it fetches whole small files (not byte ranges of one big file), it works on any dumb static host and every shard is a plain, compressible, cacheable CDN object.

> **Status: clean-slate rebuild (attempt #3), in design.**
> This library is currently being (re)designed via a wayfinder map before any implementation. The design decisions are being locked first, then a clean v1.0 will be built against the resulting spec.
>
> The previous working prototype (attempt #2) is preserved on the [`attempt-2-reference`](https://github.com/shivan2418/static-shard/tree/attempt-2-reference) branch for reference only — it is **not** the basis for the rebuild.

## License

MIT
