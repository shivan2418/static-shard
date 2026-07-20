# Prior Art & Pattern Naming: "static-shard"

Concept under study: a build-time tool + zero-backend client JS library that **shards** a large (100MB–1GB+) JSON/NDJSON/CSV dataset into many small chunks, builds **inverted indexes** (value → chunk) and numeric **range metadata**, generates a **typed client**, and at query time fetches only the relevant chunks over plain HTTP from static hosting, then filters in memory.

## TL;DR

The *problem* — "query a large dataset in the browser with no database and no backend, served from static file hosting" — is well-trodden and has an established, named pattern. The *specific mechanism* static-shard uses (build-time splitting into **many small files** + an **inverted index / range metadata** sidecar + a **generated typed client**, all **JSON-native with no WASM and a zero-dependency client**) is a genuinely uncommon combination. The dominant prior art solves the same problem the *opposite* way: keep **one big file** and use **HTTP Range requests** to read byte slices of it inside a **WASM engine** (SQLite via sql.js-httpvfs, DuckDB-WASM over Parquet, PMTiles for map tiles, COG for imagery). The closest well-known relative is **phiresky's sql.js-httpvfs** (SQLite over HTTP Range). The closest *architectural* relative to static-shard's "split + sidecar index" idea is actually **Pagefind**, which shards its index into many chunks and lazily fetches only the needed ones — but Pagefind does full-text search, not general structured querying. Net: not novel as a *goal*, but the *JSON-native, multi-file, codegen'd, WASM-free* execution is a defensible niche.

## The pattern name(s)

There is no single canonical name; the space uses several overlapping terms. The most defensible, primary-source-backed labels:

- **"Cloud-optimized format" / "cloud-native" data** — the umbrella idea: lay out a file (or fileset) on plain object storage so clients read only the parts they need. Coined/popularized in geospatial (Cloud Optimized GeoTIFF, cogeo.org) and now generalized ("this template drove Zarr, PMTiles, etc."). This is the *design principle* static-shard embodies. Primary source: cogeo.org, guide.cloudnativegeo.org.
- **"HTTP Range querying" / "byte serving" / "partial fetch"** — the *transport* mechanism most competitors use (`Range: bytes=…`, `Accept-Ranges`). static-shard technically does NOT use this — it fetches whole small files by URL instead. Worth naming precisely because it is the main axis of difference.
- **"Query-in-place" / "in-place querying"** — querying data where it sits (object storage) without loading/importing it into a DB first. Used by DuckDB/Parquet ecosystem. Fits static-shard's spirit.
- **"Sharding" / "chunking"** — the correct word for splitting one dataset into many pieces. Pagefind and lunr issue-threads use "shard/chunk the index" for exactly static-shard's approach; SQLite/Parquet call their internal analog "pages" and "row groups."
- **"Serverless" / "static data API"** — marketing-level umbrella ("a database with no server"). Weak/ambiguous ("serverless" now mostly means Lambda/Aurora Data API, per AWS docs), so use with care.

**Recommendation for positioning static-shard:** describe it as a *"cloud-optimized, sharded static data layer"* or *"query-in-place static data API"*, and explicitly contrast "**many small files fetched whole**" vs the prevailing "**one big file read via HTTP Range**." "Sharding" is the accurate, honest verb for what the build step does.

## Prior art table

| Name | Link | Mechanism | Closeness | Key difference |
|---|---|---|---|---|
| **sql.js-httpvfs** (phiresky) | github.com/phiresky/sql.js-httpvfs | SQLite compiled to WASM (sql.js) + custom VFS that reads DB **pages** (1 KiB) from **one file** via **HTTP Range requests**; 3 virtual read-heads prefetch sequential reads. | **Very high** (same goal: query big DB from static host, read-only, no backend) | One file + Range reads inside **WASM SQLite**; static-shard = many small whole-file fetches, **no WASM**, JSON-native, custom index. |
| **DuckDB-WASM + remote Parquet** | duckdb.org/2021/10/29/duckdb-wasm ; VLDB p3574 | Full analytical SQL engine in WASM; `httpfs` reads Parquet **footer + column-chunk row groups** via **HTTP Range**, prunes row groups via **min/max column statistics**. | **Very high** (query-in-place over remote columnar file, no backend) | Heavy WASM engine + columnar Parquet + Range reads; static-shard is lightweight JSON, no engine, its "row-group stats" analog = the range metadata it builds. |
| **hyparquet** | hyperparam.app/about/opensource | Pure-JS (no WASM) Parquet reader; speculative trailing Range fetch for footer, parallel Range fetches for needed column chunks, skips row groups via min/max stats. | **High** (JS-native, no WASM, range-metadata-driven selective fetch — closest in *philosophy* to static-shard's index) | Still one Parquet file + Range reads; static-shard = own format, many files, build-time codegen. |
| **PMTiles** (Protomaps) | github.com/protomaps/PMTiles ; docs.protomaps.com/pmtiles | **Single-file** archive with 127-byte header + root/leaf directories mapping tile coords → byte offsets; any tile in ≤2 **HTTP Range requests**; served from S3, no tile server. | **High (architectural analog)** | Same "cloud-optimized single file + internal index + range reads" pattern, but for map tiles; static-shard splits into many files instead of one indexed archive. |
| **Cloud Optimized GeoTIFF (COG)** | cogeo.org ; guide.cloudnativegeo.org | Internally tiled + overviews GeoTIFF; clients issue **HTTP Range** GETs for just the tiles/zoom needed. | Medium (origin of "cloud-optimized" concept) | Imagery domain; one file + Range; but it is *the* template static-shard's design principle descends from. |
| **Pagefind** | pagefind.app ; github.com/Pagefind/pagefind | Build-time CLI indexes rendered HTML, **shards the index into many chunks (one fragment per page)**; WASM module **lazily fetches only the chunks a query narrows to**; two-stage fetch (result handles → `.data()` fragment). | **High (closest to static-shard's *architecture*)** | Splits into many files + fetches on demand — same shape as static-shard — but **full-text search only**, not general structured/range queries; uses WASM. |
| **lunr.js / Elasticlunr** | github.com/olivernn/lunr.js (issue #511) | Prebuilt full-text index shipped as one JSON blob, loaded whole into memory. | Medium | Full-text only; **no sharding by default** (index-sharding is an open, unimplemented request) — load-everything, the thing static-shard avoids. |
| **FlexSearch** | github.com/nextapps-de/flexsearch | In-memory full-text index; v0.8 adds "persistent indexes" + worker parallelism. | Medium | Full-text; designed to build/hold index in memory, not fetch-on-demand from static host. |
| **MiniSearch** | github.com/lucaong/minisearch | In-memory full-text index, supports async load. | Low–Medium | Full-text; "not suitable above ~50k records client-side." |
| **Stork / tinysearch** | (Rust→WASM search) | Build-time index + WASM search over a static index file. | Medium | Full-text; WASM; single index file. |
| **Datasette + datasette-lite** | github.com/simonw/datasette-lite | datasette-lite runs full Datasette (Python) in-browser via **Pyodide WASM**, downloads whole SQLite DB(s) and queries locally. | Medium | Downloads the **entire** DB into browser; heavy Python/WASM stack; not fetch-only-what-you-need. |
| **absurd-sql / AbsurderSQL** | github.com/jlongster/absurd-sql | SQLite (WASM) with a VFS that stores/reads DB blocks in **IndexedDB**. | Low | Local persistence/replication model, not fetch-from-static-host; you must get the data into IndexedDB first. |
| **PouchDB** | pouchdb.com | Replicates a CouchDB database into IndexedDB for offline/local query. | Low | Replication/sync model; needs a CouchDB-compatible backend; contrasts with fetch-on-demand. |
| **fergies-inverted-index / search-index** | github.com/fergiemcdowall/fergies-inverted-index | JS inverted index over objects, backed by LevelDB (node) / IndexedDB (browser). | Low–Medium | Local index store, not a static-hosted sharded fetch model; but conceptually the same "inverted index over JSON objects" primitive static-shard builds. |

## The two (three) closest relatives

**sql.js-httpvfs (phiresky) — closest well-known relative by *goal*.** phiresky compiles SQLite to WASM via emscripten and wraps sql.js with "a virtual file system that fetches chunks of the database with HTTP Range requests." The DB is stored as **one file** with a small page size (he uses 1 KiB); a query does a handful of page reads (e.g. "SQLite does 7 page reads for that query"), and a prefetcher with "three separate virtual read heads" exponentially grows request size for sequential scans so a full scan costs a number of requests "logarithmic in the total byte length." It needs only a static host that supports Range (GitHub Pages, S3, Netlify, Cloudflare all do) and is strictly **read-only**. This is the canonical "database on static hosting" and the single closest competitor. It differs from static-shard on three axes: (1) it keeps **one file** and slices it with **byte-range reads** rather than sharding into **many whole files**; (2) it runs a real **SQL engine in WASM**, whereas static-shard ships a JSON-native, zero-dependency, WASM-free client; (3) it uses SQLite's own B-tree pages/indexes rather than a **build-time-generated inverted index + typed client**.

**DuckDB-WASM over remote Parquet — closest relative by *analytics capability*.** DuckDB-WASM is "an in-process analytical SQL database for the browser" that, via the `httpfs` extension, reads a remote Parquet file with HTTP Range requests, "maintains exponentially growing readahead buffers," and "can skip entire row groups based on metadata statistics" (min/max per column chunk). It is the most capable no-backend query engine and directly embodies **query-in-place** over **cloud-optimized columnar** files. static-shard's "range metadata → fetch only relevant chunks" is conceptually the *same optimization* as Parquet's row-group pruning — but static-shard implements it as an external sidecar index over shard files rather than as footer statistics inside one Parquet file, and does it with plain JSON and no multi-MB WASM engine. (**hyparquet** is worth calling out here as the WASM-free JS variant: pure-JS Parquet reads driven by footer + column stats + range fetches — philosophically the nearest thing to static-shard that already exists, differing mainly in that it targets the Parquet format and one file rather than a custom many-file shard layout.)

**PMTiles — closest *architectural* analog.** A single file containing a header, a root directory, and leaf directories that map keys to byte offsets, letting any record be fetched in "at most two HTTP range requests regardless of the file's total size," served straight from S3 with no tile server. This is exactly static-shard's design principle (cloud-optimized, index-then-fetch, static hosting) applied to map tiles. The instructive contrast: PMTiles proved you can put the **index inside one range-queried archive**; static-shard chooses instead to **externalize the index and shard into many files** — trading PMTiles' single-object simplicity for the ability to serve individual chunks as cache-friendly whole objects on any dumb host (no Range support required).

## Where static-shard is genuinely different

1. **Many small whole files, not one range-read file.** Every major relative (SQLite/httpvfs, DuckDB/Parquet, PMTiles, COG, hyparquet) keeps **one big file** and reads byte ranges. static-shard shards into **many independent files fetched whole by URL**. Consequence: it works on **any** static host including ones with poor/no `Range` support, and each shard is an independently cacheable CDN object. This is a real, defensible distinction.
2. **JSON/NDJSON/CSV-native, no WASM, zero-dependency client.** Relatives ship a WASM engine (SQLite ~1MB+, DuckDB several MB, Pagefind/Stork WASM). A tiny JS-only client that speaks the source formats directly is a genuine lightness advantage, especially for cold-start/first-query latency and bundle size.
3. **Build-time typed codegen.** None of the SQL/search relatives generate a **typed client** bound to the dataset's schema at build time. This is closer to GraphQL-codegen / Prisma DX than to any static-data tool, and appears to be an unclaimed differentiator in this space.
4. **General structured + numeric-range querying, not full-text.** The many-small-files sharded pattern's best existing exemplar (Pagefind) is full-text only. static-shard applying that shape to **structured filtering and numeric range queries** is a gap in the current landscape.

## Gaps / opportunities the prior art suggests

- **Prefetch heuristics.** sql.js-httpvfs's "3 virtual read heads + exponential readahead" and DuckDB's growing readahead buffers show that adaptive prefetch materially cuts request counts. static-shard's many-file model should think about request amplification (too many tiny fetches) and consider batching / manifest-driven prefetch.
- **Columnar layout & predicate pushdown.** Parquet's row-group + column statistics are the mature version of static-shard's "range metadata." Adopting column-oriented shards would enable projection pushdown (fetch only needed fields), which the current row/chunk model may not.
- **Single-archive option.** PMTiles shows a single indexed archive can beat many files on request count and simplicity while staying range-served. A PMTiles-style single-file mode could be a future option where hosts support Range.
- **Write/mutation and freshness.** All the closest relatives are read-only; static-shard likely is too. Not a gap vs competitors, but a shared ceiling worth stating.
- **Discoverability / naming.** No competitor owns "sharded static data API for structured queries." The terminology is fragmented — an opportunity to define the category, but also a marketing risk (users search for "SQLite on static hosting," "DuckDB in browser," "static site search").
- **What competitors do that static-shard doesn't:** full SQL / joins / aggregation (DuckDB, SQLite), mature formats with ecosystem tooling (Parquet), proven prefetch, and (for search tools) ranking/fuzzy matching. static-shard trades all of that for lightness + typing + host-agnosticism.

## Sources

- sql.js-httpvfs repo: https://github.com/phiresky/sql.js-httpvfs
- phiresky, "Hosting SQLite databases on Github Pages": https://phiresky.github.io/blog/2021/hosting-sqlite-databases-on-github-pages/
- sql.js-httpvfs on npm: https://www.npmjs.com/package/sql.js-httpvfs
- DuckDB-Wasm announcement: https://duckdb.org/2021/10/29/duckdb-wasm
- DuckDB-Wasm VLDB paper (Kohn et al.): https://www.vldb.org/pvldb/vol15/p3574-kohn.pdf
- DuckDB-Wasm (MotherDuck overview): https://motherduck.com/blog/duckdb-wasm-in-browser/
- hyparquet / Hyperparam open source: https://hyperparam.app/about/opensource
- PMTiles repo: https://github.com/protomaps/PMTiles
- PMTiles concepts docs: https://docs.protomaps.com/pmtiles/
- PMTiles V3 blog: https://protomaps.com/blog/pmtiles-v3-whats-new/
- Cloud Optimized GeoTIFF: https://cogeo.org/
- Cloud-Native Geospatial Formats Guide (COG): https://guide.cloudnativegeo.org/cloud-optimized-geotiffs/intro.html
- OGC COG standard: https://docs.ogc.org/is/21-026/21-026.html
- Pagefind: https://pagefind.app/
- Pagefind repo: https://github.com/Pagefind/pagefind
- "Introducing Pagefind" (CloudCannon): https://cloudcannon.com/blog/introducing-pagefind/
- lunr.js index-sharding issue #511: https://github.com/olivernn/lunr.js/issues/511
- FlexSearch repo: https://github.com/nextapps-de/flexsearch
- MiniSearch alternatives / limits: https://comforterp.com/article/best-minisearch-alternatives-javascript-search-tools
- datasette-lite repo: https://github.com/simonw/datasette-lite
- Datasette: https://github.com/simonw/datasette
- absurd-sql repo: https://github.com/jlongster/absurd-sql
- AbsurderSQL: https://github.com/npiesco/absurder-sql
- SQLite persistence on the web (PowerSync): https://powersync.com/blog/sqlite-persistence-on-the-web
- fergies-inverted-index: https://github.com/fergiemcdowall/fergies-inverted-index
- Empirical Evaluation of Columnar Storage Formats (VLDB): https://www.vldb.org/pvldb/vol17/p148-zeng.pdf
- AWS Aurora Serverless Data API (on "serverless" terminology): https://aws.amazon.com/blogs/database/introducing-the-data-api-for-amazon-aurora-serverless-v2-and-amazon-aurora-provisioned-clusters/
