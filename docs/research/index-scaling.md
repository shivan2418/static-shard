# R1 — Index-Scaling Patterns for `static-shard`

*Research ticket resolution. Date: 2026-07-19.*

## TL;DR

A single-file manifest is fine up to roughly **1 MB gzipped** (comfortably a few hundred thousand postings). It starts to hurt in the **1–10 MB** range and becomes a self-defeating bottleneck past **~10 MB gzipped**, because *every* client pays that download before running a single query — exactly the 400 MB-of-JSON problem the project exists to avoid, just relocated. The mature analogs (Pagefind, Parquet, SQLite-over-HTTP, Lucene) all converge on the same answer: **make the index itself hierarchical and fetched on demand** — ship a tiny root/entry file up front, and split the bulky inverted index into many alphabetically/hash-ranged *index shards* that the client fetches only when a query touches their key range. For `static-shard` specifically (many whole files, no WASM, no HTTP Range), the recommended design is a **two-tier manifest**: a small always-loaded root manifest (schema + per-shard range metadata + a chunk directory) plus lazily-fetched per-field index-chunk files keyed by sorted value range, with delta-encoded/roaring-bitmap postings and optional per-data-shard bloom filters. Pagefind is the near-exact precedent and should be the template.

---

## The scaling problem quantified

The manifest carries three things of very different growth behavior:

| Component | Grows with | Rough size |
|---|---|---|
| Schema | # fields | trivial (bytes–KB) |
| Per-shard range metadata (min/max per field) | # shards × # fields | linear, usually small |
| **Inverted index (value → shard-id list)** | **cardinality C × avg shards-per-value** | **the dominant term** |

### Per-shard range metadata

If the data is split into `S` shards with `F` fields, min/max metadata is `O(S × F)`. This is the same growth law Parquet's footer has (`O(row_groups × columns)`) — see below. For 10,000 shards × 20 fields that's ~200k min/max pairs; at ~20 bytes/pair JSON that's ~4 MB raw, but it delta/range-compresses well and is bounded. This is usually *not* the thing that blows up first, but it is not free — Parquet hits multi-hundred-MB footers in pathological cases (593k row groups → 2.9 GB footer), so unbounded shard counts matter.

### The inverted index — the real problem

Model: `N` records, sharded so each shard holds `r` records, giving `S = N/r` shards. An indexed field of cardinality `C`:

- **Postings-list size** = for each distinct value, the set of shard-ids containing it. If a value appears in `k` records placed roughly randomly across shards, it touches `min(k, S)` shards. A shard-id costs ~2–4 bytes packed (or ~5–7 bytes as JSON-with-commas).
- **Total index entries** ≈ `Σ_value (shards touched)`.

Two regimes:

1. **Low cardinality / clustered** (e.g. `country`, `year`, a boolean): C is small, and if data is *sorted* before sharding, each value lands in a contiguous run of shards, so postings are tiny (a range, or a handful of ids). Index stays KB-sized. Great.
2. **High cardinality / scattered** (e.g. `user_id`, `email`, free-text tokens, a near-unique key): worst case C ≈ N and each value is a near-unique posting. The index degenerates to roughly *one entry per record per indexed field* — i.e. it re-encodes the dataset's key column. For `N = 10M` records that's ~10M postings; even at 4 bytes each that's **40 MB** before the value strings themselves. Add the term dictionary (the value strings) and it can exceed the size of the column it indexes.

**Crossover rules of thumb (gzipped, single manifest):**

- **< ~1 MB**: single manifest is clearly fine. Corresponds to ≲ ~200–300k total postings, or low-cardinality fields at any N.
- **~1–10 MB**: single manifest works but is a noticeable up-front tax on every page load; start range-pruning aggressively and consider splitting the highest-cardinality field out.
- **> ~10 MB**: single manifest is a bottleneck — shard the index. This is reached by any high-cardinality field (C approaching N) once N is in the low millions, or by many medium-cardinality indexed fields combined.

Key asymmetry that motivates everything below: the manifest is **downloaded in full by 100% of clients**, whereas the point of the tool is that data shards are downloaded selectively. An index that isn't *also* selectively downloaded violates the core invariant.

---

## Prior-art approaches

| Approach | Who uses it | How it works | Applicability to `static-shard` (many whole files · no WASM · no Range) |
|---|---|---|---|
| **Sharded index chunks, alphabetical ranges** | **Pagefind** | Root `pagefind-entry.json` → `.pf_meta` (holds a *chunk directory*: `from`/`to` sorted-word boundaries → which `.pf_index` file). Client loads meta, binary-searches ranges, fetches only the chunk(s) covering the query term, then fetches per-page `.pf_fragment` files for results. ~40 KB index chunks, 1–10 KB fragments. | **Direct fit.** Whole-file fetches, no Range needed, ranges live in a small meta file. The reference design. (WASM is Pagefind's *query engine*, not required by the layout — the layout is portable to JS.) |
| **Footer + per-row-group column stats (min/max)** | **Parquet** | Thrift footer read whole; holds schema + per-row-group per-column min/max/null counts for predicate pushdown. `O(row_groups × columns)`. | This *is* `static-shard`'s per-shard range metadata. Confirms the pattern works — and warns it grows `O(S×F)` and must be bounded (truncate long min/max strings; cap shard count). |
| **Two-level page index (ColumnIndex/OffsetIndex)** | **Parquet** (v2) | Separate structures near the footer, *pointed to* by footer offsets, deserialized only when a query is selective. Second, finer level below the coarse footer stats. | Motivates a **coarse→fine tier**: keep coarse per-shard ranges in the root, push finer/heavier index detail into separately-fetched files referenced by offset/name. |
| **Paged B-tree index over HTTP Range** | **sql.js-httpvfs** (phiresky) | DB is fixed pages (1–4 KiB). Index is a B-tree *on disk*; a lookup reads only the ~log(N) pages on the root→leaf path via Range requests. Prefetch heads coalesce sequential reads. A key lookup moves ~1 KB from a huge DB. | Concept applies (index paginated + fetched on demand) but the **mechanism needs HTTP Range**, which `static-shard` forgoes. Translate the *idea* to whole-file chunks, not byte ranges. |
| **Lazy-loaded partial index (requested, unsolved)** | **lunr.js #511 / #76 / #222** | Community asks to shard `invertedIndex` + `fieldVectors` by term and load on demand. Not implemented — lunr's serialized index is one blob; splitting it cleanly is hard because scoring needs global stats. | Cautionary tale: retrofitting sharding onto a monolithic index format is painful. Design the shardable layout **up front**. |
| **FST / block-tree term dictionary, front-coding** | **Lucene / Elasticsearch** (`.tip`/`.tim`) | `.tip` holds an in-memory FST mapping term prefix → on-disk block in `.tim`; `.tim` stores terms in blocks (25–48 terms) front-coded (shared-prefix elision). Keeps the giant term dictionary off-heap; only touched blocks are read. | The **term dictionary** (your distinct field *values*) is a cost `static-shard` also pays. Front-coding sorted string values + a small prefix directory is a cheap, no-dependency win. |
| **Roaring bitmaps for postings** | **Lucene/ES filter cache, many DBs** | 32-bit ids split by high 16 bits into containers; each container is an array (<4096 vals, 2 B/val), a bitmap (8 KB, dense), or run-length. Fast AND/OR without full decompression. | **Adopt for postings lists** (value → shard-id set). Shard-ids are small dense-ish integers → roaring is compact and supports multi-clause intersection cheaply in JS (roaring-wasm/roaring npm exist, but a plain-array container impl is trivial). |
| **Frame-of-reference delta + bit packing** | **Lucene postings** | Postings in blocks of 256, delta-encoded, bit-packed to min bits/value. | Simplest possible postings compression; Pagefind's own "delta-encode page numbers" gave ~45% shrink. Cheap to adopt. |
| **Per-file bloom filter** | **Cassandra/Scylla SSTables, Parquet SBBF** | Compact probabilistic filter per file answers "definitely not present" → skip the file with zero data read. Parquet SBBF = 256-bit blocks. | **Adopt per data-shard** for equality/membership on high-cardinality fields where range-pruning fails. Lets the client skip fetching shards without an exhaustive inverted index. Filters are small and can live in the root or in a fetched sidecar. |

---

## Deep dives

### 1. Pagefind's sharded index (most relevant)

Pagefind is the closest architectural analog to `static-shard` and the strongest template. Layout (files under `/pagefind/`):

- **`pagefind-entry.json`** — tiny entry point. Loaded first; points at the current `.pf_meta` file (versioned/hashed name).
- **`.pf_meta`** — the *metadata index* (CBOR binary). Contains: format version, the page list (page hash + word count), **the index-chunk directory**, and filter/sort definitions. The chunk directory is the crux: each entry is a `{ from, to, hash }` triple that "communicates the `pagefind/index/*.pf_index` file we need to load when searching for a word that sorts between `from` and `to`." So the meta file holds only *range boundaries*, not the postings.
- **`.pf_index` files** (`pagefind/index/`) — the sharded inverted index. Each chunk holds a `WordIndex` = list of `PackedWord` (the word string + a `PackedPage` list of page numbers and in-page word locations). Chunks are arranged so words sort into ordered, non-overlapping ranges — "searching for 'CloudCannon' doesn't need to load the region containing 'Jamstack'." **~40 KB per index chunk.** A 10k-page site produces on the order of dozens of chunks (build logs report e.g. "Created 27 index chunks").
- **`.pf_fragment` files** (`pagefind/fragment/`) — one per page; the actual content/metadata/anchors used to render a result. **~1–10 KB each.**

**Query flow (whole-file fetches, no Range):**
1. Load `pagefind-entry.json` (once).
2. Load the `.pf_meta` chunk directory (once per session).
3. Stem the query term, binary-search the chunk directory's `from`/`to` ranges, fetch **only** the matching `.pf_index` chunk(s). (Note: chunk selection must use the *stemmed* term — Pagefind issue #478 was a bug where the unstemmed term picked the wrong chunk.)
4. Compute matching pages from the postings; fetch **only** those pages' `.pf_fragment` files to render results.

**Compression/encoding facts:** index files are CBOR via `minicbor`, **compressed at build time and served as-is** (no reliance on server gzip). Delta-encoding page numbers and word locations made chunks **~45% smaller**. Net result: full-text search over all of MDN in **< 300 KB total including the WASM engine**; most sites ~100 KB. Critically, that total is *per-query selective*, not an up-front whole-index download.

**Lesson for `static-shard`:** the meta file is a **directory of ranges**, deliberately kept small and postings-free; the heavy postings live in range-keyed chunks fetched on demand. This maps one-to-one onto a `static-shard` two-tier manifest.

### 2. Parquet metadata scaling

Parquet's **footer** (Thrift-serialized) is read *whole* before any data: it carries schema, row counts, and **per-row-group per-column statistics (min/max/null counts)** that drive predicate pushdown / row-group pruning. This is the mature analog of `static-shard`'s per-shard range metadata, and it teaches the failure mode:

- **Growth is `O(row_groups × columns)`.** You cannot seek into the footer to one row group — a reader must fetch and Thrift-decode the whole footer first. Wide tables and huge row-group counts blow it up: real reports of 676 MB `_metadata`, and a pathological 52-col × 593k-row-group file with a **2.9 GB** footer that overflows an i32 length field.
- **Min/max stats are the main bloat.** Omitting column statistics improved decode ~30%. Writers **truncate long min/max values** (store `"B".."C"` instead of full strings) specifically to bound index size — a directly transferable trick.
- **Thrift decode, not transfer, dominates** once bytes are in memory — a reminder that a giant JSON manifest costs parse time on top of download.
- **Second tier: Page Index (ColumnIndex + OffsetIndex).** These finer per-page min/max structures live *separately* near the footer and are **referenced by offset in the column-chunk metadata**, so a non-selective scan never deserializes them. This is the coarse→fine, fetch-only-if-needed pattern: keep the always-read tier small, push finer detail behind a pointer.

**Lesson:** per-shard range metadata works and is worth keeping in the root — but bound it (cap shard count, truncate min/max strings), and if it grows, split the *fine* detail into pointer-referenced sidecars.

### 3. SQLite paged index (sql.js-httpvfs)

phiresky's sql.js-httpvfs hosts a read-only SQLite file on static hosting and queries it from the browser. The insight most relevant to `static-shard`: **the index is itself paginated and fetched on demand.** SQLite stores everything in fixed pages (default 4 KiB; the demo uses 1 KiB to cut per-request overhead). An indexed lookup walks a B-tree, reading only the `~log(N)` pages on the root→leaf path — e.g. "two page reads for the index lookup, two for the table data." A VFS shim turns each page read into an **HTTP Range request**, so a key lookup transfers ~1 KB from an arbitrarily large DB instead of the whole file. A prefetcher with three "read heads" exponentially grows request size on sequential access, so a complex query needs ~10–20 requests / 130–270 KB instead of hundreds of tiny ones. A **covering index** keeps the whole query inside the index B-tree; a table *scan* is catastrophic (downloads everything).

**Caveat for `static-shard`:** the whole mechanism depends on **HTTP Range requests**, which this project deliberately does not use. So the *implementation* doesn't transfer — but the *principle* does: paginate the index and fetch only the pieces a query traverses. `static-shard` achieves the same principle with **many whole small files** (Pagefind's approach) rather than byte ranges into one file. It also validates two design pressures: (a) request *count* matters, so chunk granularity shouldn't be so fine that a query needs dozens of fetches; (b) index layout must let the common query stay "covering" (answerable from the index/manifest + a few shards, never a full scan).

---

## Recommended design for `static-shard`

### Decision rule: single manifest vs sharded index

Compute the projected manifest size at build time and pick a mode automatically:

- **Single manifest** when the fully-built, gzipped manifest is **< ~1 MB** (typical: only low/medium-cardinality fields indexed, or small N). Keep it simple — one file, done.
- **Shard the index** when it exceeds **~1 MB gzipped** (hard requirement above ~5–10 MB). Emit the two-tier layout below. Make the threshold a config knob; emit a build-time warning when a single field's postings dominate.

Also decide **per field**, not globally: keep small low-cardinality fields inline in the root manifest, and only externalize the high-cardinality field(s) whose postings dominate. Most datasets have one or two "problem" fields.

### Layout when sharding (Pagefind-shaped, no WASM, no Range)

**Tier 1 — root manifest (`manifest.json`, always fetched, keep small — target < 500 KB gzipped):**
- Schema.
- Per-shard range metadata (min/max per field) for range-pruning — **truncate long string min/max** (Parquet trick); if this alone exceeds budget, move it to a fetched sidecar per Tier 2.
- For each indexed field, a **chunk directory**: an ordered list of `{ from, to, file }` entries giving the sorted value-range boundaries for each index-chunk file (exactly Pagefind's `.pf_meta` chunk table). Boundaries only — no postings.
- Optionally, small per-shard **bloom-filter** blobs (or a pointer to a bloom sidecar) for high-cardinality equality fields.
- Build metadata / format version (so the client can reject incompatible manifests).

**Tier 2 — index-chunk files (`index/<field>/<hash>.json` or binary, fetched on demand):**
- Keyed by **sorted value range**. Client resolves a query value → binary-search the chunk directory → fetch the one (or few) chunk file(s) whose `[from,to]` covers it.
- Each chunk = a **front-coded** term sub-dictionary (shared-prefix elision over the sorted values in range) + postings lists (value → shard-id set).
- **Postings compression:** delta-encode + bit-pack sorted shard-ids (Lucene FOR / Pagefind's delta trick — ~45% observed), or **roaring bitmaps** for dense/large sets (array container < 4096 ids at 2 B each, bitmap container for dense). Prefer roaring when postings support multi-clause AND/OR, since it intersects without full decompression.
- **Target chunk size ~30–60 KB** (Pagefind's ~40 KB is a good anchor) — small enough for selective fetch, large enough that a query rarely needs many chunks (respecting the sql.js-httpvfs request-count lesson).

**Tier 3 — data shards (unchanged):** whole files, fetched only after pruning + index lookup narrows the set.

### Query flow
1. Fetch root manifest once (cache aggressively; content-hash filenames for immutability).
2. Range-pruning from per-shard min/max eliminates shards for range predicates — no index fetch needed.
3. For equality/membership on an indexed high-cardinality field: binary-search that field's chunk directory, fetch the covering index chunk(s), read the postings → candidate shard set.
4. Optionally intersect with per-shard bloom filters to drop shards the index can't (or to avoid needing a complete inverted index at all).
5. Fetch only the surviving data shards; filter in memory.

### Compression
- **Compress index chunks at build time**, serve as-is (Pagefind does this) so the design doesn't depend on server-side gzip/brotli config. Still set `Content-Encoding` when you can.
- Root manifest: gzip/brotli; keep it JSON for debuggability unless size forces CBOR/MessagePack.
- Content-hash all Tier-1/Tier-2 filenames → `Cache-Control: immutable`, so repeat visits and shared chunks cost nothing.

### Bloom filters / roaring bitmaps — verdict
- **Roaring bitmaps: adopt for postings** once you shard. Shard-ids are small integers; roaring is compact, fast to intersect, and well-trodden (Lucene, many DBs). For small postings a plain sorted delta array is enough — pick per-list by size (roaring already does this internally).
- **Per-shard bloom filters: adopt selectively** for high-cardinality equality where range-pruning is useless and a full inverted index would be huge. A bloom filter per data-shard (Parquet SBBF-style, ~a few hundred bytes to KBs each) lets the client skip shards with "definitely not present," potentially **replacing** the inverted index for the nastiest fields — trading a small false-positive fetch rate for a bounded, N-independent-per-shard index size. This is the single most promising way to cap the cost of a near-unique key field.

### What to fetch up-front vs on demand
- **Up front:** schema, per-shard range metadata (bounded/truncated), chunk directories (ranges only), format version. Optionally small bloom filters.
- **On demand:** index chunks (by value range), data shards, large bloom sidecars, fine-grained secondary stats.

---

## Open questions for the "Index & manifest design" decision

1. **Sorted-load ordering is the biggest lever.** If records are sorted by the primary indexed field before sharding, low-cardinality postings collapse to ranges and range-pruning does almost everything — but you can only physically sort by *one* field. Which field wins, and how much does the index degrade for the *other* indexed fields under that ordering? (Most important open question — see final message.)
2. **Chunk granularity vs request count:** what chunk size / how many chunks per query is acceptable given no HTTP Range and no request coalescing? Need a target for max fetches per query (sql.js-httpvfs kept it to ~10–20 *with* prefetch; we have neither).
3. **Bloom filter vs inverted index for high-cardinality fields:** accept a false-positive fetch rate (extra whole-shard downloads) in exchange for an N-independent index? What FP rate is tolerable given shard file sizes?
4. **Roaring bitmap dependency cost:** pull in a JS/WASM roaring lib (bundle size, and WASM which the project otherwise avoids) vs a hand-rolled delta-array postings format? Where's the crossover where roaring's compression justifies it?
5. **Multi-field AND queries:** when a query constrains two indexed fields, do we intersect postings client-side (needs both chunks fetched) or pick the most selective field and post-filter? Intersection favors roaring.
6. **Manifest/index versioning & partial rebuilds:** content-hashed chunk names give cache immutability but complicate incremental dataset updates (cf. Pagefind's "updateable indexes?" discussion #831). Is the dataset append-mostly, full-rebuild, or mutable?
7. **Per-shard metadata ceiling:** at what shard count does even the bounded `O(S×F)` range metadata blow the root budget (the Parquet footer failure mode)? Do we need a coarse→fine split of the range metadata itself (Parquet Page Index pattern)?

---

## Sources

**Pagefind**
- Pagefind homepage — https://pagefind.app/
- Getting Started / docs — https://pagefind.app/docs/
- Indexing config — https://pagefind.app/docs/indexing/
- Metadata docs — https://pagefind.app/docs/metadata/
- CloudCannon: "Introducing Pagefind: static low-bandwidth search at scale" (authors' announcement, chunking + payload numbers) — https://cloudcannon.com/blog/introducing-pagefind/
- Will Smidlein, "Pagefind" (file layout: pagefind-entry.json / .pf_meta chunk `from`/`to` ranges / .pf_index / .pf_fragment) — https://blog.willsmidlein.com/posts/2025/jan/21/pagefind/
- Bill Mill / llimllib notes on Pagefind (chunking, payload) — https://notes.billmill.org/programming/javascript/pagefind.html
- GitHub issue #478 — chunk selection must use the *stemmed* word — https://github.com/CloudCannon/pagefind/issues/478
- GitHub discussion #831 — updateable indexes — https://github.com/Pagefind/pagefind/discussions/831
- Pagefind CHANGELOG (CBOR/minicbor, delta-encoding ~45% smaller chunks, build-time compression) — https://github.com/CloudCannon/pagefind/blob/main/CHANGELOG.md

**lunr.js**
- Issue #511 — "Lazy loading partial index?" (shard invertedIndex/fieldVectors by term) — https://github.com/olivernn/lunr.js/issues/511
- Issue #76 — "Split Indexes" — https://github.com/olivernn/lunr.js/issues/76
- Issue #222 — large index (800k items) — https://github.com/olivernn/lunr.js/issues/222
- Prebuilding indexes guide — https://lunrjs.com/guides/index_prebuilding.html

**Parquet**
- Apache Parquet — Page Index (ColumnIndex / OffsetIndex) — https://parquet.apache.org/docs/file-format/pageindex/
- "Anatomy of a Parquet File" (Towards Data Science) — https://towardsdatascience.com/anatomy-of-a-parquet-file/
- Apache DataFusion blog: "Parquet Pruning in DataFusion: Read Only What Matters" — https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/
- arrow-rs issue #5770 — metadata sizes for 1000+ columns (O(row_group×column), truncated min/max, stats bloat) — https://github.com/apache/arrow-rs/issues/5770
- dask issue #8027 — `_metadata` too large to parse — https://github.com/dask/dask/issues/8027
- polars issue #23162 — footer > 2 GB aborts (i32 overflow) — https://github.com/pola-rs/polars/issues/23162

**SQLite over HTTP**
- phiresky, "Hosting SQLite databases on GitHub Pages" (paged B-tree, Range requests, prefetch read-heads, ~1 KB/lookup, covering index) — https://phiresky.github.io/blog/2021/hosting-sqlite-databases-on-github-pages/
- phiresky/sql.js-httpvfs repo — https://github.com/phiresky/sql.js-httpvfs

**Inverted-index / term-dictionary techniques**
- Elastic blog: "Frame of Reference and Roaring Bitmaps" (FOR blocks of 256, roaring containers, 4096 threshold, 2 B/val arrays, 8 KB bitmaps) — https://www.elastic.co/blog/frame-of-reference-and-roaring-bitmaps
- Roaring bitmaps overview (Medium, A. Desai) — https://medium.com/@amit.desai03/roaring-bitmaps-fast-data-structure-for-inverted-indexes-5490fa4d1b27
- Lucene BlockTreeTermsWriter (.tim/.tip, FST prefix → block, 25–48 terms/block) — https://lucene.apache.org/core/4_8_0/core/org/apache/lucene/codecs/BlockTreeTermsWriter.html
- Mike McCandless, "Using Finite State Transducers in Lucene" — https://blog.mikemccandless.com/2010/12/using-finite-state-transducers-in.html
- "Compressed String Dictionaries" (front-coding survey, arXiv) — https://arxiv.org/pdf/1101.5506
- Lucene issue #12513 — tantivy-style term dictionary / Trie default (Lucene 10.3) — https://github.com/apache/lucene/issues/12513

**Bloom filters**
- Apache Cassandra — Bloom Filters (skip SSTables that can't contain a row) — https://cassandra.apache.org/doc/latest/cassandra/managing/operating/bloom_filters.html
- ScyllaDB glossary — Bloom filter (per-shard, CPU-local) — https://www.scylladb.com/glossary/bloom-filter/
- Pydantic Logfire — "Bloom filter folding in Parquet" (SBBF, 256-bit blocks) — https://pydantic.dev/articles/bloom-filter-folding-parquet-logfire
