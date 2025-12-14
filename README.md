# static-shard

Query large static datasets efficiently by splitting them into chunks. Perfect for static sites that need to serve 500MB+ datasets without a database.

## The Problem

You have a large dataset (500MB+) that you want to serve from a static site. Loading the entire file would be slow and wasteful - users typically only need a small subset of the data.

## The Solution

`static-shard` splits your data into small chunks and generates a type-safe client that:
- Loads only the chunks needed for each query
- Uses indices to skip irrelevant chunks entirely
- Caches chunks for fast repeated queries

## Installation

```bash
npm install static-shard
```

## Quick Start

### 1. Build your data

```bash
# Basic usage
npx static-shard build data.json --output ./dist

# With options
npx static-shard build data.json \
  --output ./dist \
  --chunk-size 5mb \
  --chunk-by userId \
  --index "category,status"
```

### 2. Use the generated client

```typescript
import { createClient } from './dist/client';

const db = createClient({ basePath: '/data' });

// Query with filters
const products = await db.query({
  where: {
    category: 'electronics',
    price: { gte: 100, lte: 500 }
  },
  orderBy: 'price',
  limit: 20
});

// Get by primary key
const user = await db.get('user-123');

// Count records
const count = await db.count({ where: { status: 'active' } });
```

## CLI Commands

### `build`

Process a data file and generate chunks + client.

```bash
npx static-shard build <input> --output <dir> [options]
```

| Option | Description | Default |
|--------|-------------|---------|
| `-o, --output <dir>` | Output directory (required) | - |
| `-s, --chunk-size <size>` | Target chunk size | `5mb` |
| `-c, --chunk-by <field>` | Field to sort and chunk by | auto-detected |
| `-i, --index <fields>` | Comma-separated fields to index | auto-detected |
| `-f, --format <format>` | Input format (json, ndjson, csv) | auto-detected |

### `inspect`

Analyze a data file and get recommendations.

```bash
npx static-shard inspect <input> [options]
```

| Option | Description | Default |
|--------|-------------|---------|
| `-n, --sample <count>` | Records to sample for analysis | `1000` |
| `-f, --format <format>` | Input format | auto-detected |

Example output:

```
File: products.json
Size: 450.2 MB
Total records: 1,250,000

SCHEMA
  id: string [PRIMARY]
  name: string
  category: string [INDEXED] - cardinality: 25
  price: number - range: 0.99 - 9999.99
  inStock: boolean [INDEXED] - cardinality: 2

RECOMMENDATIONS
  Recommended --chunk-by: id
  Recommended --index: category,inStock

EXAMPLE COMMAND
  npx static-shard build products.json --output ./dist --chunk-by id --index "category,inStock"
```

## Output Structure

```
dist/
├── manifest.json     # Schema, chunk metadata, indices
├── chunks/
│   ├── 0.json       # First chunk of records
│   ├── 1.json
│   └── ...
└── client.ts        # Type-safe query client
```

### manifest.json

Contains everything the client needs to optimize queries:

- **schema**: Field names, types, and statistics
- **chunks**: List of chunks with record counts and field ranges
- **indices**: Inverted indices mapping values to chunk IDs

### client.ts

A self-contained TypeScript file with:

- Type definitions inferred from your data
- Query builder with autocomplete support
- Runtime that fetches and caches chunks

## Query API

### `query(options)`

Find records matching criteria.

```typescript
const results = await db.query({
  where: {
    // Exact match
    status: 'active',

    // Numeric comparisons
    age: { gte: 18, lt: 65 },

    // String operations
    name: { contains: 'john' },
    email: { endsWith: '@example.com' },

    // In list
    category: { in: ['electronics', 'clothing'] }
  },
  orderBy: 'createdAt',
  // or: orderBy: { field: 'createdAt', direction: 'desc' }
  limit: 50,
  offset: 100
});
```

### `get(id)`

Get a single record by primary key.

```typescript
const user = await db.get('user-123');
// Returns Record | null
```

### `count(options)`

Count matching records.

```typescript
// Total count (fast - reads manifest only)
const total = await db.count();

// Filtered count (loads relevant chunks)
const active = await db.count({ where: { status: 'active' } });
```

### `getSchema()`

Get schema information.

```typescript
const schema = await db.getSchema();
// { fields: [...], primaryField: 'id' }
```

### `clearCache()`

Clear the in-memory chunk cache.

```typescript
db.clearCache();
```

## How It Works

### Chunking

Data is sorted by a primary field and split into chunks of ~5MB each. This ensures related records are grouped together and queries can skip entire chunks.

### Indexing

Low-cardinality fields (like `status`, `category`) are indexed in the manifest. The index maps each unique value to the chunk IDs containing that value.

### Query Optimization

When you query:

1. **Index lookup**: For indexed fields, find chunks containing the value
2. **Range pruning**: For numeric ranges, skip chunks outside the range
3. **Parallel fetch**: Load candidate chunks simultaneously
4. **In-memory filter**: Apply remaining filters to loaded records

Example: Querying `{ category: 'electronics', price: { gte: 100 } }` on 100 chunks:
- Index lookup: `category='electronics'` → chunks [3, 7, 12, 45]
- Range pruning: `price >= 100` → chunks [7, 12, 45]
- Result: Only 3 chunks loaded instead of 100

## Supported Formats

| Format | Extension | Description |
|--------|-----------|-------------|
| JSON | `.json` | Array of objects `[{...}, {...}]` |
| NDJSON | `.ndjson`, `.jsonl` | Newline-delimited JSON |
| CSV | `.csv` | Comma-separated with headers |

## Browser Compatibility

The generated client uses:
- `fetch` API
- `Map` and `Set`
- `async/await`

Works in all modern browsers (Chrome 67+, Firefox 68+, Safari 14+, Edge 79+).

## Hosting

Deploy the output directory to any static host:

- **Vercel/Netlify**: Just deploy the folder
- **S3/CloudFlare R2**: Upload files, enable public access
- **GitHub Pages**: Commit to your repo

The client fetches chunks via HTTP, so CORS must allow your domain.

## Performance Tips

1. **Choose chunk size wisely**: Smaller chunks = more requests, larger = more wasted bandwidth
2. **Index selectively**: Only index fields you filter by frequently
3. **Use `limit`**: Avoid loading all matches when you only need a few
4. **Cache aggressively**: Chunks are immutable, set long cache headers

## TypeScript Support

The generated client is fully typed:

```typescript
// Record type matches your data
interface Record {
  id: string;
  name: string;
  age: number;
  // ...
}

// WhereClause only allows valid fields and operators
type WhereClause = {
  id?: string | StringOperators;
  age?: number | NumericOperators;
  // ...
};
```

## License

MIT
