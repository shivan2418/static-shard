// src/client/index.ts
function eq(field, value) {
  return { field, operator: "eq", value };
}
function neq(field, value) {
  return { field, operator: "neq", value };
}
function gt(field, value) {
  return { field, operator: "gt", value };
}
function gte(field, value) {
  return { field, operator: "gte", value };
}
function lt(field, value) {
  return { field, operator: "lt", value };
}
function lte(field, value) {
  return { field, operator: "lte", value };
}
function contains(field, value) {
  return { field, operator: "contains", value };
}
function startsWith(field, value) {
  return { field, operator: "startsWith", value };
}
function endsWith(field, value) {
  return { field, operator: "endsWith", value };
}
function inArray(field, values) {
  return { field, operator: "in", value: values };
}
function stringField(field) {
  return {
    eq: (value) => eq(field, value),
    neq: (value) => neq(field, value),
    contains: (value) => contains(field, value),
    startsWith: (value) => startsWith(field, value),
    endsWith: (value) => endsWith(field, value),
    in: (values) => inArray(field, values)
  };
}
function numericField(field) {
  return {
    eq: (value) => eq(field, value),
    neq: (value) => neq(field, value),
    gt: (value) => gt(field, value),
    gte: (value) => gte(field, value),
    lt: (value) => lt(field, value),
    lte: (value) => lte(field, value),
    in: (values) => inArray(field, values)
  };
}
function booleanField(field) {
  return {
    eq: (value) => eq(field, value),
    neq: (value) => neq(field, value)
  };
}
var QueryBuilder = class {
  client;
  _conditions = [];
  _orderBy;
  _limit;
  _offset;
  constructor(client) {
    this.client = client;
  }
  /**
   * Add a where condition. Multiple calls use AND logic.
   */
  where(condition) {
    this._conditions.push(condition);
    return this;
  }
  /**
   * Set sort order
   */
  orderBy(field, direction = "asc") {
    this._orderBy = { field, direction };
    return this;
  }
  /**
   * Limit the number of results
   */
  limit(count) {
    this._limit = count;
    return this;
  }
  /**
   * Skip a number of results
   */
  offset(count) {
    this._offset = count;
    return this;
  }
  /**
   * Build the options object for internal use
   */
  buildOptions() {
    return {
      conditions: this._conditions.length > 0 ? this._conditions : void 0,
      orderBy: this._orderBy,
      limit: this._limit,
      offset: this._offset
    };
  }
  /**
   * Execute the query and return all matching results
   */
  async execute() {
    return this.client.executeQuery(this.buildOptions());
  }
  /**
   * Execute the query and return only the first result (or null)
   */
  async first() {
    const results = await this.client.executeQuery({
      ...this.buildOptions(),
      limit: 1
    });
    return results[0] ?? null;
  }
  /**
   * Get the count of matching records
   */
  async count() {
    return this.client.executeCount({
      conditions: this._conditions.length > 0 ? this._conditions : void 0
    });
  }
};
var StaticShardClient = class {
  basePath;
  manifest = null;
  chunkCache = /* @__PURE__ */ new Map();
  constructor(options) {
    this.basePath = options.basePath.replace(/\/$/, "");
  }
  /**
   * Load the manifest file
   */
  async loadManifest() {
    if (this.manifest) return this.manifest;
    const response = await fetch(`${this.basePath}/manifest.json`);
    if (!response.ok) {
      throw new Error(`Failed to load manifest: ${response.statusText}`);
    }
    this.manifest = await response.json();
    return this.manifest;
  }
  /**
   * Load a chunk by ID
   */
  async loadChunk(chunkId) {
    const cached = this.chunkCache.get(chunkId);
    if (cached) return cached;
    const manifest = await this.loadManifest();
    const chunkMeta = manifest.chunks.find((c) => c.id === chunkId);
    if (!chunkMeta) {
      throw new Error(`Chunk not found: ${chunkId}`);
    }
    const response = await fetch(`${this.basePath}/${chunkMeta.path}`);
    if (!response.ok) {
      throw new Error(`Failed to load chunk ${chunkId}: ${response.statusText}`);
    }
    const records = await response.json();
    this.chunkCache.set(chunkId, records);
    return records;
  }
  /**
   * Find chunk IDs that might contain matching records
   */
  findCandidateChunks(manifest, conditions) {
    if (!conditions || conditions.length === 0) {
      return manifest.chunks.map((c) => c.id);
    }
    let candidateChunks = null;
    for (const condition of conditions) {
      const { field, operator, value } = condition;
      const index = manifest.indices[field];
      if (operator === "eq" && index) {
        const strValue = String(value);
        const chunks = index[strValue] || [];
        if (candidateChunks === null) {
          candidateChunks = new Set(chunks);
        } else {
          const current = candidateChunks;
          candidateChunks = new Set(chunks.filter((c) => current.has(c)));
        }
      }
      if (["gt", "gte", "lt", "lte"].includes(operator)) {
        const rangeValue = value;
        const matchingChunks = manifest.chunks.filter((chunk) => {
          const range = chunk.fieldRanges[field];
          if (!range) return true;
          const min = range.min;
          const max = range.max;
          switch (operator) {
            case "gt":
              return max > rangeValue;
            case "gte":
              return max >= rangeValue;
            case "lt":
              return min < rangeValue;
            case "lte":
              return min <= rangeValue;
          }
          return true;
        }).map((c) => c.id);
        if (candidateChunks === null) {
          candidateChunks = new Set(matchingChunks);
        } else {
          const current = candidateChunks;
          candidateChunks = new Set(matchingChunks.filter((c) => current.has(c)));
        }
      }
    }
    return candidateChunks ? [...candidateChunks] : manifest.chunks.map((c) => c.id);
  }
  /**
   * Check if a record matches all conditions
   */
  matchesConditions(record, conditions) {
    if (!conditions || conditions.length === 0) return true;
    for (const condition of conditions) {
      const { field, operator, value: condValue } = condition;
      const recordValue = record[field];
      switch (operator) {
        case "eq":
          if (recordValue !== condValue) return false;
          break;
        case "neq":
          if (recordValue === condValue) return false;
          break;
        case "gt":
          if (typeof recordValue !== "number" || recordValue <= condValue) return false;
          break;
        case "gte":
          if (typeof recordValue !== "number" || recordValue < condValue) return false;
          break;
        case "lt":
          if (typeof recordValue !== "number" || recordValue >= condValue) return false;
          break;
        case "lte":
          if (typeof recordValue !== "number" || recordValue > condValue) return false;
          break;
        case "contains":
          if (typeof recordValue !== "string" || !recordValue.includes(condValue)) return false;
          break;
        case "startsWith":
          if (typeof recordValue !== "string" || !recordValue.startsWith(condValue)) return false;
          break;
        case "endsWith":
          if (typeof recordValue !== "string" || !recordValue.endsWith(condValue)) return false;
          break;
        case "in":
          if (!condValue.includes(recordValue)) return false;
          break;
      }
    }
    return true;
  }
  /**
   * Start a chainable query
   */
  query() {
    return new QueryBuilder(this);
  }
  /**
   * Execute a query with options (internal, used by QueryBuilder)
   */
  async executeQuery(options = {}) {
    const manifest = await this.loadManifest();
    const candidateChunkIds = this.findCandidateChunks(manifest, options.conditions);
    const chunkPromises = candidateChunkIds.map((id) => this.loadChunk(id));
    const chunks = await Promise.all(chunkPromises);
    let results = [];
    for (const chunk of chunks) {
      for (const record of chunk) {
        if (this.matchesConditions(record, options.conditions)) {
          results.push(record);
        }
      }
    }
    if (options.orderBy) {
      const field = typeof options.orderBy === "string" ? options.orderBy : options.orderBy.field;
      const direction = typeof options.orderBy === "string" ? "asc" : options.orderBy.direction;
      results.sort((a, b) => {
        const aVal = a[field];
        const bVal = b[field];
        if (aVal === bVal) return 0;
        if (aVal === null || aVal === void 0) return 1;
        if (bVal === null || bVal === void 0) return -1;
        const cmp = aVal < bVal ? -1 : 1;
        return direction === "asc" ? cmp : -cmp;
      });
    }
    const offset = options.offset || 0;
    const limit = options.limit;
    if (offset > 0 || limit !== void 0) {
      results = results.slice(offset, limit !== void 0 ? offset + limit : void 0);
    }
    return results;
  }
  /**
   * Get a single record by primary key
   */
  async get(id) {
    const manifest = await this.loadManifest();
    const primaryField = manifest.schema.primaryField;
    if (!primaryField) {
      throw new Error("No primary field defined in schema");
    }
    const results = await this.executeQuery({
      conditions: [eq(primaryField, id)],
      limit: 1
    });
    return results[0] || null;
  }
  /**
   * Count records matching a query (internal, used by QueryBuilder)
   */
  async executeCount(options = {}) {
    const manifest = await this.loadManifest();
    if (!options.conditions || options.conditions.length === 0) {
      return manifest.totalRecords;
    }
    const results = await this.executeQuery({ conditions: options.conditions });
    return results.length;
  }
  /**
   * Get schema information
   */
  async getSchema() {
    const manifest = await this.loadManifest();
    return manifest.schema;
  }
  /**
   * Clear the chunk cache
   */
  clearCache() {
    this.chunkCache.clear();
  }
};
function createClient(options) {
  return new StaticShardClient(options);
}

export {
  eq,
  neq,
  gt,
  gte,
  lt,
  lte,
  contains,
  startsWith,
  endsWith,
  inArray,
  stringField,
  numericField,
  booleanField,
  QueryBuilder,
  StaticShardClient,
  createClient
};
//# sourceMappingURL=chunk-RQ4DLMRC.js.map