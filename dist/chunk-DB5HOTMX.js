// src/client/index.ts
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
  findCandidateChunks(manifest, where) {
    if (!where) {
      return manifest.chunks.map((c) => c.id);
    }
    let candidateChunks = null;
    for (const [field, condition] of Object.entries(where)) {
      const index = manifest.indices[field];
      if (index && (typeof condition === "string" || typeof condition === "number" || typeof condition === "boolean")) {
        const value = String(condition);
        const chunks = index[value] || [];
        if (candidateChunks === null) {
          candidateChunks = new Set(chunks);
        } else {
          const current = candidateChunks;
          candidateChunks = new Set(chunks.filter((c) => current.has(c)));
        }
      } else if (typeof condition === "object" && condition !== null && "eq" in condition) {
        const value = String(condition.eq);
        const chunks = index?.[value] || [];
        if (index) {
          if (candidateChunks === null) {
            candidateChunks = new Set(chunks);
          } else {
            const current = candidateChunks;
            candidateChunks = new Set(chunks.filter((c) => current.has(c)));
          }
        }
      }
      if (typeof condition === "object" && condition !== null) {
        const rangeCondition = condition;
        const hasRangeOp = "gt" in rangeCondition || "gte" in rangeCondition || "lt" in rangeCondition || "lte" in rangeCondition;
        if (hasRangeOp) {
          const matchingChunks = manifest.chunks.filter((chunk) => {
            const range = chunk.fieldRanges[field];
            if (!range) return true;
            const min = range.min;
            const max = range.max;
            if (rangeCondition.gt !== void 0 && max <= rangeCondition.gt) return false;
            if (rangeCondition.gte !== void 0 && max < rangeCondition.gte) return false;
            if (rangeCondition.lt !== void 0 && min >= rangeCondition.lt) return false;
            if (rangeCondition.lte !== void 0 && min > rangeCondition.lte) return false;
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
    }
    return candidateChunks ? [...candidateChunks] : manifest.chunks.map((c) => c.id);
  }
  /**
   * Check if a record matches the where clause
   */
  matchesWhere(record, where) {
    if (!where) return true;
    for (const [field, condition] of Object.entries(where)) {
      const value = record[field];
      if (typeof condition !== "object" || condition === null) {
        if (value !== condition) return false;
        continue;
      }
      const ops = condition;
      if ("eq" in ops && value !== ops.eq) return false;
      if ("neq" in ops && value === ops.neq) return false;
      if ("gt" in ops && (typeof value !== "number" || value <= ops.gt)) return false;
      if ("gte" in ops && (typeof value !== "number" || value < ops.gte)) return false;
      if ("lt" in ops && (typeof value !== "number" || value >= ops.lt)) return false;
      if ("lte" in ops && (typeof value !== "number" || value > ops.lte)) return false;
      if ("contains" in ops && (typeof value !== "string" || !value.includes(ops.contains))) return false;
      if ("startsWith" in ops && (typeof value !== "string" || !value.startsWith(ops.startsWith))) return false;
      if ("endsWith" in ops && (typeof value !== "string" || !value.endsWith(ops.endsWith))) return false;
      if ("in" in ops && !ops.in.includes(value)) return false;
    }
    return true;
  }
  /**
   * Query records
   */
  async query(options = {}) {
    const manifest = await this.loadManifest();
    const candidateChunkIds = this.findCandidateChunks(manifest, options.where);
    const chunkPromises = candidateChunkIds.map((id) => this.loadChunk(id));
    const chunks = await Promise.all(chunkPromises);
    let results = [];
    for (const chunk of chunks) {
      for (const record of chunk) {
        if (this.matchesWhere(record, options.where)) {
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
    const results = await this.query({
      where: { [primaryField]: id },
      limit: 1
    });
    return results[0] || null;
  }
  /**
   * Count records matching a query
   */
  async count(options = {}) {
    const manifest = await this.loadManifest();
    if (!options.where) {
      return manifest.totalRecords;
    }
    const results = await this.query({ where: options.where });
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
  StaticShardClient,
  createClient
};
//# sourceMappingURL=chunk-DB5HOTMX.js.map