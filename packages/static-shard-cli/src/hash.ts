import { createHash } from "node:crypto";

const HASH_LENGTH = 16;

/** Short deterministic content-hash used for shard/chunk file naming. */
export function contentHash(content: string | Buffer): string {
  return createHash("sha256").update(content).digest("hex").slice(0, HASH_LENGTH);
}
