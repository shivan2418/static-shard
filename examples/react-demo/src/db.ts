/**
 * Database client with full TypeScript types
 *
 * This file re-exports the generated client with proper types.
 * When you type `db.query().` you'll get IntelliSense for:
 * - .where({ ... }) - with field-specific autocomplete
 * - .orderBy(...) - with sortable field suggestions
 * - .limit(...)
 * - .offset(...)
 * - .execute()
 * - .first()
 * - .count()
 */
// Import types and client from the generated client
export {
  Client,
  createClient
} from '../output/client'
export type {
    Item,
    WhereClause,
    SortableField,
    TypedQueryBuilder
} from '../output/client'

// Re-export a configured client instance
import { createClient } from '../output/client'

// Create client pointing to the output directory (relative to where the app is served)
export const db = createClient({ basePath: '/output' })
