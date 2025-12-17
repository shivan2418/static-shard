// Re-export from generated client
export { Client, createClient, db } from '../output/client'
export type { Item, SortableField, TypedQueryBuilder, ItemCondition } from '../output/client'

// Import for examples below
import { db } from '../output/client'

// Example: Get all electronics
const electronics = await db.query()
  .where(db.category.eq('electronics'))
  .execute()

// Example: Get expensive items (price >= 100)
const expensive = await db.query()
  .where(db.price.gte(100))
  .orderBy('price', 'desc')
  .execute()

// Example: Get in-stock electronics under $500
const cheapElectronics = await db.query()
  .where(db.category.eq('electronics'))
  .where(db.price.lte(500))
  .where(db.inStock.eq(true))
  .execute()

// Example: Search by name
const searchResults = await db.query()
  .where(db.name.contains('Phone'))
  .first()

// Example: Paginated results
const page1 = await db.query()
  .orderBy('price', 'asc')
  .limit(5)
  .offset(0)
  .execute()

// Example: Count matching items
const count = await db.query()
  .where(db.inStock.eq(true))
  .count()
