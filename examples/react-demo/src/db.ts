// Import from generated client
import { Client, createClient as baseCreateClient } from '../output/client'
export { Client } from '../output/client'
export type { Item, SortableField, TypedQueryBuilder, ItemCondition } from '../output/client'

// Create client with correct basePath for the app
export const createClient = baseCreateClient
export const db = new Client({ basePath: '/output' })
