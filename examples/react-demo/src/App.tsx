import { useState, useEffect } from 'react'
import { db, type Item, type TypedQueryBuilder } from './db'

function App() {
  const [items, setItems] = useState<Item[]>([])
  const [category, setCategory] = useState<string>('')
  const [inStock, setInStock] = useState<string>('')
  const [minPrice, setMinPrice] = useState<string>('')
  const [maxPrice, setMaxPrice] = useState<string>('')
  const [orderBy, setOrderBy] = useState<string>('')
  const [limit, setLimit] = useState<string>('')
  const [code, setCode] = useState<string>('')

  useEffect(() => {
    runQuery()
  }, [category, inStock, minPrice, maxPrice, orderBy, limit])

  const runQuery = async () => {
    // Build query using the chainable API - TypeScript provides IntelliSense here!
    let query: TypedQueryBuilder = db.query()

    if (category) {
      query = query.where({ category })
    }
    if (inStock) {
      query = query.where({ inStock: inStock === 'true' })
    }
    if (minPrice || maxPrice) {
      const priceFilter: { gte?: number; lte?: number } = {}
      if (minPrice) priceFilter.gte = parseFloat(minPrice)
      if (maxPrice) priceFilter.lte = parseFloat(maxPrice)
      query = query.where({ price: priceFilter })
    }
    if (orderBy) {
      const [field, dir] = orderBy.split('-') as [keyof Item, 'asc' | 'desc']
      // TypeScript knows field must be a SortableField!
      if (field === 'price' || field === 'name' || field === 'category' || field === 'id') {
        query = query.orderBy(field, dir)
      }
    }
    if (limit) {
      query = query.limit(parseInt(limit))
    }

    try {
      const results = await query.execute()
      setItems(results)
      updateCodeDisplay()
    } catch (err) {
      console.error(err)
    }
  }

  const updateCodeDisplay = () => {
    let code = 'db.query()'

    if (category) {
      code += `\n  .where({ category: "${category}" })`
    }
    if (inStock) {
      code += `\n  .where({ inStock: ${inStock} })`
    }
    if (minPrice || maxPrice) {
      const parts = []
      if (minPrice) parts.push(`gte: ${minPrice}`)
      if (maxPrice) parts.push(`lte: ${maxPrice}`)
      code += `\n  .where({ price: { ${parts.join(', ')} } })`
    }
    if (orderBy) {
      const [field, dir] = orderBy.split('-')
      code += `\n  .orderBy("${field}", "${dir}")`
    }
    if (limit) {
      code += `\n  .limit(${limit})`
    }

    code += '\n  .execute()'
    setCode(code)
  }

  return (
    <div style={{ fontFamily: 'system-ui', maxWidth: 1000, margin: '0 auto', padding: 20 }}>
      <h1>Static Shard - Chainable Query Demo</h1>

      <div style={{ background: '#f5f5f5', padding: 20, borderRadius: 8, marginBottom: 20 }}>
        <h3>Build your query:</h3>

        <div style={{ display: 'flex', gap: 10, marginBottom: 10, alignItems: 'center' }}>
          <label style={{ minWidth: 100 }}>Category:</label>
          <select value={category} onChange={e => setCategory(e.target.value)} style={{ padding: 8 }}>
            <option value="">All</option>
            <option value="electronics">electronics</option>
            <option value="furniture">furniture</option>
          </select>
        </div>

        <div style={{ display: 'flex', gap: 10, marginBottom: 10, alignItems: 'center' }}>
          <label style={{ minWidth: 100 }}>In Stock:</label>
          <select value={inStock} onChange={e => setInStock(e.target.value)} style={{ padding: 8 }}>
            <option value="">All</option>
            <option value="true">Yes</option>
            <option value="false">No</option>
          </select>
        </div>

        <div style={{ display: 'flex', gap: 10, marginBottom: 10, alignItems: 'center' }}>
          <label style={{ minWidth: 100 }}>Min Price:</label>
          <input
            type="number"
            value={minPrice}
            onChange={e => setMinPrice(e.target.value)}
            placeholder="e.g. 100"
            style={{ padding: 8 }}
          />
        </div>

        <div style={{ display: 'flex', gap: 10, marginBottom: 10, alignItems: 'center' }}>
          <label style={{ minWidth: 100 }}>Max Price:</label>
          <input
            type="number"
            value={maxPrice}
            onChange={e => setMaxPrice(e.target.value)}
            placeholder="e.g. 500"
            style={{ padding: 8 }}
          />
        </div>

        <div style={{ display: 'flex', gap: 10, marginBottom: 10, alignItems: 'center' }}>
          <label style={{ minWidth: 100 }}>Order By:</label>
          <select value={orderBy} onChange={e => setOrderBy(e.target.value)} style={{ padding: 8 }}>
            <option value="">None</option>
            <option value="price-asc">Price (Low to High)</option>
            <option value="price-desc">Price (High to Low)</option>
            <option value="name-asc">Name (A-Z)</option>
            <option value="name-desc">Name (Z-A)</option>
          </select>
        </div>

        <div style={{ display: 'flex', gap: 10, marginBottom: 10, alignItems: 'center' }}>
          <label style={{ minWidth: 100 }}>Limit:</label>
          <input
            type="number"
            value={limit}
            onChange={e => setLimit(e.target.value)}
            placeholder="e.g. 5"
            style={{ padding: 8 }}
          />
        </div>

        <pre style={{
          background: '#1e1e1e',
          color: '#d4d4d4',
          padding: 15,
          borderRadius: 4,
          fontFamily: 'monospace',
          fontSize: 13,
          overflow: 'auto'
        }}>
          {code || 'db.query().execute()'}
        </pre>
      </div>

      <div style={{ background: 'white', padding: 20, borderRadius: 8 }}>
        <h3>Results ({items.length} items)</h3>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ background: '#f9fafb' }}>
              <th style={{ padding: 10, textAlign: 'left', borderBottom: '1px solid #eee' }}>ID</th>
              <th style={{ padding: 10, textAlign: 'left', borderBottom: '1px solid #eee' }}>Name</th>
              <th style={{ padding: 10, textAlign: 'left', borderBottom: '1px solid #eee' }}>Category</th>
              <th style={{ padding: 10, textAlign: 'left', borderBottom: '1px solid #eee' }}>Price</th>
              <th style={{ padding: 10, textAlign: 'left', borderBottom: '1px solid #eee' }}>In Stock</th>
            </tr>
          </thead>
          <tbody>
            {items.map(item => (
              <tr key={item.id}>
                <td style={{ padding: 10, borderBottom: '1px solid #eee' }}>{item.id}</td>
                <td style={{ padding: 10, borderBottom: '1px solid #eee' }}>{item.name}</td>
                <td style={{ padding: 10, borderBottom: '1px solid #eee' }}>{item.category}</td>
                <td style={{ padding: 10, borderBottom: '1px solid #eee' }}>${item.price.toFixed(2)}</td>
                <td style={{ padding: 10, borderBottom: '1px solid #eee' }}>{item.inStock ? 'Yes' : 'No'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}

export default App
