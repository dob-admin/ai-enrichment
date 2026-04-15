// src/lib/keepa.js
import axios from 'axios'
import { withRetry } from './retry.js'

const client = axios.create({
  baseURL: 'https://api.keepa.com',
  timeout: 15000,
})

const key = () => process.env.KEEPA_API_KEY

// Look up by UPC or EAN
export async function lookupByUPC(upc) {
  if (!upc || !key()) return null
  return _productLookup({ code: upc })
}

// Look up by ASIN — most precise
export async function lookupByASIN(asin) {
  if (!asin || !key()) return null
  return _productLookup({ asin })
}

// Text search fallback — brand + model name/number
export async function searchProduct(query) {
  if (!query || !key()) return null
  // Clean query: replace underscores with spaces, remove special chars, trim to 100 chars
  const cleanQuery = query
    .replace(/_/g, ' ')
    .replace(/[^a-zA-Z0-9 .-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 100)
  if (!cleanQuery || cleanQuery.length < 5) return null
  try {
    const r = await withRetry(
      () => client.get('/search', { params: { key: key(), domain: 1, type: 1, term: cleanQuery } }),
      `Keepa /search "${cleanQuery}"`
    )
    const asins = r.data?.asinList
    if (!asins?.length) return null
    return await lookupByASIN(asins[0])
  } catch (err) {
    if (err.response?.status === 500) {
      // Keepa search 500 usually means bad query — log quietly and move on
      console.log(`  - Keepa search: no result (bad query)`)
      return null
    }
    console.error(`  Keepa search failed for "${cleanQuery}":`, err.message)
    return null
  }
}

async function _productLookup(params) {
  try {
    const r = await withRetry(
      () => client.get('/product', { params: { key: key(), domain: 1, ...params } }),
      `Keepa /product`
    )
    const products = r.data?.products
    if (!products?.length) return null
    return normalizeKeepaProduct(products[0])
  } catch (err) {
    console.error(`  Keepa lookup failed:`, err.message)
    return null
  }
}

function normalizeKeepaProduct(product) {
  if (!product) return null
  const imageUrls = (product.imagesCSV || '').split(',').filter(Boolean)
    .map(id => `https://images-na.ssl-images-amazon.com/images/I/${id}`)
  const features = product.features || []
  return {
    name: product.title || null,
    brand: product.brand || null,
    description: [product.description, features.join(' ')].filter(Boolean).join('\n\n') || null,
    features,
    category: product.categoryTree?.[product.categoryTree.length - 1]?.name || null,
    imageUrls,
    upc: product.upcList?.[0] || null,
    asin: product.asin || null,
    color: product.color || null,
    size: product.size || null,
    model: product.model || null,
    manufacturer: product.manufacturer || null,
    // Current buy box price — Keepa stores prices in cents, divide by 100
    // -1 means not available
    currentPrice: (product.stats?.current?.[0] > 0)
      ? product.stats.current[0] / 100
      : null,
    raw: {
      title: product.title,
      brand: product.brand,
      features,
      description: product.description,
      color: product.color,
      size: product.size,
      model: product.model,
    },
  }
}
