// src/lib/goflow.js
// GoFlow API — https://goflow.com/api-spec
// Base URL: https://{subdomain}.api.goflow.com/v1
// Auth: Authorization: Bearer {token}
// Beta endpoints require: X-Beta-Contact header
import axios from 'axios'
import { withRetry } from './retry.js'

const getClient = () => axios.create({
  baseURL: `https://${process.env.GOFLOW_SUBDOMAIN}.api.goflow.com/v1`,
  headers: {
    'Authorization': `Bearer ${process.env.GOFLOW_API_TOKEN}`,
    'X-Beta-Contact': process.env.GOFLOW_BETA_CONTACT,
    'Content-Type': 'application/json',
  },
  timeout: 10000,
})

// Look up a product by item number, then fetch its Amazon listing to get ASIN
export async function lookupByItemNumber(itemNumber) {
  if (!itemNumber || !process.env.GOFLOW_API_TOKEN) return null

  try {
    // Step 1: Get product — with 429 retry
    const productRes = await withRetry(
      () => getClient().get('/products', { params: { 'filters[item_number:eq]': itemNumber } }),
      `GoFlow /products ${itemNumber}`
    )
    const products = productRes.data?.data || []
    if (!products.length) return null

    const product = products[0]
    const normalized = normalizeGoFlowProduct(product)

    // Step 2: Get listings for this product to find ASIN — with 429 retry
    try {
      const listingsRes = await withRetry(
        () => getClient().get('/listings', { params: { 'filters[product.id:eq]': product.id } }),
        `GoFlow /listings ${product.id}`
      )
      const listings = listingsRes.data?.data || []

      // Find Amazon USA listing — ASIN is in store_page_url as /dp/{ASIN}
      const amazonListing = listings.find(l => l.store?.channel === 'amazon_marketplace_usa')

      if (amazonListing?.store_page_url) {
        const asinMatch = amazonListing.store_page_url.match(/\/dp\/([A-Z0-9]{10})/)
        if (asinMatch) {
          normalized.asin = asinMatch[1]
          normalized.storePageUrl = amazonListing.store_page_url
        }
      }
      // Capture price from any listing
      const listingWithPrice = listings.find(l => l.price?.amount > 0)
      if (listingWithPrice) {
        normalized.listingPrice = listingWithPrice.price.amount
      }
    } catch (listingErr) {
      // Listings lookup failed — proceed without ASIN
      console.log(`  - GoFlow listings lookup failed: ${listingErr.message}`)
    }

    return normalized
  } catch (err) {
    if (err.response?.status === 404) return null
    console.error(`  GoFlow lookup failed for ${itemNumber}:`, err.message)
    return null
  }
}

// Kept for compatibility
export async function lookupByUPC(upc) { return null }

function normalizeGoFlowProduct(product) {
  if (!product) return null
  const details = product.details || {}
  const identifiers = product.identifiers || []

  const rawUpc = identifiers.find(i => i.type?.toUpperCase() === 'UPC')?.value || null
  const upc = rawUpc ? rawUpc.split('_')[0] : null
  const ean = identifiers.find(i => i.type?.toUpperCase() === 'EAN')?.value || null
  const mpn = identifiers.find(i => i.type?.toUpperCase() === 'MPN')?.value || null
  // Note: ASIN is fetched separately from listings, not identifiers
  // Some products may have ASIN in identifiers too
  const asinFromIdentifiers = identifiers.find(i => i.type?.toUpperCase() === 'ASIN')?.value || null

  return {
    name: details.name || details.purchase_name || null,
    brand: details.brand || details.manufacturer || null,
    description: details.description || null,
    category: details.category || null,
    condition: details.condition || null,
    imageUrls: [],
    upc, ean, mpn,
    asin: asinFromIdentifiers, // may be overwritten by listings lookup
    goflowId: product.id || null,
    itemNumber: product.item_number || null,
    raw: {
      name: details.name,
      purchase_name: details.purchase_name,
      brand: details.brand,
      manufacturer: details.manufacturer,
      description: details.description,
      category: details.category,
      condition: details.condition,
      // identifiers excluded — upc/ean/mpn already extracted as top-level fields above
    },
  }
}
