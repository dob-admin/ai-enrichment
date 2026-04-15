// src/lib/webSearch.js
// Web search and product page fetching for enrichment
import axios from 'axios'
import * as cheerio from 'cheerio'

const fetchClient = axios.create({
  timeout: 15000,
  headers: {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
  },
  maxRedirects: 5,
})

// Fetch and parse a product page
// Returns structured content for Claude to reason over
export async function fetchProductPage(url) {
  if (!url) return null

  try {
    const response = await fetchClient.get(url)
    const $ = cheerio.load(response.data)

    // Remove noise
    $('script, style, nav, footer, header, .cookie-banner, .popup').remove()

    // Extract key content
    const title = $('h1').first().text().trim() ||
                  $('[class*="product-title"]').first().text().trim() ||
                  $('title').text().trim()

    // Get description — try common product description selectors
    const descriptionSelectors = [
      '[class*="product-description"]',
      '[class*="product-detail"]',
      '[id*="description"]',
      '[class*="description"]',
      '.pdp-description',
      '.product-info',
    ]
    let description = ''
    for (const sel of descriptionSelectors) {
      const text = $(sel).first().text().trim()
      if (text.length > 50) { description = text; break }
    }
    if (!description) {
      description = $('main').text().trim().slice(0, 3000)
    }

    // Extract image URLs — prefer large/full size images
    const imageUrls = []
    $('img').each((_, el) => {
      const src = $(el).attr('src') ||
                  $(el).attr('data-src') ||
                  $(el).attr('data-lazy-src')
      if (src && isProductImage(src, url)) {
        imageUrls.push(absoluteUrl(src, url))
      }
    })

    // Also check for JSON-LD product data (often has clean image arrays)
    const jsonLdImages = extractJsonLdImages($)
    const allImages = [...new Set([...jsonLdImages, ...imageUrls])]

    return {
      url,
      title,
      description: description.slice(0, 5000),
      imageUrls: allImages.slice(0, 15),
      rawText: $('body').text().replace(/\s+/g, ' ').trim().slice(0, 8000),
    }
  } catch (err) {
    console.error(`Failed to fetch ${url}:`, err.message)
    return null
  }
}

// Search for a product page given brand + model info
// Returns the best candidate URL
export async function searchForProductPage(brand, modelNumber, modelName, upc) {
  // Build search query — try multiple strategies
  const queries = buildSearchQueries(brand, modelNumber, modelName, upc)

  for (const query of queries) {
    const url = await trySearchQuery(query)
    if (url) return url
  }

  return null
}

function buildSearchQueries(brand, modelNumber, modelName, upc) {
  const queries = []

  if (brand && modelNumber) {
    // Prefer brand's own site first
    queries.push(`site:${brandDomain(brand)} ${modelNumber}`)
    // Then major retailers
    queries.push(`${brand} ${modelNumber} site:amazon.com OR site:zappos.com OR site:dsw.com`)
    queries.push(`${brand} ${modelNumber}`)
  }

  if (brand && modelName) {
    queries.push(`${brand} "${modelName}"`)
  }

  if (upc) {
    queries.push(`${upc} product`)
  }

  return queries.filter(Boolean)
}

// Map known brands to their domains for site-specific searches
function brandDomain(brand) {
  const domains = {
    'brooks': 'brooksrunning.com',
    'asics': 'asics.com',
    'new balance': 'newbalance.com',
    'nike': 'nike.com',
    'adidas': 'adidas.com',
    'hoka': 'hoka.com',
    'altra': 'altrarunning.com',
    'merrell': 'merrell.com',
    'salomon': 'salomon.com',
    'ariat': 'ariat.com',
    'keen': 'keenfootwear.com',
    'dansko': 'dansko.com',
    'birkenstock': 'birkenstock.com',
    'ugg': 'ugg.com',
    'saucony': 'saucony.com',
    'on': 'on.com',
    'lego': 'lego.com',
  }
  return domains[brand?.toLowerCase()] || null
}

async function trySearchQuery(query) {
  // Use DuckDuckGo HTML search (no API key needed)
  try {
    const searchUrl = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`
    const response = await fetchClient.get(searchUrl)
    const $ = cheerio.load(response.data)

    // Extract first few result URLs
    const urls = []
    $('.result__url, .result__a').each((_, el) => {
      const href = $(el).attr('href')
      if (href && isUsableProductUrl(href)) {
        urls.push(href)
      }
    })

    return urls[0] || null
  } catch {
    return null
  }
}

function isProductImage(src, pageUrl) {
  if (!src) return false
  const lower = src.toLowerCase()
  // Skip obvious non-product images
  if (lower.includes('logo') ||
      lower.includes('icon') ||
      lower.includes('banner') ||
      lower.includes('badge') ||
      lower.includes('pixel') ||
      lower.includes('tracking') ||
      lower.endsWith('.gif')) return false
  // Must be a real image format
  return /\.(jpg|jpeg|png|webp)/i.test(lower) ||
         lower.includes('image') ||
         lower.includes('photo') ||
         lower.includes('product')
}

function isUsableProductUrl(url) {
  if (!url) return false
  const blocked = ['youtube.com', 'facebook.com', 'twitter.com',
                   'instagram.com', 'pinterest.com', 'reddit.com']
  return !blocked.some(b => url.includes(b)) &&
         (url.startsWith('http://') || url.startsWith('https://'))
}

function absoluteUrl(src, base) {
  if (src.startsWith('http')) return src
  try {
    return new URL(src, base).href
  } catch {
    return src
  }
}

function extractJsonLdImages($) {
  const images = []
  $('script[type="application/ld+json"]').each((_, el) => {
    try {
      const data = JSON.parse($(el).html())
      const schemas = Array.isArray(data) ? data : [data]
      for (const schema of schemas) {
        if (schema['@type'] === 'Product') {
          const img = schema.image
          if (typeof img === 'string') images.push(img)
          else if (Array.isArray(img)) images.push(...img)
        }
      }
    } catch {}
  })
  return images.filter(Boolean)
}
