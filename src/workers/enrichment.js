// src/workers/enrichment.js
// Worker 2: AI enrichment pipeline — the main worker
import 'dotenv/config'
import { fileURLToPath } from 'url'
import {
  getEnrichmentQueue,
  findMatchesByModel,
  writeEnrichmentFields,
} from '../lib/airtable.js'
import { lookupByItemNumber } from '../lib/goflow.js'
import { lookupByUPC as keepaLookupByUPC, lookupByASIN, searchProduct as keepaSearch } from '../lib/keepa.js'
import { enrichRecord } from '../lib/claude.js'
import { buildWritePayload } from '../lib/fieldWriter.js'
import { parseItemNumber, cleanUPC } from '../lib/parser.js'
import { WorkerLogger } from '../lib/logger.js'
import { FIELDS, WEBSITE, FOOTWEAR_STORES, AI_STATUS } from '../config/fields.js'

const BATCH_SIZE = parseInt(process.env.ENRICH_BATCH_SIZE || '20')

async function run() {
  console.log(`[Enrichment] Starting run at ${new Date().toISOString()}`)
  const logger = new WorkerLogger('enrich')

  const records = await getEnrichmentQueue(BATCH_SIZE)
  console.log(`[Enrichment] Processing ${records.length} records`)

  const results = { complete: 0, partial: 0, notFound: 0, error: 0 }

  const statusKey = (status) => {
    if (status === AI_STATUS.COMPLETE) return 'complete'
    if (status === AI_STATUS.PARTIAL) return 'partial'
    return 'notFound'
  }

  for (const record of records) {
    const itemNumber = record.fields[FIELDS.ITEM_NUMBER]
    console.log(`\n[Enrichment] Processing: ${itemNumber}`)

    try {
      const result = await processRecord(record)
      results[statusKey(result.status)]++
      const missingCount = result.missingFields?.length || 0
      console.log(`  → ${result.status}${missingCount > 0 ? ` (${missingCount} optional fields missing)` : ''}`)
      if (result.validationIssues?.length) {
        console.log(`  ⚠ Validation: ${result.validationIssues.join('; ')}`)
      }
      logger.log({
        itemNumber,
        website: record.fields[FIELDS.WEBSITE],
        brand: record.fields[FIELDS.BRAND_CORRECT_SPELL] || record.fields[FIELDS.BRAND],
        outcome: result.status,
        sourcesUsed: result.sourcesUsed,
        fieldsWritten: result.fieldsWritten,
        missingFields: result.missingFields,
        validationWarnings: result.validationIssues,
        priceWritten: result.priceWritten,
      })
    } catch (err) {
      console.error(`  ERROR processing ${itemNumber}:`, err.message)
      results.error++
      await safeWriteNotFound(record.id, err.message)
      logger.log({ itemNumber, website: record.fields[FIELDS.WEBSITE], outcome: 'Error', errorMessage: err.message })
    }
  }

  await logger.finish(results)
  console.log(`\n[Enrichment] Done — Complete: ${results.complete}, Partial: ${results.partial}, Not Found: ${results.notFound}, Errors: ${results.error}`)
  return { processed: results.complete + results.partial + results.notFound + results.error, resolved: results.complete + results.partial }
}

export { run }

async function processRecord(record) {
  const fields = record.fields
  const itemNumber = fields[FIELDS.ITEM_NUMBER]

  if (!itemNumber) {
    console.log(`  SKIP record ${record.id} — no item number found`)
    return { status: 'Not Found', missingFields: ['Item Number'], validationIssues: [] }
  }

  const parsed = parseItemNumber(itemNumber)

  if (!parsed) {
    console.log(`  SKIP ${itemNumber} — could not parse item number`)
    return { status: 'Not Found', missingFields: ['parseable item number'], validationIssues: [] }
  }

  const website = fields[FIELDS.WEBSITE]

  const airtableData = {
    itemNumber,
    brand: fields[FIELDS.BRAND],
    brandCorrectSpelling: fields[FIELDS.BRAND_CORRECT_SPELL],
    condition: fields[FIELDS.CONDITION],
    conditionCode: fields[FIELDS.CONDITION_TYPE] || parsed.conditionCode,
    manualConditionType: fields[FIELDS.MANUAL_CONDITION],
    website,
    productName: fields[FIELDS.PRODUCT_NAME],
    purchaseName: fields[FIELDS.PURCHASE_NAME],
    upcCode: fields[FIELDS.UPC_CODE],
    globalItemNumber: fields[FIELDS.GLOBAL_ITEM_NUMBER],
    sdoColor: fields[FIELDS.SDO_COLOR],
    sdoGender: fields[FIELDS.SDO_GENDER],
    sdoAgeRange: fields[FIELDS.SDO_AGE_RANGE],
    sdoModelName: fields[FIELDS.SDO_MODEL_NAME],
    sdoModelNumber: fields[FIELDS.SDO_MODEL_NUMBER],
    brandSite: fields[FIELDS.BRAND_SITE],
    otherSite: fields[FIELDS.OTHER_SITE],
    asin: fields[FIELDS.ASIN],
    itemCost: fields[FIELDS.ITEM_COST],
  }

  const recordContext = { airtableData, parsedItem: parsed }

  // ─── STEP 1: AIRTABLE MATCH CHECK ───────────────────────────────────────
  // Check if we already have a processed record with this model number
  const modelNumber = airtableData.sdoModelNumber || parsed.modelNumber
  const brand = airtableData.brandCorrectSpelling || airtableData.brand

  const airtableMatches = modelNumber
    ? await findMatchesByModel(modelNumber, brand)
    : []

  if (airtableMatches.length > 0) {
    const variantMatch = findVariantMatch(airtableMatches, airtableData.sdoColor)
    const productMatch = airtableMatches[0]

    if (variantMatch) {
      // Full match — copy everything including images
      console.log(`  ✓ Airtable variant match found`)
      return await writeFromAirtableMatch(record.id, variantMatch, 'variant', recordContext)
    } else {
      // Product-level match — copy content but still need images
      console.log(`  ✓ Airtable product match found (no image match)`)
      const partialData = extractProductLevelFields(productMatch)
      // Still need images — proceed to external sources with partial data pre-filled
      return await enrichWithSources(record.id, recordContext, partialData)
    }
  }

  // ─── STEP 2: EXTERNAL ENRICHMENT ────────────────────────────────────────
  return await enrichWithSources(record.id, recordContext, null)
}

// Enrich from external sources: GoFlow → Keepa → Not Found
async function enrichWithSources(recordId, recordContext, partialAirtableData) {
  const { airtableData, parsedItem } = recordContext
  const sources = []

  // ─── SOURCE 0: Airtable data already on the record ───────────────────
  // Everything BQ/GoFlow already populated — pass as a source to Claude
  const airtableSource = buildAirtableSource(airtableData)
  if (airtableSource) {
    sources.push({ type: 'Airtable (existing)', ...airtableSource })
    console.log(`  ✓ Airtable existing data: ${airtableSource.name || 'unnamed'}`)
  }

  // ─── SOURCE 1: GoFlow by item number ─────────────────────────────────
  let goflowASIN = null
  let goflowUPC = null
  console.log(`  → GoFlow lookup: ${airtableData.itemNumber}`)
  const goflowData = await lookupByItemNumber(airtableData.itemNumber)
  if (goflowData) {
    sources.push({ type: 'GoFlow', ...goflowData })
    goflowASIN = goflowData.asin
    goflowUPC = goflowData.upc
    console.log(`  ✓ GoFlow: ${goflowData.name || 'unnamed'}`)
  } else {
    console.log(`  - GoFlow: no result`)
  }

  // ─── SOURCE 2: Keepa ─────────────────────────────────────────────────
  // Collect all possible lookup keys from every source
  const asin = airtableData.asin || goflowASIN || null
  const upc = cleanUPC(airtableData.upcCode) ||
    goflowUPC ||
    extractUPCFromItemNumber(parsedItem.raw)

  if (asin) {
    console.log(`  → Keepa ASIN: ${asin}`)
    const keepaData = await lookupByASIN(asin)
    if (keepaData) {
      sources.push({ type: 'Keepa', ...keepaData })
      console.log(`  ✓ Keepa: ${keepaData.name || 'unnamed'}`)
    } else {
      console.log(`  - Keepa ASIN: no result`)
    }
  } else if (upc) {
    console.log(`  → Keepa UPC: ${upc}`)
    const keepaData = await keepaLookupByUPC(upc)
    if (keepaData) {
      sources.push({ type: 'Keepa', ...keepaData })
      console.log(`  ✓ Keepa: ${keepaData.name || 'unnamed'}`)
    } else {
      console.log(`  - Keepa UPC: no result`)
    }
  }

  // ─── SOURCE 3: Keepa text search fallback ────────────────────────────
  // Only if Keepa didn't already return something
  const keepaAlreadyFound = sources.some(s => s.type === 'Keepa')
  if (!keepaAlreadyFound) {
    const brand = airtableData.brandCorrectSpelling || airtableData.brand
    const model = airtableData.sdoModelNumber || airtableData.sdoModelName || parsedItem.modelNumber
    const productName = airtableData.productName || airtableData.purchaseName
    const searchQuery = buildKeepaSearchQuery(brand, model, productName)
    if (searchQuery) {
      console.log(`  → Keepa search: "${searchQuery}"`)
      const keepaData = await keepaSearch(searchQuery)
      if (keepaData) {
        sources.push({ type: 'Keepa (search)', ...keepaData })
        console.log(`  ✓ Keepa search: ${keepaData.name || 'unnamed'}`)
      } else {
        console.log(`  - Keepa search: no result`)
      }
    }
  }

  // ─── No sources found at all ─────────────────────────────────────────
  if (sources.length === 0 && !partialAirtableData) {
  
    console.log(`  ✗ No sources found`)
    return await writeNotFound(recordId, recordContext)
  }

  // ─── STEP 3: CLAUDE ENRICHMENT ──────────────────────────────────────
  // Merge partial Airtable data into record context so Claude knows what's pre-filled
  if (partialAirtableData) {
    recordContext.preFilledData = partialAirtableData
  }

  console.log(`  → Calling Claude (${sources.length} source(s))`)
  const claudeOutput = await enrichRecord(recordContext, sources)

  // Merge pre-filled Airtable data with Claude output
  if (partialAirtableData) {
    for (const [key, value] of Object.entries(partialAirtableData)) {
      if (value && !claudeOutput[key]) {
        claudeOutput[key] = value
      }
    }
  }

  // Inject price from sources if Claude didn't find one
  if (!claudeOutput.price) {
    // GoFlow listing price
    const goflowSource = sources.find(s => s.type === 'GoFlow')
    if (goflowSource?.listingPrice > 0) {
      claudeOutput.price = goflowSource.listingPrice
    }
    // Keepa current price (buyBoxPrice in cents / 100)
    const keepaSource = sources.find(s => s.type?.startsWith('Keepa'))
    if (!claudeOutput.price && keepaSource?.currentPrice > 0) {
      claudeOutput.price = keepaSource.currentPrice
    }
  }

  // ─── STEP 4: VALIDATE AND WRITE ─────────────────────────────────────
  const { fields, status, missingFields, validationIssues } = buildWritePayload(
    claudeOutput,
    recordContext
  )

  await writeEnrichmentFields(recordId, fields)

  return { status, missingFields, validationIssues, sourcesUsed: sources.map(s => s.type), fieldsWritten: Object.keys(fields || {}), priceWritten: claudeOutput.price || null }
}

// Build a source object from existing Airtable data on the record
function buildAirtableSource(airtableData) {
  const hasData = airtableData.productName || airtableData.purchaseName ||
    airtableData.sdoModelName || airtableData.sdoModelNumber ||
    airtableData.sdoColor || airtableData.sdoGender
  if (!hasData) return null
  return {
    name: airtableData.productName || airtableData.purchaseName || null,
    brand: airtableData.brandCorrectSpelling || airtableData.brand || null,
    color: airtableData.sdoColor || null,
    gender: airtableData.sdoGender || null,
    modelName: airtableData.sdoModelName || null,
    modelNumber: airtableData.sdoModelNumber || null,
    description: null,
    imageUrls: [],
    raw: {
      productName: airtableData.productName,
      purchaseName: airtableData.purchaseName,
      sdoModelName: airtableData.sdoModelName,
      sdoModelNumber: airtableData.sdoModelNumber,
      sdoColor: airtableData.sdoColor,
      sdoGender: airtableData.sdoGender,
      sdoAgeRange: airtableData.sdoAgeRange,
      upcCode: airtableData.upcCode,
      asin: airtableData.asin,
    }
  }
}

// Build Keepa search query from available data
function buildKeepaSearchQuery(brand, model, productName) {
  if (brand && model) return `${brand} ${model}`
  if (brand && productName) return `${brand} ${productName}`
  if (productName && productName.length > 10) return productName
  return null
}

// Extract UPC from a structured item number (12-13 digit sequence embedded in string)
function extractUPCFromItemNumber(itemNumber) {
  if (!itemNumber) return null
  const match = itemNumber.match(/\b(\d{12,14})\b/)
  return match ? match[1] : null
}

// Write data copied directly from an Airtable match
async function writeFromAirtableMatch(recordId, matchRecord, matchType, recordContext) {
  const matchFields = matchRecord.fields

  // Build Claude-compatible output from Airtable data
  const claudeOutput = {
    title: matchFields[FIELDS.TITLE],
    description: matchFields[FIELDS.DESCRIPTION],
    seoDescription: matchFields[FIELDS.SEO_DESCRIPTION],
    shopifyCategory: matchFields[FIELDS.SHOPIFY_CATEGORY],
    googleShoppingCategory: matchFields[FIELDS.GOOGLE_CATEGORY],
    material: matchFields[FIELDS.MATERIAL],
    option1Value: matchType === 'variant' ? matchFields[FIELDS.SDO_COLOR] : null,
    imageUrls: matchType === 'variant'
      ? (matchFields[FIELDS.PRODUCT_IMAGES] || []).map(img => img.url)
      : [],
    confidence: 'high',
    missingFields: matchType === 'product' ? ['Product Images'] : [],
    validationIssues: [],
    sourceUsed: `Airtable ${matchType} match`,
  }

  const { fields, status, missingFields, validationIssues } = buildWritePayload(
    claudeOutput,
    recordContext
  )

  await writeEnrichmentFields(recordId, fields)
  return { status, missingFields, validationIssues }
}

async function writeNotFound(recordId, recordContext) {
  const claudeOutput = {
    confidence: 'low',
    missingFields: ['all'],
    validationIssues: [],
    error: false,
  }
  const { fields, status } = buildWritePayload(claudeOutput, recordContext)
  await writeEnrichmentFields(recordId, fields)
  return { status: AI_STATUS.NOT_FOUND, missingFields: ['All fields'], validationIssues: [] }
}

async function safeWriteNotFound(recordId, errorMessage) {
  try {
    const updateFields = {}
    if (process.env.AI_STATUS_FIELD_ID) {
      updateFields[FIELDS.AI_STATUS] = AI_STATUS.NOT_FOUND
    }
    if (process.env.AI_MISSING_FIELDS_FIELD_ID) {
      updateFields[FIELDS.AI_MISSING] = `Error: ${errorMessage}`
    }
    if (Object.keys(updateFields).length) {
      await writeEnrichmentFields(recordId, updateFields)
    }
  } catch {}
}

// Find a variant-level match (same model + same color)
function findVariantMatch(matches, color) {
  if (!color) return null
  return matches.find(m => {
    const matchColor = m.fields[FIELDS.SDO_COLOR]
    return matchColor && matchColor.toLowerCase() === color.toLowerCase()
  }) || null
}

// Extract product-level fields from an Airtable match record
function extractProductLevelFields(record) {
  const f = record.fields
  return {
    title: f[FIELDS.TITLE] || null,
    description: f[FIELDS.DESCRIPTION] || null,
    seoDescription: f[FIELDS.SEO_DESCRIPTION] || null,
    shopifyCategory: f[FIELDS.SHOPIFY_CATEGORY] || null,
    googleShoppingCategory: f[FIELDS.GOOGLE_CATEGORY] || null,
    material: f[FIELDS.MATERIAL] || null,
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  run().catch(err => {
    console.error('[Enrichment] Fatal error:', err)
    process.exit(1)
  })
}
