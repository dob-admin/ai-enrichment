// src/workers/loop.js
// Continuous enrichment loop — runs forever, processes all in-scope records
// until both Product Info Valid and Variant Info Valid = YES, then sets PD Ready Hold
// Parks records as Needs VA after MAX_ATTEMPTS with specific reason

import 'dotenv/config'
import Airtable from 'airtable'
import { FIELDS, WEBSITE, FOOTWEAR_STORES, NPCN_STORES, AI_STATUS, CONDITION_LABELS, LOOP_STATUS } from '../config/fields.js'
import { lookupByItemNumber as goflowLookup } from '../lib/goflow.js'
import { lookupByUPC as keepaLookupByUPC, lookupByASIN, searchProduct as keepaSearch } from '../lib/keepa.js'
import { enrichRecord } from '../lib/claude.js'
import { buildWritePayload } from '../lib/fieldWriter.js'
import { parseItemNumber, cleanUPC } from '../lib/parser.js'
import { findMatchesByModel } from '../lib/airtable.js'
import { WorkerLogger } from '../lib/logger.js'

const MAX_ATTEMPTS = 5
const RATE_DELAY = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')
const MIN_COST_THRESHOLD = parseFloat(process.env.MIN_COST_THRESHOLD || '1.00')

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID)
const table = () => base(process.env.AIRTABLE_TABLE_ID)
const delay = ms => new Promise(r => setTimeout(r, ms))

// ─── Queue fetch ──────────────────────────────────────────────────────────────
async function fetchQueue() {
  const records = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      {${FIELDS.TOTAL_INVENTORY}} > 0,
      {${FIELDS.PD_READY}} = 0,
      {${FIELDS.PD_READY_HOLD}} = 0,
      NOT({${FIELDS.WEBSITE}} = 'ignore'),
      NOT({${FIELDS.WEBSITE}} = ''),
      {${FIELDS.SHOPIFY_PRODUCT_ID}} = BLANK(),
      OR({${FIELDS.LOOP_STATUS}} = BLANK(), {${FIELDS.LOOP_STATUS}} = '${LOOP_STATUS.PENDING}')
    )`,
    fields: [
      FIELDS.ITEM_NUMBER,
      FIELDS.BRAND,
      FIELDS.BRAND_CORRECT_SPELL,
      FIELDS.CONDITION,
      FIELDS.CONDITION_TYPE,
      FIELDS.MANUAL_CONDITION,
      FIELDS.WEBSITE,
      FIELDS.PRODUCT_NAME,
      FIELDS.PURCHASE_NAME,
      FIELDS.UPC_CODE,
      FIELDS.GLOBAL_ITEM_NUMBER,
      FIELDS.TOTAL_INVENTORY,
      FIELDS.SDO_COLOR,
      FIELDS.SDO_GENDER,
      FIELDS.SDO_AGE_RANGE,
      FIELDS.SDO_MODEL_NAME,
      FIELDS.SDO_MODEL_NUMBER,
      FIELDS.SDO_MEN_SIZE,
      FIELDS.SDO_MEN_WIDTH,
      FIELDS.SDO_WOMEN_SIZE,
      FIELDS.SDO_WOMEN_WIDTH,
      FIELDS.SDO_YOUTH_SIZE,
      FIELDS.SDO_RETAIL_PRICE,
      FIELDS.BRAND_SITE,
      FIELDS.OTHER_SITE,
      FIELDS.ITEM_COST,
      FIELDS.ENRICHMENT_ATTEMPTS,
      FIELDS.LOOP_STATUS,
      FIELDS.PRODUCT_INFO_VALID,
      FIELDS.VARIANT_INFO_VALID,
      FIELDS.AI_COST_CHECK,
      FIELDS.COST_FIX,
      FIELDS.VARIANT_BARCODE,
    ],
  }).eachPage((page, next) => { records.push(...page); next() })
  return records
}

// ─── UPC extraction from item number string ───────────────────────────────────
function extractUPCFromString(str) {
  if (!str) return null
  const match = str.match(/(?<![0-9])(\d{12,14})(?![0-9])/)
  return match ? match[1] : null
}

// ─── Cost resolution ──────────────────────────────────────────────────────────
function hasCost(fields) {
  const cost = fields[FIELDS.ITEM_COST] || 0
  const costFix = fields[FIELDS.COST_FIX]?.name
  if (costFix === 'No Data') return false
  return cost > MIN_COST_THRESHOLD
}

// ─── Write helper ─────────────────────────────────────────────────────────────
async function writeFields(recordId, fields) {
  const clean = Object.fromEntries(
    Object.entries(fields).filter(([, v]) => v !== undefined && v !== null)
  )
  if (!Object.keys(clean).length) return
  await delay(RATE_DELAY)
  return table().update(recordId, clean)
}

// ─── Park record for VA ───────────────────────────────────────────────────────
async function parkForVA(recordId, reason) {
  console.log(`  → Parking for VA: ${reason}`)
  await writeFields(recordId, {
    [FIELDS.LOOP_STATUS]: LOOP_STATUS.NEEDS_VA,
    [FIELDS.VA_NEEDED]: reason,
  })
}

// ─── Mark done ────────────────────────────────────────────────────────────────
async function markDone(recordId) {
  await writeFields(recordId, {
    [FIELDS.LOOP_STATUS]: LOOP_STATUS.DONE,
    [FIELDS.PD_READY_HOLD]: true,
    [FIELDS.VA_NEEDED]: null,
  })
}

// ─── Process one record ───────────────────────────────────────────────────────
async function processRecord(record) {
  const f = record.fields
  const itemNumber = f[FIELDS.ITEM_NUMBER]
  const website = f[FIELDS.WEBSITE]
  const attempts = (f[FIELDS.ENRICHMENT_ATTEMPTS] || 0) + 1

  console.log(`\n[Loop] ${itemNumber} (attempt ${attempts}/${MAX_ATTEMPTS})`)

  // Increment attempt counter immediately
  await writeFields(record.id, {
    [FIELDS.ENRICHMENT_ATTEMPTS]: attempts,
    [FIELDS.LOOP_STATUS]: LOOP_STATUS.PENDING,
  })

  // ── PHASE 1: COST CHECK ───────────────────────────────────────────────────
  if (!hasCost(f)) {
    if (attempts >= MAX_ATTEMPTS) {
      await parkForVA(record.id, 'Missing cost — unable to find after exhausting all sources')
      return 'parked'
    }
    console.log(`  → Cost missing — skipping enrichment until cost is resolved`)
    return 'skip'
  }

  // ── PHASE 2: UPC RESOLUTION ───────────────────────────────────────────────
  let upc = cleanUPC(f[FIELDS.UPC_CODE])
  if (!upc) {
    // Try to extract from item number string
    upc = extractUPCFromString(itemNumber)
    if (upc) {
      console.log(`  → Extracted UPC from item number: ${upc}`)
      await writeFields(record.id, { [FIELDS.UPC_CODE]: upc })
    }
  }

  // ── PHASE 3: ENRICH ───────────────────────────────────────────────────────
  const parsed = parseItemNumber(itemNumber)
  if (!parsed) {
    await parkForVA(record.id, 'Could not parse item number')
    return 'parked'
  }

  const airtableData = {
    itemNumber,
    brand: f[FIELDS.BRAND],
    brandCorrectSpelling: f[FIELDS.BRAND_CORRECT_SPELL],
    condition: f[FIELDS.CONDITION],
    conditionCode: f[FIELDS.CONDITION_TYPE] || parsed.conditionCode,
    manualConditionType: f[FIELDS.MANUAL_CONDITION],
    website,
    productName: f[FIELDS.PRODUCT_NAME],
    purchaseName: f[FIELDS.PURCHASE_NAME],
    upcCode: upc || f[FIELDS.UPC_CODE],
    globalItemNumber: f[FIELDS.GLOBAL_ITEM_NUMBER],
    sdoColor: f[FIELDS.SDO_COLOR],
    sdoGender: f[FIELDS.SDO_GENDER],
    sdoAgeRange: f[FIELDS.SDO_AGE_RANGE],
    sdoModelName: f[FIELDS.SDO_MODEL_NAME],
    sdoModelNumber: f[FIELDS.SDO_MODEL_NUMBER],
    sdoMenSize: f[FIELDS.SDO_MEN_SIZE],
    sdoMenWidth: f[FIELDS.SDO_MEN_WIDTH],
    sdoWomenSize: f[FIELDS.SDO_WOMEN_SIZE],
    sdoWomenWidth: f[FIELDS.SDO_WOMEN_WIDTH],
    sdoYouthSize: f[FIELDS.SDO_YOUTH_SIZE],
    sdoRetailPrice: f[FIELDS.SDO_RETAIL_PRICE],
    brandSite: f[FIELDS.BRAND_SITE],
    otherSite: f[FIELDS.OTHER_SITE],
    asin: null, // ASIN sourced from GoFlow only
    itemCost: f[FIELDS.ITEM_COST],
  }

  const recordContext = { airtableData, parsedItem: parsed }
  const sources = []

  // Airtable existing data
  const airtableSource = buildAirtableSource(airtableData)
  if (airtableSource) sources.push({ type: 'Airtable (existing)', ...airtableSource })

  // Airtable model match
  const modelNumber = airtableData.sdoModelNumber || parsed.modelNumber
  const brand = airtableData.brandCorrectSpelling || airtableData.brand
  if (modelNumber) {
    const matches = await findMatchesByModel(modelNumber, brand)
    if (matches.length > 0) {
      console.log(`  ✓ Airtable model match found`)
      const variantMatch = matches.find(m => {
        const mc = m.fields[FIELDS.SDO_COLOR]
        return mc && airtableData.sdoColor &&
          mc.toLowerCase() === airtableData.sdoColor.toLowerCase()
      })
      const match = variantMatch || matches[0]
      sources.push({ type: 'Airtable match', ...extractMatchData(match, !!variantMatch) })
    }
  }

  // GoFlow
  console.log(`  → GoFlow: ${itemNumber}`)
  const goflowData = await goflowLookup(itemNumber)
  let goflowASIN = null
  let goflowUPC = null
  if (goflowData) {
    sources.push({ type: 'GoFlow', ...goflowData })
    goflowASIN = goflowData.asin
    goflowUPC = goflowData.upc
    // Write UPC if we found one and don't have it yet
    if (goflowUPC && !upc) {
      await writeFields(record.id, { [FIELDS.UPC_CODE]: goflowUPC })
      upc = goflowUPC
    }
    console.log(`  ✓ GoFlow: ${goflowData.name || 'unnamed'}`)
  }

  // Keepa — ASIN first, then UPC, then search
  const asin = goflowASIN
  const finalUPC = upc || goflowUPC || extractUPCFromString(parsed.raw)

  if (asin) {
    console.log(`  → Keepa ASIN: ${asin}`)
    const keepaData = await lookupByASIN(asin)
    if (keepaData) {
      sources.push({ type: 'Keepa', ...keepaData })
      console.log(`  ✓ Keepa: ${keepaData.name || 'unnamed'}`)
    }
  } else if (finalUPC) {
    console.log(`  → Keepa UPC: ${finalUPC}`)
    const keepaData = await keepaLookupByUPC(finalUPC)
    if (keepaData) {
      sources.push({ type: 'Keepa', ...keepaData })
      // Write UPC back if we used it successfully
      if (!f[FIELDS.UPC_CODE] && finalUPC) {
        await writeFields(record.id, { [FIELDS.UPC_CODE]: finalUPC })
      }
      console.log(`  ✓ Keepa: ${keepaData.name || 'unnamed'}`)
    }
  }

  // Keepa search fallback
  if (!sources.some(s => s.type === 'Keepa')) {
    const searchQuery = buildKeepaSearchQuery(brand, modelNumber, airtableData.productName || airtableData.purchaseName)
    if (searchQuery) {
      console.log(`  → Keepa search: "${searchQuery}"`)
      const keepaData = await keepaSearch(searchQuery)
      if (keepaData) {
        sources.push({ type: 'Keepa (search)', ...keepaData })
        console.log(`  ✓ Keepa search: ${keepaData.name || 'unnamed'}`)
      }
    }
  }

  // No sources at all
  if (sources.length === 0) {
    console.log(`  ✗ No sources found`)
    if (attempts >= MAX_ATTEMPTS) {
      await parkForVA(record.id, 'No product data found after exhausting all sources')
      return 'parked'
    }
    await writeFields(record.id, { [FIELDS.AI_STATUS]: AI_STATUS.NOT_FOUND })
    return 'not_found'
  }

  // Claude enrichment
  console.log(`  → Calling Claude (${sources.length} source(s))`)
  const claudeOutput = await enrichRecord(recordContext, sources)

  // Inject price if Claude didn't find one
  if (!claudeOutput.price) {
    const goflowSource = sources.find(s => s.type === 'GoFlow')
    if (goflowSource?.listingPrice > 0) claudeOutput.price = goflowSource.listingPrice
    const keepaSource = sources.find(s => s.type?.startsWith('Keepa'))
    if (!claudeOutput.price && keepaSource?.currentPrice > 0) claudeOutput.price = keepaSource.currentPrice
  }

  // Build and write payload
  const { fields, status, missingFields } = buildWritePayload(claudeOutput, recordContext)
  await writeFields(record.id, fields)

  console.log(`  → ${status}${missingFields?.length ? ` (missing: ${missingFields.join(', ')})` : ''}`)

  // ── PHASE 4: CHECK FORMULAS ───────────────────────────────────────────────
  // Re-fetch to get updated formula values
  await delay(RATE_DELAY * 2) // give Airtable a moment to recalculate
  const freshRecords = await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `RECORD_ID() = '${record.id}'`,
    fields: [FIELDS.PRODUCT_INFO_VALID, FIELDS.VARIANT_INFO_VALID, FIELDS.PRODUCT_INVALID_WHY, FIELDS.VARIANT_INVALID_WHY],
  }).firstPage()
  const fresh = freshRecords[0]
  const productValid = fresh?.fields[FIELDS.PRODUCT_INFO_VALID]
  const variantValid = fresh?.fields[FIELDS.VARIANT_INFO_VALID]

  console.log(`  → Product Valid: ${productValid} | Variant Valid: ${variantValid}`)

  if (productValid === 'YES' && variantValid === 'YES') {
    await markDone(record.id)
    console.log(`  ✓ DONE — PD Ready Hold set`)
    return 'done'
  }

  // Not done yet — check if we've hit max attempts
  if (attempts >= MAX_ATTEMPTS) {
    // Identify what's still blocking
    const productWhy = fresh.fields[FIELDS.PRODUCT_INVALID_WHY] || ''
    const variantWhy = fresh.fields[FIELDS.VARIANT_INVALID_WHY] || ''
    const reason = [productWhy, variantWhy].filter(Boolean).join(' | ') || 'Validation failed after max attempts'
    await parkForVA(record.id, reason)
    return 'parked'
  }

  return 'retry'
}

// ─── Helper functions ─────────────────────────────────────────────────────────
function buildAirtableSource(data) {
  const hasData = data.productName || data.purchaseName || data.sdoModelName || data.sdoModelNumber || data.sdoColor || data.sdoGender
  if (!hasData) return null
  return {
    name: data.productName || data.purchaseName || null,
    brand: data.brandCorrectSpelling || data.brand || null,
    color: data.sdoColor || null,
    gender: data.sdoGender || null,
    modelName: data.sdoModelName || null,
    modelNumber: data.sdoModelNumber || null,
    description: null,
    imageUrls: [],
    raw: {
      productName: data.productName,
      purchaseName: data.purchaseName,
      sdoModelName: data.sdoModelName,
      sdoModelNumber: data.sdoModelNumber,
      sdoColor: data.sdoColor,
      sdoGender: data.sdoGender,
      sdoAgeRange: data.sdoAgeRange,
      upcCode: data.upcCode,
      
    }
  }
}

function extractMatchData(record, isVariant) {
  const f = record.fields
  return {
    name: f[FIELDS.TITLE] || null,
    brand: null,
    description: f[FIELDS.DESCRIPTION] || null,
    imageUrls: isVariant ? (f[FIELDS.PRODUCT_IMAGES] || []).map(img => img.url) : [],
    raw: {
      title: f[FIELDS.TITLE],
      shopifyCategory: f[FIELDS.SHOPIFY_CATEGORY],
      googleCategory: f[FIELDS.GOOGLE_CATEGORY],
      material: f[FIELDS.MATERIAL],
      option1Value: isVariant ? f[FIELDS.SDO_COLOR] : null,
    }
  }
}

function buildKeepaSearchQuery(brand, model, productName) {
  if (brand && model) return `${brand} ${model}`
  if (brand && productName) return `${brand} ${productName}`
  if (productName && productName.length > 10) return productName
  return null
}

// ─── Main loop ────────────────────────────────────────────────────────────────
async function run() {
  const logger = new WorkerLogger('loop')
  let passCount = 0

  console.log(`[Loop] Starting continuous enrichment loop at ${new Date().toISOString()}`)
  console.log(`[Loop] Max attempts per record: ${MAX_ATTEMPTS}`)

  while (true) {
    passCount++
    const passStart = Date.now()
    console.log(`\n${'═'.repeat(50)}`)
    console.log(`[Loop] Pass ${passCount} starting at ${new Date().toISOString()}`)
    console.log(`${'═'.repeat(50)}`)

    const records = await fetchQueue()
    console.log(`[Loop] Found ${records.length} records in queue`)

    if (records.length === 0) {
      console.log(`[Loop] Queue empty — sleeping 60s before next pass`)
      await delay(60000)
      continue
    }

    const results = { done: 0, retry: 0, parked: 0, skip: 0, not_found: 0, error: 0 }

    for (const record of records) {
      try {
        const outcome = await processRecord(record)
        results[outcome] = (results[outcome] || 0) + 1
      } catch (err) {
        console.error(`  ERROR: ${err.message}`)
        results.error++
        // Write error to VA Needed so it's visible
        try {
          await writeFields(record.id, {
            [FIELDS.VA_NEEDED]: `Error: ${err.message.slice(0, 200)}`,
          })
        } catch {}
      }
    }

    const durationS = ((Date.now() - passStart) / 1000).toFixed(0)
    console.log(`\n[Loop] Pass ${passCount} complete in ${durationS}s`)
    console.log(`  Done: ${results.done} | Retry: ${results.retry} | Parked: ${results.parked} | Skip: ${results.skip} | Errors: ${results.error}`)

    await logger.finish({
      complete: results.done,
      partial: results.retry,
      notFound: results.not_found,
    })
  }
}

run().catch(err => {
  console.error('[Loop] Fatal:', err)
  process.exit(1)
})
