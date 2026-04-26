

// src/workers/loop.js
// ══════════════════════════════════════════════════════════════════════════════
// DOB AI ENRICHMENT — MONOLITH
// ══════════════════════════════════════════════════════════════════════════════
// Continuous enrichment worker. Single file, all responsibilities.
//
// Flow per record:
//   1. Cost check — use existing cost OR queue for Inventory Values report batch
//   2. UPC resolution — from UPC Code or parse from item number
//   3. UPC historical match — if same UPC shipped before anywhere, copy enrichment
//   4. Gender/size extraction cascade (SDO/REBOUND only) — populate SDO_* fields
//   5. Skip Claude if all required fields already populated
//   6. Call Claude for enrichment when needed
//   7. Write payload to Airtable
//   8. Re-fetch, verify PIV+VIV both YES, set PD Ready Hold; else park or retry
//
// Env vars:
//   AIRTABLE_API_KEY, AIRTABLE_BASE_ID, AIRTABLE_TABLE_ID
//   ANTHROPIC_API_KEY
//   GOFLOW_SUBDOMAIN, GOFLOW_API_TOKEN, GOFLOW_BETA_CONTACT
//   KEEPA_API_KEY
//   MIN_COST_THRESHOLD (default 1.00)
//   AIRTABLE_RATE_DELAY_MS (default 250)
//   DRY_RUN_LIMIT (optional — cap records processed in a single pass, then exit)
//   RESET_PARKED (optional — set "true" to clear Loop Status + Enrichment Attempts
//                 on ALL parked records at startup, then continue normally)
// ══════════════════════════════════════════════════════════════════════════════

import 'dotenv/config'
import { fileURLToPath } from 'url'
import Airtable from 'airtable'
import axios from 'axios'
import Anthropic from '@anthropic-ai/sdk'

import {
  FIELDS, WEBSITE, FOOTWEAR_STORES, NPCN_STORES, AI_STATUS, LOOP_STATUS,
  CONDITION_LABELS, APPROVED_MATERIALS, AI_COST_CHECK, COST_FIX,
} from '../config/fields.js'
import { lookupGoogleCategory } from '../config/taxonomy.js'
import { parseItemNumber, cleanUPC, generateSiblingCandidates } from '../lib/parser.js'
import { withRetry } from '../lib/retry.js'
import { WorkerLogger } from '../lib/logger.js'
import { buildSystemPrompt, buildUserMessage } from '../prompts/enrichmentPrompt.js'

// ══════════════════════════════════════════════════════════════════════════════
// CONSTANTS & CONFIG
// ══════════════════════════════════════════════════════════════════════════════

const MAX_ATTEMPTS = 3
const RATE_DELAY = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')
const MIN_COST_THRESHOLD = parseFloat(process.env.MIN_COST_THRESHOLD || '0.02')

const DRY_RUN_LIMIT = process.env.DRY_RUN_LIMIT
  ? parseInt(process.env.DRY_RUN_LIMIT) : null
const RESET_PARKED = process.env.RESET_PARKED === 'true'

// Cost recovery batching
// GoFlow's Inventory Values report caps product_item_number.values at 100 per
// call (per API spec). Submitting more would return a 400. Keep at spec limit.
const COST_BATCH_SIZE = 100
const COST_BATCH_TIMEOUT_MS = 5 * 60 * 1000  // 5 min
const COST_REPORT_POLL_INTERVAL_MS = 5000    // 5s

// AI Logs table
const AI_LOGS_TABLE_ID = 'tblOLMPJyFRGatA59'

// Enrichment Path values (new singleSelect field on AI Logs table)
const PATH = {
  UPC_MATCH:            'upc_match',
  SKIP_CLAUDE_EXISTING: 'skip_claude_existing',
  CLAUDE_FULL:          'claude_full',
  COST_RECOVERY_PENDING:'cost_recovery_pending',
  COST_NO_DATA:         'cost_no_data',
  PARKED:               'parked',
}

// ══════════════════════════════════════════════════════════════════════════════
// CLIENTS
// ══════════════════════════════════════════════════════════════════════════════

const airtable = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
  .base(process.env.AIRTABLE_BASE_ID)
const table = () => airtable(process.env.AIRTABLE_TABLE_ID)
const logsTable = () => airtable(AI_LOGS_TABLE_ID)

const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY })

const goflow = axios.create({
  baseURL: `https://${process.env.GOFLOW_SUBDOMAIN}.api.goflow.com/v1`,
  headers: {
    'Authorization': `Bearer ${process.env.GOFLOW_API_TOKEN}`,
    'X-Beta-Contact': process.env.GOFLOW_BETA_CONTACT,
    'Content-Type': 'application/json',
  },
  timeout: 15000,
})

const keepa = axios.create({
  baseURL: 'https://api.keepa.com',
  timeout: 15000,
})

// ══════════════════════════════════════════════════════════════════════════════
// UTILITIES
// ══════════════════════════════════════════════════════════════════════════════

const delay = (ms) => new Promise(r => setTimeout(r, ms))

/**
 * Compute final price honoring the rule: price must never fall below cost.
 * - If cost is missing/invalid: use price candidate (if valid), else null.
 * - If price candidate is missing: default to standard markup (cost * 1.5 + $7).
 * - If price candidate exists but < cost: bump to cost + $7.
 * - Otherwise: use price candidate as-is.
 * Returns a number rounded to 2 decimals, or null.
 */
function applyPriceFloor(priceCandidate, cost) {
  const c = Number(cost) || 0
  const p = Number(priceCandidate) || 0

  if (c <= 0) return p > 0 ? parseFloat(p.toFixed(2)) : null
  if (p <= 0) return parseFloat((c * 1.5 + 7).toFixed(2))
  if (p < c)  return parseFloat((c + 7).toFixed(2))
  return parseFloat(p.toFixed(2))
}

function truthy(v) {
  if (v === null || v === undefined) return false
  if (typeof v === 'string') return v.trim() !== ''
  if (typeof v === 'number') return v !== 0
  if (Array.isArray(v)) return v.length > 0
  if (typeof v === 'object') return !!v.name || Object.keys(v).length > 0
  return !!v
}

// Extract UPC substring from item number (12-14 consecutive digits, not embedded in other digits)
function extractUPCFromString(str) {
  if (!str) return null
  const m = str.match(/(?<![0-9])(\d{12,14})(?![0-9])/)
  return m ? m[1] : null
}

// Normalize a brand string for comparison: lowercase, strip punctuation, collapse whitespace.
// "Van's"  → "vans" ; "Cole-Haan, Inc." → "cole haan inc"
function normalizeBrandForMatch(s) {
  if (!s) return ''
  return String(s)
    .toLowerCase()
    .replace(/['\u2019]/g, '')        // strip apostrophes first (Van's → vans, not van s)
    .replace(/[^a-z0-9]+/g, ' ')      // non-alphanumeric → space
    .trim()
    .replace(/\s+/g, ' ')
}

// Returns true if item's brand and Keepa result's brand are compatible, or if
// we can't make a determination (missing data on either side). Only returns
// false when we're confident they disagree.
//
// Used to reject Keepa lookups that resolve to a different product than the
// item-number implies — typically caused by repurposed/recycled UPCs.
function brandsMatch(itemBrand, keepaBrand) {
  const a = normalizeBrandForMatch(itemBrand)
  const b = normalizeBrandForMatch(keepaBrand)
  if (!a || !b) return true                           // can't compare → don't reject
  if (a === b) return true
  if (a.includes(b) || b.includes(a)) return true     // "Merrell" vs "Merrell Footwear"
  const aFirst = a.split(' ')[0]
  const bFirst = b.split(' ')[0]
  if (aFirst && bFirst && aFirst === bFirst && aFirst.length >= 3) return true  // "Cole Haan" vs "Cole Haan Inc"
  return false
}

// ══════════════════════════════════════════════════════════════════════════════
// AIRTABLE: QUEUE FETCH
// ══════════════════════════════════════════════════════════════════════════════

const QUEUE_FIELDS = [
  FIELDS.ITEM_NUMBER, FIELDS.BRAND, FIELDS.BRAND_CORRECT_SPELL,
  FIELDS.CONDITION, FIELDS.CONDITION_TYPE, FIELDS.MANUAL_CONDITION,
  FIELDS.WEBSITE, FIELDS.PRODUCT_NAME, FIELDS.PURCHASE_NAME,
  FIELDS.UPC_CODE, FIELDS.GLOBAL_ITEM_NUMBER, FIELDS.TOTAL_INVENTORY,
  FIELDS.SDO_COLOR, FIELDS.SDO_GENDER, FIELDS.SDO_AGE_RANGE,
  FIELDS.SDO_MODEL_NAME, FIELDS.SDO_MODEL_NUMBER,
  FIELDS.SDO_MEN_SIZE, FIELDS.SDO_MEN_WIDTH,
  FIELDS.SDO_WOMEN_SIZE, FIELDS.SDO_WOMEN_WIDTH,
  FIELDS.SDO_YOUTH_SIZE, FIELDS.SDO_YOUTH_WIDTH,
  FIELDS.SDO_RETAIL_PRICE, FIELDS.BRAND_SITE, FIELDS.OTHER_SITE,
  FIELDS.ITEM_COST, FIELDS.PRICE, FIELDS.AVAILABLE_VALUE_AVG,
  FIELDS.AI_COST_CHECK, FIELDS.COST_FIX,
  FIELDS.ENRICHMENT_ATTEMPTS, FIELDS.LOOP_STATUS,
  FIELDS.AI_STATUS, FIELDS.VARIANT_BARCODE,
  FIELDS.PRODUCT_INFO_VALID, FIELDS.VARIANT_INFO_VALID,
  FIELDS.TITLE, FIELDS.DESCRIPTION, FIELDS.SEO_DESCRIPTION,
  FIELDS.SHOPIFY_CATEGORY, FIELDS.PRODUCT_IMAGES,
  FIELDS.OPTION_1_VALUE, FIELDS.OPTION_2_CUSTOM, FIELDS.OPTION_3_CUSTOM,
  FIELDS.MATERIAL,
]

async function fetchQueue() {
  const records = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      {${FIELDS.TOTAL_INVENTORY}} > 0,
      {${FIELDS.PD_READY}} = 0,
      {${FIELDS.PD_READY_HOLD}} = 0,
      NOT(LOWER({${FIELDS.WEBSITE}}) = 'ignore'),
      NOT({${FIELDS.WEBSITE}} = ''),
      {${FIELDS.SHOPIFY_PRODUCT_ID}} = BLANK(),
      OR({${FIELDS.LOOP_STATUS}} = BLANK(), {${FIELDS.LOOP_STATUS}} = '${LOOP_STATUS.PENDING}')
    )`,
    fields: QUEUE_FIELDS,
  }).eachPage((page, next) => { records.push(...page); next() })
  return records
}

async function writeFields(recordId, fields) {
  const clean = Object.fromEntries(
    Object.entries(fields).filter(([, v]) => v !== undefined)
  )
  if (!Object.keys(clean).length) return
  await delay(RATE_DELAY)
  return table().update(recordId, clean)
}

async function parkForVA(recordId, reason, loggerCtx) {
  console.log(`  → Parking for VA: ${reason}`)
  await writeFields(recordId, {
    [FIELDS.LOOP_STATUS]: LOOP_STATUS.NEEDS_VA,
    [FIELDS.VA_NEEDED]: reason,
  })
  if (loggerCtx) loggerCtx.path = PATH.PARKED
}

async function markDone(recordId, loggerCtx) {
  await writeFields(recordId, {
    [FIELDS.LOOP_STATUS]: LOOP_STATUS.DONE,
    [FIELDS.PD_READY_HOLD]: true,
    [FIELDS.VA_NEEDED]: '',
    [FIELDS.AI_STATUS]: AI_STATUS.COMPLETE,
    [FIELDS.AI_MISSING]: '',
  })
  console.log(`  ✓ DONE — PD Ready Hold set`)
}

async function markCostNoData(recordId) {
  console.log(`  → Cost unrecoverable — marking as No Data`)
  await writeFields(recordId, {
    [FIELDS.LOOP_STATUS]: LOOP_STATUS.NEEDS_VA,
    [FIELDS.VA_NEEDED]: 'Cost not found after exhausting GoFlow Inventory Values report',
    [FIELDS.COST_FIX]: COST_FIX.NO_DATA,
  })
}

// ══════════════════════════════════════════════════════════════════════════════
// AIRTABLE: UPC HISTORICAL MATCH
// ══════════════════════════════════════════════════════════════════════════════
// Find a shipped record (Shopify Product ID populated) with the same Variant Barcode.
// Variant Barcode is the formula-stripped UPC (no _UVG suffix), so we match cleanly
// across conditions.

const MATCH_FIELDS = [
  FIELDS.ITEM_NUMBER, FIELDS.WEBSITE,
  FIELDS.TITLE, FIELDS.DESCRIPTION, FIELDS.SEO_DESCRIPTION,
  FIELDS.SHOPIFY_CATEGORY, FIELDS.GOOGLE_CATEGORY, FIELDS.MATERIAL,
  FIELDS.PRODUCT_IMAGES,
  FIELDS.OPTION_1_VALUE, FIELDS.OPTION_2_CUSTOM,
  FIELDS.SDO_COLOR, FIELDS.SDO_GENDER, FIELDS.SDO_AGE_RANGE,
  FIELDS.SDO_MEN_SIZE, FIELDS.SDO_MEN_WIDTH,
  FIELDS.SDO_WOMEN_SIZE, FIELDS.SDO_WOMEN_WIDTH,
  FIELDS.SDO_YOUTH_SIZE, FIELDS.SDO_YOUTH_WIDTH,
]

async function findMatchByUPC(cleanUpc) {
  if (!cleanUpc) return null
  const safe = cleanUpc.replace(/'/g, "\\'")
  const records = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      {${FIELDS.VARIANT_BARCODE}} = '${safe}',
      {${FIELDS.PD_READY}} = 1
    )`,
    fields: MATCH_FIELDS,
    maxRecords: 5,
    sort: [{ field: FIELDS.ITEM_NUMBER, direction: 'desc' }],
  }).eachPage((page, next) => { records.push(...page); next() })
  return records[0] || null
}

// ══════════════════════════════════════════════════════════════════════════════
// AIRTABLE: SIBLING COST LOOKUP (Stage 1)
// ══════════════════════════════════════════════════════════════════════════════
// Find a record with the same base item number OR same UPC that ALREADY has
// a valid cost. Used as a cheap pre-check before firing a GoFlow report.
//
// Pools two queries:
//   1.A  {GLOBAL_ITEM_NUMBER} = base         → cross-condition siblings
//   1.B  {VARIANT_BARCODE}    = cleanUpc     → cross-size/colorway/brand via UPC
//
// Combines results, returns the MIN non-zero cost. Min (not max) is the
// conservative choice — avoids inflating price markup when siblings disagree.
//
// Returns: { cost, source } or null.

async function findSiblingCostInAirtable(candidates) {
  const { base, upc } = candidates || {}
  if (!base && !upc) return null

  const records = []

  // Query 1.A — base match (cross-condition)
  if (base) {
    const safe = base.replace(/'/g, "\\'")
    try {
      await table().select({
        returnFieldsByFieldId: true,
        filterByFormula: `AND(
          {${FIELDS.GLOBAL_ITEM_NUMBER}} = '${safe}',
          {${FIELDS.ITEM_COST}} > ${MIN_COST_THRESHOLD}
        )`,
        fields: [FIELDS.ITEM_NUMBER, FIELDS.ITEM_COST],
        maxRecords: 10,
      }).eachPage((page, next) => { records.push(...page); next() })
    } catch (err) {
      console.log(`  - Airtable sibling (base) query failed: ${err.message}`)
    }
  }

  // Query 1.B — UPC match (cross-size/colorway)
  if (upc) {
    const safe = String(upc).replace(/'/g, "\\'")
    try {
      await table().select({
        returnFieldsByFieldId: true,
        filterByFormula: `AND(
          {${FIELDS.VARIANT_BARCODE}} = '${safe}',
          {${FIELDS.ITEM_COST}} > ${MIN_COST_THRESHOLD}
        )`,
        fields: [FIELDS.ITEM_NUMBER, FIELDS.ITEM_COST],
        maxRecords: 10,
      }).eachPage((page, next) => { records.push(...page); next() })
    } catch (err) {
      console.log(`  - Airtable sibling (upc) query failed: ${err.message}`)
    }
  }

  if (records.length === 0) return null

  // Min non-zero cost across all sibling hits (conservative choice).
  let minCost = Infinity
  let source = null
  for (const r of records) {
    const c = Number(r.fields[FIELDS.ITEM_COST])
    if (c > MIN_COST_THRESHOLD && c < minCost) {
      minCost = c
      source = r.fields[FIELDS.ITEM_NUMBER]
    }
  }
  if (minCost === Infinity) return null
  return { cost: minCost, source }
}

// ══════════════════════════════════════════════════════════════════════════════
// AIRTABLE: SIBLING ENRICHMENT LOOKUP (wider-key fallback after findMatchByUPC)
// ══════════════════════════════════════════════════════════════════════════════
// Runs ONLY if findMatchByUPC returned null. Queries for a shipped record
// sharing the same base item number (cross-condition siblings only). Uses
// the existing MATCH_FIELDS + buildUPCMatchPayload shape for drop-in use.
//
// Intentionally narrow — brand+model siblings are NOT used for enrichment
// because colorway-specific fields (Option 1 Value, SDO_Color) would be
// wrong for a different-colorway sibling. Same-base siblings are same
// physical SKU, different condition only — safe to copy.

async function findSiblingEnrichmentByBase(base) {
  if (!base) return null
  const safe = base.replace(/'/g, "\\'")
  const records = []
  try {
    await table().select({
      returnFieldsByFieldId: true,
      filterByFormula: `AND(
        {${FIELDS.GLOBAL_ITEM_NUMBER}} = '${safe}',
        {${FIELDS.PD_READY}} = 1
      )`,
      fields: MATCH_FIELDS,
      maxRecords: 5,
      sort: [{ field: FIELDS.ITEM_NUMBER, direction: 'desc' }],
    }).eachPage((page, next) => { records.push(...page); next() })
  } catch (err) {
    console.log(`  - Sibling enrichment query failed: ${err.message}`)
    return null
  }
  return records[0] || null
}


// ══════════════════════════════════════════════════════════════════════════════
// GOFLOW: PRODUCT LOOKUP
// ══════════════════════════════════════════════════════════════════════════════

async function goflowLookup(itemNumber) {
  if (!itemNumber || !process.env.GOFLOW_API_TOKEN) return null
  try {
    const res = await withRetry(
      () => goflow.get('/products', { params: { 'filters[item_number:eq]': itemNumber } }),
      `GoFlow /products ${itemNumber}`
    )
    const products = res.data?.data || []
    if (!products.length) return null
    const product = products[0]
    const norm = normalizeGoflow(product)

    // Also fetch listings for ASIN + price
    try {
      const listingsRes = await withRetry(
        () => goflow.get('/listings', { params: { 'filters[product.id:eq]': product.id } }),
        `GoFlow /listings ${product.id}`
      )
      const listings = listingsRes.data?.data || []
      const amazon = listings.find(l => l.store?.channel === 'amazon_marketplace_usa')
      if (amazon?.store_page_url) {
        const asinMatch = amazon.store_page_url.match(/\/dp\/([A-Z0-9]{10})/)
        if (asinMatch) {
          norm.asin = asinMatch[1]
          norm.storePageUrl = amazon.store_page_url
        }
      }
      const priced = listings.find(l => l.price?.amount > 0)
      if (priced) norm.listingPrice = priced.price.amount
    } catch (e) {
      console.log(`  - GoFlow listings lookup failed: ${e.message}`)
    }
    return norm
  } catch (err) {
    if (err.response?.status === 404) return null
    console.error(`  GoFlow lookup failed for ${itemNumber}:`, err.message)
    return null
  }
}

function normalizeGoflow(product) {
  if (!product) return null
  const details = product.details || {}
  const identifiers = product.identifiers || []
  const rawUpc = identifiers.find(i => i.type?.toUpperCase() === 'UPC')?.value || null
  const upc = rawUpc ? rawUpc.split('_')[0] : null
  const ean = identifiers.find(i => i.type?.toUpperCase() === 'EAN')?.value || null
  const mpn = identifiers.find(i => i.type?.toUpperCase() === 'MPN')?.value || null
  const asin = identifiers.find(i => i.type?.toUpperCase() === 'ASIN')?.value || null
  return {
    name: details.name || details.purchase_name || null,
    brand: details.brand || details.manufacturer || null,
    description: details.description || null,
    category: details.category || null,
    condition: details.condition || null,
    imageUrls: [],
    upc, ean, mpn, asin,
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
    },
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// GOFLOW: INVENTORY VALUES REPORT (COST RECOVERY)
// ══════════════════════════════════════════════════════════════════════════════
// Submits an async report for a batch of item_numbers, polls until done,
// fetches the file, and returns { itemNumber: avgCost } map.
//
// Rate limit: 6 reports/hour. We respect this by batching.

async function submitInventoryValuesReport(itemNumbers) {
  const body = {
    columns: ['product_item_number', 'inventory_value_average', 'available_value_average', 'inventory'],
    filters: {
      product_item_number: { values: itemNumbers, operator: 'in' },
    },
    format: 'json',
    limit: 1000000,
  }
  const res = await withRetry(
    () => goflow.post('/reports/inventory/values', body),
    `GoFlow reports/inventory/values (${itemNumbers.length} items)`
  )
  return res.data.id  // report id
}

async function pollReport(reportId, maxWaitMs = 180000) {
  const deadline = Date.now() + maxWaitMs
  while (Date.now() < deadline) {
    const res = await withRetry(
      () => goflow.get(`/reports/${reportId}`),
      `GoFlow reports/${reportId}`
    )
    if (res.data.status === 'completed') return res.data
    if (res.data.status === 'failed') {
      throw new Error(`Report ${reportId} failed: ${res.data.error || 'unknown'}`)
    }
    await delay(COST_REPORT_POLL_INTERVAL_MS)
  }
  throw new Error(`Report ${reportId} did not complete within ${maxWaitMs}ms`)
}

async function fetchReportFile(fileUrl) {
  // Note: the file URL is already authenticated with bearer, use same axios
  const res = await withRetry(
    () => axios.get(fileUrl, {
      headers: {
        'Authorization': `Bearer ${process.env.GOFLOW_API_TOKEN}`,
        'X-Beta-Contact': process.env.GOFLOW_BETA_CONTACT,
      },
      timeout: 60000,
    }),
    `GoFlow report file`
  )
  return res.data  // array of row objects
}

// ═══ Cost-recovery pending-list state (module-scoped) ════════════════════════
// Each entry: { recordId, itemNumber, siblings }
//   itemNumber — the record's own GoFlow item number (primary lookup target)
//   siblings   — enumerated condition-variant item numbers (Stage 2 sibling
//                lookup fallback). If the primary returns $0, any sibling with
//                non-zero cost satisfies the record.
// The batch dedupes item numbers globally (primary + all siblings across all
// records) and submits up to COST_BATCH_SIZE unique names. On apply, each
// record takes MIN non-zero cost across its primary + siblings.
let costPending = []
let costLastFireAt = 0

function addToCostPending(recordId, itemNumber, siblings = []) {
  costPending.push({ recordId, itemNumber, siblings })
}

// For rate-limit planning: the pending list is sized by UNIQUE item names
// (primary + siblings deduped), not by record count. N records with 8
// siblings each collapse to ~8N unique names in the worst case.
function pendingUniqueItemCount() {
  const set = new Set()
  for (const p of costPending) {
    if (p.itemNumber) set.add(p.itemNumber)
    for (const s of p.siblings || []) set.add(s)
  }
  return set.size
}

function shouldFireCostBatch() {
  if (pendingUniqueItemCount() >= COST_BATCH_SIZE) return true
  if (costPending.length >= 1 && (Date.now() - costLastFireAt) >= COST_BATCH_TIMEOUT_MS) return true
  return false
}

async function fireCostBatch(loggerInst) {
  if (!costPending.length) return

  // Pull as many records as we can fit under COST_BATCH_SIZE unique item names.
  // Each record contributes 1 primary + N siblings (typically ≤8 total). Walk
  // records in FIFO and stop before exceeding the unique-name cap.
  const uniqueNames = new Set()
  const batch = []
  const remainder = []
  for (const p of costPending) {
    const candidateNames = new Set(uniqueNames)
    if (p.itemNumber) candidateNames.add(p.itemNumber)
    for (const s of p.siblings || []) candidateNames.add(s)
    if (candidateNames.size <= COST_BATCH_SIZE) {
      batch.push(p)
      for (const n of candidateNames) uniqueNames.add(n)
    } else if (batch.length === 0) {
      // Single record's candidate set exceeds batch size — take it anyway,
      // truncated to COST_BATCH_SIZE. Alternative is infinite hang.
      const trimmedNames = [p.itemNumber, ...(p.siblings || [])].filter(Boolean).slice(0, COST_BATCH_SIZE)
      batch.push({ ...p, siblings: trimmedNames.filter(n => n !== p.itemNumber) })
      for (const n of trimmedNames) uniqueNames.add(n)
    } else {
      remainder.push(p)
    }
  }
  costPending = remainder
  costLastFireAt = Date.now()

  const itemNumbers = [...uniqueNames]
  console.log(`\n[Cost Batch] Submitting ${itemNumbers.length} unique items (covering ${batch.length} records) to Inventory Values report`)

  let reportId
  try {
    reportId = await submitInventoryValuesReport(itemNumbers)
    console.log(`[Cost Batch] Report ${reportId} submitted — polling`)
  } catch (err) {
    console.error(`[Cost Batch] Submit failed: ${err.message}`)
    // Put records back on pending so they retry next batch (don't lose them)
    // Keep costLastFireAt = Date.now() (set above) so we wait 5 min before retry — avoids tight loop
    costPending = [...batch, ...costPending]
    return
  }

  let report
  try {
    report = await pollReport(reportId)
  } catch (err) {
    console.error(`[Cost Batch] Poll failed: ${err.message}`)
    costPending = [...batch, ...costPending]
    return
  }

  let rows
  try {
    rows = await fetchReportFile(report.completed.file_url)
  } catch (err) {
    console.error(`[Cost Batch] Fetch file failed: ${err.message}`)
    costPending = [...batch, ...costPending]
    return
  }

  console.log(`[Cost Batch] Report ${reportId} returned ${rows.length} rows`)

  // Build map: itemNumber → best avg cost
  // Each item_number may appear in multiple warehouses; take the max non-zero avg
  // across warehouses (per-item aggregation).
  const costMap = {}
  for (const row of rows) {
    const item = row.product_item_number
    const avg = row.available_value_average || row.inventory_value_average || 0
    if (avg > MIN_COST_THRESHOLD && (!costMap[item] || avg > costMap[item])) {
      costMap[item] = avg
    }
  }

  // Apply to each pending record
  // For each record: check primary + all siblings in costMap, use MIN non-zero.
  // Min (not max) is the conservative choice when siblings disagree — avoids
  // inflating markup. A matching sibling is treated the same as a primary hit.
  let found = 0
  let notFound = 0
  for (const { recordId, itemNumber, siblings } of batch) {
    const candidates = [itemNumber, ...(siblings || [])].filter(Boolean)
    let minCost = Infinity
    let source = null
    for (const name of candidates) {
      const c = costMap[name]
      if (c && c > MIN_COST_THRESHOLD && c < minCost) {
        minCost = c
        source = name
      }
    }
    const cost = minCost === Infinity ? null : minCost

    if (cost && cost > MIN_COST_THRESHOLD) {
      await writeFields(recordId, {
        [FIELDS.ITEM_COST]: cost,
        [FIELDS.AI_COST_CHECK]: AI_COST_CHECK.FOUND,
        [FIELDS.COST_FIX]: COST_FIX.INPUTTED,
      })
      const sourceTag = source === itemNumber ? 'primary' : `sibling:${source}`
      console.log(`  ✓ Cost found for ${itemNumber}: $${cost.toFixed(2)} [${sourceTag}]`)
      found++
      if (loggerInst) {
        loggerInst.log({
          itemNumber,
          outcome: PATH.COST_RECOVERY_PENDING,
          enrichmentPath: PATH.COST_RECOVERY_PENDING,
          costFound: cost,
          costSource: source === itemNumber ? 'Inventory Values Report' : `Inventory Values Report (sibling: ${source})`,
        })
      }
    } else {
      await markCostNoData(recordId)
      notFound++
      if (loggerInst) {
        loggerInst.log({
          itemNumber,
          outcome: PATH.COST_NO_DATA,
          enrichmentPath: PATH.COST_NO_DATA,
        })
      }
    }
  }
  console.log(`[Cost Batch] Applied: ${found} found, ${notFound} no-data`)
}

// ══════════════════════════════════════════════════════════════════════════════
// KEEPA: PRODUCT LOOKUP
// ══════════════════════════════════════════════════════════════════════════════

const keepaKey = () => process.env.KEEPA_API_KEY

async function keepaLookupByUPC(upc) {
  if (!upc || !keepaKey()) return null
  return keepaProductLookup({ code: upc })
}

async function keepaLookupByASIN(asin) {
  if (!asin || !keepaKey()) return null
  return keepaProductLookup({ asin })
}

async function keepaSearch(query) {
  if (!query || !keepaKey()) return null
  const clean = query
    .replace(/_/g, ' ')
    .replace(/[^a-zA-Z0-9 .-]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 100)
  if (!clean || clean.length < 5) return null
  try {
    const r = await withRetry(
      () => keepa.get('/search', { params: { key: keepaKey(), domain: 1, type: 1, term: clean } }),
      `Keepa /search "${clean}"`
    )
    const asins = r.data?.asinList
    if (!asins?.length) return null
    return await keepaLookupByASIN(asins[0])
  } catch (err) {
    if (err.response?.status === 500) {
      console.log(`  - Keepa search: no result (bad query)`)
      return null
    }
    console.error(`  Keepa search failed for "${clean}":`, err.message)
    return null
  }
}

async function keepaProductLookup(params) {
  try {
    const r = await withRetry(
      () => keepa.get('/product', { params: { key: keepaKey(), domain: 1, ...params } }),
      `Keepa /product`
    )
    const products = r.data?.products
    if (!products?.length) return null
    return normalizeKeepa(products[0])
  } catch (err) {
    console.error(`  Keepa lookup failed:`, err.message)
    return null
  }
}

function normalizeKeepa(product) {
  if (!product) return null
  const imageUrls = (product.imagesCSV || '').split(',').filter(Boolean)
    .map(id => `https://images-na.ssl-images-amazon.com/images/I/${id}`)
  const features = product.features || []
  const categoryPath = (product.categoryTree || []).map(c => c?.name).filter(Boolean)
  return {
    name: product.title || null,
    brand: product.brand || null,
    description: [product.description, features.join(' ')].filter(Boolean).join('\n\n') || null,
    features,
    category: categoryPath[categoryPath.length - 1] || null,
    categoryPath,  // full hierarchy: ["Clothing, Shoes & Jewelry", "Men", "Shoes", "Athletic", "Running"]
    imageUrls,
    upc: product.upcList?.[0] || null,
    asin: product.asin || null,
    color: product.color || null,
    size: product.size || null,
    model: product.model || null,
    manufacturer: product.manufacturer || null,
    currentPrice: (product.stats?.current?.[0] > 0)
      ? product.stats.current[0] / 100 : null,
    raw: {
      title: product.title, brand: product.brand, features,
      description: product.description, color: product.color,
      size: product.size, model: product.model,
    },
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// FOOTWEAR DETECTION (across all websites)
// ══════════════════════════════════════════════════════════════════════════════
// SDO/REBOUND are by definition footwear stores. LTV/RTV are NPCN, which now
// holds both former-SDO/REBOUND footwear (post-migration) AND non-footwear
// items. We use Keepa's category path as the source of truth for LTV/RTV: if
// the path contains any footwear keyword, run shoe enrichment; otherwise leave
// the record on the existing LTV/RTV (non-shoe) codepath.

const FOOTWEAR_KEYWORDS = [
  'shoe', 'sneaker', 'boot', 'sandal', 'footwear',
  'loafer', 'slipper', 'flip-flop', 'flip flop',
  'mule', 'clog', 'oxford', 'moccasin', 'espadrille',
]

function isFootwearByKeepaCategory(keepaSource) {
  if (!keepaSource?.categoryPath?.length) return false
  const joined = keepaSource.categoryPath.join(' / ').toLowerCase()
  return FOOTWEAR_KEYWORDS.some(kw => joined.includes(kw))
}

function isFootwearItem(record, sources) {
  const website = record.fields[FIELDS.WEBSITE]
  // SDO/REBOUND are always footwear (legacy stores).
  if (FOOTWEAR_STORES.includes(website)) return true
  // LTV/RTV: defer to Keepa. No Keepa or non-footwear category → not footwear.
  if (NPCN_STORES.includes(website)) {
    const keepaSource = sources?.find(s => s.type?.startsWith('Keepa'))
    return isFootwearByKeepaCategory(keepaSource)
  }
  return false
}

// ══════════════════════════════════════════════════════════════════════════════
// ATTRIBUTE EXTRACTION CASCADE (FOOTWEAR ONLY)
// ══════════════════════════════════════════════════════════════════════════════
// Derives gender, age range, and size fields from available sources.
//
// Vocabulary:
//   Gender: Male | Female | Unisex | Kids
//   Age Range: Adult | Youth
//
// Write rules (per user's spec):
//   Adult + Male   → SDO_Men_Size + SDO_Men_Width
//   Adult + Female → SDO_Women_Size + SDO_Women_Width
//   Adult + Unisex → BOTH Men and Women fields (same size value)
//   Youth + Kids   → SDO_Youth_Size + SDO_Youth_Width
//   Width defaults to "M" when not specified

// Normalize any gender-like string to canonical {Male, Female, Unisex, Kids}
function normalizeGender(raw) {
  if (!raw) return null
  const s = String(raw).toLowerCase().trim()
  if (/\b(youth|junior|kid|kids|child|children|toddler|infant|baby|boy|girl)\b/.test(s)) return 'Kids'
  if (/\b(women|woman|womens|female|ladies|lady)\b/.test(s)) return 'Female'
  if (/\b(men|mens|man|male)\b/.test(s)) return 'Male'
  if (/\bunisex\b/.test(s)) return 'Unisex'
  return null
}

// Derive age range from gender
function ageRangeFromGender(gender) {
  if (gender === 'Kids') return 'Youth'
  if (gender === 'Male' || gender === 'Female' || gender === 'Unisex') return 'Adult'
  return null
}

// Extract a size-like substring from a free-text string (Keepa/GoFlow name or desc)
// Returns first plausible size match, or null.
//
// Handles:
//   "Size 10", "Size 9.5", "Size 12W"               → Size keyword
//   "US 10"                                          → US prefix
//   "10 M US", "9.5-M US"                            → num + letter-width + US
//   "6 Medium US", "9.5 Wide US", "10 Extra Wide US" → num + WORD-width + US
//   "10M"                                            → tight number+M
//   "..., Gunsmoke, 11", "..., Brindle/Birch, 9.5"   → trailing comma-separated number
const WIDTH_WORD_TO_LETTER = {
  'medium': 'M',
  'narrow': 'N',
  'wide': 'W',
  'extra wide': 'EE',
  'extra narrow': 'B',
}

function extractSizeFromText(text) {
  if (!text) return null
  const s = String(text)

  // Patterns ordered most-specific → most-general. First match wins.
  // Each entry: { re, widthFromMatch } — widthFromMatch(m) returns width letter or null.
  const patterns = [
    // Word-width form: "6 Medium US", "9.5 Extra Wide US"
    {
      re: /\b([0-9]{1,2}(?:\.[0-9])?)\s+(Medium|Wide|Narrow|Extra\s+Wide|Extra\s+Narrow)\s+US\b/i,
      widthFromMatch: (m) => {
        const key = m[2].toLowerCase().replace(/\s+/g, ' ')
        return WIDTH_WORD_TO_LETTER[key] || null
      },
    },
    // Explicit "Size" or "Sz" keyword — "Size 10", "Size 12W", "Sz 9.5"
    {
      re: /\b(?:Size|Sz)[\s:]*([0-9]{1,2}(?:\.[0-9])?)[\s-]*([A-Z]{1,3})?\b/i,
      widthFromMatch: (m) => (m[2] && /^[A-Z]{1,3}$/.test(m[2])) ? m[2].toUpperCase() : null,
    },
    // "US 10" / "US: 10"
    {
      re: /\bUS[\s:]*([0-9]{1,2}(?:\.[0-9])?)\b/i,
      widthFromMatch: () => null,
    },
    // Number + letter-width + US: "10 M US", "9.5-EE US"
    {
      re: /\b([0-9]{1,2}(?:\.[0-9])?)[\s-]*(M|W|D|B|EE|XW|N|C)\s*US\b/i,
      widthFromMatch: (m) => m[2].toUpperCase(),
    },
    // Tight number+M: "10M" (common Keepa form)
    {
      re: /\b([0-9]{1,2}(?:\.[0-9])?)M\b/,
      widthFromMatch: () => 'M',
    },
    // Trailing comma-separated number: "..., Gunsmoke, 11", "..., Brindle/Birch, 9.5"
    // Anchored to end-of-string to avoid matching middle-of-string numbers.
    {
      re: /,\s*([0-9]{1,2}(?:\.[0-9])?)\s*$/,
      widthFromMatch: () => null,
    },
  ]

  for (const { re, widthFromMatch } of patterns) {
    const m = s.match(re)
    if (m) {
      return { size: m[1], width: widthFromMatch(m) }
    }
  }
  return null
}

// Extract width-only from free text (e.g., "10 EE" → EE)
function extractWidthFromText(text) {
  if (!text) return null
  const s = String(text)
  const m = s.match(/\b(?:[0-9]+(?:\.[0-9])?)\s*(M|W|D|B|EE|N|C|XW)\b/i)
  return m ? m[1].toUpperCase() : null
}

// Last-resort one-size detector for bag/accessory items.
// Used only after parseSizeString + all source extractors fail. Conservative
// keyword list — only words that are unambiguous bag/accessory indicators.
// Osprey bag-line names (Fairview, Farpoint, Daylite, Talon, Poco) are included
// because in this data the brand exclusively ships bags, and these item numbers
// carry capacity (e.g., "Fairview 40") instead of sizes — which the R-to-L
// scanner correctly rejects as a size but leaves nothing to write.
const ONE_SIZE_KEYWORD_RE = /(?<![A-Za-z])(?:duffel|duffle|tote|backpack|daypack|rucksack|eye\s*shield|eyeshield|gridiron|fairview|farpoint|daylite|talon|poco)(?![A-Za-z])/i

function detectOneSize(text) {
  if (!text) return null
  return ONE_SIZE_KEYWORD_RE.test(String(text)) ? 'OS' : null
}

// Parser size string (e.g., "Size_10_5M" or "10_5M") → { size, width }
// Supports: numeric (10, 10.5, 10M, 10_5M), letter (S/M/L/XL), one-size (OS),
// gender-letter (M12, W8, J4), dual-gender (M8W10, M7/W8), UK/US/EU prefix.
// Returns object with optional isDual/genderLetter flags for routing.
function parseSizeString(sizeStr) {
  if (!sizeStr) return null
  let s = String(sizeStr).trim()
  // Strip leading "Size_" / "Sz_" / "Size:" markers (single or repeated)
  s = s.replace(/^(?:size|sz):?[_\s:]+/i, '')
  // Strip leading gender/age qualifier prefixes: Womens, Mens, Junior's, Youth, Kids, Girls, Boys, Children's
  s = s.replace(/^(?:Women(?:'?s)?|Men(?:'?s)?|Junior(?:'?s)?|Youth|Kid(?:'?s)?|Girl(?:'?s)?|Boy(?:'?s)?|Child(?:ren)?(?:'?s)?)[_\s]+/i, '')
  // Strip trailing UPC-like digit runs (10–14 digits preceded by _ or whitespace)
  s = s.replace(/[_\s]\d{10,14}$/, '')
  if (!s) return null

  // Jeans: 34x36 (waist x inseam) — before numeric so it wins
  let m = s.match(/^(\d{2})x(\d{2})(?=[_\s/]|$)/i)
  if (m) {
    return { size: `${m[1]}x${m[2]}`, width: null }
  }

  // Combined UK/US: UK8_5_US9, UK_5_US_7, UK 5.5/US 7 — prefer US value (we store US sizes)
  m = s.match(/^UK[_\s]?(\d{1,2}(?:[._]\d)?)[_\s/]+US[_\s]?(\d{1,2}(?:[._]\d)?)(?:[_\s]?(\d?[MWwDBEN]{1,2}))?(?=[_\s/]|$|\b)/i)
  if (m) {
    return {
      size: m[2].replace('_', '.'),
      width: m[3] ? m[3].toUpperCase() : null,
    }
  }

  // Dual-gender: M8W10, M4_5W6, M7/W8, M10/W12, M/5 W7, M/5_W7, M10/ W12
  m = s.match(/^([MW])[_\s/]?(\d{1,2}(?:[._]\d)?)[_\s/]*([MW])[_\s/]?(\d{1,2}(?:[._]\d)?)/i)
  if (m) {
    return {
      size: `${m[1].toUpperCase()}${m[2].replace('_', '.')}/${m[3].toUpperCase()}${m[4].replace('_', '.')}`,
      width: null,
      menSize: m[2].replace('_', '.'),
      womenSize: m[4].replace('_', '.'),
      isDual: true,
    }
  }

  // Letter-with-separator: L/M, M/L, S/M (hat or combined sizes)
  m = s.match(/^(XS|S|M|L|XL)[_/\s]+(XS|S|M|L|XL)(?=[_\s/]|$|\b)/i)
  if (m) {
    return { size: `${m[1].toUpperCase()}/${m[2].toUpperCase()}`, width: null }
  }

  // Letter-only: XS, S, M, L, XL, XXL, XXXL, Small, Medium, Large, MDLG, Powerstep [A-F] or K
  m = s.match(/^(XXXL|XXL|XL|XXS|XS|Small|Medium|Large|MDLG|S|M|L|A|B|C|D|E|F|K)(?=[_\s/]|$|\b)/i)
  if (m) {
    let letter = m[1].toUpperCase()
    if (letter === 'SMALL') letter = 'S'
    else if (letter === 'MEDIUM') letter = 'M'
    else if (letter === 'LARGE') letter = 'L'
    else if (letter === 'MDLG') letter = 'L'   // MDLG = Medium/Large, normalize to L
    return { size: letter, width: null }
  }

  // One-size: OS, O/S, O_S, OSFM, One Size, OSFA
  if (/^(O[_/]?S|OSFM|ONE[_\s]?SIZE|OSFA)(?=[_\s/]|$|\b)/i.test(s)) {
    return { size: 'OS', width: null }
  }

  // Gender-letter + number: M12, W8, J4, C6, K11, Y5
  m = s.match(/^([MWCJKY])(\d{1,2}(?:[._]\d)?)(?=[_\s/]|$|\b)/i)
  if (m) {
    return {
      size: m[2].replace('_', '.'),
      width: null,
      genderLetter: m[1].toUpperCase(),
    }
  }

  // UK/US/EU prefix: UK_6_5, UK8.5, EU36, US_10, US 10M, EU_40_N, US12W
  m = s.match(/^(?:UK|US|EU)[_\s]?(\d{1,2}(?:[._]\d)?)[_\s]?(\d?[MWwDBEN]{1,2}|XW|XN|EEE|EEEE)?(?=[_\s/]|$|\b)/i)
  if (m) {
    return {
      size: m[1].replace('_', '.'),
      width: m[2] ? m[2].toUpperCase() : null,
    }
  }

  // Numeric with optional half and optional (wider) width: 10, 10.5, 10_5, 10M, 10_5M, 11_5_2E, 9M_B
  m = s.match(/^(\d{1,2})(?:[._](\d))?[_\s]*(\d?[MWwDBEN]{1,2}|XW|XN|EEE|EEEE)?(?=[_\s/]|$|\b)/)
  if (m) {
    const whole = m[1]
    const half  = m[2]
    return {
      size: half ? `${whole}.${half}` : whole,
      width: m[3] ? m[3].toUpperCase() : null,
    }
  }

  return null
}

// Main extraction cascade — for SDO/REBOUND only
// Returns { gender, ageRange, sizeFields: {fieldId: value, ...} } or { skip: true } if not footwear
function extractAttributes(record, sources, parsed, upcMatch) {
  if (!isFootwearItem(record, sources)) return { skip: true }

  const f = record.fields

  // ── Step 1: Gather candidates from every source ───────────────────────────
  let gender = null
  let ageRange = null
  let size = null
  let width = null

  // Priority 1: existing SDO fields on this record (preserve anything already there)
  if (truthy(f[FIELDS.SDO_GENDER])) {
    gender = normalizeGender(f[FIELDS.SDO_GENDER])
  }
  if (!gender && truthy(f[FIELDS.SDO_AGE_RANGE])) {
    const ar = String(f[FIELDS.SDO_AGE_RANGE]).toLowerCase()
    if (/youth|kid/.test(ar)) gender = 'Kids'
  }
  if (truthy(f[FIELDS.SDO_AGE_RANGE])) {
    ageRange = normalizeAgeRange(f[FIELDS.SDO_AGE_RANGE])
  }

  // Priority 2: UPC historical match (already copied to record, but use explicitly)
  if (upcMatch) {
    const m = upcMatch.fields
    if (!gender && truthy(m[FIELDS.SDO_GENDER])) gender = normalizeGender(m[FIELDS.SDO_GENDER])
    if (!ageRange && truthy(m[FIELDS.SDO_AGE_RANGE])) ageRange = normalizeAgeRange(m[FIELDS.SDO_AGE_RANGE])
    // Pick size from whichever match field is populated
    if (truthy(m[FIELDS.SDO_MEN_SIZE])) size = size || m[FIELDS.SDO_MEN_SIZE]
    if (truthy(m[FIELDS.SDO_WOMEN_SIZE])) size = size || m[FIELDS.SDO_WOMEN_SIZE]
    if (truthy(m[FIELDS.SDO_YOUTH_SIZE])) size = size || m[FIELDS.SDO_YOUTH_SIZE]
    if (truthy(m[FIELDS.SDO_MEN_WIDTH])) width = width || m[FIELDS.SDO_MEN_WIDTH]
    if (truthy(m[FIELDS.SDO_WOMEN_WIDTH])) width = width || m[FIELDS.SDO_WOMEN_WIDTH]
    if (truthy(m[FIELDS.SDO_YOUTH_WIDTH])) width = width || m[FIELDS.SDO_YOUTH_WIDTH]
  }

  // Priority 3: Keepa structured fields + name regex
  const keepaSource = sources.find(s => s.type?.startsWith('Keepa'))
  if (keepaSource) {
    if (!gender) {
      gender = normalizeGender(keepaSource.name) ||
               normalizeGender(keepaSource.description) ||
               normalizeGender(keepaSource.features?.join(' '))
    }
    if (!size && keepaSource.size) {
      // Keepa's size field is unstructured (e.g., "10 M US" or "Size 10")
      const parsed = extractSizeFromText(keepaSource.size)
      if (parsed) {
        size = size || parsed.size
        width = width || parsed.width
      }
    }
    if (!size) {
      const parsed = extractSizeFromText(keepaSource.name)
      if (parsed) {
        size = size || parsed.size
        width = width || parsed.width
      }
    }
  }

  // Priority 4: GoFlow name regex
  const goflowSource = sources.find(s => s.type === 'GoFlow')
  if (goflowSource) {
    if (!gender) {
      gender = normalizeGender(goflowSource.name) ||
               normalizeGender(goflowSource.description)
    }
    if (!size) {
      const parsed = extractSizeFromText(goflowSource.name) ||
                     extractSizeFromText(goflowSource.description)
      if (parsed) {
        size = size || parsed.size
        width = width || parsed.width
      }
    }
  }

  // Priority 5: parser.js item-number extraction (primary source for structured Rebound items)
  // `dualSize` holds per-gender sizes when a dual-gender format was parsed;
  // used by Step 4 below to write SDO_MEN_SIZE and SDO_WOMEN_SIZE separately.
  let dualSize = null
  if (!size && parsed?.size) {
    const p = parseSizeString(parsed.size)
    if (p) {
      if (p.isDual) {
        dualSize = { men: p.menSize, women: p.womenSize }
        size     = p.menSize   // fallback single value for downstream writers
        if (!gender) gender = 'Unisex'
      } else {
        size  = p.size
        width = width || p.width
        // Use gender-letter to inform gender when we don't already have it
        if (!gender && p.genderLetter) {
          if (p.genderLetter === 'M') gender = 'Male'
          else if (p.genderLetter === 'W') gender = 'Female'
          else if (['J','C','K','Y'].includes(p.genderLetter)) gender = 'Kids'
        }
      }
    }
  }
  // Also try to extract gender from raw item number text
  if (!gender && parsed?.raw) {
    gender = normalizeGender(parsed.raw)
  }

  // ── Step 2: Fallbacks ─────────────────────────────────────────────────────
  if (!gender) gender = 'Unisex'
  if (!ageRange) ageRange = ageRangeFromGender(gender)
  if (!width) width = 'M'  // spec default

  // ── Step 2.5: Last-resort one-size detection from item text ──────────────
  // For bags, totes, accessories (eye shield, etc.) where no numeric shoe size
  // exists. Runs only if all prior sources failed to produce a size. Writes
  // 'OS' so the record can clear without inventing a wrong shoe size.
  if (!size && parsed?.baseItemNumber && detectOneSize(parsed.baseItemNumber)) {
    size = 'OS'
    width = null
  }
  if (!size) {
    const keepaName = sources.find(s => s.type?.startsWith('Keepa'))?.name
    const goflowName = sources.find(s => s.type === 'GoFlow')?.name
    if (detectOneSize(keepaName) || detectOneSize(goflowName)) {
      size = 'OS'
      width = null
    }
  }

  // ── Step 3: If no size found anywhere, park (per user's spec) ─────────────
  if (!size) return { skip: false, noSize: true }

  // ── Step 4: Build field writes per gender+age combination ─────────────────
  const sizeFields = {
    [FIELDS.SDO_GENDER]: gender,
    [FIELDS.SDO_AGE_RANGE]: ageRange,
  }

  if (ageRange === 'Adult' && gender === 'Male') {
    sizeFields[FIELDS.SDO_MEN_SIZE] = size
    sizeFields[FIELDS.SDO_MEN_WIDTH] = width
  } else if (ageRange === 'Adult' && gender === 'Female') {
    sizeFields[FIELDS.SDO_WOMEN_SIZE] = size
    sizeFields[FIELDS.SDO_WOMEN_WIDTH] = width
  } else if (ageRange === 'Adult' && gender === 'Unisex') {
    sizeFields[FIELDS.SDO_MEN_SIZE]    = dualSize?.men   ?? size
    sizeFields[FIELDS.SDO_WOMEN_SIZE]  = dualSize?.women ?? size
    sizeFields[FIELDS.SDO_MEN_WIDTH]   = width
    sizeFields[FIELDS.SDO_WOMEN_WIDTH] = width
  } else if (ageRange === 'Youth' && gender === 'Kids') {
    sizeFields[FIELDS.SDO_YOUTH_SIZE] = size
    sizeFields[FIELDS.SDO_YOUTH_WIDTH] = width
  } else {
    // Odd combination (shouldn't happen after normalization) — default to Unisex Adult
    sizeFields[FIELDS.SDO_MEN_SIZE]    = dualSize?.men   ?? size
    sizeFields[FIELDS.SDO_WOMEN_SIZE]  = dualSize?.women ?? size
    sizeFields[FIELDS.SDO_MEN_WIDTH]   = width
    sizeFields[FIELDS.SDO_WOMEN_WIDTH] = width
  }

  return { skip: false, gender, ageRange, size, width, sizeFields }
}

function normalizeAgeRange(raw) {
  if (!raw) return null
  const s = String(raw).toLowerCase().trim()
  if (/youth|kid|infant|toddler|baby|child|junior/.test(s)) return 'Youth'
  return 'Adult'  // Adult, Adullt, Adults, Aduly, etc.
}

// ══════════════════════════════════════════════════════════════════════════════
// UPC MATCH: APPLY MATCHED RECORD'S FIELDS TO CURRENT RECORD
// ══════════════════════════════════════════════════════════════════════════════
// Copies enrichment data from a shipped record with same UPC.
// Per user's spec: copy everything EXCEPT Option 3 (condition is current-record-specific).
// For RTV, Option 3 is re-derived from current record's condition code.

function buildUPCMatchPayload(matchRecord, currentRecord, parsed) {
  const m = matchRecord.fields
  const website = currentRecord.fields[FIELDS.WEBSITE]
  const isRTV = website === WEBSITE.RTV

  const fields = {}

  // Core enrichment — copy from match
  if (truthy(m[FIELDS.TITLE])) fields[FIELDS.TITLE] = m[FIELDS.TITLE]
  if (truthy(m[FIELDS.DESCRIPTION])) fields[FIELDS.DESCRIPTION] = m[FIELDS.DESCRIPTION]
  if (truthy(m[FIELDS.SEO_DESCRIPTION])) fields[FIELDS.SEO_DESCRIPTION] = m[FIELDS.SEO_DESCRIPTION]
  if (truthy(m[FIELDS.SHOPIFY_CATEGORY])) fields[FIELDS.SHOPIFY_CATEGORY] = m[FIELDS.SHOPIFY_CATEGORY]
  if (truthy(m[FIELDS.GOOGLE_CATEGORY])) fields[FIELDS.GOOGLE_CATEGORY] = m[FIELDS.GOOGLE_CATEGORY]
  if (truthy(m[FIELDS.MATERIAL])) fields[FIELDS.MATERIAL] = m[FIELDS.MATERIAL]

  // Images
  if (m[FIELDS.PRODUCT_IMAGES]?.length) {
    fields[FIELDS.PRODUCT_IMAGES] = m[FIELDS.PRODUCT_IMAGES].map(img => ({ url: img.url }))
    fields[FIELDS.VARIANT_IMAGE_INDEX] = 1
  }

  // Option 1 Value (colorway / color). Mirror to SDO_Color when match has it
  // populated — that itself signals the match record was a footwear item.
  if (truthy(m[FIELDS.OPTION_1_VALUE])) {
    fields[FIELDS.OPTION_1_VALUE] = m[FIELDS.OPTION_1_VALUE]
    if (truthy(m[FIELDS.SDO_COLOR])) fields[FIELDS.SDO_COLOR] = m[FIELDS.OPTION_1_VALUE]
  }

  // The match record is "footwear-shaped" if it has any SDO_* size data —
  // a strong signal that we should let the Option 2 Value formula derive
  // size+width from the SDO_* fields we copy below, not short-circuit it
  // by copying OPTION_2_CUSTOM forward.
  const matchHasShoeData =
    truthy(m[FIELDS.SDO_MEN_SIZE]) ||
    truthy(m[FIELDS.SDO_WOMEN_SIZE]) ||
    truthy(m[FIELDS.SDO_YOUTH_SIZE])

  // Option 2 Custom (NPCN only — size). Skip when match had shoe data, so the
  // current record's Option 2 Value formula can derive size+width from SDO_*.
  if (NPCN_STORES.includes(website) && truthy(m[FIELDS.OPTION_2_CUSTOM]) && !matchHasShoeData) {
    fields[FIELDS.OPTION_2_CUSTOM] = m[FIELDS.OPTION_2_CUSTOM]
  }

  // SDO_* size/width fields. No website gate — if the match record has these
  // populated, it was a footwear item, and the same shoe data applies to the
  // current record (UPC match implies same product). truthy() per-field
  // prevents copying nulls onto non-shoe records that matched a recycled UPC.
  if (truthy(m[FIELDS.SDO_GENDER])) fields[FIELDS.SDO_GENDER] = normalizeGender(m[FIELDS.SDO_GENDER]) || m[FIELDS.SDO_GENDER]
  if (truthy(m[FIELDS.SDO_AGE_RANGE])) fields[FIELDS.SDO_AGE_RANGE] = normalizeAgeRange(m[FIELDS.SDO_AGE_RANGE])
  if (truthy(m[FIELDS.SDO_MEN_SIZE])) fields[FIELDS.SDO_MEN_SIZE] = m[FIELDS.SDO_MEN_SIZE]
  if (truthy(m[FIELDS.SDO_MEN_WIDTH])) fields[FIELDS.SDO_MEN_WIDTH] = m[FIELDS.SDO_MEN_WIDTH]
  if (truthy(m[FIELDS.SDO_WOMEN_SIZE])) fields[FIELDS.SDO_WOMEN_SIZE] = m[FIELDS.SDO_WOMEN_SIZE]
  if (truthy(m[FIELDS.SDO_WOMEN_WIDTH])) fields[FIELDS.SDO_WOMEN_WIDTH] = m[FIELDS.SDO_WOMEN_WIDTH]
  if (truthy(m[FIELDS.SDO_YOUTH_SIZE])) fields[FIELDS.SDO_YOUTH_SIZE] = m[FIELDS.SDO_YOUTH_SIZE]
  if (truthy(m[FIELDS.SDO_YOUTH_WIDTH])) fields[FIELDS.SDO_YOUTH_WIDTH] = m[FIELDS.SDO_YOUTH_WIDTH]

  // Option 3 Custom — RTV only, derive from CURRENT record's condition (never copy from match)
  if (isRTV) {
    const code = currentRecord.fields[FIELDS.CONDITION_TYPE] || parsed?.conditionCode
    if (code && CONDITION_LABELS[code]) {
      fields[FIELDS.OPTION_3_CUSTOM] = CONDITION_LABELS[code]
    }
  }

  // Price: compute using cost-floor helper. The UPC match path has no
  // external price source, so pass null and let the helper default to
  // cost × 1.5 + $7.
  const cost = currentRecord.fields[FIELDS.ITEM_COST]
  const uPrice = applyPriceFloor(null, cost)
  if (uPrice !== null) fields[FIELDS.PRICE] = uPrice

  // AI Status
  fields[FIELDS.AI_STATUS] = AI_STATUS.COMPLETE

  return fields
}

// ══════════════════════════════════════════════════════════════════════════════
// SKIP-CLAUDE CHECK
// ══════════════════════════════════════════════════════════════════════════════
// Returns true if this record has all the required Claude-output fields already
// populated (so we can skip the Claude call entirely).

function hasAllRequiredEnrichmentFields(record) {
  const f = record.fields
  const website = f[FIELDS.WEBSITE]

  const basics = [
    f[FIELDS.TITLE], f[FIELDS.DESCRIPTION], f[FIELDS.SEO_DESCRIPTION],
    f[FIELDS.SHOPIFY_CATEGORY], f[FIELDS.PRODUCT_IMAGES],
  ]
  if (!basics.every(truthy)) return false

  if (FOOTWEAR_STORES.includes(website)) {
    return truthy(f[FIELDS.OPTION_1_VALUE])
  }
  if (website === WEBSITE.LTV) {
    return true  // opt1/opt2 optional
  }
  if (website === WEBSITE.RTV) {
    return truthy(f[FIELDS.OPTION_3_CUSTOM])
  }
  return false
}

// ══════════════════════════════════════════════════════════════════════════════
// CLAUDE ENRICHMENT
// ══════════════════════════════════════════════════════════════════════════════

async function callClaude(recordContext, sources) {
  const systemPrompt = buildSystemPrompt(recordContext.airtableData.website)
  const userMessage = buildUserMessage(recordContext, sources)

  try {
    const response = await withRetry(
      () => anthropic.messages.create({
        model: 'claude-sonnet-4-6',
        max_tokens: 2000,
        messages: [{ role: 'user', content: userMessage }],
        system: [
          {
            type: 'text',
            text: systemPrompt,
            cache_control: { type: 'ephemeral' },
          },
        ],
      }),
      'Anthropic enrichRecord'
    )

    // Cache diagnostic — one compact line per Claude call for future cost verification.
    if (response.usage) {
      const u = response.usage
      console.log(`  [cache] in=${u.input_tokens} out=${u.output_tokens} read=${u.cache_read_input_tokens || 0} write=${u.cache_creation_input_tokens || 0}`)
    }

    const text = response.content
      .filter(b => b.type === 'text')
      .map(b => b.text)
      .join('')
    const clean = text.replace(/^```json\s*/i, '').replace(/\s*```$/, '').trim()
    return JSON.parse(clean)
  } catch (err) {
    if (err instanceof SyntaxError) {
      console.error('Claude returned invalid JSON:', err.message)
      return {
        confidence: 'low',
        missingFields: ['all'],
        validationIssues: ['Claude returned unparseable response'],
        error: true,
      }
    }
    throw err
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// BUILD WRITE PAYLOAD FROM CLAUDE OUTPUT
// ══════════════════════════════════════════════════════════════════════════════
// (adapted from fieldWriter.js, inlined here)

function buildClaudeWritePayload(claudeOutput, recordContext) {
  const { airtableData, parsedItem } = recordContext
  const website = airtableData.website
  // isFootwear is computed once after Keepa is fetched and stashed on
  // recordContext (see Phase 5 of processOne). Falls back to website-only
  // detection for safety if the flag wasn't set.
  const isFootwear = recordContext.isFootwear ?? FOOTWEAR_STORES.includes(website)
  const isNPCN = NPCN_STORES.includes(website)
  const isRTV = website === WEBSITE.RTV

  const fields = {}
  const missingFields = [...(claudeOutput.missingFields || [])]

  // Title
  if (claudeOutput.title) {
    let title = claudeOutput.title
    if (isRTV && parsedItem.conditionCode && CONDITION_LABELS[parsedItem.conditionCode]) {
      title = `${title} ${CONDITION_LABELS[parsedItem.conditionCode]}`
    }
    fields[FIELDS.TITLE] = title
  } else {
    missingFields.push('Title')
  }

  if (claudeOutput.description) fields[FIELDS.DESCRIPTION] = claudeOutput.description
  else missingFields.push('Description')

  if (claudeOutput.seoDescription) {
    fields[FIELDS.SEO_DESCRIPTION] = claudeOutput.seoDescription.slice(0, 160)
  } else missingFields.push('SEO Description')

  if (claudeOutput.shopifyCategory) {
    fields[FIELDS.SHOPIFY_CATEGORY] = claudeOutput.shopifyCategory
    const googleId = lookupGoogleCategory(claudeOutput.shopifyCategory)
    if (googleId) fields[FIELDS.GOOGLE_CATEGORY] = googleId
  } else missingFields.push('Shopify Category')

  if (isFootwear && claudeOutput.material?.length) {
    const filtered = claudeOutput.material.filter(m => APPROVED_MATERIALS.includes(m))
    if (filtered.length) fields[FIELDS.MATERIAL] = filtered
  }

  if (claudeOutput.option1Value) {
    fields[FIELDS.OPTION_1_VALUE] = claudeOutput.option1Value
    if (isFootwear) fields[FIELDS.SDO_COLOR] = claudeOutput.option1Value
  } else if (isFootwear) {
    missingFields.push('Option 1 Value (colorway)')
  }

  // For non-footwear NPCN records, write Claude's size to Option 2 Custom.
  // For footwear, leave Option 2 Custom blank — the Airtable formula derives
  // size+width from SDO_*_Size + SDO_*_Width fields. Writing Custom here would
  // short-circuit the formula (it returns Custom Value when present) and width
  // would never render in the variant.
  if (isNPCN && !isFootwear && claudeOutput.option2CustomValue) {
    fields[FIELDS.OPTION_2_CUSTOM] = claudeOutput.option2CustomValue
  }

  if (isRTV) {
    // CONDITION_LABELS is authoritative — Claude has been observed misinterpreting codes
    // (e.g., outputting "Used - No Missing Box" for NMB instead of "New - Missing Box").
    // Only fall back to Claude's output if we don't have a parsed condition code.
    const conditionText = (parsedItem.conditionCode && CONDITION_LABELS[parsedItem.conditionCode])
      || claudeOutput.option3CustomValue
      || null
    if (conditionText) fields[FIELDS.OPTION_3_CUSTOM] = conditionText
    else missingFields.push('Option 3 Custom Value (used condition)')
  }

  if (claudeOutput.imageUrls?.length) {
    fields[FIELDS.PRODUCT_IMAGES] = claudeOutput.imageUrls.map(url => ({ url }))
    fields[FIELDS.VARIANT_IMAGE_INDEX] = 1
  } else {
    missingFields.push('Product Images')
  }

  // Price — apply cost-floor rule.
  const cost = airtableData.itemCost || null
  const finalPrice = applyPriceFloor(claudeOutput.price, cost)
  if (finalPrice !== null) fields[FIELDS.PRICE] = finalPrice
  else missingFields.push('price')

  // Manual condition type fallback
  if (parsedItem.conditionCode && !parsedItem.conditionCodeFromEnd) {
    if (!airtableData.manualConditionType) {
      fields[FIELDS.MANUAL_CONDITION] = parsedItem.conditionCode
    }
  }

  const dedupedMissing = [...new Set(missingFields)]
  const requiredFields = getRequiredFields(website)
  const missingRequired = dedupedMissing.filter(f => requiredFields.includes(f))

  let status
  if (claudeOutput.error) status = AI_STATUS.NOT_FOUND
  else if (missingRequired.length === 0 && claudeOutput.confidence !== 'low') status = AI_STATUS.COMPLETE
  else if (Object.keys(fields).length > 2) status = AI_STATUS.PARTIAL
  else status = AI_STATUS.NOT_FOUND

  fields[FIELDS.AI_STATUS] = status
  if (dedupedMissing.length > 0) fields[FIELDS.AI_MISSING] = dedupedMissing.join(', ')

  return { fields, status, missingFields: dedupedMissing }
}

function getRequiredFields(website) {
  const base = ['Title', 'Description', 'SEO Description', 'Shopify Category']
  switch (website) {
    case WEBSITE.SDO:
    case WEBSITE.REBOUND:
      return [...base, 'Option 1 Value (colorway)', 'Product Images']
    case WEBSITE.LTV:
      return [...base, 'Product Images']
    case WEBSITE.RTV:
      return [...base, 'Product Images', 'Option 3 Custom Value (used condition)']
    default:
      return base
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// SOURCE BUILDERS (for Claude input)
// ══════════════════════════════════════════════════════════════════════════════

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
    },
  }
}

function buildKeepaSearchQuery(brand, modelNumber, productName) {
  if (brand && modelNumber) return `${brand} ${modelNumber}`
  if (brand && productName) return `${brand} ${productName}`
  if (productName && productName.length > 10) return productName
  return null
}

// ══════════════════════════════════════════════════════════════════════════════
// PROCESS ONE RECORD — MAIN FLOW
// ══════════════════════════════════════════════════════════════════════════════

async function processRecord(record, loggerInst) {
  const f = record.fields
  const itemNumber = f[FIELDS.ITEM_NUMBER]
  const website = f[FIELDS.WEBSITE]
  const rawAttempts = f[FIELDS.ENRICHMENT_ATTEMPTS] || 0
  const attempts = rawAttempts + 1

  console.log(`\n[Loop] ${itemNumber} [${website}] (attempt ${attempts}/${MAX_ATTEMPTS})`)

  const logCtx = {
    itemNumber, website, attempts,
    path: null,  // set as we decide
    sourcesUsed: [],
    fieldsWritten: [],
  }

  // Increment attempt counter, mark Pending
  await writeFields(record.id, {
    [FIELDS.ENRICHMENT_ATTEMPTS]: attempts,
    [FIELDS.LOOP_STATUS]: LOOP_STATUS.PENDING,
  })

  // ── PHASE 1: COST CHECK ───────────────────────────────────────────────────
  const cost = f[FIELDS.ITEM_COST] || 0
  const costFix = f[FIELDS.COST_FIX]
  const costFixVal = costFix?.name || costFix
  if (costFixVal === COST_FIX.NO_DATA) {
    // rawAttempts === 0 means a fresh arrival (first run or VA-reset).
    // An auto-stamped No Data from a previous cycle shouldn't permanently
    // block a fresh retry — clear it and let cost recovery try again.
    if (rawAttempts === 0) {
      console.log(`  → Clearing stale COST_FIX=No Data on fresh run`)
      await writeFields(record.id, { [FIELDS.COST_FIX]: null })
      // Fall through to cost recovery below.
    } else if (attempts >= MAX_ATTEMPTS) {
      await parkForVA(record.id, 'Missing cost — previously marked No Data', logCtx)
      writeLog(loggerInst, logCtx, PATH.COST_NO_DATA)
      return 'parked'
    } else {
      return 'skip'
    }
  }
  if (cost <= MIN_COST_THRESHOLD) {
    // Generate sibling candidates once — used for both inline Airtable lookup
    // and the GoFlow batch (via addToCostPending's siblings param).
    const candidates = generateSiblingCandidates(itemNumber, f[FIELDS.UPC_CODE])

    // ── Stage 1: Airtable sibling cost lookup (inline, cheap) ──────────────
    // Query records sharing the same base item number OR same UPC that already
    // have a valid cost. Uses the GLOBAL_ITEM_NUMBER + VARIANT_BARCODE formula
    // fields. If a match exists, write its cost and skip the GoFlow batch
    // entirely.
    const atSibling = await findSiblingCostInAirtable(candidates)
    if (atSibling) {
      console.log(`  ✓ Cost from Airtable sibling (${atSibling.source}): $${atSibling.cost.toFixed(2)}`)
      await writeFields(record.id, {
        [FIELDS.ITEM_COST]: atSibling.cost,
        [FIELDS.AI_COST_CHECK]: AI_COST_CHECK.FOUND,
        [FIELDS.COST_FIX]: COST_FIX.INPUTTED,
      })
      if (loggerInst) {
        loggerInst.log({
          itemNumber,
          outcome: PATH.COST_RECOVERY_PENDING,
          enrichmentPath: PATH.COST_RECOVERY_PENDING,
          costFound: atSibling.cost,
          costSource: `Airtable sibling (${atSibling.source})`,
        })
      }
      // Continue processing the record in the same pass — cost is now resolved.
      // Fall through to Phase 2 below.
    } else {
      // ── Stage 2: queue for GoFlow batch WITH sibling candidates ──────────
      // fireCostBatch will submit primary + siblings in one report, and on
      // apply take MIN non-zero cost across primary + all siblings.
      const siblings = (candidates.enumeratedSiblings || []).filter(s => s !== itemNumber)
      console.log(`  → Cost missing — queued for Inventory Values report batch (${siblings.length} sibling candidates)`)
      addToCostPending(record.id, itemNumber, siblings)
      writeLog(loggerInst, logCtx, PATH.COST_RECOVERY_PENDING)
      return 'cost_pending'
    }
  }

  // ── PHASE 2: UPC RESOLUTION ───────────────────────────────────────────────
  let upc = cleanUPC(f[FIELDS.UPC_CODE])
  if (!upc) {
    upc = extractUPCFromString(itemNumber)
    if (upc) {
      console.log(`  → Extracted UPC from item number: ${upc}`)
      await writeFields(record.id, { [FIELDS.UPC_CODE]: upc })
    }
  }

  // ── PHASE 3: PARSE ITEM NUMBER ────────────────────────────────────────────
  const parsed = parseItemNumber(itemNumber)
  if (!parsed) {
    await parkForVA(record.id, 'Could not parse item number', logCtx)
    writeLog(loggerInst, logCtx, PATH.PARKED)
    return 'parked'
  }

  // ── PHASE 4: UPC HISTORICAL MATCH ─────────────────────────────────────────
  let upcMatch = null
  if (upc) {
    upcMatch = await findMatchByUPC(upc)
    if (upcMatch) {
      console.log(`  ✓ UPC match found: ${upcMatch.fields[FIELDS.ITEM_NUMBER]}`)
      const payload = buildUPCMatchPayload(upcMatch, record, parsed)
      await writeFields(record.id, payload)
      logCtx.sourcesUsed.push('UPC Match')
      logCtx.fieldsWritten = Object.keys(payload).map(k => resolveFieldName(k))
      // After write, validate
      const valid = await checkValidation(record.id)
      if (valid.both) {
        await markDone(record.id, logCtx)
        writeLog(loggerInst, logCtx, PATH.UPC_MATCH)
        return 'done'
      }
      // Validation failed even after UPC match — attempts maxed?
      if (attempts >= MAX_ATTEMPTS) {
        await parkForVA(record.id, [valid.productWhy, valid.variantWhy].filter(Boolean).join(' | ') || 'Validation failed after UPC match', logCtx)
        writeLog(loggerInst, logCtx, PATH.PARKED)
        return 'parked'
      }
      // Otherwise retry next pass
      return 'retry'
    }
  }

  // ── PHASE 4.5: SIBLING ENRICHMENT BY BASE (wider-key fallback) ────────────
  // Runs only if Phase 4 UPC match returned null. Queries for a shipped
  // record sharing the same base item number (cross-condition sibling). Safe
  // to copy because same base = same physical SKU, just a different condition
  // — colorway/size fields transfer correctly. Reuses buildUPCMatchPayload.
  if (!upcMatch) {
    const baseForEnrich = parsed?.baseItemNumber
    if (baseForEnrich) {
      const siblingMatch = await findSiblingEnrichmentByBase(baseForEnrich)
      if (siblingMatch) {
        console.log(`  ✓ Sibling enrichment match found: ${siblingMatch.fields[FIELDS.ITEM_NUMBER]}`)
        const payload = buildUPCMatchPayload(siblingMatch, record, parsed)
        await writeFields(record.id, payload)
        logCtx.sourcesUsed.push('Sibling Match (base)')
        logCtx.fieldsWritten = Object.keys(payload).map(k => resolveFieldName(k))
        const valid = await checkValidation(record.id)
        if (valid.both) {
          await markDone(record.id, logCtx)
          writeLog(loggerInst, logCtx, PATH.UPC_MATCH)
          return 'done'
        }
        if (attempts >= MAX_ATTEMPTS) {
          await parkForVA(record.id, [valid.productWhy, valid.variantWhy].filter(Boolean).join(' | ') || 'Validation failed after sibling match', logCtx)
          writeLog(loggerInst, logCtx, PATH.PARKED)
          return 'parked'
        }
        return 'retry'
      }
    }
  }

  // ── PHASE 5: EXTERNAL SOURCE FETCH ────────────────────────────────────────
  const airtableData = buildAirtableData(record, upc, parsed)
  const recordContext = { airtableData, parsedItem: parsed }
  const sources = []

  const airtableSrc = buildAirtableSource(airtableData)
  if (airtableSrc) sources.push({ type: 'Airtable (existing)', ...airtableSrc })

  // GoFlow
  console.log(`  → GoFlow: ${itemNumber}`)
  const goflowData = await goflowLookup(itemNumber)
  let goflowASIN = null, goflowUPC = null
  if (goflowData) {
    sources.push({ type: 'GoFlow', ...goflowData })
    goflowASIN = goflowData.asin
    goflowUPC = goflowData.upc
    if (goflowUPC && !upc) {
      await writeFields(record.id, { [FIELDS.UPC_CODE]: goflowUPC })
      upc = goflowUPC
    }
    console.log(`  ✓ GoFlow: ${goflowData.name || 'unnamed'}`)
  }

  // Keepa
  const finalUPC = upc || goflowUPC || extractUPCFromString(parsed.raw)
  // Expected brand from the item — used to reject Keepa mismatches from recycled UPCs.
  // Priority: resolved Correct Spelling > raw Brand > parser's first-chunk guess.
  const expectedBrand = airtableData.brandCorrectSpelling || airtableData.brand || parsed.brand
  if (goflowASIN) {
    console.log(`  → Keepa ASIN: ${goflowASIN}`)
    const keepaData = await keepaLookupByASIN(goflowASIN)
    if (keepaData) {
      if (!brandsMatch(expectedBrand, keepaData.brand)) {
        console.log(`  ✗ Keepa brand mismatch: item="${expectedBrand}" keepa="${keepaData.brand}" — rejecting`)
      } else {
        sources.push({ type: 'Keepa', ...keepaData })
        console.log(`  ✓ Keepa: ${keepaData.name || 'unnamed'}`)
      }
    }
  } else if (finalUPC) {
    console.log(`  → Keepa UPC: ${finalUPC}`)
    const keepaData = await keepaLookupByUPC(finalUPC)
    if (keepaData) {
      if (!brandsMatch(expectedBrand, keepaData.brand)) {
        console.log(`  ✗ Keepa brand mismatch: item="${expectedBrand}" keepa="${keepaData.brand}" — rejecting`)
      } else {
        sources.push({ type: 'Keepa', ...keepaData })
        if (!f[FIELDS.UPC_CODE] && finalUPC) {
          await writeFields(record.id, { [FIELDS.UPC_CODE]: finalUPC })
        }
        console.log(`  ✓ Keepa: ${keepaData.name || 'unnamed'}`)
      }
    }
  }

  if (!sources.some(s => s.type === 'Keepa')) {
    const modelNumber = airtableData.sdoModelNumber || parsed.modelNumber
    const brand = airtableData.brandCorrectSpelling || airtableData.brand
    const searchQuery = buildKeepaSearchQuery(brand, modelNumber, airtableData.productName || airtableData.purchaseName)
    if (searchQuery) {
      console.log(`  → Keepa search: "${searchQuery}"`)
      const keepaData = await keepaSearch(searchQuery)
      if (keepaData) {
        if (!brandsMatch(expectedBrand, keepaData.brand)) {
          console.log(`  ✗ Keepa search brand mismatch: item="${expectedBrand}" keepa="${keepaData.brand}" — rejecting`)
        } else {
          sources.push({ type: 'Keepa (search)', ...keepaData })
          console.log(`  ✓ Keepa search: ${keepaData.name || 'unnamed'}`)
        }
      }
    }
  }

  logCtx.sourcesUsed = sources.map(s => s.type)

  // Compute footwear flag once, after all sources are populated. Used by
  // Phase 6 below and by buildClaudeWritePayload via recordContext.
  const isFootwear = isFootwearItem(record, sources)
  recordContext.isFootwear = isFootwear

  // ── PHASE 6: GENDER/SIZE EXTRACTION (footwear only) ──────────────────────
  if (isFootwear) {
    const attrs = extractAttributes(record, sources, parsed, null)
    if (attrs.noSize) {
      if (attempts >= MAX_ATTEMPTS) {
        await parkForVA(record.id, 'Size not extractable from any source', logCtx)
        writeLog(loggerInst, logCtx, PATH.PARKED)
        return 'parked'
      }
      return 'retry'
    }
    if (attrs.sizeFields) {
      await writeFields(record.id, attrs.sizeFields)
      console.log(`  ✓ Wrote gender/size: ${attrs.gender}/${attrs.ageRange} size ${attrs.size}${attrs.width}`)
    }
  }

  // ── PHASE 7: SKIP CLAUDE IF ALREADY ENRICHED ──────────────────────────────
  // Re-fetch to check current state (we may have just written gender/size)
  const current = await refetchRecord(record.id)
  if (hasAllRequiredEnrichmentFields(current)) {
    console.log(`  → All required enrichment fields already present — skipping Claude`)
    // Reconcile Price against current Cost — covers records enriched on a prior
    // pass before cost was known, or where cost was later updated by recovery.
    // Without this, stale Price + new Cost would fail validation and park here.
    const currentCost  = current.fields[FIELDS.ITEM_COST]  || 0
    const currentPrice = current.fields[FIELDS.PRICE]      || 0
    const reconciledPrice = applyPriceFloor(currentPrice, currentCost)
    if (reconciledPrice !== null && reconciledPrice !== currentPrice) {
      await writeFields(record.id, { [FIELDS.PRICE]: reconciledPrice })
      console.log(`  → Price reconciled: $${currentPrice} → $${reconciledPrice}`)
    }
    const valid = await checkValidation(record.id)
    if (valid.both) {
      await markDone(record.id, logCtx)
      writeLog(loggerInst, logCtx, PATH.SKIP_CLAUDE_EXISTING)
      return 'done'
    }
    if (attempts >= MAX_ATTEMPTS) {
      await parkForVA(record.id, [valid.productWhy, valid.variantWhy].filter(Boolean).join(' | ') || 'Validation failed', logCtx)
      writeLog(loggerInst, logCtx, PATH.PARKED)
      return 'parked'
    }
    return 'retry'
  }

  // ── PHASE 8: CLAUDE ENRICHMENT ────────────────────────────────────────────
  if (sources.length === 0) {
    console.log(`  ✗ No sources found`)
    if (attempts >= MAX_ATTEMPTS) {
      await parkForVA(record.id, 'No product data found after exhausting all sources', logCtx)
      writeLog(loggerInst, logCtx, PATH.PARKED)
      return 'parked'
    }
    await writeFields(record.id, { [FIELDS.AI_STATUS]: AI_STATUS.NOT_FOUND })
    return 'not_found'
  }

  // Compute price from sources (or cost fallback) BEFORE Claude runs.
  // Price is deterministic — no point paying Claude tokens to guess it.
  // Order: GoFlow Amazon listing → Keepa Amazon current → cost × 1.5 + $7
  // In all cases, enforce: price must be >= cost. If not, bump to cost + $7.
  let sourcePrice = null
  const goflowSource = sources.find(s => s.type === 'GoFlow')
  const keepaSource  = sources.find(s => s.type?.startsWith('Keepa'))
  if (goflowSource?.listingPrice > 0)     sourcePrice = goflowSource.listingPrice
  else if (keepaSource?.currentPrice > 0) sourcePrice = keepaSource.currentPrice
  const computedPrice = applyPriceFloor(sourcePrice, cost)

  if (computedPrice) {
    await writeFields(record.id, { [FIELDS.PRICE]: computedPrice })
    console.log(`  → Price pre-set: $${computedPrice}`)
  }

  console.log(`  → Calling Claude (${sources.length} source(s))`)
  const claudeOutput = await callClaude(recordContext, sources)

  // Claude's price output (if any) is overridden by our pre-computed price.
  // This keeps price deterministic and removes it as a retry trigger.
  if (computedPrice) claudeOutput.price = computedPrice

  const { fields, status, missingFields } = buildClaudeWritePayload(claudeOutput, recordContext)
  await writeFields(record.id, fields)
  console.log(`  → ${status}${missingFields?.length ? ` (missing: ${missingFields.join(', ')})` : ''}`)
  logCtx.fieldsWritten = Object.keys(fields).map(k => resolveFieldName(k))

  // ── PHASE 9: VALIDATE AND FINALIZE ────────────────────────────────────────
  writeLog(loggerInst, logCtx, PATH.CLAUDE_FULL)  // always log the Claude call (independent of validation outcome)
  const valid = await checkValidation(record.id)
  if (valid.both) {
    await markDone(record.id, logCtx)
    return 'done'
  }

  if (attempts >= MAX_ATTEMPTS) {
    const reason = [valid.productWhy, valid.variantWhy].filter(Boolean).join(' | ') || 'Validation failed after max attempts'
    await parkForVA(record.id, reason, logCtx)
    writeLog(loggerInst, logCtx, PATH.PARKED)
    return 'parked'
  }
  return 'retry'
}

// ──────────────────────────────────────────────────────────────────────────────
// Helpers used by processRecord
// ──────────────────────────────────────────────────────────────────────────────

function buildAirtableData(record, upc, parsed) {
  const f = record.fields
  return {
    itemNumber: f[FIELDS.ITEM_NUMBER],
    brand: f[FIELDS.BRAND],
    brandCorrectSpelling: f[FIELDS.BRAND_CORRECT_SPELL],
    condition: f[FIELDS.CONDITION],
    conditionCode: f[FIELDS.CONDITION_TYPE] || parsed.conditionCode,
    manualConditionType: f[FIELDS.MANUAL_CONDITION],
    website: f[FIELDS.WEBSITE],
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
    asin: null,
    itemCost: f[FIELDS.ITEM_COST],
  }
}

async function refetchRecord(recordId) {
  const records = await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `RECORD_ID() = '${recordId}'`,
    fields: QUEUE_FIELDS,
  }).firstPage()
  return records[0]
}

async function checkValidation(recordId) {
  await delay(RATE_DELAY * 2)  // let Airtable recalculate formulas
  const records = await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `RECORD_ID() = '${recordId}'`,
    fields: [FIELDS.PRODUCT_INFO_VALID, FIELDS.VARIANT_INFO_VALID, FIELDS.PRODUCT_INVALID_WHY, FIELDS.VARIANT_INVALID_WHY],
  }).firstPage()
  const f = records[0]?.fields || {}
  const productValid = f[FIELDS.PRODUCT_INFO_VALID]
  const variantValid = f[FIELDS.VARIANT_INFO_VALID]
  console.log(`  → Product Valid: ${productValid} | Variant Valid: ${variantValid}`)
  return {
    both: productValid === 'YES' && variantValid === 'YES',
    productValid, variantValid,
    productWhy: f[FIELDS.PRODUCT_INVALID_WHY] || '',
    variantWhy: f[FIELDS.VARIANT_INVALID_WHY] || '',
  }
}

// Log via shared WorkerLogger — uses existing per-record log() method
// Adds enrichmentPath to the log entry
function writeLog(loggerInst, ctx, path) {
  if (!loggerInst) return
  loggerInst.log({
    itemNumber: ctx.itemNumber,
    website: ctx.website,
    outcome: path,
    sourcesUsed: ctx.sourcesUsed,
    fieldsWritten: ctx.fieldsWritten,
    enrichmentPath: path,
  })
}

// Field-ID → human name helper (for FIELDS_WRITTEN column in log)
const FIELD_NAME_MAP = Object.fromEntries(Object.entries(FIELDS).map(([k, v]) => [v, k]))
function resolveFieldName(fieldId) {
  return FIELD_NAME_MAP[fieldId] || fieldId
}

// ══════════════════════════════════════════════════════════════════════════════
// RESET PARKED (startup helper, triggered by RESET_PARKED=true)
// ══════════════════════════════════════════════════════════════════════════════

async function resetAllParked() {
  console.log(`[Reset Parked] Fetching records with Loop Status = Needs VA...`)
  const ids = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `{${FIELDS.LOOP_STATUS}} = '${LOOP_STATUS.NEEDS_VA}'`,
    fields: [FIELDS.ITEM_NUMBER],
  }).eachPage((page, next) => {
    for (const r of page) ids.push(r.id)
    next()
  })
  console.log(`[Reset Parked] Found ${ids.length} parked records to reset`)

  const BATCH = 10
  let done = 0
  for (let i = 0; i < ids.length; i += BATCH) {
    const batch = ids.slice(i, i + BATCH)
    const updates = batch.map(id => ({
      id,
      fields: {
        [FIELDS.LOOP_STATUS]: null,
        [FIELDS.ENRICHMENT_ATTEMPTS]: 0,
        [FIELDS.VA_NEEDED]: '',
      },
    }))
    try {
      await delay(RATE_DELAY)
      await table().update(updates)
      done += batch.length
      process.stdout.write(`\r  Reset ${done}/${ids.length}`)
    } catch (err) {
      console.error(`\n  Reset batch error: ${err.message}`)
    }
  }
  console.log(`\n[Reset Parked] Done — reset ${done} records`)
}

// ══════════════════════════════════════════════════════════════════════════════
// MAIN LOOP
// ══════════════════════════════════════════════════════════════════════════════

async function run() {
  const loggerInst = new WorkerLogger('loop')
  let passCount = 0
  let totalProcessedThisBoot = 0

  console.log(`[Loop] Starting at ${new Date().toISOString()}`)
  console.log(`[Loop] Max attempts per record: ${MAX_ATTEMPTS}`)
  if (DRY_RUN_LIMIT !== null) console.log(`[Loop] DRY_RUN_LIMIT set: ${DRY_RUN_LIMIT}`)
  if (RESET_PARKED) console.log(`[Loop] RESET_PARKED set: will reset parked records on startup`)

  if (RESET_PARKED) {
    await resetAllParked()
  }

  while (true) {
    passCount++
    const passStart = Date.now()
    console.log(`\n${'═'.repeat(60)}`)
    console.log(`[Loop] Pass ${passCount} starting at ${new Date().toISOString()}`)
    console.log(`${'═'.repeat(60)}`)

    const records = await fetchQueue()
    console.log(`[Loop] Found ${records.length} records in queue`)

    // Optional DRY_RUN_LIMIT cap — process at most N records this entire run
    let batch = records
    if (DRY_RUN_LIMIT !== null) {
      const remaining = DRY_RUN_LIMIT - totalProcessedThisBoot
      if (remaining <= 0) {
        console.log(`[Loop] DRY_RUN_LIMIT reached (${DRY_RUN_LIMIT} records) — exiting`)
        await drainCostQueue(loggerInst)  // one last batch if pending
        await loggerInst.finish({ dryRunExit: true })
        process.exit(0)
      }
      batch = records.slice(0, remaining)
      console.log(`[Loop] DRY_RUN_LIMIT: processing ${batch.length} of ${records.length}`)
    }

    if (batch.length === 0) {
      // No queue — still check if we should drain cost pending
      if (shouldFireCostBatch()) {
        await fireCostBatch(loggerInst)
      }
      console.log(`[Loop] Queue empty — sleeping 60s`)
      await delay(60000)
      continue
    }

    const results = { done: 0, retry: 0, parked: 0, skip: 0, cost_pending: 0, not_found: 0, error: 0 }

    for (const record of batch) {
      try {
        const outcome = await processRecord(record, loggerInst)
        results[outcome] = (results[outcome] || 0) + 1
        totalProcessedThisBoot++

        // Fire cost batch if trigger conditions met
        if (shouldFireCostBatch()) {
          await fireCostBatch(loggerInst)
        }
      } catch (err) {
        console.error(`  ERROR: ${err.message}`)
        console.error(err.stack)
        results.error++
        try {
          await writeFields(record.id, {
            [FIELDS.VA_NEEDED]: `Error: ${err.message.slice(0, 200)}`,
          })
        } catch {}
      }
    }

    // End-of-pass: drain any remaining cost pending records
    if (costPending.length > 0) {
      console.log(`\n[Loop] End of pass — ${costPending.length} records in cost pending`)
      if (shouldFireCostBatch()) await fireCostBatch(loggerInst)
    }

    const durationS = ((Date.now() - passStart) / 1000).toFixed(0)
    console.log(`\n[Loop] Pass ${passCount} complete in ${durationS}s`)
    console.log(`  Done: ${results.done} | Retry: ${results.retry} | Parked: ${results.parked} | Skip: ${results.skip} | CostPending: ${results.cost_pending} | Errors: ${results.error}`)

    await loggerInst.finish({
      complete: results.done,
      partial: results.retry,
      notFound: results.not_found,
      parked: results.parked,
      cost_pending: results.cost_pending,
    })

    // If DRY_RUN_LIMIT set and we've hit it, exit
    if (DRY_RUN_LIMIT !== null && totalProcessedThisBoot >= DRY_RUN_LIMIT) {
      console.log(`[Loop] DRY_RUN_LIMIT reached — draining cost queue and exiting`)
      await drainCostQueue(loggerInst)
      process.exit(0)
    }
  }
}

async function drainCostQueue(loggerInst) {
  if (!costPending.length) return
  console.log(`[Loop] Draining cost queue (${costPending.length} records)`)
  costLastFireAt = 0  // force fire
  while (costPending.length > 0) {
    await fireCostBatch(loggerInst)
    await delay(1000)
  }
}

// ══════════════════════════════════════════════════════════════════════════════
// ENTRY POINT
// ══════════════════════════════════════════════════════════════════════════════

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  run().catch(err => {
    console.error('[Loop] Fatal:', err)
    process.exit(1)
  })
}

export { run, processRecord, extractAttributes, findMatchByUPC }
