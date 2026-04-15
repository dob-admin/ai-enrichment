// src/workers/costLookup.js
// Worker 5: Cost Lookup — finds missing costs via UPC chain + model sibling matching
import 'dotenv/config'
import { fileURLToPath } from 'url'
import { exitIfLocked } from '../lib/lock.js'
import Airtable from 'airtable'
import {
  FIELDS,
  WEBSITE,
  AI_COST_CHECK,
  COST_FIX,
} from '../config/fields.js'
import { lookupByItemNumber } from '../lib/goflow.js'
import { WorkerLogger } from '../lib/logger.js'
import { parseItemNumber, cleanUPC } from '../lib/parser.js'

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
  .base(process.env.AIRTABLE_BASE_ID)
const table = () => base(process.env.AIRTABLE_TABLE_ID)
const brandsTable = () => base(process.env.AIRTABLE_BRANDS_TABLE_ID)
const delay = (ms) => new Promise(r => setTimeout(r, ms))
const RATE_DELAY = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')
const VALID_WEBSITES = [WEBSITE.SDO, WEBSITE.REBOUND, WEBSITE.LTV, WEBSITE.RTV]
const BATCH_SIZE = parseInt(process.env.BRAND_BATCH_SIZE || '50')

// Generic words that are never model candidates
const GENERIC_WORDS = new Set([
  'size', 'us', 'uvg', 'uln', 'ugd', 'uai', 'nmb', 'udf',
  'toddler', 'mens', 'womens', 'youth', 'kids', 'junior', 'boys', 'girls',
  'wide', 'narrow', 'medium', 'extra', 'with', 'and', 'the', 'for',
  'new', 'used', 'like', 'good', 'very', 'missing', 'box', 'pack',
  'women', 'running', 'fast', 'cooling', 'accessories', 'salt', 'fish',
  'hiking', 'walking', 'trail', 'casual', 'sport', 'classic', 'original',
  'black', 'white', 'blue', 'red', 'grey', 'gray', 'brown', 'green',
  'inch', 'piece', 'bulk', 'case', 'set', 'pair', 'pack', 'lot',
  'moisturizing', 'canvas', 'marble', 'launch', 'travel', 'switch',
  'cream', 'classic', 'premium', 'deluxe', 'comfort', 'sport', 'ultra',
  'nano', 'micro', 'mini', 'maxi', 'mega', 'super', 'plus',
])

// Normalize a string the same way BQ Brands key formula does
function normalizeKey(str) {
  return str ? str.toLowerCase().replace(/[^a-z0-9]/g, '') : ''
}

// Load all brand correct spellings from BQ Brands — fresh every run
async function loadBrandSet() {
  const brandSet = new Set()
  const brandKeys = new Set()
  await brandsTable().select({
    returnFieldsByFieldId: true,
    fields: [FIELDS.BQ_CORRECT_SPELL],
  }).eachPage((page, next) => {
    for (const r of page) {
      const spelling = r.fields[FIELDS.BQ_CORRECT_SPELL]
      if (spelling) {
        brandSet.add(spelling.toLowerCase().trim())
        brandKeys.add(normalizeKey(spelling))
      }
    }
    next()
  })
  console.log(`[Cost Lookup] Loaded ${brandSet.size} brands from BQ Brands`)
  return { brandSet, brandKeys }
}

async function run({ batchSize } = {}) {
  await exitIfLocked('Cost Lookup')
  console.log(`[Cost Lookup] Starting run at ${new Date().toISOString()}`)
  const logger = new WorkerLogger('cost')

  // Load brands fresh every run
  const { brandSet, brandKeys } = await loadBrandSet()

  const records = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      {${FIELDS.TOTAL_INVENTORY}} > 0,
      NOT({${FIELDS.WEBSITE}} = 'ignore'),
      NOT({${FIELDS.WEBSITE}} = ''),
      {${FIELDS.AI_COST_CHECK}} = BLANK(),
      NOT({${FIELDS.COST_FIX}} = 'No Data'),
      {${FIELDS.SHOPIFY_PRODUCT_ID}} = BLANK()
    )`,
    fields: [
      FIELDS.ITEM_NUMBER,
      FIELDS.WEBSITE,
      FIELDS.ITEM_COST,
      FIELDS.BRAND,
      FIELDS.BRAND_CORRECT_SPELL,
      FIELDS.UPC_CODE,
      FIELDS.COST_FIX,
    ],
    maxRecords: batchSize || BATCH_SIZE,
  }).eachPage((page, next) => { records.push(...page); next() })

  console.log(`[Cost Lookup] Processing ${records.length} records`)

  const results = { good: 0, found: 0, missing: 0, skipped: 0 }

  for (const record of records) {
    const f = record.fields
    const itemNumber = f[FIELDS.ITEM_NUMBER]
    const website = f[FIELDS.WEBSITE]
    const cost = f[FIELDS.ITEM_COST] || 0
    const costFix = f[FIELDS.COST_FIX]

    if (costFix === COST_FIX.NO_DATA) { results.skipped++; continue }
    if (!VALID_WEBSITES.includes(website)) { results.skipped++; continue }

    // ── CASE 1: Cost already valid ────────────────────────────────────────
    if (cost > 0.01) {
      await writeField(record.id, { [FIELDS.AI_COST_CHECK]: AI_COST_CHECK.GOOD })
      logger.log({ itemNumber, website: f[FIELDS.WEBSITE], brand: f[FIELDS.BRAND_CORRECT_SPELL] || f[FIELDS.BRAND], outcome: 'Good' })
      results.good++
      continue
    }

    console.log(`\n[Cost Lookup] ${itemNumber} — searching...`)

    const brand = f[FIELDS.BRAND_CORRECT_SPELL] || f[FIELDS.BRAND] || ''
    const found = await findCostViaChain(f, brand, brandSet, brandKeys)

    if (found && found.cost > 0.01) {
      await writeField(record.id, {
        [FIELDS.ITEM_COST]: found.cost,
        [FIELDS.AI_COST_CHECK]: AI_COST_CHECK.FOUND,
      })
      console.log(`  ✓ Found cost: $${found.cost}`)
      logger.log({ itemNumber, website: f[FIELDS.WEBSITE], brand: f[FIELDS.BRAND_CORRECT_SPELL] || f[FIELDS.BRAND], outcome: 'Found', costFound: found.cost, costSource: found.source, modelCandidates: found.modelCandidates })
      results.found++
    } else {
      await writeField(record.id, { [FIELDS.AI_COST_CHECK]: AI_COST_CHECK.MISSING })
      console.log(`  ✗ Could not find cost`)
      logger.log({ itemNumber, website: f[FIELDS.WEBSITE], brand: f[FIELDS.BRAND_CORRECT_SPELL] || f[FIELDS.BRAND], outcome: 'Missing', modelCandidates: [] })
      results.missing++
    }
  }

  await logger.finish(results)
  console.log(`\n[Cost Lookup] Done — Good: ${results.good}, Found: ${results.found}, Missing: ${results.missing}, Skipped: ${results.skipped}`)
  return results
}

export { run }

async function findCostViaChain(fields, brand, brandSet, brandKeys) {
  const itemNumber = fields[FIELDS.ITEM_NUMBER]
  const parsed = parseItemNumber(itemNumber)
  const upc = cleanUPC(fields[FIELDS.UPC_CODE]) ||
    (parsed?.isUPC ? parsed.baseItemNumber : extractUPCFromString(itemNumber))

  const modelCandidates = new Set()

  // ── Step 1: UPC → find matching records → extract model candidates ────
  if (upc) {
    await delay(RATE_DELAY)
    // Search both UPC Code field AND item number string (UPC often only in item number)
    const upcMatches = await table().select({
      returnFieldsByFieldId: true,
      filterByFormula: `OR(
        FIND('${esc(upc)}', {${FIELDS.UPC_CODE}}) > 0,
        FIND('${esc(upc)}', {${FIELDS.ITEM_NUMBER}}) > 0
      )`,
      fields: [FIELDS.ITEM_NUMBER, FIELDS.ITEM_COST, FIELDS.AVAILABLE_VALUE_AVG,
               FIELDS.BRAND_CORRECT_SPELL, FIELDS.BRAND],
      maxRecords: 10,
    }).firstPage()

    for (const r of upcMatches) {
      const matchItemNum = r.fields[FIELDS.ITEM_NUMBER] || ''
      // Skip if it's the same record
      if (matchItemNum === itemNumber) continue
      // If matched record already has cost AND brand matches, use it directly
      const matchCost = r.fields[FIELDS.AVAILABLE_VALUE_AVG] > 0
        ? r.fields[FIELDS.AVAILABLE_VALUE_AVG]
        : r.fields[FIELDS.ITEM_COST]
      const sibBrand = r.fields[FIELDS.BRAND_CORRECT_SPELL] || r.fields[FIELDS.BRAND] || ''
      const sibKey = normalizeKey(sibBrand)
      const brandKey = normalizeKey(brand)
      const brandMatch = sibKey === brandKey ||
        (brandKey.length > 3 && sibKey.includes(brandKey)) ||
        (sibKey.length > 3 && brandKey.includes(sibKey))
      if (matchCost > 0.01 && brandMatch) {
        console.log(`  → UPC item number match cost: $${matchCost} (${matchItemNum})`)
        return { cost: matchCost, source: matchItemNum, modelCandidates: [] }
      }
      // Extract model candidates from its item number
      extractModelCandidates(matchItemNum, brand, brandSet, brandKeys)
        .forEach(m => modelCandidates.add(m))
    }
  }

  // Also extract from the original item number
  extractModelCandidates(itemNumber, brand, brandSet, brandKeys)
    .forEach(m => modelCandidates.add(m))

  // ── Step 2: Search siblings by model — require brand match ───────────
  const brandKey = normalizeKey(brand)

  for (const model of modelCandidates) {
    await delay(RATE_DELAY)
    try {
      const siblings = await table().select({
        returnFieldsByFieldId: true,
        filterByFormula: `AND(
          FIND('${esc(model)}', {${FIELDS.ITEM_NUMBER}}) > 0,
          OR(
            {${FIELDS.AVAILABLE_VALUE_AVG}} > 0,
            {${FIELDS.ITEM_COST}} > 0.01
          )
        )`,
        fields: [
          FIELDS.ITEM_NUMBER,
          FIELDS.ITEM_COST,
          FIELDS.AVAILABLE_VALUE_AVG,
          FIELDS.BRAND_CORRECT_SPELL,
          FIELDS.BRAND,
        ],
        maxRecords: 5,
      }).firstPage()

      // Filter siblings by brand match
      const brandMatched = siblings.filter(r => {
        const sibBrand = r.fields[FIELDS.BRAND_CORRECT_SPELL] || r.fields[FIELDS.BRAND] || ''
        const sibKey = normalizeKey(sibBrand)
        // Match if brand keys are the same, or one contains the other
        // Handles truncated brand names like "New" matching "newbalance"
        return sibKey === brandKey ||
          (brandKey.length > 2 && sibKey.includes(brandKey)) ||
          (sibKey.length > 2 && brandKey.includes(sibKey)) ||
          (brandKey.length > 4 && sibKey.startsWith(brandKey.slice(0, 4))) ||
          (sibKey.length > 4 && brandKey.startsWith(sibKey.slice(0, 4)))
      })

      if (brandMatched.length) {
        const costs = brandMatched.map(r => {
          const avg = r.fields[FIELDS.AVAILABLE_VALUE_AVG]
          const ic = r.fields[FIELDS.ITEM_COST]
          return avg > 0 ? avg : ic
        }).filter(c => c > 0.01).sort((a, b) => a - b)

        if (costs.length) {
          const cost = costs[Math.floor(costs.length / 2)] // median
          console.log(`  → Model "${model}" brand-matched siblings: [${costs.map(c => '$'+c).join(', ')}] → median $${cost}`)
          return { cost, source: siblings.map(r => r.fields[FIELDS.ITEM_NUMBER]).join(', '), modelCandidates: [...modelCandidates] }
        }
      } else if (siblings.length) {
        console.log(`  - Model "${model}" found siblings but brand mismatch — skipping`)
      }
    } catch {
      // Skip if formula fails on special chars
    }
  }

  // ── Step 3: GoFlow fallback ───────────────────────────────────────────
  try {
    const goflow = await lookupByItemNumber(itemNumber)
    const gfCost = goflow?.raw?.pricing?.default_cost || null
    if (gfCost > 0.01) {
      console.log(`  → GoFlow cost: $${gfCost}`)
      return { cost: gfCost, source: 'GoFlow', modelCandidates: [] }
    }
  } catch {}

  return null
}

// Extract meaningful model candidates — skip brands and generic words
function extractModelCandidates(itemNumber, brand, brandSet, brandKeys) {
  const candidates = new Set()
  const brandKey = normalizeKey(brand)
  const parts = itemNumber.split(/[\s_\-\/]+/)

  for (const part of parts) {
    const p = part.trim()
    if (p.length < 4) continue

    const pLower = p.toLowerCase()
    const pKey = normalizeKey(p)

    // Skip generic words
    if (GENERIC_WORDS.has(pLower)) continue

    // Skip if it's a known brand (exact key match)
    if (brandKeys.has(pKey)) continue

    // Skip current record's own brand
    if (pKey === brandKey || brandKey.includes(pKey) || pKey.includes(brandKey)) continue

    // Skip long pure digit strings (UPCs, barcodes)
    if (/^\d{8,}$/.test(p)) continue

    // Skip size values like C9, M10, W8, 2E, 4E
    if (/^[CMWXYZ]\d+(\.\d+)?$/i.test(p)) continue
    if (/^\d+[EWDBC]$/i.test(p)) continue

    // Skip condition codes
    if (/^(UVG|ULN|UGD|UAI|NMB|UDF)$/i.test(p)) continue

    // Accept numeric model numbers and alphanumeric codes
    if (/^\d{4,7}$/.test(p) || /^[A-Z0-9]{4,12}$/i.test(p)) {
      candidates.add(p)
    }
  }

  return candidates
}

function extractUPCFromString(itemNumber) {
  // Use non-digit boundaries since _ is a word char and breaks \b
  const match = itemNumber.match(/(?<![0-9])(\d{12,13})(?![0-9])/)
  return match ? match[1] : null
}

async function writeField(recordId, fields) {
  await delay(RATE_DELAY)
  const clean = Object.fromEntries(
    Object.entries(fields).filter(([, v]) => v !== undefined && v !== null)
  )
  if (!Object.keys(clean).length) return
  return table().update(recordId, clean)
}

function esc(str) {
  return str ? str.replace(/'/g, "\\'") : ''
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  run().catch(err => {
    console.error('[Cost Lookup] Fatal error:', err)
    process.exit(1)
  })
}
