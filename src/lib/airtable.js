// src/lib/airtable.js
import Airtable from 'airtable'
import { FIELDS, AI_COST_CHECK, COST_FIX, AI_STATUS, PD_STATUS } from '../config/fields.js'

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
  .base(process.env.AIRTABLE_BASE_ID)

const table = () => base(process.env.AIRTABLE_TABLE_ID)
const brandsTable = () => base(process.env.AIRTABLE_BRANDS_TABLE_ID)

const delay = (ms) => new Promise(r => setTimeout(r, ms))
const RATE_DELAY = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')

// Fetch ALL records needing brand resolution
// Criteria: Total Inventory > 0, Brand empty, Brand Worker Status empty
// No maxRecords limit — worker processes the full queue each run
export async function getMissingBrandRecords() {
  const records = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      {${FIELDS.BRAND}} = BLANK(),
      {${FIELDS.BRAND_WORKER_STATUS}} = BLANK(),
      {${FIELDS.TOTAL_INVENTORY}} > 0
    )`,
    fields: [
      FIELDS.ITEM_NUMBER,
      FIELDS.BRAND,
      FIELDS.PRODUCT_NAME,
      FIELDS.CONDITION,
      FIELDS.TOTAL_INVENTORY,
    ],
  }).eachPage((page, next) => {
    records.push(...page)
    next()
  })
  return records
}

// Fetch records ready for AI enrichment
export async function getEnrichmentQueue(limit = 20) {
  const records = []

  // Cost gate — only enrich records with confirmed cost
  const costGate = `OR(
    {${FIELDS.AI_COST_CHECK}} = 'Good',
    {${FIELDS.AI_COST_CHECK}} = 'Found',
    {${FIELDS.COST_FIX}} = 'Inputted'
  )`

  // First pass: no AI status yet + cost confirmed
  // AI_STATUS field may not exist yet on fresh deploys — handle gracefully
  const firstPass = `AND({${FIELDS.AI_STATUS}} = BLANK(), ${costGate})`

  // Second pass: VA filled in missing fields and marked Complete
  // Guard against re-queuing if Claude already wrote Complete on a prior attempt
  const vaPass = `AND(
    {${FIELDS.PD_ENRICHMENT_STATUS}} = '${PD_STATUS.COMPLETE}',
    NOT({${FIELDS.AI_STATUS}} = '${AI_STATUS.COMPLETE}')
  )`

  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      OR(${firstPass}, ${vaPass}),
      {${FIELDS.TOTAL_INVENTORY}} > 0,
      {${FIELDS.PD_READY}} = 0,
      {${FIELDS.PD_READY_HOLD}} = 0,
      NOT({${FIELDS.WEBSITE}} = 'ignore'),
      NOT({${FIELDS.WEBSITE}} = ''),
      {${FIELDS.SHOPIFY_PRODUCT_ID}} = BLANK()
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
      FIELDS.PD_ENRICHMENT_STATUS,
    ],
    maxRecords: limit,
  }).eachPage((page, next) => {
    records.push(...page)
    next()
  })
  return records
}

// Search for existing processed records with same model number
// Used for Airtable-match enrichment path
export async function findMatchesByModel(modelNumber, brand) {
  if (!modelNumber || !brand) return []
  const records = []
  const safeModel = modelNumber.replace(/'/g, "\\'")
  const safeBrand = brand.replace(/'/g, "\\'")

  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      OR(
        {${FIELDS.SDO_MODEL_NUMBER}} = '${safeModel}',
        {${FIELDS.SDO_MODEL_NAME}} = '${safeModel}'
      ),
      {${FIELDS.BRAND_CORRECT_SPELL}} = '${safeBrand}',
      {${FIELDS.PRODUCT_INFO_VALID}} = 'YES'
    )`,
    fields: [
      FIELDS.TITLE,
      FIELDS.DESCRIPTION,
      FIELDS.SEO_DESCRIPTION,
      FIELDS.SHOPIFY_CATEGORY,
      FIELDS.GOOGLE_CATEGORY,
      FIELDS.MATERIAL,
      FIELDS.SDO_COLOR,
      FIELDS.SDO_GENDER,
      FIELDS.SDO_AGE_RANGE,
      FIELDS.PRODUCT_IMAGES,
      FIELDS.VARIANT_IMAGE_INDEX,
    ],
    maxRecords: 10,
  }).eachPage((page, next) => {
    records.push(...page)
    next()
  })
  return records
}

// Load all brands from BQ Brands table into memory
export async function loadAllBrands() {
  const brands = []
  await brandsTable().select({
    returnFieldsByFieldId: true,
    fields: [
      FIELDS.BQ_TITLE,
      FIELDS.BQ_CORRECT_SPELL,
      FIELDS.BQ_KEY,
      FIELDS.BQ_NEW,
      FIELDS.BQ_USED,
    ],
  }).eachPage((page, next) => {
    brands.push(...page)
    next()
  })
  return brands
}

// Write brand worker result to a record.
// On Found: writes Brand (correct spelling), Brand Name Match, Brand Worker Status = Found.
// On Not Found: writes Brand Name Match (candidate tried, may be null), Brand Worker Status = Not Found.
export async function writeBrandResult(recordId, { status, brand, matchValue }) {
  await delay(RATE_DELAY)
  const fields = {
    [FIELDS.BRAND_WORKER_STATUS]: status,
  }
  if (brand) fields[FIELDS.BRAND] = brand
  if (matchValue !== undefined) fields[FIELDS.BRAND_NAME_MATCH] = matchValue || ''
  return table().update(recordId, fields)
}

// Write all enrichment fields in a single PATCH call
export async function writeEnrichmentFields(recordId, fields) {
  if (!fields || Object.keys(fields).length === 0) return null
  // Filter out undefined values (e.g. AI fields if field IDs not configured yet)
  const cleanFields = Object.fromEntries(
    Object.entries(fields).filter(([k, v]) => k && v !== undefined && v !== null)
  )
  if (Object.keys(cleanFields).length === 0) return null
  await delay(RATE_DELAY)
  return table().update(recordId, cleanFields)
}

// Get summary counts for status reporter
export async function getStatusCounts() {
  const counts = { complete: 0, partial: 0, notFound: 0, pending: 0 }


  const allRecords = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `{${FIELDS.TOTAL_INVENTORY}} > 0`,
    fields: [FIELDS.AI_STATUS, FIELDS.WEBSITE],
  }).eachPage((page, next) => {
    allRecords.push(...page)
    next()
  })

  for (const r of allRecords) {
    const status = r.fields[FIELDS.AI_STATUS]
    if (status === 'Complete') counts.complete++
    else if (status === 'Partial') counts.partial++
    else if (status === 'Not Found') counts.notFound++
    else counts.pending++
  }

  return counts
}
