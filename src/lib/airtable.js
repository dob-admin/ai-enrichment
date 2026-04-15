// src/lib/airtable.js
import Airtable from 'airtable'
import { FIELDS, AI_COST_CHECK, COST_FIX } from '../config/fields.js'

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
  .base(process.env.AIRTABLE_BASE_ID)

const table = () => base(process.env.AIRTABLE_TABLE_ID)
const brandsTable = () => base(process.env.AIRTABLE_BRANDS_TABLE_ID)

const delay = (ms) => new Promise(r => setTimeout(r, ms))
const RATE_DELAY = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')

// Fetch records needing brand resolution
export async function getMissingBrandRecords(limit = 50) {
  const records = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      OR({${FIELDS.BRAND}} = '', {${FIELDS.WEBSITE}} = ''),
      {${FIELDS.TOTAL_INVENTORY}} > 0
    )`,
    fields: [
      FIELDS.ITEM_NUMBER,
      FIELDS.BRAND,
      FIELDS.PRODUCT_NAME,
      FIELDS.CONDITION,
      FIELDS.TOTAL_INVENTORY,
    ],
    maxRecords: limit,
  }).eachPage((page, next) => {
    records.push(...page)
    next()
  })
  return records
}

// Fetch records ready for AI enrichment
export async function getEnrichmentQueue(limit = 20) {
  const records = []

  // AI_STATUS field may not exist yet — handle gracefully
  const statusFilter = process.env.AI_STATUS_FIELD_ID
    ? `{${FIELDS.AI_STATUS}} = BLANK()`
    : `TRUE()`

  // Cost gate — only enrich records with confirmed cost
  const costGate = `OR(
    {${FIELDS.AI_COST_CHECK}} = 'Good',
    {${FIELDS.AI_COST_CHECK}} = 'Found',
    {${FIELDS.COST_FIX}} = 'Inputted'
  )`

  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      ${statusFilter},
      ${costGate},
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

// Write brand to a record (brand resolution worker)
export async function writeBrand(recordId, brand) {
  await delay(RATE_DELAY)
  return table().update(recordId, {
    [FIELDS.BRAND]: brand,
  })
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

  if (!process.env.AI_STATUS_FIELD_ID) return counts

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
