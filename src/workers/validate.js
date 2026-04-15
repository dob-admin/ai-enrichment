// src/workers/validate.js
// Worker 4: Validation repair — scans PD Ready Hold records that are still
// invalid and fixes what it can automatically, flags what it can't
import 'dotenv/config'
import Airtable from 'airtable'
import { FIELDS, AI_STATUS, WEBSITE, FOOTWEAR_STORES, AI_COST_CHECK } from '../config/fields.js'

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
  .base(process.env.AIRTABLE_BASE_ID)
const table = () => base(process.env.AIRTABLE_TABLE_ID)
const delay = (ms) => new Promise(r => setTimeout(r, ms))
const RATE_DELAY = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')

async function run() {
  console.log(`[Validate] Starting run at ${new Date().toISOString()}`)

  // Fetch records that are PD Ready Hold but still invalid
  const records = []
  await table().select({
    returnFieldsByFieldId: true,
    filterByFormula: `AND(
      {${FIELDS.PD_READY_HOLD}} = 1,
      OR(
        {${FIELDS.PRODUCT_INFO_VALID}} = 'NO',
        {${FIELDS.VARIANT_INFO_VALID}} = 'NO'
      ),
      NOT({${FIELDS.WEBSITE}} = 'ignore')
    )`,
    fields: [
      FIELDS.ITEM_NUMBER,
      FIELDS.WEBSITE,
      FIELDS.PRODUCT_INFO_VALID,
      FIELDS.VARIANT_INFO_VALID,
      FIELDS.PRODUCT_INVALID_WHY,
      FIELDS.VARIANT_INVALID_WHY,
      FIELDS.TITLE,
      FIELDS.DESCRIPTION,
      FIELDS.SEO_DESCRIPTION,
      FIELDS.SHOPIFY_CATEGORY,
      FIELDS.PRODUCT_IMAGES,
      FIELDS.VARIANT_IMAGE_INDEX,
      FIELDS.MATERIAL,
      FIELDS.OPTION_1_VALUE,
      FIELDS.PRICE,
      FIELDS.ITEM_COST,
      FIELDS.AI_STATUS,
      FIELDS.AI_MISSING,
      FIELDS.CONDITION_TYPE,
      FIELDS.MANUAL_CONDITION,
    ],
    maxRecords: 50,
  }).eachPage((page, next) => { records.push(...page); next() })

  console.log(`[Validate] Found ${records.length} invalid PD Hold records`)

  const results = { fixed: 0, partial: 0, flagged: 0 }

  for (const record of records) {
    const f = record.fields
    const itemNumber = f[FIELDS.ITEM_NUMBER]
    const website = f[FIELDS.WEBSITE]
    const productWhy = f[FIELDS.PRODUCT_INVALID_WHY] || ''
    const variantWhy = f[FIELDS.VARIANT_INVALID_WHY] || ''

    console.log(`\n[Validate] ${itemNumber}`)
    console.log(`  Product: ${f[FIELDS.PRODUCT_INFO_VALID]} — ${productWhy}`)
    console.log(`  Variant: ${f[FIELDS.VARIANT_INFO_VALID]} — ${variantWhy}`)

    const fixes = {}
    const unfixable = []

    // ── PRODUCT INFO FIXES ───────────────────────────────────────────────

    // Missing Variant Image Index
    if (!f[FIELDS.VARIANT_IMAGE_INDEX] || f[FIELDS.VARIANT_IMAGE_INDEX] < 1) {
      if (f[FIELDS.PRODUCT_IMAGES]?.length) {
        fixes[FIELDS.VARIANT_IMAGE_INDEX] = 1
        console.log(`  ✓ Fix: set Variant Image Index = 1`)
      } else {
        unfixable.push('Product Images missing')
      }
    }

    // ── VARIANT INFO FIXES ───────────────────────────────────────────────

    // Missing or invalid price
    const cost = f[FIELDS.ITEM_COST] || 0
    const currentPrice = f[FIELDS.PRICE] || 0

    if (cost <= 0.01) {
      // No cost — reset AI Cost Check so cost worker retries on next cycle
      fixes[FIELDS.AI_COST_CHECK] = null  // null clears singleSelect in Airtable
      console.log(`  ✓ Fix: resetting AI Cost Check to blank for cost worker retry`)
    } else if (currentPrice <= 0 || currentPrice <= cost) {
      const newPrice = parseFloat((cost * 1.5).toFixed(2))
      fixes[FIELDS.PRICE] = newPrice
      console.log(`  ✓ Fix: price → $${newPrice} (cost × 1.5)`)
    }

    // Missing Material (SDO/Rebound only)
    if (FOOTWEAR_STORES.includes(website) && !f[FIELDS.MATERIAL]?.length) {
      unfixable.push('Material missing — needs manual lookup')
    }

    // Missing Option 1 Value (SDO/Rebound only)
    if (FOOTWEAR_STORES.includes(website) && !f[FIELDS.OPTION_1_VALUE]) {
      unfixable.push('Option 1 Value (colorway) missing — needs manual lookup')
    }

    // Missing Title
    if (!f[FIELDS.TITLE]) {
      unfixable.push('Title missing — needs enrichment or manual entry')
    }

    // Missing Description
    if (!f[FIELDS.DESCRIPTION]) {
      unfixable.push('Description missing — needs enrichment or manual entry')
    }

    // Missing Product Images
    if (!f[FIELDS.PRODUCT_IMAGES]?.length) {
      unfixable.push('Product Images missing — needs manual upload')
    }

    // Missing Shopify Category
    if (!f[FIELDS.SHOPIFY_CATEGORY]) {
      unfixable.push('Shopify Category missing — needs enrichment or manual entry')
    }

    // ── WRITE FIXES ──────────────────────────────────────────────────────
    if (Object.keys(fixes).length > 0) {
      // Update AI Missing Fields to reflect remaining issues
      if (process.env.AI_MISSING_FIELDS_FIELD_ID && unfixable.length > 0) {
        fixes[FIELDS.AI_MISSING] = unfixable.join(', ')
      }

      try {
        await delay(RATE_DELAY)
        // Pass null values explicitly to clear fields (e.g. AI_COST_CHECK reset)
        await table().update(record.id, fixes)
        console.log(`  → Applied ${Object.keys(fixes).length} fix(es)`)

        if (unfixable.length === 0) {
          results.fixed++
        } else {
          results.partial++
          console.log(`  ⚠ Still needs: ${unfixable.join('; ')}`)
        }
      } catch (err) {
        console.error(`  ERROR writing fixes: ${err.message}`)
      }
    } else if (unfixable.length > 0) {
      // Nothing automatable — update AI Missing Fields for VA
      if (process.env.AI_MISSING_FIELDS_FIELD_ID) {
        try {
          await delay(RATE_DELAY)
          await table().update(record.id, {
            [FIELDS.AI_MISSING]: `[Validate] ${unfixable.join(', ')}`,
          })
        } catch {}
      }
      results.flagged++
      console.log(`  ✗ No auto-fix available: ${unfixable.join('; ')}`)
    }
  }

  console.log(`\n[Validate] Done — Fixed: ${results.fixed}, Partially fixed: ${results.partial}, Flagged for manual: ${results.flagged}`)
}

run().catch(err => {
  console.error('[Validate] Fatal error:', err)
  process.exit(1)
})
