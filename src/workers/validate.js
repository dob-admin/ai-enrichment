// src/workers/validate.js
// Worker 4: Validation repair — scans PD Ready Hold records that are still
// invalid and fixes what it can automatically, flags what it can't
import 'dotenv/config'
import { fileURLToPath } from 'url'
import { exitIfLocked } from '../lib/lock.js'
import Airtable from 'airtable'
import { FIELDS, AI_STATUS, WEBSITE, FOOTWEAR_STORES, AI_COST_CHECK } from '../config/fields.js'
import { WorkerLogger } from '../lib/logger.js'

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
  .base(process.env.AIRTABLE_BASE_ID)
const table = () => base(process.env.AIRTABLE_TABLE_ID)
const delay = (ms) => new Promise(r => setTimeout(r, ms))
const RATE_DELAY = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')

async function run({ batchSize, skipLockCheck } = {}) {
  if (!skipLockCheck) await exitIfLocked('Validate')
  console.log(`[Validate] Starting run at ${new Date().toISOString()}`)
  const logger = new WorkerLogger('validate')

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
    maxRecords: batchSize || 50,
  }).eachPage((page, next) => { records.push(...page); next() })

  console.log(`[Validate] Found ${records.length} invalid PD Hold records`)

  const results = { fixed: 0, partial: 0, flagged: 0, costReset: 0, reEnqueued: 0 }

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
    const needsReEnrich = []

    // ── PRODUCT INFO FIXES ───────────────────────────────────────────────

    // Missing Variant Image Index
    if (!f[FIELDS.VARIANT_IMAGE_INDEX] || f[FIELDS.VARIANT_IMAGE_INDEX] < 1) {
      if (f[FIELDS.PRODUCT_IMAGES]?.length) {
        fixes[FIELDS.VARIANT_IMAGE_INDEX] = 1
        console.log(`  ✓ Fix: set Variant Image Index = 1`)
      } else {
        needsReEnrich.push('Product Images (variant image index)')
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

    // Missing Material (SDO/Rebound only) — re-enrich, don't flag VA
    if (FOOTWEAR_STORES.includes(website) && !f[FIELDS.MATERIAL]?.length) {
      needsReEnrich.push('Material')
    }

    // Missing Option 1 Value (SDO/Rebound only) — re-enrich, don't flag VA
    if (FOOTWEAR_STORES.includes(website) && !f[FIELDS.OPTION_1_VALUE]) {
      needsReEnrich.push('Option 1 Value (colorway)')
    }

    // Missing Title — re-enrich
    if (!f[FIELDS.TITLE]) {
      needsReEnrich.push('Title')
    }

    // Missing Description — re-enrich
    if (!f[FIELDS.DESCRIPTION]) {
      needsReEnrich.push('Description')
    }

    // Missing Product Images — re-enrich
    if (!f[FIELDS.PRODUCT_IMAGES]?.length) {
      needsReEnrich.push('Product Images')
    }

    // Missing Shopify Category — re-enrich
    if (!f[FIELDS.SHOPIFY_CATEGORY]) {
      needsReEnrich.push('Shopify Category')
    }

    // If any content fields are missing, clear PD_READY_HOLD so enrich re-runs
    if (needsReEnrich.length > 0) {
      fixes[FIELDS.PD_READY_HOLD] = false
      if (process.env.AI_STATUS_FIELD_ID) {
        fixes[FIELDS.AI_STATUS] = null  // clear so enrich first pass picks it up
      }
      console.log(`  ↩ Re-enqueue: clearing PD Ready Hold + AI Status for missing: ${needsReEnrich.join(', ')}`)
    }

    // ── WRITE FIXES ──────────────────────────────────────────────────────
    if (Object.keys(fixes).length > 0) {
      // Update AI Missing Fields to reflect remaining issues
      if (process.env.AI_MISSING_FIELDS_FIELD_ID && unfixable.length > 0) {
        fixes[FIELDS.AI_MISSING] = unfixable.join(', ')
      }

      // A cost-only reset is NOT a resolution — the record is still invalid,
      // it's just queued for the cost worker to retry. Don't count as fixed.
      const onlyCostReset = Object.keys(fixes).every(
        k => k === FIELDS.AI_COST_CHECK || k === FIELDS.AI_MISSING
      )

      // Re-enqueue: PD_READY_HOLD cleared so enrich picks it up again
      const isReEnqueue = fixes[FIELDS.PD_READY_HOLD] === false

      try {
        await delay(RATE_DELAY)
        await table().update(record.id, fixes)
        console.log(`  → Applied ${Object.keys(fixes).length} fix(es)`)

        if (isReEnqueue) {
          results.reEnqueued++
          logger.log({ itemNumber, website: f[FIELDS.WEBSITE], outcome: 'ReEnqueued', fieldsWritten: Object.keys(fixes), missingFields: needsReEnrich })
        } else if (onlyCostReset) {
          results.costReset++
          console.log(`  ⏳ Cost reset queued — needs cost worker re-run`)
          logger.log({ itemNumber, website: f[FIELDS.WEBSITE], outcome: 'CostReset', fieldsWritten: Object.keys(fixes), missingFields: unfixable })
        } else if (unfixable.length === 0) {
          results.fixed++
          logger.log({ itemNumber, website: f[FIELDS.WEBSITE], outcome: 'Fixed', fieldsWritten: Object.keys(fixes), missingFields: unfixable })
        } else {
          results.partial++
          console.log(`  ⚠ Still needs: ${unfixable.join('; ')}`)
          logger.log({ itemNumber, website: f[FIELDS.WEBSITE], outcome: 'Partial', fieldsWritten: Object.keys(fixes), missingFields: unfixable })
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
      logger.log({ itemNumber, website: f[FIELDS.WEBSITE], outcome: 'Flagged', missingFields: unfixable })
    }
  }

  await logger.finish(results)
  console.log(`\n[Validate] Done — Fixed: ${results.fixed}, Partially fixed: ${results.partial}, Re-enqueued: ${results.reEnqueued}, Cost Reset: ${results.costReset}, Flagged for manual: ${results.flagged}`)
  // reEnqueued and costReset are NOT resolved — they need enrich/cost to re-run
  return { processed: results.fixed + results.partial + results.flagged + results.costReset + results.reEnqueued, resolved: results.fixed + results.partial }
}

export { run }

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  run().catch(err => {
    console.error('[Validate] Fatal error:', err)
    process.exit(1)
  })
}
