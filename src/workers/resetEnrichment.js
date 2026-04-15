// src/workers/resetEnrichment.js
// ONE-SHOT: Clears all AI enrichment fields on records where AI Status is set.
// Run manually from Railway or locally. Exits when done.
//
// Step 1 — Fetch all records where AI Enrichment Status is not empty
// Step 2 — Clear 13 fields (12 → null/[], 1 → false) in batches of 10
// Step 3 — Report final count
//
// SAFE: Never touches cost, brand, website, condition, item number,
//       size/width fields, SEO Title, Price, or PD Ready (final checkbox).

import 'dotenv/config'
import Airtable from 'airtable'

// ─── Field IDs to clear ───────────────────────────────────────────────────────

const AI_STATUS_FIELD_ID        = 'fldsJ3tp3XPmd82NR'  // AI Enrichment Status  → null
const AI_MISSING_FIELD_ID       = 'fldmgcC2eAxKRyUKt'  // AI Missing Fields     → null
const TITLE_FIELD_ID            = 'fldhbkyCE3ZK3huqf'  // Title                 → null
const DESCRIPTION_FIELD_ID      = 'fld8s6bi94sxJiqZE'  // Description           → null
const SEO_DESC_FIELD_ID         = 'fldtmIK6WVA93On8C'  // SEO Description       → null
const SHOPIFY_CAT_FIELD_ID      = 'fldE4yEqLZKe5UgPZ'  // Shopify Category      → null
const GOOGLE_CAT_FIELD_ID       = 'fldPGP1Xxrf75nC6U'  // Google Category       → null
const MATERIAL_FIELD_ID         = 'fldojvxXhW2UUH0My'  // Material              → []
const OPTION_1_VALUE_FIELD_ID   = 'fld3Wla73i6UIMOfF'  // Option 1 Value        → null
const SDO_COLOR_FIELD_ID        = 'fld5FU5pikJMxFCag'  // SDO Color             → null
const PRODUCT_IMAGES_FIELD_ID   = 'fldfOrq8jm703glZC'  // Product Images        → []
const VARIANT_IMG_IDX_FIELD_ID  = 'fldYLJxgnXASagxsN'  // Variant Image Index   → null
const PD_READY_HOLD_FIELD_ID    = 'fldhMJKOKLtxOnlmi'  // PD Ready Hold         → false

// The payload applied to every matched record
const CLEAR_PAYLOAD = {
  [AI_STATUS_FIELD_ID]:        null,
  [AI_MISSING_FIELD_ID]:       null,
  [TITLE_FIELD_ID]:            null,
  [DESCRIPTION_FIELD_ID]:      null,
  [SEO_DESC_FIELD_ID]:         null,
  [SHOPIFY_CAT_FIELD_ID]:      null,
  [GOOGLE_CAT_FIELD_ID]:       null,
  [MATERIAL_FIELD_ID]:         [],    // multipleSelects — must be empty array
  [OPTION_1_VALUE_FIELD_ID]:   null,
  [SDO_COLOR_FIELD_ID]:        null,
  [PRODUCT_IMAGES_FIELD_ID]:   [],    // multipleAttachments — must be empty array
  [VARIANT_IMG_IDX_FIELD_ID]:  null,
  [PD_READY_HOLD_FIELD_ID]:    false, // checkbox — false, not null
}

// ─── Config ───────────────────────────────────────────────────────────────────

const RATE_DELAY    = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')
const BATCH_SIZE    = 10  // Airtable max records per update call

const delay = (ms) => new Promise(r => setTimeout(r, ms))

// ─── Main ─────────────────────────────────────────────────────────────────────

async function run() {
  const startTime = Date.now()

  // Validate env
  if (!process.env.AIRTABLE_API_KEY)   throw new Error('Missing AIRTABLE_API_KEY')
  if (!process.env.AIRTABLE_BASE_ID)   throw new Error('Missing AIRTABLE_BASE_ID')
  if (!process.env.AIRTABLE_TABLE_ID)  throw new Error('Missing AIRTABLE_TABLE_ID')

  const base  = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID)
  const table = base(process.env.AIRTABLE_TABLE_ID)

  console.log(`\n${'═'.repeat(55)}`)
  console.log(`[Reset] Starting enrichment field reset`)
  console.log(`[Reset] ${new Date().toISOString()}`)
  console.log(`${'═'.repeat(55)}`)

  // ── Step 1: Fetch all record IDs where AI Status is set ──────────────────

  console.log(`\n[Reset] Step 1 — Fetching records where AI Enrichment Status is not empty...`)

  const recordIds = []

  await table.select({
    returnFieldsByFieldId: true,
    filterByFormula: `NOT({${AI_STATUS_FIELD_ID}} = BLANK())`,
    fields: [AI_STATUS_FIELD_ID],  // fetch only the filter field — we only need IDs
  }).eachPage((page, next) => {
    for (const r of page) recordIds.push(r.id)
    process.stdout.write(`\r  Fetched ${recordIds.length} records...`)
    next()
  })

  console.log(`\n[Reset] Found ${recordIds.length} records to clear.`)

  if (recordIds.length === 0) {
    console.log(`[Reset] Nothing to do. Exiting.`)
    return
  }

  // ── Step 2: Clear fields in batches of 10 ────────────────────────────────

  console.log(`\n[Reset] Step 2 — Clearing fields in batches of ${BATCH_SIZE}...`)

  let cleared  = 0
  let errors   = 0
  const totalBatches = Math.ceil(recordIds.length / BATCH_SIZE)

  for (let i = 0; i < recordIds.length; i += BATCH_SIZE) {
    const batch     = recordIds.slice(i, i + BATCH_SIZE)
    const batchNum  = Math.floor(i / BATCH_SIZE) + 1
    const updates   = batch.map(id => ({ id, fields: CLEAR_PAYLOAD }))

    try {
      await delay(RATE_DELAY)
      await table.update(updates)
      cleared += batch.length
      process.stdout.write(`\r  Batch ${batchNum}/${totalBatches} — cleared ${cleared}/${recordIds.length}`)
    } catch (err) {
      errors += batch.length
      console.error(`\n  ERROR batch ${batchNum}: ${err.message}`)
      // Continue — don't abort the whole run for one bad batch
    }
  }

  // ── Step 3: Final report ──────────────────────────────────────────────────

  const durationS = ((Date.now() - startTime) / 1000).toFixed(1)

  console.log(`\n\n${'═'.repeat(55)}`)
  console.log(`[Reset] COMPLETE`)
  console.log(`  Records matched:  ${recordIds.length}`)
  console.log(`  Records cleared:  ${cleared}`)
  if (errors > 0) {
    console.log(`  Errors:           ${errors}  ← check logs above`)
  }
  console.log(`  Duration:         ${durationS}s`)
  console.log(`${'═'.repeat(55)}\n`)
}

run().catch(err => {
  console.error('[Reset] Fatal error:', err)
  process.exit(1)
})
