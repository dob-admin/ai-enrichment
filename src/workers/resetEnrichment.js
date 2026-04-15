// src/workers/resetEnrichment.js
// ONE-SHOT: Clears all AI enrichment fields on records where AI Status is set,
// EXCEPT records where PD Ready is checked OR both valid columns say YES.

import 'dotenv/config'
import Airtable from 'airtable'

const AI_STATUS_FIELD_ID        = 'fldsJ3tp3XPmd82NR'
const AI_MISSING_FIELD_ID       = 'fldmgcC2eAxKRyUKt'
const TITLE_FIELD_ID            = 'fldhbkyCE3ZK3huqf'
const DESCRIPTION_FIELD_ID      = 'fld8s6bi94sxJiqZE'
const SEO_DESC_FIELD_ID         = 'fldtmIK6WVA93On8C'
const SHOPIFY_CAT_FIELD_ID      = 'fldE4yEqLZKe5UgPZ'
const GOOGLE_CAT_FIELD_ID       = 'fldPGP1Xxrf75nC6U'
const MATERIAL_FIELD_ID         = 'fldojvxXhW2UUH0My'
const OPTION_1_VALUE_FIELD_ID   = 'fld3Wla73i6UIMOfF'
const SDO_COLOR_FIELD_ID        = 'fld5FU5pikJMxFCag'
const PRODUCT_IMAGES_FIELD_ID   = 'fldfOrq8jm703glZC'
const VARIANT_IMG_IDX_FIELD_ID  = 'fldYLJxgnXASagxsN'
const PD_READY_HOLD_FIELD_ID    = 'fldhMJKOKLtxOnlmi'
const PD_READY_FIELD_ID         = 'fldeNIKuNPpDg12AW'
const PRODUCT_INFO_VALID_ID     = 'fld5ngh2MlP8N6dpt'
const VARIANT_INFO_VALID_ID     = 'fldrcRaE8mI1iNFkg'

const CLEAR_PAYLOAD = {
  [AI_STATUS_FIELD_ID]:        null,
  [AI_MISSING_FIELD_ID]:       null,
  [TITLE_FIELD_ID]:            null,
  [DESCRIPTION_FIELD_ID]:      null,
  [SEO_DESC_FIELD_ID]:         null,
  [SHOPIFY_CAT_FIELD_ID]:      null,
  [GOOGLE_CAT_FIELD_ID]:       null,
  [MATERIAL_FIELD_ID]:         [],
  [OPTION_1_VALUE_FIELD_ID]:   null,
  [SDO_COLOR_FIELD_ID]:        null,
  [PRODUCT_IMAGES_FIELD_ID]:   [],
  [VARIANT_IMG_IDX_FIELD_ID]:  null,
  [PD_READY_HOLD_FIELD_ID]:    false,
}

const RATE_DELAY = parseInt(process.env.AIRTABLE_RATE_DELAY_MS || '250')
const BATCH_SIZE = 10
const delay = ms => new Promise(r => setTimeout(r, ms))

async function run() {
  const startTime = Date.now()

  if (!process.env.AIRTABLE_API_KEY) throw new Error('Missing AIRTABLE_API_KEY')
  if (!process.env.AIRTABLE_BASE_ID) throw new Error('Missing AIRTABLE_BASE_ID')
  if (!process.env.AIRTABLE_TABLE_ID) throw new Error('Missing AIRTABLE_TABLE_ID')

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID)
  const table = base(process.env.AIRTABLE_TABLE_ID)

  console.log(`\n${'═'.repeat(55)}`)
  console.log(`[Reset] Starting enrichment field reset`)
  console.log(`[Reset] Skipping: PD Ready = true OR both valid = YES`)
  console.log(`[Reset] ${new Date().toISOString()}`)
  console.log(`${'═'.repeat(55)}`)

  console.log(`\n[Reset] Fetching records where AI Status is set...`)

  const recordIds = []
  let skipped = 0

  await table.select({
    returnFieldsByFieldId: true,
    filterByFormula: `NOT({${AI_STATUS_FIELD_ID}} = BLANK())`,
    fields: [
      AI_STATUS_FIELD_ID,
      PD_READY_FIELD_ID,
      PRODUCT_INFO_VALID_ID,
      VARIANT_INFO_VALID_ID,
    ],
  }).eachPage((page, next) => {
    for (const r of page) {
      const f = r.fields
      const pdReady = f[PD_READY_FIELD_ID]
      const productValid = f[PRODUCT_INFO_VALID_ID]
      const variantValid = f[VARIANT_INFO_VALID_ID]

      // Skip if PD Ready is checked
      if (pdReady) { skipped++; continue }

      // Skip if both valid columns say YES
      if (productValid === 'YES' && variantValid === 'YES') { skipped++; continue }

      recordIds.push(r.id)
    }
    process.stdout.write(`\r  Fetched ${recordIds.length + skipped} records (${skipped} protected)...`)
    next()
  })

  console.log(`\n[Reset] Found ${recordIds.length} records to clear, ${skipped} protected (skipped)`)

  if (recordIds.length === 0) {
    console.log(`[Reset] Nothing to clear. Exiting.`)
    return
  }

  console.log(`\n[Reset] Clearing fields in batches of ${BATCH_SIZE}...`)

  let cleared = 0
  let errors = 0
  const totalBatches = Math.ceil(recordIds.length / BATCH_SIZE)

  for (let i = 0; i < recordIds.length; i += BATCH_SIZE) {
    const batch = recordIds.slice(i, i + BATCH_SIZE)
    const batchNum = Math.floor(i / BATCH_SIZE) + 1
    const updates = batch.map(id => ({ id, fields: CLEAR_PAYLOAD }))

    try {
      await delay(RATE_DELAY)
      await table.update(updates)
      cleared += batch.length
      process.stdout.write(`\r  Batch ${batchNum}/${totalBatches} — cleared ${cleared}/${recordIds.length}`)
    } catch (err) {
      errors += batch.length
      console.error(`\n  ERROR batch ${batchNum}: ${err.message}`)
    }
  }

  const durationS = ((Date.now() - startTime) / 1000).toFixed(1)

  console.log(`\n\n${'═'.repeat(55)}`)
  console.log(`[Reset] COMPLETE`)
  console.log(`  Records protected:  ${skipped}`)
  console.log(`  Records cleared:    ${cleared}`)
  if (errors > 0) console.log(`  Errors:             ${errors}`)
  console.log(`  Duration:           ${durationS}s`)
  console.log(`${'═'.repeat(55)}\n`)
}

run().catch(err => {
  console.error('[Reset] Fatal error:', err)
  process.exit(1)
})
