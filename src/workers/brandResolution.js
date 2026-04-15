// src/workers/brandResolution.js
// Worker 1: Resolves missing brand fields by matching item numbers to BQ Brands
import 'dotenv/config'
import { fileURLToPath } from 'url'
import Fuse from 'fuse.js'
import {
  getMissingBrandRecords,
  loadAllBrands,
  writeBrand,
} from '../lib/airtable.js'
import { extractBrandCandidate } from '../lib/parser.js'
import { WorkerLogger } from '../lib/logger.js'
import { FIELDS } from '../config/fields.js'

const BATCH_SIZE = parseInt(process.env.BRAND_BATCH_SIZE || '50')
const CONFIDENCE_THRESHOLD = 0.85 // Fuse.js score is 0–1, lower = better match

async function run() {
  console.log(`[Brand Resolution] Starting run at ${new Date().toISOString()}`)
  const logger = new WorkerLogger('brand')

  // Load all brands into memory for matching
  console.log('[Brand Resolution] Loading BQ Brands...')
  const allBrands = await loadAllBrands()
  console.log(`[Brand Resolution] Loaded ${allBrands.length} brands`)

  // Build fuzzy search index on Brand Correct Spelling values
  const brandIndex = allBrands
    .map(r => ({
      correctSpelling: r.fields[FIELDS.BQ_CORRECT_SPELL],
      title: r.fields[FIELDS.BQ_TITLE],
      key: r.fields[FIELDS.BQ_KEY],
    }))
    .filter(b => b.correctSpelling)

  const fuse = new Fuse(brandIndex, {
    keys: ['correctSpelling', 'title'],
    threshold: 1 - CONFIDENCE_THRESHOLD, // Fuse uses inverted scale
    includeScore: true,
  })

  // Fetch records needing brand resolution
  const records = await getMissingBrandRecords(BATCH_SIZE)
  console.log(`[Brand Resolution] Processing ${records.length} records`)

  let matched = 0
  let skipped = 0

  for (const record of records) {
    const itemNumber = record.fields[FIELDS.ITEM_NUMBER]
    const candidate = extractBrandCandidate(itemNumber)

    if (!candidate) {
      console.log(`  SKIP ${itemNumber} — no brand candidate extractable`)
      skipped++
      continue
    }

    // Fuzzy match against BQ Brands
    const results = fuse.search(candidate)

    if (!results.length) {
      console.log(`  SKIP ${itemNumber} — no match for "${candidate}"`)
      skipped++
      continue
    }

    const best = results[0]
    const score = 1 - (best.score || 0) // Convert back to 0–1 confidence

    if (score < CONFIDENCE_THRESHOLD) {
      console.log(`  SKIP ${itemNumber} — low confidence match "${best.item.correctSpelling}" (${(score * 100).toFixed(0)}%)`)
      skipped++
      continue
    }

    // Write brand to Airtable — automation handles Website + Brand Correct Spelling
    try {
      await writeBrand(record.id, best.item.title)
      console.log(`  MATCH ${itemNumber} → "${best.item.correctSpelling}" (${(score * 100).toFixed(0)}%)`)
      logger.log({ itemNumber, outcome: 'Fixed', brand: best.item.correctSpelling })
      matched++
    } catch (err) {
      console.error(`  ERROR writing brand for ${itemNumber}:`, err.message)
      skipped++
    }
  }

  await logger.finish({ matched, skipped })
  console.log(`[Brand Resolution] Done — matched: ${matched}, skipped: ${skipped}`)
  return { processed: matched + skipped, resolved: matched }
}

export { run }

run().catch(err => {
  console.error('[Brand Resolution] Fatal error:', err)
  process.exit(1)
})
