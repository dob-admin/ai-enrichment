// src/workers/brandResolution.js
// Worker 1: Resolves missing brand fields by matching item numbers to BQ Brands
//
// Criteria (handled by getMissingBrandRecords):
//   - Total Inventory > 0
//   - Brand is empty
//   - Brand Worker Status is empty
//
// For each record:
//   - Extract brand candidate from item number
//   - Fuzzy-match against BQ Brands (Title + Correct Spelling)
//   - If matched above confidence threshold: write Brand = Correct Spelling,
//     Brand Name Match = Correct Spelling, Brand Worker Status = Found
//   - If no match (or no candidate extractable): write Brand Name Match = candidate (or blank),
//     Brand Worker Status = Not Found
//
// Worker ignores any record where Brand Worker Status is already populated.
// No per-run batch limit — the worker exhausts the queue on each scheduled run.

import 'dotenv/config'
import { fileURLToPath } from 'url'
import { exitIfLocked } from '../lib/lock.js'
import Fuse from 'fuse.js'
import {
  getMissingBrandRecords,
  loadAllBrands,
  writeBrandResult,
} from '../lib/airtable.js'
import { extractBrandCandidate } from '../lib/parser.js'
import { WorkerLogger } from '../lib/logger.js'
import { FIELDS, BRAND_WORKER_STATUS } from '../config/fields.js'

const CONFIDENCE_THRESHOLD = 0.85 // Fuse.js score is 0–1, lower = better match

async function run({ skipLockCheck } = {}) {
  if (!skipLockCheck) await exitIfLocked('Brand Resolution')
  console.log(`[Brand Resolution] Starting run at ${new Date().toISOString()}`)
  const logger = new WorkerLogger('brand')

  // Load all brands into memory for matching
  console.log('[Brand Resolution] Loading BQ Brands...')
  const allBrands = await loadAllBrands()
  console.log(`[Brand Resolution] Loaded ${allBrands.length} brands`)

  // Build fuzzy search index on Title + Correct Spelling
  // Title holds the raw/variant spellings (including UPC entries);
  // Correct Spelling is the canonical value we write to the Brand field.
  const brandIndex = allBrands
    .map(r => ({
      correctSpelling: r.fields[FIELDS.BQ_CORRECT_SPELL],
      title: r.fields[FIELDS.BQ_TITLE],
      key: r.fields[FIELDS.BQ_KEY],
    }))
    .filter(b => b.correctSpelling && b.title)

  const fuse = new Fuse(brandIndex, {
    keys: ['title', 'correctSpelling'],
    threshold: 1 - CONFIDENCE_THRESHOLD, // Fuse uses inverted scale
    includeScore: true,
  })

  // Fetch every record needing brand resolution (no limit)
  const records = await getMissingBrandRecords()
  console.log(`[Brand Resolution] Processing ${records.length} records`)

  let found = 0
  let notFound = 0
  let errors = 0

  for (const record of records) {
    const itemNumber = record.fields[FIELDS.ITEM_NUMBER]
    const candidate = extractBrandCandidate(itemNumber)

    // No candidate extractable — write Not Found with blank match
    if (!candidate) {
      try {
        await writeBrandResult(record.id, {
          status: BRAND_WORKER_STATUS.NOT_FOUND,
          matchValue: '',
        })
        console.log(`  NOT FOUND ${itemNumber} — no candidate extractable`)
        logger.log({ itemNumber, outcome: 'Not Found', reason: 'no candidate' })
        notFound++
      } catch (err) {
        console.error(`  ERROR ${itemNumber}:`, err.message)
        errors++
      }
      continue
    }

    // Fuzzy match against BQ Brands
    const results = fuse.search(candidate)
    const best = results[0]
    const score = best ? 1 - (best.score || 0) : 0

    // No match or confidence too low — write Not Found with the candidate for auditing
    if (!best || score < CONFIDENCE_THRESHOLD) {
      try {
        await writeBrandResult(record.id, {
          status: BRAND_WORKER_STATUS.NOT_FOUND,
          matchValue: candidate,
        })
        const reason = best
          ? `low confidence "${best.item.correctSpelling}" (${(score * 100).toFixed(0)}%)`
          : `no match for "${candidate}"`
        console.log(`  NOT FOUND ${itemNumber} — ${reason}`)
        logger.log({ itemNumber, outcome: 'Not Found', candidate, reason })
        notFound++
      } catch (err) {
        console.error(`  ERROR ${itemNumber}:`, err.message)
        errors++
      }
      continue
    }

    // Match — write Brand + Match + Found
    try {
      await writeBrandResult(record.id, {
        status: BRAND_WORKER_STATUS.FOUND,
        brand: best.item.correctSpelling,
        matchValue: best.item.correctSpelling,
      })
      console.log(`  FOUND ${itemNumber} → "${best.item.correctSpelling}" (${(score * 100).toFixed(0)}%)`)
      logger.log({
        itemNumber,
        outcome: 'Found',
        brand: best.item.correctSpelling,
        candidate,
        score: Math.round(score * 100),
      })
      found++
    } catch (err) {
      console.error(`  ERROR writing brand for ${itemNumber}:`, err.message)
      errors++
    }
  }

  await logger.finish({ found, notFound, errors })
  console.log(`[Brand Resolution] Done — found: ${found}, not found: ${notFound}, errors: ${errors}`)
  return { processed: records.length, found, notFound, errors }
}

export { run }

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  run().catch(err => {
    console.error('[Brand Resolution] Fatal error:', err)
    process.exit(1)
  })
}
