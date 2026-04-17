// src/workers/brandResolution.js
// Worker 1: Resolves missing brand fields by matching item numbers to BQ Brands
//
// Criteria (handled by getMissingBrandRecords):
//   - Total Inventory > 0
//   - Brand is empty
//   - Brand Worker Status is empty
//
// For each record:
//   - Extract ordered list of brand candidates from item number
//   - Fuzzy-match each candidate against BQ Brands (Title + Correct Spelling)
//   - Pick the highest-confidence hit across all candidates
//   - If best hit >= threshold: write Brand = matched Correct Spelling,
//     Brand Name Match = matched Correct Spelling, Brand Worker Status = Found
//   - Otherwise: write Brand Name Match = candidates tried (comma-separated),
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
import { extractBrandCandidates } from '../lib/parser.js'
import { WorkerLogger } from '../lib/logger.js'
import { FIELDS, BRAND_WORKER_STATUS } from '../config/fields.js'

const CONFIDENCE_THRESHOLD = 0.85 // Fuse.js confidence score 0-1, higher = better match

async function run({ skipLockCheck } = {}) {
  if (!skipLockCheck) await exitIfLocked('Brand Resolution')
  console.log(`[Brand Resolution] Starting run at ${new Date().toISOString()}`)
  const logger = new WorkerLogger('brand')

  // Load all brands into memory for matching
  console.log('[Brand Resolution] Loading BQ Brands...')
  const allBrands = await loadAllBrands()
  console.log(`[Brand Resolution] Loaded ${allBrands.length} brands`)

  // Build fuzzy search index on Title + Correct Spelling.
  // Title holds the raw/variant spellings (including UPC entries as-is);
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
    const candidates = extractBrandCandidates(itemNumber)

    // No candidates extractable at all — write Not Found with blank match
    if (!candidates.length) {
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

    // Try every candidate — keep the highest-confidence hit.
    let bestHit = null
    let bestScore = 0
    let bestCandidate = null
    for (const c of candidates) {
      const results = fuse.search(c)
      if (!results.length) continue
      const score = 1 - (results[0].score || 0)
      if (score > bestScore) {
        bestScore = score
        bestHit = results[0].item
        bestCandidate = c
      }
    }

    // No match above threshold — write Not Found with candidates tried for auditing
    if (!bestHit || bestScore < CONFIDENCE_THRESHOLD) {
      try {
        const matchAudit = candidates.join(' | ')
        await writeBrandResult(record.id, {
          status: BRAND_WORKER_STATUS.NOT_FOUND,
          matchValue: matchAudit,
        })
        const reason = bestHit
          ? `best "${bestHit.correctSpelling}" (${(bestScore * 100).toFixed(0)}%) below threshold`
          : `no Fuse hits across ${candidates.length} candidate(s)`
        console.log(`  NOT FOUND ${itemNumber} — ${reason}`)
        logger.log({ itemNumber, outcome: 'Not Found', candidates: matchAudit, reason })
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
        brand: bestHit.correctSpelling,
        matchValue: bestHit.correctSpelling,
      })
      console.log(`  FOUND ${itemNumber} → "${bestHit.correctSpelling}" (${(bestScore * 100).toFixed(0)}% via "${bestCandidate}")`)
      logger.log({
        itemNumber,
        outcome: 'Found',
        brand: bestHit.correctSpelling,
        candidate: bestCandidate,
        score: Math.round(bestScore * 100),
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
