// src/workers/backlog.js
// One-shot backlog processor — runs all phases sequentially until exhausted
// Triggered manually via Railway deploy (no cron). Exits when done.
// Phase order: Cost → Brand → Validate → Enrich → Report
// Progress detection prevents infinite loops on stuck records

import 'dotenv/config'
import { WorkerLogger } from '../lib/logger.js'
import { acquireLock, releaseLock } from '../lib/lock.js'

// ─── Phase runners ────────────────────────────────────────────────────────────
// Each phase runs one batch and returns { processed, resolved }
// processed = records touched, resolved = records actually advanced (not stuck)

async function runCostPhase(logger) {
  const { run } = await import('./costLookup.js')
  return run({ batchSize: 99999 })
}

async function runBrandPhase(logger) {
  const { run } = await import('./brandResolution.js')
  return run({ batchSize: 99999 })
}

async function runValidatePhase(logger) {
  const { run } = await import('./validate.js')
  return run({ batchSize: 99999 })
}

async function runEnrichPhase(logger) {
  const { run } = await import('./enrichment.js')
  return run({ batchSize: 99999 })
}

async function runReportPhase() {
  const { run } = await import('./statusReport.js')
  return run()
}

// ─── Phase loop ───────────────────────────────────────────────────────────────
// Runs a phase repeatedly until the queue is empty or no progress is made
async function exhaustPhase(phaseName, runFn, logger) {
  console.log(`\n${'═'.repeat(50)}`)
  console.log(`[Backlog] Starting phase: ${phaseName.toUpperCase()}`)
  console.log(`${'═'.repeat(50)}`)

  let totalProcessed = 0
  let totalResolved = 0
  let round = 0

  while (true) {
    round++
    console.log(`\n[Backlog] ${phaseName} — round ${round}`)

    const result = await runFn(logger)

    const processed = result?.processed ?? result?.total ?? 0
    const resolved = result?.resolved ?? result?.good ?? result?.found ?? result?.fixed ?? result?.complete ?? 0
    const skipped = processed - resolved

    totalProcessed += processed
    totalResolved += resolved

    console.log(`[Backlog] ${phaseName} round ${round} — processed: ${processed}, resolved: ${resolved}, skipped: ${skipped}`)

    // Exit conditions:
    // 1. Queue empty — nothing was processed
    if (processed === 0) {
      console.log(`[Backlog] ${phaseName} — queue empty, moving on`)
      break
    }

    // 2. No progress — everything processed but nothing actually resolved
    // This catches stuck records (Missing costs, Flagged validates, unresolvable brands)
    if (processed > 0 && resolved === 0) {
      console.log(`[Backlog] ${phaseName} — no progress (${processed} processed, 0 resolved), moving on`)
      break
    }
  }

  console.log(`[Backlog] ${phaseName} complete — total processed: ${totalProcessed}, total resolved: ${totalResolved}`)
  logger.log({
    outcome: `${phaseName} phase complete`,
    fieldsWritten: [],
    missingFields: [],
    validationWarnings: [`rounds: ${round}, processed: ${totalProcessed}, resolved: ${totalResolved}`],
  })

  return { totalProcessed, totalResolved }
}

// ─── Main ─────────────────────────────────────────────────────────────────────
async function run() {
  const startTime = Date.now()
  const logger = new WorkerLogger('backlog')

  console.log(`\n${'█'.repeat(50)}`)
  console.log(`[Backlog] Starting full backlog run at ${new Date().toISOString()}`)
  console.log(`${'█'.repeat(50)}`)

  await acquireLock()

  const summary = {}

  try {
    // Phase 1: Cost
    summary.cost = await exhaustPhase('cost', runCostPhase, logger)

    // Phase 2: Brand
    summary.brand = await exhaustPhase('brand', runBrandPhase, logger)

    // Phase 3: Validate
    summary.validate = await exhaustPhase('validate', runValidatePhase, logger)

    // Phase 4: Enrich
    summary.enrich = await exhaustPhase('enrich', runEnrichPhase, logger)

    // Phase 5: Report — always runs once as final snapshot
    console.log(`\n${'═'.repeat(50)}`)
    console.log(`[Backlog] Running final status report`)
    console.log(`${'═'.repeat(50)}`)
    await runReportPhase()

  } catch (err) {
    console.error(`[Backlog] Fatal error:`, err.message)
    logger.log({ outcome: 'Error', errorMessage: err.message })
  } finally {
    await releaseLock()
  }

  const durationS = ((Date.now() - startTime) / 1000).toFixed(0)
  const breakdown = Object.entries(summary)
    .map(([phase, s]) => `${phase}: ${s?.totalResolved ?? 0} resolved`)
    .join(', ')

  console.log(`\n${'█'.repeat(50)}`)
  console.log(`[Backlog] Complete in ${durationS}s — ${breakdown}`)
  console.log(`${'█'.repeat(50)}`)

  await logger.finish({
    complete: summary.enrich?.totalResolved ?? 0,
    duration: parseInt(durationS),
  })
}

run().catch(err => {
  console.error('[Backlog] Fatal:', err)
  process.exit(1)
})
