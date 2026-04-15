// src/workers/statusReport.js
// Worker 3: Hourly status summary
import 'dotenv/config'
import { fileURLToPath } from 'url'
import { exitIfLocked } from '../lib/lock.js'
import { getStatusCounts } from '../lib/airtable.js'
import { WorkerLogger } from '../lib/logger.js'

async function run() {
  const startTime = Date.now()
  const logger = new WorkerLogger('report')
  const timestamp = new Date().toISOString()
  const counts = await getStatusCounts()
  const total = counts.complete + counts.partial + counts.notFound + counts.pending
  const processed = counts.complete + counts.partial + counts.notFound

  // Also get cost stats
  const costStats = await getCostStats()

  const report = `
╔══════════════════════════════════════╗
║   DOB AI Enrichment — Status Report  ║
║   ${timestamp.slice(0, 19).replace('T', ' ')}         ║
╠══════════════════════════════════════╣
║  Total in-inventory records: ${String(total).padStart(6)} ║
║  Processed:                  ${String(processed).padStart(6)} ║
║    ✓ Complete:               ${String(counts.complete).padStart(6)} ║
║    ◑ Partial:                ${String(counts.partial).padStart(6)} ║
║    ✗ Not Found:              ${String(counts.notFound).padStart(6)} ║
║  Pending (unprocessed):      ${String(counts.pending).padStart(6)} ║
╠══════════════════════════════════════╣
║  Cost Check Status:                  ║
║    ✓ Good:                   ${String(costStats.good).padStart(6)} ║
║    ✓ Found:                  ${String(costStats.found).padStart(6)} ║
║    ✗ Missing:                ${String(costStats.missing).padStart(6)} ║
║    ⏳ Unchecked:              ${String(costStats.unchecked).padStart(6)} ║
╚══════════════════════════════════════╝`

  console.log(report)

  // Log run summary to AI Logs
  const breakdown = `complete: ${counts.complete}, partial: ${counts.partial}, notFound: ${counts.notFound}, pending: ${counts.pending}, cost_good: ${costStats.good}, cost_found: ${costStats.found}, cost_missing: ${costStats.missing}, cost_unchecked: ${costStats.unchecked}`
  await logger.finish({
    complete: counts.complete,
    partial: counts.partial,
    notFound: counts.notFound,
    pending: counts.pending,
    cost_good: costStats.good,
    cost_found: costStats.found,
    cost_missing: costStats.missing,
  })
}

async function getCostStats() {
  try {
    const Airtable = (await import('airtable')).default
    const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
      .base(process.env.AIRTABLE_BASE_ID)
    const table = base(process.env.AIRTABLE_TABLE_ID)

    const stats = { good: 0, found: 0, missing: 0, unchecked: 0 }
    await table.select({
      returnFieldsByFieldId: true,
      filterByFormula: `AND(
        {fld6pOENdKtV98qZu} > 0,
        NOT({fldriFW7lmzK6sfBk} = 'ignore'),
        NOT({fldriFW7lmzK6sfBk} = ''),
        {fldXciVH3n8EMjxxu} = BLANK()
      )`,
      fields: ['fldjJAFcnpntaN7l4'],
    }).eachPage((page, next) => {
      for (const r of page) {
        const val = r.fields['fldjJAFcnpntaN7l4']?.name
        if (val === 'Good') stats.good++
        else if (val === 'Found') stats.found++
        else if (val === 'Missing') stats.missing++
        else stats.unchecked++
      }
      next()
    })
    return stats
  } catch {
    return { good: 0, found: 0, missing: 0, unchecked: 0 }
  }
}

export { run }

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  run().catch(err => {
    console.error('[Status Report] Error:', err)
    process.exit(1)
  })
}
