// src/workers/statusReport.js
// Worker 3: Hourly status summary
import 'dotenv/config'
import { getStatusCounts } from '../lib/airtable.js'

async function run() {
  const timestamp = new Date().toISOString()
  const counts = await getStatusCounts()
  const total = counts.complete + counts.partial + counts.notFound + counts.pending
  const processed = counts.complete + counts.partial + counts.notFound

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
╚══════════════════════════════════════╝`

  console.log(report)
}

run().catch(err => {
  console.error('[Status Report] Error:', err)
  process.exit(1)
})
