// src/lib/logger.js
// Logs worker activity to the AI Logs table in Airtable
// Every record processed gets a log entry, plus a run summary at the end
import Airtable from 'airtable'

const LOGS_TABLE_ID = 'tblOLMPJyFRGatA59'

const LOG_FIELDS = {
  RUN_ID:             'fld2QbXZKckME0Rnq',
  TIMESTAMP:          'fldx155EEdoWPoZnn',
  WORKER:             'fldSZQsmhnby9juRf',
  ITEM_NUMBER:        'fldOB5Ek28lX6zwfU',
  WEBSITE:            'fldbX9qGwHFX4iztf',
  BRAND:              'fldcApm4XjBysFSUe',
  OUTCOME:            'fldGTLTjenjFwITFl',
  SOURCES_USED:       'fldtgHMBiCqoOuIDR',
  FIELDS_WRITTEN:     'fldis2SCTX6I6Y2EH',
  MISSING_FIELDS:     'fldlMRjKeu66kI1bN',
  VALIDATION_WARNINGS:'fldC5GDvFccwRYDuE',
  ERROR_MESSAGE:      'fldJOvVvx3LkgUNnw',
  COST_FOUND:         'fldNe2HWW6Oqq61qS',
  COST_SOURCE:        'fldSL2W6aHIlZq0Me',
  PRICE_WRITTEN:      'fldx6gg5VeXWVyyvC',
  MODEL_CANDIDATES:   'fldlkjGPUcWaLipQn',
  IS_RUN_SUMMARY:     'fld9EK0pJae06zVcs',
  RUN_DURATION_S:     'fldIQMu2ctq9HILqp',
  RUN_TOTAL_PROCESSED:'fldV3dVJWzJtyWvxP',
  RUN_BREAKDOWN:      'fldRH0SNFbpzecbxG',
}

export class WorkerLogger {
  constructor(workerName) {
    this.workerName = workerName
    this.runId = `${workerName}-${new Date().toISOString()}`
    this.startTime = Date.now()
    this.queue = []
    this.base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
      .base(process.env.AIRTABLE_BASE_ID)
  }

  // Log a single record outcome
  log(entry) {
    this.queue.push({
      runId: this.runId,
      worker: this.workerName,
      timestamp: new Date().toISOString(),
      ...entry,
    })
    // Flush in batches of 10
    if (this.queue.length >= 10) {
      this._flush().catch(e => console.error('[Logger] Flush error:', e.message))
    }
  }

  // Write run summary and flush remaining queue
  async finish(stats) {
    // Flush remaining records
    await this._flush()

    // Write run summary
    const duration = (Date.now() - this.startTime) / 1000
    const breakdown = Object.entries(stats)
      .map(([k, v]) => `${k}: ${v}`)
      .join(', ')

    try {
      await this.base(LOGS_TABLE_ID).create([{
        fields: {
          [LOG_FIELDS.RUN_ID]:             this.runId,
          [LOG_FIELDS.TIMESTAMP]:          new Date().toISOString(),
          [LOG_FIELDS.WORKER]:             this.workerName,
          [LOG_FIELDS.IS_RUN_SUMMARY]:     true,
          [LOG_FIELDS.RUN_DURATION_S]:     parseFloat(duration.toFixed(1)),
          [LOG_FIELDS.RUN_TOTAL_PROCESSED]:Object.values(stats).reduce((a, b) => a + b, 0),
          [LOG_FIELDS.RUN_BREAKDOWN]:      breakdown,
        }
      }])
    } catch (e) {
      console.error('[Logger] Summary write error:', e.message)
    }
  }

  async _flush() {
    if (!this.queue.length) return
    const batch = this.queue.splice(0, 10)
    try {
      await this.base(LOGS_TABLE_ID).create(
        batch.map(e => ({ fields: this._toFields(e) }))
      )
    } catch (err) {
      console.error('[Logger] Write error:', err.message)
    }
  }

  _toFields(e) {
    const fields = {}
    if (e.runId)               fields[LOG_FIELDS.RUN_ID]              = e.runId
    if (e.timestamp)           fields[LOG_FIELDS.TIMESTAMP]           = e.timestamp
    if (e.worker)              fields[LOG_FIELDS.WORKER]              = e.worker
    if (e.itemNumber)          fields[LOG_FIELDS.ITEM_NUMBER]         = e.itemNumber
    if (e.website)             fields[LOG_FIELDS.WEBSITE]             = e.website
    if (e.brand)               fields[LOG_FIELDS.BRAND]               = String(e.brand).slice(0, 255)
    if (e.outcome)             fields[LOG_FIELDS.OUTCOME]             = e.outcome
    if (e.sourcesUsed)         fields[LOG_FIELDS.SOURCES_USED]        = Array.isArray(e.sourcesUsed) ? e.sourcesUsed.join('\n') : e.sourcesUsed
    if (e.fieldsWritten)       fields[LOG_FIELDS.FIELDS_WRITTEN]      = Array.isArray(e.fieldsWritten) ? e.fieldsWritten.join('\n') : e.fieldsWritten
    if (e.missingFields)       fields[LOG_FIELDS.MISSING_FIELDS]      = Array.isArray(e.missingFields) ? e.missingFields.join('\n') : e.missingFields
    if (e.validationWarnings)  fields[LOG_FIELDS.VALIDATION_WARNINGS] = Array.isArray(e.validationWarnings) ? e.validationWarnings.join('\n') : e.validationWarnings
    if (e.errorMessage)        fields[LOG_FIELDS.ERROR_MESSAGE]       = String(e.errorMessage).slice(0, 5000)
    if (e.costFound != null)   fields[LOG_FIELDS.COST_FOUND]          = e.costFound
    if (e.costSource)          fields[LOG_FIELDS.COST_SOURCE]         = e.costSource
    if (e.priceWritten != null)fields[LOG_FIELDS.PRICE_WRITTEN]       = e.priceWritten
    if (e.modelCandidates)     fields[LOG_FIELDS.MODEL_CANDIDATES]    = Array.isArray(e.modelCandidates) ? e.modelCandidates.join(', ') : e.modelCandidates
    return fields
  }
}
