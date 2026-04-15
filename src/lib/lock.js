// src/lib/lock.js
// Pipeline semaphore — prevents standard workers from running while backlog is active
// Table: Pipeline Config (tbl3WZ1zp2Xa6soU8)
// Record: PIPELINE_LOCK (recDjcDmFZTkE3XIe)
// Value field: fldJQJw8mpvfWQzba

import Airtable from 'airtable'

const CONFIG_TABLE_ID = 'tbl3WZ1zp2Xa6soU8'
const LOCK_RECORD_ID = 'recDjcDmFZTkE3XIe'
const VALUE_FIELD_ID = 'fldJQJw8mpvfWQzba'
const UPDATED_AT_FIELD_ID = 'fld7UzMoqjeNO9ftH'

function getBase() {
  return new Airtable({ apiKey: process.env.AIRTABLE_API_KEY })
    .base(process.env.AIRTABLE_BASE_ID)
}

// Returns true if pipeline is locked
export async function isLocked() {
  try {
    const record = await getBase()(CONFIG_TABLE_ID).find(LOCK_RECORD_ID)
    return record.fields[VALUE_FIELD_ID] === 'locked'
  } catch (err) {
    // If we can't read the lock, assume unlocked so workers don't get stuck
    console.warn('[lock] Could not read pipeline lock, assuming unlocked:', err.message)
    return false
  }
}

// Acquire lock — called by backlog worker at start
export async function acquireLock() {
  await getBase()(CONFIG_TABLE_ID).update(LOCK_RECORD_ID, {
    [VALUE_FIELD_ID]: 'locked',
    [UPDATED_AT_FIELD_ID]: new Date().toISOString(),
  })
  console.log('[lock] Pipeline locked — standard workers will pause')
}

// Release lock — called by backlog worker at end
export async function releaseLock() {
  await getBase()(CONFIG_TABLE_ID).update(LOCK_RECORD_ID, {
    [VALUE_FIELD_ID]: 'unlocked',
    [UPDATED_AT_FIELD_ID]: new Date().toISOString(),
  })
  console.log('[lock] Pipeline unlocked — standard workers will resume')
}

// Check lock and exit if locked — called at top of each standard worker
export async function exitIfLocked(workerName) {
  const locked = await isLocked()
  if (locked) {
    console.log(`[${workerName}] Pipeline is locked (backlog running) — exiting`)
    process.exit(0)
  }
}
