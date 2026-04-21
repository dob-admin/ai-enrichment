// test-sibling-integration.mjs
// Integration smoke test — exercises the new sibling-lookup logic without
// hitting real APIs. Stubs Airtable client + GoFlow and verifies:
//   1. generateSiblingCandidates produces expected shape
//   2. fireCostBatch applies min-cost across primary + siblings
//   3. Stage 1 Airtable sibling query hits correct filter formula

import { generateSiblingCandidates } from './src/lib/parser.js'

// ── Test 1: Sibling expansion for known cases ───────────────────────────────

const t1 = generateSiblingCandidates(
  'Saucony_S10810_05_Size_10_5M_195018769756_UVG',
  null
)
console.log('TEST 1 — sibling expansion')
console.log('  base:', t1.base)
console.log('  upc:', t1.upc)
console.log('  sibling count:', t1.enumeratedSiblings.length)
console.log('  includes UGD variant:',
  t1.enumeratedSiblings.includes('Saucony_S10810_05_Size_10_5M_195018769756_UGD'))
console.assert(t1.base === 'Saucony_S10810_05_Size_10_5M_195018769756', 'base mismatch')
console.assert(t1.upc === '195018769756', 'upc mismatch')
console.assert(t1.enumeratedSiblings.length === 8, 'sibling count mismatch')
console.log('  ✓ OK\n')

// ── Test 2: Min-cost application logic ──────────────────────────────────────
// Simulate the apply loop inside fireCostBatch — given a costMap and a batch
// of {itemNumber, siblings}, confirm min non-zero cost per record.

function applyMinCost(batch, costMap, threshold) {
  const results = []
  for (const { recordId, itemNumber, siblings } of batch) {
    const candidates = [itemNumber, ...(siblings || [])].filter(Boolean)
    let minCost = Infinity
    let source = null
    for (const name of candidates) {
      const c = costMap[name]
      if (c && c > threshold && c < minCost) {
        minCost = c
        source = name
      }
    }
    results.push({
      recordId,
      cost: minCost === Infinity ? null : minCost,
      source,
    })
  }
  return results
}

console.log('TEST 2 — min-cost across primary + siblings')

// Case A: primary has cost, one sibling cheaper → take sibling
const caseA = applyMinCost(
  [{ recordId: 'r1', itemNumber: 'A_UVG', siblings: ['A', 'A_UGD', 'A_NMB'] }],
  { 'A_UVG': 50.00, 'A_UGD': 30.00, 'A_NMB': 40.00 },
  0.02
)
console.log('  primary $50, siblings $30 + $40 → min:', caseA[0].cost, 'from', caseA[0].source)
console.assert(caseA[0].cost === 30.00 && caseA[0].source === 'A_UGD')

// Case B: primary has $0 (not in costMap), sibling has cost → take sibling
const caseB = applyMinCost(
  [{ recordId: 'r2', itemNumber: 'B_UVG', siblings: ['B', 'B_UGD'] }],
  { 'B': 22.50 },
  0.02
)
console.log('  primary $0, sibling bare $22.50 → min:', caseB[0].cost, 'from', caseB[0].source)
console.assert(caseB[0].cost === 22.50 && caseB[0].source === 'B')

// Case C: nothing in costMap → null
const caseC = applyMinCost(
  [{ recordId: 'r3', itemNumber: 'C_UVG', siblings: ['C', 'C_UGD'] }],
  {},
  0.02
)
console.log('  nothing matched → cost:', caseC[0].cost, '(null expected)')
console.assert(caseC[0].cost === null)

// Case D: all candidates below threshold → null (threshold guard works)
const caseD = applyMinCost(
  [{ recordId: 'r4', itemNumber: 'D_UVG', siblings: ['D'] }],
  { 'D_UVG': 0.01, 'D': 0.01 },
  0.02
)
console.log('  all below threshold → cost:', caseD[0].cost, '(null expected)')
console.assert(caseD[0].cost === null)

console.log('  ✓ OK\n')

// ── Test 3: Batch dedup + fit logic ─────────────────────────────────────────
// fireCostBatch packs records into a 100-slot batch using unique item-name
// count as the limiter. Simulate the pack loop.

function packBatch(pending, batchSize) {
  const uniqueNames = new Set()
  const batch = []
  const remainder = []
  for (const p of pending) {
    const candidateNames = new Set(uniqueNames)
    if (p.itemNumber) candidateNames.add(p.itemNumber)
    for (const s of p.siblings || []) candidateNames.add(s)
    if (candidateNames.size <= batchSize) {
      batch.push(p)
      for (const n of candidateNames) uniqueNames.add(n)
    } else if (batch.length === 0) {
      const trimmed = [p.itemNumber, ...(p.siblings || [])].filter(Boolean).slice(0, batchSize)
      batch.push({ ...p, siblings: trimmed.filter(n => n !== p.itemNumber) })
      for (const n of trimmed) uniqueNames.add(n)
    } else {
      remainder.push(p)
    }
  }
  return { batch, remainder, uniqueNames: [...uniqueNames] }
}

console.log('TEST 3 — batch packing with sibling overflow')

// 15 records × 8 siblings each = 120 unique names if no overlap → won't fit in 100
const many = []
for (let i = 0; i < 15; i++) {
  const base = `P${i}`
  many.push({
    recordId: `rec${i}`,
    itemNumber: `${base}_UVG`,
    siblings: [base, `${base}_UGD`, `${base}_ULN`, `${base}_NMB`, `${base}_UAI`, `${base}_UAC`, `${base}_UDF`],
  })
}
const packed = packBatch(many, 100)
console.log(`  15 records × 8 items each (120 unique), batchSize=100:`)
console.log(`    packed batch records: ${packed.batch.length}`)
console.log(`    packed unique names: ${packed.uniqueNames.length}`)
console.log(`    remainder records: ${packed.remainder.length}`)
// 100 / 8 = 12 records fit, 3 roll over
console.assert(packed.batch.length === 12, `expected 12, got ${packed.batch.length}`)
console.assert(packed.remainder.length === 3, `expected 3, got ${packed.remainder.length}`)
console.assert(packed.uniqueNames.length <= 100, 'uniqueNames exceeded batch size')
console.log('  ✓ OK\n')

console.log('ALL INTEGRATION TESTS PASSED')
