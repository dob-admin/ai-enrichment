// test-regression-phase3.mjs
// Spot-check that the Phase 3 parser behavior is unchanged after adding
// generateSiblingCandidates. Samples real cases from Phase 3's 20/20 regression.

import { parseItemNumber } from './src/lib/parser.js'

const cases = [
  // Size_10_5M_UPC_COND — Brooks/Saucony/etc pattern
  {
    input: 'Saucony_S10810_05_Size_10_5M_195018769756_UVG',
    expect: {
      baseItemNumber: 'Saucony_S10810_05_Size_10_5M_195018769756',
      conditionCode: 'UVG',
      isUPC: false,
      brand: 'Saucony',
    },
  },
  // Pure UPC
  {
    input: '622356588317_UVG',
    expect: {
      baseItemNumber: '622356588317',
      conditionCode: 'UVG',
      isUPC: true,
    },
  },
  // Space-separated
  {
    input: 'Merrell J06011W - Size 12_720026451463_UGD',
    expect: {
      baseItemNumber: 'Merrell J06011W - Size 12_720026451463',
      conditionCode: 'UGD',
      isUPC: false,
      brand: 'Merrell',
    },
  },
  // Lowercase suffix — case-insensitive regex in parser
  {
    input: 'Asics_1011B547_401_Size_10_4550456178045_nmb',
    expect: {
      baseItemNumber: 'Asics_1011B547_401_Size_10_4550456178045',
      conditionCode: 'NMB',
      isUPC: false,
      brand: 'Asics',
    },
  },
  // Pure UPC without condition
  {
    input: '810096859945',
    expect: {
      baseItemNumber: '810096859945',
      isUPC: true,
      conditionCode: null,
    },
  },
]

let passed = 0
let failed = 0
for (const c of cases) {
  const got = parseItemNumber(c.input)
  const errs = []
  for (const [k, v] of Object.entries(c.expect)) {
    if (got[k] !== v) errs.push(`${k}: got '${got[k]}' want '${v}'`)
  }
  if (errs.length === 0) {
    passed++
    console.log(`✓ ${c.input}`)
  } else {
    failed++
    console.log(`✗ ${c.input}`)
    for (const e of errs) console.log(`    ${e}`)
    console.log(`    full:`, JSON.stringify(got, null, 2).slice(0, 300))
  }
}

console.log(`\n${passed}/${passed + failed} passed`)
if (failed > 0) process.exit(1)
