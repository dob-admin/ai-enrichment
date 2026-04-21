// test-sibling-candidates.mjs
// Eval-slice test for parser.js generateSiblingCandidates.
// Real failing item numbers from the 785-record "cost not found" set.

import { generateSiblingCandidates } from './src/lib/parser.js'

const cases = [
  // Standard Rebound pattern: brand_model_size_upc_COND
  {
    itemNumber: 'Saucony_S10810_05_Size_10_5M_195018769756_UVG',
    upcCode: null,
    expect: {
      base: 'Saucony_S10810_05_Size_10_5M_195018769756',
      upc:  '195018769756',
      siblingCount: 8,  // base + 7 condition codes
      siblingIncludes: [
        'Saucony_S10810_05_Size_10_5M_195018769756',
        'Saucony_S10810_05_Size_10_5M_195018769756_UVG',
        'Saucony_S10810_05_Size_10_5M_195018769756_UGD',
        'Saucony_S10810_05_Size_10_5M_195018769756_NMB',
      ],
    },
  },
  // Brand_UPC_COND (short form)
  {
    itemNumber: 'Crocs_196265108800_UGD',
    upcCode: null,
    expect: {
      base: 'Crocs_196265108800',
      upc:  '196265108800',
      siblingCount: 8,
      siblingIncludes: ['Crocs_196265108800', 'Crocs_196265108800_UVG', 'Crocs_196265108800_ULN'],
    },
  },
  // Pure UPC + condition
  {
    itemNumber: '622356588317_UVG',
    upcCode: null,
    expect: {
      base: '622356588317',
      upc:  '622356588317',
      siblingCount: 8,
      siblingIncludes: ['622356588317', '622356588317_NMB'],
    },
  },
  // Space-separated (Camelbak style)
  {
    itemNumber: 'Camelbak 1478601000 Repack 4 50oz_886798034942_UGD',
    upcCode: null,
    expect: {
      base: 'Camelbak 1478601000 Repack 4 50oz_886798034942',
      upc:  '886798034942',
      siblingCount: 8,
    },
  },
  // Lowercase condition suffix (21 records affected)
  {
    itemNumber: 'Asics_1011B547_401_Size_10_4550456178045_nmb',
    upcCode: null,
    expect: {
      base: 'Asics_1011B547_401_Size_10_4550456178045',  // parser.js strips case-insensitively
      upc:  '4550456178045',  // 13-digit UPC
      siblingCount: 8,
    },
  },
  // UPC passed via upcCode arg (with condition suffix — should be cleaned)
  {
    itemNumber: 'Reef_196985461254_UVG',
    upcCode: '196985461254_UVG',
    expect: {
      base: 'Reef_196985461254',
      upc:  '196985461254',
      siblingCount: 8,
    },
  },
  // Free-text / malformed — no condition, no UPC
  {
    itemNumber: 'Round cut, Scattered design, White, Gold-tone plated',
    upcCode: null,
    expect: {
      base: 'Round cut, Scattered design, White, Gold-tone plated',
      upc:  null,
      siblingCount: 8,  // base + suffixes, but none useful
    },
  },
  // Malformed SDO — has ASIN-like identifier in upcCode. cleanUPC passes it
  // through as-is (no 12-14-digit validation); matches existing loop.js
  // behavior. Query consumer will simply not find matches.
  {
    itemNumber: 'Bruno_Marc_Goldman_Size_12',
    upcCode: 'B08BP9DHJK',
    expect: {
      base: 'Bruno_Marc_Goldman_Size_12',
      upc:  'B08BP9DHJK',
      siblingCount: 8,
    },
  },
  // Null item number
  {
    itemNumber: null,
    upcCode: null,
    expect: { base: null, upc: null, siblingCount: 0 },
  },
  // Merrell hyphen-space pattern
  {
    itemNumber: 'Merrell J06011W - Size 12_720026451463_UGD',
    upcCode: null,
    expect: {
      base: 'Merrell J06011W - Size 12_720026451463',
      upc:  '720026451463',
      siblingCount: 8,
    },
  },
]

let passed = 0
let failed = 0
const failures = []

for (const c of cases) {
  const got = generateSiblingCandidates(c.itemNumber, c.upcCode)
  const errs = []

  if (got.base !== c.expect.base) {
    errs.push(`base: got '${got.base}' want '${c.expect.base}'`)
  }
  if (got.upc !== c.expect.upc) {
    errs.push(`upc: got '${got.upc}' want '${c.expect.upc}'`)
  }
  if (got.enumeratedSiblings.length !== c.expect.siblingCount) {
    errs.push(`siblingCount: got ${got.enumeratedSiblings.length} want ${c.expect.siblingCount}`)
  }
  for (const needed of (c.expect.siblingIncludes || [])) {
    if (!got.enumeratedSiblings.includes(needed)) {
      errs.push(`siblings missing: '${needed}'`)
    }
  }

  if (errs.length === 0) {
    passed++
    console.log(`✓ ${c.itemNumber || '(null)'}`)
  } else {
    failed++
    failures.push({ itemNumber: c.itemNumber, errs, got })
  }
}

console.log(`\n${passed}/${passed + failed} passed`)
if (failed > 0) {
  console.log('\nFAILURES:')
  for (const f of failures) {
    console.log(`\n✗ ${f.itemNumber}`)
    for (const e of f.errs) console.log(`    ${e}`)
    console.log(`    actual:`, JSON.stringify(f.got, null, 2).slice(0, 400))
  }
  process.exit(1)
}
