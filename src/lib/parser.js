// src/lib/parser.js
// Parses DOB item numbers into structured components

const CONDITION_CODES = ['NMB', 'ULN', 'UVG', 'UGD', 'UAI', 'UDF']
const CONDITION_REGEX = new RegExp(`_(${CONDITION_CODES.join('|')})$`, 'i')
const UPC_REGEX = /^\d{8,14}$/

export function parseItemNumber(itemNumber) {
  if (!itemNumber) return null

  const raw = itemNumber.trim()

  // Extract condition code from end of item number
  const conditionMatch = raw.match(CONDITION_REGEX)
  const conditionCode = conditionMatch
    ? conditionMatch[1].toUpperCase()
    : null

  const baseItemNumber = conditionCode
    ? raw.slice(0, -(conditionCode.length + 1))
    : raw

  // Detect UPC-only: digits only after stripping condition
  const isUPC = UPC_REGEX.test(baseItemNumber)

  // For structured item numbers (Brand_ModelNumber_Size pattern)
  let brand = null
  let modelNumber = null
  let size = null

  if (!isUPC && baseItemNumber.includes('_')) {
    const parts = baseItemNumber.split('_')
    brand = parts[0] || null

    // Find size — look for parts that match size patterns
    // e.g. "Size", "Sz", or numeric size values
    const sizeIdx = parts.findIndex(p =>
      /^(size|sz)$/i.test(p) ||
      /^\d+(\.\d+)?[WDE]?$/.test(p)
    )

    if (sizeIdx > 1) {
      // Everything between brand and size is model number
      modelNumber = parts.slice(1, sizeIdx).join('_')
      size = parts.slice(sizeIdx).join('_')
    } else if (parts.length >= 2) {
      modelNumber = parts.slice(1).join('_')
    }
  }

  // Extract condition code from anywhere in item number if not at end
  // Used to populate Manual Condition Type when formula can't extract
  let embeddedCondition = conditionCode
  if (!embeddedCondition) {
    for (const code of CONDITION_CODES) {
      if (raw.includes(`_${code}_`) || raw.includes(`_${code}`)) {
        embeddedCondition = code
        break
      }
    }
  }

  return {
    raw,
    baseItemNumber,
    conditionCode: embeddedCondition,
    conditionCodeFromEnd: conditionCode, // only if at end (formula-compatible)
    isUPC,
    brand,
    modelNumber,
    size,
  }
}

// Extract clean UPC from UPC Code field (strips condition suffix)
export function cleanUPC(upcCode) {
  if (!upcCode) return null
  const underscoreIdx = upcCode.indexOf('_')
  return underscoreIdx > -1 ? upcCode.slice(0, underscoreIdx) : upcCode
}

// Extract brand name candidates from item number for brand resolution.
// Returns an ordered array of candidates (deduplicated) — the worker tries
// each against BQ Brands and picks the best-confidence hit.
//
// Patterns handled:
//   Saucony_S10810_05_Size_..._UVG               → ["Saucony"]
//   Cole_Haan_C30416_Size_11M_..._UVG            → ["Cole", "Cole Haan", "Cole Haan C30416"]
//   On_Running_35_99234_Size_8_5_UVG             → ["On", "On Running", "On Running 35"]
//   Merrell J06011W - Size 12_720..._UGD         → ["Merrell J06011W - Size 12", ..., "Merrell"]
//   Teva-1019234 Hurricane Sandal-..._UPC_UVG    → [..., "Teva"]
//   206121-100M4W6 CrocsClassic CRO..._UPC_UGD   → [..., "CrocsClassic", ...]
//   Van's Men's C&D Authentic..._UPC_UVG         → [..., "Van's", "Vans"]
//   810096859945_ULN (pure UPC)                  → ["810096859945"]
//   PL Niyo_..._ULN (junk)                       → ["PL Niyo", "PL"] (will Not Found — correct)
export function extractBrandCandidates(itemNumber) {
  if (!itemNumber) return []
  const parsed = parseItemNumber(itemNumber)

  // UPC-only item number — just use the clean UPC.
  if (parsed?.isUPC) {
    return [parsed.baseItemNumber]
  }

  const out = []
  const push = (v) => {
    if (!v) return
    const trimmed = v.trim()
    if (trimmed && !out.includes(trimmed)) out.push(trimmed)
  }

  const base = (parsed?.baseItemNumber || itemNumber).trim()
  const uparts = base.split('_').filter(Boolean)
  const firstChunk = uparts[0] || base

  // Candidate family A: full first underscore chunk, then progressively shorter word prefixes.
  // Handles `Merrell J06011W - Size 12` → full, ..., `Merrell`
  push(firstChunk)
  if (/\s/.test(firstChunk)) {
    const words = firstChunk.split(/\s+/).filter(Boolean)
    for (let n = words.length - 1; n >= 1; n--) {
      push(words.slice(0, n).join(' '))
    }
  }

  // Candidate family B: for underscore-as-separator-between-words brands
  // (Cole_Haan, On_Running, Sea_to_Summit) join the first 2 and first 3 parts with spaces.
  if (uparts.length >= 2) push(`${uparts[0]} ${uparts[1]}`)
  if (uparts.length >= 3) push(`${uparts[0]} ${uparts[1]} ${uparts[2]}`)

  // Candidate family C: split the first chunk on hyphens
  // Catches `Teva-1019234 Hurricane` where the hyphen separates brand from model.
  if (firstChunk.includes('-')) {
    const hparts = firstChunk.split('-').filter(Boolean)
    if (hparts[0]) push(hparts[0].split(/\s+/)[0])
  }

  // Candidate family D: apostrophe-normalized variants (Van's → Vans).
  const extras = []
  for (const c of out) {
    if (c.includes("'") || c.includes('\u2019')) {
      extras.push(c.replace(/['\u2019]/g, ''))
    }
  }
  for (const n of extras) push(n)

  // Candidate family E: scan each word in the first chunk — covers junk-prefix cases
  // like `206121-100M4W6 CrocsClassic CRO Slide` where the brand is not the first word.
  // Push every alphabetic word of length >= 3. Fuse will reject noise.
  const allWords = firstChunk.split(/[\s\-]+/).filter(Boolean)
  for (const w of allWords) {
    if (/^[A-Za-z][A-Za-z'\u2019]{2,}$/.test(w)) push(w)
  }

  return out
}

// Backward-compat: single-candidate API — returns the first (best-shot) candidate.
export function extractBrandCandidate(itemNumber) {
  const candidates = extractBrandCandidates(itemNumber)
  return candidates[0] || null
}
