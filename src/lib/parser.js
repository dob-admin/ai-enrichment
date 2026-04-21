// src/lib/parser.js
// Parses DOB item numbers into structured components

const CONDITION_CODES = ['NMB', 'ULN', 'UVG', 'UGD', 'UAI', 'UAC', 'UDF']
const CONDITION_REGEX = new RegExp(`_(${CONDITION_CODES.join('|')})$`, 'i')
const UPC_REGEX = /^\d{8,14}$/

// Size-candidate patterns used by the right-to-left scanner.
// Split into two so single-letter sizes (S/M/L) require UPPERCASE — this avoids
// false positives from possessive-apostrophe stripping (e.g. "Champion Men's_UPC"
// → raw "Champion Men s_UPC" — we must NOT treat trailing "s" as a size).
const _LETTER_SIZE_WORD = /^(?:XXXL|XXL|XL|XXS|XS|MDLG|Small|Medium|Large)$/i
const _LETTER_SIZE_CHAR = /^[SML]$/           // no /i flag — uppercase only
const _matchesLetterSize = tok => _LETTER_SIZE_WORD.test(tok) || _LETTER_SIZE_CHAR.test(tok)
const _DIGIT_SIZE  = /^\d{1,3}(?:\.\d+)?[WDEB]?$/i
const _DIGIT_ONLY  = /^\d{1,2}(?:\.\d+)?$/
const _DUAL_GEN    = /^[MW]\d{1,2}(?:\.\d+)?\/?[MW]\d{1,2}(?:\.\d+)?$/i
const _GENDER_LTR  = /^[MWCJKY]\d{1,2}(?:\.\d+)?$/i
const _PREFIX_NUM  = /^(?:US|UK|EU)\d{1,2}(?:\.\d+)?[A-Z]{0,2}$/i
const _ONE_SIZE    = /^(?:OS|OSFM|OSFA|O\/S)$/i
const _EMBEDDED_TAIL = /^(.+?)([MWCJKY]\d{1,2})$/i

// Split SKU-like tokens whose tail is a gender-letter size.
//   "204536-001C11"            → ["204536-001", "C11"]
//   "11016-410M11"             → ["11016-410", "M11"]
//   "204536-530J3/206991-530J3" → ["204536-530J3/206991-530", "J3"]
//   "Crocs"                    → ["Crocs"]        (no digits in prefix)
//   "C10" / "M11"              → ["C10"] / ["M11"] (prefix empty)
// Requires the prefix to contain at least one digit AND be length >= 3
// — this avoids splitting product-name words or already-standalone sizes.
function expandTailSizes(sparts) {
  const out = []
  for (const tok of sparts) {
    const m = tok.match(_EMBEDDED_TAIL)
    if (m && /\d/.test(m[1]) && m[1].length >= 3) {
      out.push(m[1])
      out.push(m[2])
    } else {
      out.push(tok)
    }
  }
  return out
}

// Right-to-left scan for a size-token position. Returns index into `sparts`
// or -1 if no candidate found. Order of checks per position matters:
//   1. Compound O + S (two adjacent tokens)  → return prev idx (so size="O_S")
//   2. Letter-size (extend through consecutive letter-sizes like L_XL;
//      if preceded by a digit-only token, combine as "9.5 M" → use digit idx)
//   3. One-size (OS|OSFM|OSFA|O/S as single token)
//   4. Dual-gender numeric (M8W10, M10/W12)
//   5. Gender-letter (C6, J3, M11)
//   6. US/UK/EU-prefixed (US9, UK11, EU40)
//   7. Bare digit (1–3 digits, optional .5, optional W/D/E/B)
//      — rejected if any later token is alpha with length >= 3 (a color/word,
//        e.g. "Osprey_..._40_NJBlue" — "40" is bag capacity, not a size).
function scanForSizeToken(sparts) {
  for (let i = sparts.length - 1; i >= 1; i--) {
    const tok = sparts[i]

    // 1. Compound O+S (bags one-size written as two tokens: "…_O_S")
    if (/^S$/i.test(tok) && i >= 1 && /^O$/i.test(sparts[i - 1])) {
      return i - 1
    }

    // 2. Letter-size
    if (_matchesLetterSize(tok)) {
      // Digit-before-letter combine: "9.5 M" → use digit position so
      // parseSizeString sees "9.5_M" and captures both size and width.
      if (i >= 1 && _DIGIT_ONLY.test(sparts[i - 1])) return i - 1
      // Extend backward through consecutive letter-sizes (L_XL, M/L hat range)
      let j = i
      while (j - 1 >= 1 && _matchesLetterSize(sparts[j - 1]) && !_DIGIT_ONLY.test(sparts[j - 1])) {
        j--
      }
      return j
    }

    // 3. One-size (compound token)
    if (_ONE_SIZE.test(tok)) return i

    // 4. Dual-gender numeric
    if (_DUAL_GEN.test(tok)) return i

    // 5. Gender-letter
    if (_GENDER_LTR.test(tok)) return i

    // 6. Prefix-num (US9, UK11, EU40)
    if (_PREFIX_NUM.test(tok)) return i

    // 7. Bare digit — reject if followed by alpha(≥3) tokens (color/word context)
    //    "Osprey_Fairview_40_NJBlue" → "40" rejected because "NJBlue" is 6 alpha chars.
    if (_DIGIT_SIZE.test(tok)) {
      const rest = sparts.slice(i + 1)
      const hasColorLike = rest.some(t => /^[A-Za-z]{3,}$/.test(t))
      if (!hasColorLike) return i
    }
  }
  return -1
}

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

  // Accept underscore OR whitespace as part separator. Use sparts (space+underscore
  // split) for size-token detection; keep parts (underscore-only) available for
  // legacy brand/model behavior where relevant.
  const hasSep = baseItemNumber.includes('_') || /\s/.test(baseItemNumber)
  if (!isUPC && hasSep) {
    const parts = baseItemNumber.split('_').filter(Boolean)
    const rawSparts = baseItemNumber.split(/[_\s]+/).filter(Boolean)
    // Expand SKU-like tokens whose tail is a gender-letter size (C11, J3, M11, etc.)
    // e.g. "204536-001C11" → ["204536-001", "C11"]
    const sparts = expandTailSizes(rawSparts)

    // Brand: first sparts token (handles both "Cole_Haan_…" → "Cole" and
    // "Osprey 10003685 Fairview…" → "Osprey").
    brand = sparts[0] || parts[0] || null

    // sizeIdx via explicit Size/Sz keyword (accept optional trailing colon).
    let sizeIdx = sparts.findIndex(p => /^(?:size|sz):?$/i.test(p))
    const foundByKeyword = sizeIdx !== -1

    // Fallback: multi-pattern right-to-left scan.
    if (sizeIdx === -1) {
      sizeIdx = scanForSizeToken(sparts)
    }

    // If the scan (not the keyword) found the size AND it's preceded by a
    // UK/US/EU prefix token, expand backward so parseSizeString sees the prefix.
    if (!foundByKeyword && sizeIdx >= 1 && /^(UK|US|EU)$/i.test(sparts[sizeIdx - 1])) {
      sizeIdx = sizeIdx - 1
    }

    if (sizeIdx >= 1) {
      modelNumber = sparts.slice(1, sizeIdx).join('_') || null
      size = sparts.slice(sizeIdx).join('_')
    } else if (sparts.length >= 2) {
      modelNumber = sparts.slice(1).join('_') || null
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

// Generate sibling candidates for cost/enrichment fallback lookup.
//
// "Sibling" = another record or GoFlow product that shares the same underlying
// physical item as this record, just with a different condition suffix. When
// the suffixed item in GoFlow has $0 Avg Cost, a sibling (different condition
// of the same item) often has real cost data. Example:
//
//   Saucony_S10810_05_Size_10_5M_195018769756_UVG  ($0 in GoFlow)
//   Saucony_S10810_05_Size_10_5M_195018769756      (base — same phys. item)
//   Saucony_S10810_05_Size_10_5M_195018769756_UGD  (sibling — may have cost)
//
// Returns:
//   {
//     base:               string,          // base item number, condition stripped (or null)
//     upc:                string,          // clean UPC extracted from UPC_CODE or item number (or null)
//     enumeratedSiblings: string[],        // base + base+_COND for every known condition code
//   }
//
// If item number has no condition suffix, `base` equals the input.
// If no UPC can be extracted from either upcCode or itemNumber, `upc` is null.
// `enumeratedSiblings` always includes the base itself first, so callers can
// submit this list directly to the GoFlow Inventory Values report.
export function generateSiblingCandidates(itemNumber, upcCode) {
  const result = { base: null, upc: null, enumeratedSiblings: [] }
  if (!itemNumber) return result

  const parsed = parseItemNumber(itemNumber)
  if (!parsed) return result

  // Base = item number with condition suffix stripped (case-insensitive).
  // parseItemNumber already handles both upper and lower case via /i flag.
  result.base = parsed.baseItemNumber || null

  // UPC resolution cascade: cleanUPC(upcCode) first, fallback extract from item number.
  // Mirrors loop.js phase-2 logic.
  let upc = cleanUPC(upcCode)
  if (!upc) {
    const m = (itemNumber || '').match(/(?<![0-9])(\d{12,14})(?![0-9])/)
    if (m) upc = m[1]
  }
  result.upc = upc

  // Enumerate condition-variant siblings under the same base.
  // Includes base itself (no suffix) since GoFlow often has a bare-base product
  // representing the "unconditioned" SKU — see the Saucony UI screenshot where
  // `Saucony_S10810_05_Size_10_5M` exists alongside the suffixed variants.
  if (result.base) {
    const codes = ['NMB', 'ULN', 'UVG', 'UGD', 'UAI', 'UAC', 'UDF']
    const siblings = [result.base]
    for (const code of codes) {
      siblings.push(`${result.base}_${code}`)
    }
    // Deduplicate — the record's own item number (suffixed form) is a valid
    // sibling too but MUST be included so a caller querying "any sibling with
    // cost > 0" still considers the original record's entry. However, the
    // base here is already stripped so the enumerated list won't accidentally
    // exclude the record itself when it gets added back via suffix.
    result.enumeratedSiblings = [...new Set(siblings)]
  }

  return result
}
