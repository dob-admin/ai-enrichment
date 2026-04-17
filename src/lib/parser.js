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

// Extract brand name candidate from item number for brand resolution
export function extractBrandCandidate(itemNumber) {
  if (!itemNumber) return null
  const parsed = parseItemNumber(itemNumber)

  // If structured item number, brand is first segment
  if (parsed?.brand) return parsed.brand

  // UPC-only item number (with or without condition suffix) —
  // use the clean UPC as candidate. BQ Brands has UPCs in Title field.
  if (parsed?.isUPC) return parsed.baseItemNumber

  // If item number has spaces (some raw GoFlow names), first word
  if (itemNumber.includes(' ')) {
    return itemNumber.split(' ')[0]
  }

  return null
}
