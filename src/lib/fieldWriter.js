// src/lib/fieldWriter.js
// Maps Claude's enrichment output to Airtable field IDs
// Determines what to write based on store type

import {
  FIELDS,
  WEBSITE,
  FOOTWEAR_STORES,
  NPCN_STORES,
  AI_STATUS,
  CONDITION_LABELS,
  APPROVED_MATERIALS,
} from '../config/fields.js'

// Build the Airtable update payload from Claude's output
export function buildWritePayload(claudeOutput, record) {
  const { airtableData, parsedItem } = record
  const website = airtableData.website
  const isFootwear = FOOTWEAR_STORES.includes(website)
  const isNPCN = NPCN_STORES.includes(website)
  const isRTV = website === WEBSITE.RTV

  const fields = {}
  const missingFields = [...(claudeOutput.missingFields || [])]

  // --- TITLE ---
  if (claudeOutput.title) {
    let title = claudeOutput.title
    // RTV: append condition text to make handle unique
    if (isRTV && parsedItem.conditionCode && CONDITION_LABELS[parsedItem.conditionCode]) {
      title = `${title} ${CONDITION_LABELS[parsedItem.conditionCode]}`
    }
    fields[FIELDS.TITLE] = title
  } else {
    missingFields.push('Title')
  }

  // --- DESCRIPTION ---
  if (claudeOutput.description) {
    fields[FIELDS.DESCRIPTION] = claudeOutput.description
  } else {
    missingFields.push('Description')
  }

  // --- SEO DESCRIPTION ---
  if (claudeOutput.seoDescription) {
    // Enforce 160 char limit
    fields[FIELDS.SEO_DESCRIPTION] = claudeOutput.seoDescription.slice(0, 160)
  } else {
    missingFields.push('SEO Description')
  }

  // --- SHOPIFY CATEGORY ---
  if (claudeOutput.shopifyCategory) {
    fields[FIELDS.SHOPIFY_CATEGORY] = claudeOutput.shopifyCategory
  } else {
    missingFields.push('Shopify Category')
  }

  // --- GOOGLE SHOPPING CATEGORY ---
  if (claudeOutput.googleShoppingCategory) {
    fields[FIELDS.GOOGLE_CATEGORY] = claudeOutput.googleShoppingCategory
  } else {
    missingFields.push('Google Shopping Category')
  }

  // --- MATERIAL (footwear only) ---
  if (isFootwear) {
    if (claudeOutput.material?.length) {
      // Filter to only approved options that exist in Airtable
      const filtered = claudeOutput.material.filter(m => APPROVED_MATERIALS.includes(m))
      if (filtered.length) {
        fields[FIELDS.MATERIAL] = filtered
      } else {
        missingFields.push('Material')
      }
    } else {
      missingFields.push('Material')
    }
  }

  // --- OPTION 1 VALUE (colorway) ---
  // SDO/Rebound: always required
  // NPCN: optional
  if (claudeOutput.option1Value) {
    fields[FIELDS.OPTION_1_VALUE] = claudeOutput.option1Value
    // Also write SDO_Color since it drives Variant Image Alt and Google color
    if (isFootwear) {
      fields[FIELDS.SDO_COLOR] = claudeOutput.option1Value
    }
  } else if (isFootwear) {
    missingFields.push('Option 1 Value (colorway)')
  }

  // --- OPTION 2 CUSTOM VALUE (NPCN only) ---
  // SDO/Rebound: Option 2 is a formula — never write
  if (isNPCN && claudeOutput.option2CustomValue) {
    fields[FIELDS.OPTION_2_CUSTOM] = claudeOutput.option2CustomValue
  }

  // --- OPTION 3 CUSTOM VALUE (NPCN RTV only — condition text, never blank) ---
  if (isRTV) {
    const conditionText = claudeOutput.option3CustomValue ||
      (parsedItem.conditionCode ? CONDITION_LABELS[parsedItem.conditionCode] : null)

    if (conditionText) {
      fields[FIELDS.OPTION_3_CUSTOM] = conditionText
    } else {
      missingFields.push('Option 3 Custom Value (used condition)')
    }
  }

  // --- PRODUCT IMAGES ---
  if (claudeOutput.imageUrls?.length) {
    fields[FIELDS.PRODUCT_IMAGES] = claudeOutput.imageUrls.map(url => ({ url }))
    fields[FIELDS.VARIANT_IMAGE_INDEX] = 1
  } else {
    missingFields.push('Product Images')
    // Can still proceed without images — VA will find them
  }

  // --- PRICE ---
  // Priority: Keepa price → GoFlow listing price → cost × 1.5
  const price = claudeOutput.price || null
  const cost = record.airtableData?.itemCost || null
  if (price && price > 0) {
    fields[FIELDS.PRICE] = price
  } else if (cost && cost > 0) {
    fields[FIELDS.PRICE] = parseFloat((cost * 1.5).toFixed(2))
  }
  // If no cost either, leave blank — VA will set manually

  // --- MANUAL CONDITION TYPE ---
  // Set if condition code exists but was not at end of item number
  // (meaning the formula won't auto-extract it)
  if (parsedItem.conditionCode && !parsedItem.conditionCodeFromEnd) {
    if (!airtableData.manualConditionType) {
      fields[FIELDS.MANUAL_CONDITION] = parsedItem.conditionCode
    }
  }

  // --- AI STATUS ---
  const dedupedMissing = [...new Set(missingFields)]

  // Determine required fields for this store
  const requiredFields = getRequiredFields(website)
  const missingRequired = dedupedMissing.filter(f => requiredFields.includes(f))

  let status
  if (claudeOutput.error) {
    status = AI_STATUS.NOT_FOUND
  } else if (missingRequired.length === 0 && claudeOutput.confidence !== 'low') {
    status = AI_STATUS.COMPLETE
  } else if (Object.keys(fields).length > 2) {
    // Got some data even if not everything
    status = AI_STATUS.PARTIAL
  } else {
    status = AI_STATUS.NOT_FOUND
  }

  // Write AI status fields if field IDs are configured
  if (process.env.AI_STATUS_FIELD_ID) {
    fields[FIELDS.AI_STATUS] = status
  }
  if (process.env.AI_MISSING_FIELDS_FIELD_ID && dedupedMissing.length > 0) {
    fields[FIELDS.AI_MISSING] = dedupedMissing.join(', ')
  }

  return {
    fields,
    status,
    missingFields: dedupedMissing,
    validationIssues: claudeOutput.validationIssues || [],
  }
}

// Required fields per store — used to determine Complete vs Partial
function getRequiredFields(website) {
  const base = ['Title', 'Description', 'SEO Description', 'Shopify Category']

  switch (website) {
    case WEBSITE.SDO:
    case WEBSITE.REBOUND:
      return [...base, 'Option 1 Value (colorway)', 'Product Images']
    case WEBSITE.LTV:
      return [...base, 'Product Images']
    case WEBSITE.RTV:
      return [...base, 'Product Images', 'Option 3 Custom Value (used condition)']
    default:
      return base
  }
}
