// src/config/fields.js
// All Airtable field IDs for Product Data table
// Field names are included as comments for reference

export const FIELDS = {
  // Identity
  ITEM_NUMBER:          'fldgmm7QkRDt2e9nC',  // Item Number (PK)
  BRAND:                'fldR3p0lZyNJFB3a9',  // Brand (raw)
  BRAND_CORRECT_SPELL:  'fldwS3aGSomYkthTV',  // Brand Correct Spelling
  BRAND_KEY:            'fldk6DcfgsmgaeAVw',  // Brand Key (formula)
  CONDITION:            'fld73jcFcgWW7hxe7',  // Condition (New/Used text)
  CONDITION_TYPE:       'fldeEuhjDRAUOiZf8',  // Condition Type (formula: NMB/ULN/etc)
  MANUAL_CONDITION:     'fld7mZUoDJgcFnSMp',  // Manual Condition Type (override)
  WEBSITE:              'fldriFW7lmzK6sfBk',  // Website (SDO/REBOUND/LTV/RTV)
  PRODUCT_NAME:         'fld6l3t2nbKMzKXk6',  // Product Name (raw from BQ)
  PURCHASE_NAME:        'fldK4x3A574dWAX4M',  // Purchase Name
  UPC_CODE:             'fldztKcxkNyOeUUT6',  // UPC Code
  GLOBAL_ITEM_NUMBER:   'fldzhTGrcoRSXGNny',  // Global Item Number (formula: no condition suffix)

  // Inventory
  AMZ_INVENTORY:        'fld3gWkrnCSu8re6B',  // amz_inventory_count
  BOW_INVENTORY:        'fldmn89jlHbpoXc7O',  // bow_inventory_count
  TOTAL_INVENTORY:      'fld6pOENdKtV98qZu',  // Total Inventory (formula)

  // Content — shared across all stores
  TITLE:                'fldhbkyCE3ZK3huqf',  // Title
  DESCRIPTION:          'fld8s6bi94sxJiqZE',  // Description (richText)
  SEO_TITLE:            'fldfJFWbKUq8IjZE8',  // SEO Title (set by automation)
  SEO_DESCRIPTION:      'fldtmIK6WVA93On8C',  // SEO Description
  PRODUCT_IMAGES:       'fldfOrq8jm703glZC',  // Product Images (multipleAttachments)
  VARIANT_IMAGE_INDEX:  'fldYLJxgnXASagxsN',  // Variant Image Index
  SHOPIFY_CATEGORY:     'fldE4yEqLZKe5UgPZ',  // Shopify Category
  GOOGLE_CATEGORY:      'fldPGP1Xxrf75nC6U',  // mm-google-shopping.google_product_category
  MATERIAL:             'fldojvxXhW2UUH0My',  // Material (multipleSelects)
  SHOPIFY_TAGS:         'fldSL5MW9ImnO4cMi',  // Shopify Tags

  // Variant options
  OPTION_1_VALUE:       'fld3Wla73i6UIMOfF',  // Option 1 Value (colorway)
  OPTION_2_VALUE:       'fldf2x6tBn9cT3KVq',  // Option 2 Value (formula — do not write)
  OPTION_2_CUSTOM:      'fldm31v4xfgHVbq7M',  // Option 2 Custom Value (NPCN write target)
  OPTION_3_CUSTOM:      'fld8my9cmN7qzA4Gt',  // Option 3 Custom Value (NPCN RTV condition)
  ACC_OPTION_2:         'fldZOWQkklolAA1tV',  // ACC Option 2 Value (outdated — never write)

  // SDO structured fields (CF step outputs)
  SDO_COLOR:            'fld5FU5pikJMxFCag',  // SDO_Color
  SDO_GENDER:           'fldIHvvdeMHENFciG',  // SDO_Gender
  SDO_AGE_RANGE:        'flddGCTDEBTcar7oy',  // SDO_Age Range
  SDO_MODEL_NAME:       'fldefCb9pltQHIKjk',  // SDO_Model Name
  SDO_MODEL_NUMBER:     'fldeVzsosNCy0mjdL',  // SDO_Model Number
  SDO_MEN_SIZE:         'fldvZcRRJo5TwnJzI',  // SDO_Men Size
  SDO_MEN_WIDTH:        'fldiJIUdWLc3I2VyX',  // SDO_Men Width
  SDO_WOMEN_SIZE:       'fldNmAaGGnUY5VFnD',  // SDO_Women Size
  SDO_WOMEN_WIDTH:      'fldB9O63RBrbDqlKS',  // SDO_Women Width
  SDO_YOUTH_SIZE:       'fldABB1qkgS9P2hg7',  // SDO_Youth Size
  SDO_YOUTH_WIDTH:      'fldDimU3CegncDx4a',  // SDO_Youth Width
  SDO_RETAIL_PRICE:     'fldhUJBMosCI4lRrO',  // SDO_Retail Price
  SDO_MEN_INTL:         'fldJKYH4koqMSheif',  // SDO_Men Intl Size
  SDO_WOMEN_INTL:       'fldMy5B6YdM7xPkLe',  // SDO_Women Intl Size
  SDO_YOUTH_INTL:       'flduQ87TQ29ZUC7G5',  // SDO_Youth Intl Size
  SDO_INTL_TYPE:        'flduciQ4e0CkCKA7C',  // SDO_Intl Size Type
  SDO_INTL_AU:          'fld3MDuwVoAOUipeo',  // SDO_Intl Size Type AU
  SDO_INTL_EU:          'fldqFhqaBsRHQOnla',  // SDO_Intl Size Type EU
  SDO_INTL_UK:          'fldgJ7fu6y03Nhksh',  // SDO_Intl Size Type UK

  // Computed fields — DO NOT WRITE
  REAL_GENDER:          'fldDdFyuLoHEE1Z7u',  // Real Gender (formula)
  SHOPIFY_HANDLE:       'fldEBrnQTYmQ8J5hr',  // Shopify Handle (formula)
  VENDOR:               'fldYfyeKtRrL9pyt8',  // Vendor (formula = Brand Correct Spelling)
  PRICE:                'fldHDhUU6MNrQUORt',  // Price
  AVAILABLE_VALUE_AVG:  'fldnCQTp0W5MFYMVK',  // available_value_average (GoFlow avg cost) (write target for Variant Price formula)
  ITEM_COST:            'fldxI3jS2HxLbtFXT',  // item_cost (source for Variant Cost formula)
  VARIANT_BARCODE:      'fldVbI3WrbONqwEep',  // Variant Barcode (formula)
  VARIANT_COST:         'fldTg2mNJIJOqqsKr',  // Variant Cost (formula)
  VARIANT_PRICE:        'fldur2N3IOR2rdlf8',  // Variant Price (formula)
  US_SIZE:              'fldBwexmtmpG5kPbn',  // US Size (formula)
  INTL_SIZE:            'fld4Og69DywUu5RIc',  // Intl Size (formula)

  // Validation formulas — READ ONLY
  PRODUCT_INFO_VALID:   'fld5ngh2MlP8N6dpt',  // Product Info Valid
  VARIANT_INFO_VALID:   'fldrcRaE8mI1iNFkg',  // Variant Info Valid
  PRODUCT_INVALID_WHY:  'fldB5XvbxO1feToOP',  // Product Info Invalid Reason
  VARIANT_INVALID_WHY:  'fldgYVS6YZWnTsy04',  // Variant Info Invalid Reason
  MISSING_CF:           'fld0QlyyDcVzb9mIQ',  // Missing CF (formula)

  // Workflow flags
  PD_READY:             'fldeNIKuNPpDg12AW',  // PD Ready — NEVER WRITE
  PD_READY_HOLD:        'fldhMJKOKLtxOnlmi',  // PD READY (hold) — script writes this
  PD_ESCALATE:          'fldi67XUeA00o6oT0',  // PD Escalate
  PD_ESCALATE_REASON:   'fldKq4JdfMgAMlqf7',  // PD Escalate Reason

  // URLs
  BRAND_SITE:           'flddPgrETFTU3Wj5g',  // Brand Site
  OTHER_SITE:           'fldMecx4HEXGFlThx',  // Other Site

  // Shopify sync
  SHOPIFY_PRODUCT_ID:   'fldXciVH3n8EMjxxu',  // Shopify Product ID (all stores)
  SYNC_ERROR:           'fldCEsm48AcT5swhK',  // Sync Error

  // AI enrichment — resolved at runtime after dotenv loads
  get AI_STATUS()  { return process.env.AI_STATUS_FIELD_ID },
  get AI_MISSING() { return process.env.AI_MISSING_FIELDS_FIELD_ID },

  // Cost tracking
  AI_COST_CHECK:        'fldjJAFcnpntaN7l4',  // AI Cost Check (Good/Found/Missing)
  COST_FIX:             'fldA0B85ZmLfGGKLf',  // Cost Fix (Inputted/No Data)

  // BQ Brands table fields
  BQ_TITLE:             'fld7ur4JdSDWWFNqJ',  // Title (brand name raw)
  BQ_NEW:               'fld9Sdl8DMyLFCGCn',  // NEW store assignment
  BQ_USED:              'fldxLqZ0XZYg5KY5A',  // USED store assignment
  BQ_KEY:               'fldJmSZjDsEtyiJqY',  // key (lowercase alphanumeric)
  BQ_CORRECT_SPELL:     'fld0DlL7o5PZKlfi3',  // Correct Spelling
}

// Website values
export const WEBSITE = {
  SDO:     'SDO',
  REBOUND: 'REBOUND',
  LTV:     'LTV',
  RTV:     'RTV',
  IGNORE:  'ignore',
}

// Footwear stores
export const FOOTWEAR_STORES = [WEBSITE.SDO, WEBSITE.REBOUND]
export const NPCN_STORES = [WEBSITE.LTV, WEBSITE.RTV]

// Condition codes and their full text labels (for NPCN RTV title suffix)
export const CONDITION_LABELS = {
  NMB: 'New Missing Box',
  ULN: 'Used Like New',
  UVG: 'Used Very Good',
  UGD: 'Used Good',
  UAI: 'Used As-Is',
  UDF: 'Used - Defective',
}

// Condition pricing multipliers (for reference/validation)
export const CONDITION_MULTIPLIERS = {
  NMB: 0.95,
  ULN: 0.80,
  UVG: 0.70,
  UGD: 0.60,
  UAI: 0.50,
}

// AI Enrichment Status values
export const AI_STATUS = {
  COMPLETE:   'Complete',
  PARTIAL:    'Partial',
  NOT_FOUND:  'Not Found',
}

// AI Cost Check values
export const AI_COST_CHECK = {
  GOOD:     'Good',     // cost was already > 0.01
  FOUND:    'Found',    // worker found cost from a match
  MISSING:  'Missing',  // worker couldn't find cost
}

// Cost Fix values (VA column)
export const COST_FIX = {
  INPUTTED: 'Inputted', // VA manually entered cost
  NO_DATA:  'No Data',  // VA confirmed no cost available
}

// Shopify category allowed values for footwear
export const FOOTWEAR_SHOPIFY_CATEGORIES = [
  'Athletic Shoes',
  'Baby & Toddler Shoes',
  'Boots',
  'Flats',
  'Heels',
  'Sandals',
  'Slippers',
  'Sneakers',
]

// Approved materials list (hard constraint from SOP)
export const APPROVED_MATERIALS = [
  'Leather', 'Suede', 'Canvas', 'Mesh', 'Rubber',
  'Synthetic Leather', 'Nylon', 'Polyester', 'Cotton', 'Wool',
  'Gore-Tex', 'EVA', 'PU', 'TPU', 'Cork', 'Velvet', 'Denim',
  'Recycled Polyester', 'Recycled Rubber', 'Neoprene',
  'Patent Leather', 'Natural Rubber', 'Thermoplastic (EVA)',
  'Croslite', 'Microfiber', 'Recycled P.E.T. Plastic',
  'Twill', 'Steel Toe', 'Spandex', 'Carbon Toe',
  'Composite Toe', 'Jute', 'Glass', 'Felt',
]

// Shopify taxonomy → Google Shopping category mapping
// Gender-aware for footwear
export const CATEGORY_MAPPING = {
  men: {
    'Sneakers & Athletic': { shopify: 'aa-8-1',   google: '187' },
    'Boots':               { shopify: 'aa-8-3',   google: '187' },
    'Clogs':               { shopify: 'aa-8-7',   google: '187' },
    'Dress Shoes':         { shopify: 'aa-8',     google: '187' },
    'Sandals':             { shopify: 'aa-8-6',   google: '187' },
    'Loafers':             { shopify: 'aa-8-9',   google: '187' },
    'Insoles':             { shopify: 'aa-7-5',   google: '1933' },
    'Hats':                { shopify: 'aa-2-17',  google: '173' },
    'Bags':                { shopify: 'aa-5-4',   google: '3032' },
    'Wallets':             { shopify: 'aa-5-5',   google: '2668' },
    'Gloves':              { shopify: 'aa-2-13',  google: '170' },
    'Socks':               { shopify: 'aa-1-18',  google: '209' },
    'Activewear':          { shopify: 'aa-1-1',   google: '5322' },
  },
  women: {
    'Sneakers & Athletic': { shopify: 'aa-8-1',   google: '187' },
    'Boots & Booties':     { shopify: 'aa-8-3',   google: '187' },
    'Clogs':               { shopify: 'aa-8-7',   google: '187' },
    'Flats':               { shopify: 'aa-8-9',   google: '187' },
    'Heels':               { shopify: 'aa-8-10',  google: '187' },
    'Sandals':             { shopify: 'aa-8-6',   google: '187' },
    'Loafers':             { shopify: 'aa-8-9',   google: '187' },
    'Insoles':             { shopify: 'aa-7-5',   google: '1933' },
    'Hats':                { shopify: 'aa-2-17',  google: '173' },
    'Jewelry':             { shopify: 'aa-6',     google: '188' },
    'Bags':                { shopify: 'aa-5-4',   google: '3032' },
    'Wallets':             { shopify: 'aa-5-5',   google: '2668' },
    'Socks':               { shopify: 'aa-1-18',  google: '209' },
    'Activewear':          { shopify: 'aa-1-1',   google: '5322' },
  },
  kids: {
    'Boots':               { shopify: 'aa-8-2-1', google: '187' },
    'Sneakers':            { shopify: 'aa-8-2-5', google: '187' },
    'Sandals':             { shopify: 'aa-8-2-2', google: '187' },
    'Backpacks':           { shopify: 'aa-5-4-17',google: '3032' },
  },
}
