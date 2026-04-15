# DOB AI Enrichment Pipeline

AI-powered product data enrichment for Deals Outlet Brands (SDO, Rebound, NPCN).

## Overview

Three Railway cron workers that run continuously:

| Worker | Schedule | Purpose |
|--------|----------|---------|
| `worker-brand` | Every 30 min | Resolves missing brand fields by matching item numbers to BQ Brands |
| `worker-enrich` | Every 15 min | AI enrichment — fills product data via Airtable match, GoFlow, and web |
| `worker-report` | Every hour | Logs status summary to Railway console |

## Setup

### 1. Create new Airtable fields

Add these two fields to the **Product Data** table in Airtable before first deploy:

| Field Name | Type | Options |
|---|---|---|
| `AI Enrichment Status` | Single Select | Complete, Partial, Not Found |
| `AI Missing Fields` | Single Line Text | (no options needed) |

After creating them, copy their field IDs from Airtable's field configuration.

### 2. Configure environment variables

```bash
cp .env.example .env
```

Fill in all values in `.env`:

```
AIRTABLE_API_KEY=         # from Airtable account settings
AIRTABLE_BASE_ID=app0KyG9OA1w7aZJQ
AIRTABLE_TABLE_ID=tbllHd4Hfe156drDD
AIRTABLE_BRANDS_TABLE_ID=tblyvRPoKAf0knzr0
AI_STATUS_FIELD_ID=       # from new Airtable field you created
AI_MISSING_FIELDS_FIELD_ID= # from new Airtable field you created
ANTHROPIC_API_KEY=        # same key as repricer
GOFLOW_API_KEY=           # same key as repricer
GOFLOW_BASE_URL=          # same as repricer
```

### 3. Install and test locally

```bash
npm install

# Test brand resolution (dry run — check logs before any Airtable writes)
npm run brand

# Test enrichment on one batch
npm run enrich

# Check status
npm run report
```

### 4. Deploy to Railway

1. Create new Railway project: `dob-ai-enrichment`
2. Connect this GitHub repo
3. Add all env vars to Railway project settings
4. Railway will use `railway.toml` to create the three cron services automatically

## Architecture

```
Item Number arrives in Airtable
        ↓
[Worker 1: Brand Resolution]
  Missing brand? → fuzzy match BQ Brands → write Brand field
  → Automation fires: sets Website + Brand Correct Spelling
        ↓
[Worker 2: AI Enrichment]
  Has inventory + Website + no AI Status?
        ↓
  1. Check Airtable for existing model match
     → Variant match: copy everything, done
     → Product match: copy content, still need images
     → No match: proceed to external
        ↓
  2. External sources
     UPC-only → GoFlow first, web search supplementary
     Brand+Model → existing URL or web search
        ↓
  3. Claude validates sources against record
     (brand match? product type correct? images right?)
        ↓
  4. Claude generates: title, description, SEO description,
     category, material, colorway, images
        ↓
  5. Single PATCH write to Airtable
     → Automation fires: sets SEO Title, Image Alt
     → AI Status: Complete / Partial / Not Found
        ↓
[Your Review]
  Complete records → PD Ready Hold → your review → PD Ready → Shopify
  Partial / Not Found → VA fills missing fields manually
```

## Store-specific behavior

| Field | SDO | Rebound | NPCN LTV | NPCN RTV |
|---|---|---|---|---|
| Title | Model name | Model name | Product name | Product name + condition suffix |
| Material | ✓ required | ✓ required | — | — |
| Option 1 (colorway) | ✓ required | ✓ required | if applicable | if applicable |
| Option 3 (condition) | — | — | — | ✓ never blank |

## GoFlow API notes

The GoFlow client at `src/lib/goflow.js` may need adjustment based on the actual
GoFlow API response structure. Check the field names in `normalizeGoFlowProduct()`
against a live API response and update field names as needed.

## Monitoring

Check Railway logs for each worker service. The status reporter runs hourly and
prints a summary table showing Complete / Partial / Not Found / Pending counts.
