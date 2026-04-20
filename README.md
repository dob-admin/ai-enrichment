# DOB AI Enrichment Pipeline

AI-powered product data enrichment for Deals Outlet Brands' Shopify stores (SDO, Rebound, LTV, RTV). Consumes raw inventory records from Airtable and outputs Shopify-ready listings.

## Architecture

Two workers, both deployed to Railway:

- **`worker-loop`** — continuous enrichment loop. Runs forever. Processes all in-scope records until validation formulas (PIV + VIV) return YES, then flips PD Ready Hold.
- **`worker-brand`** — brand resolution cron. Runs every 20 minutes. Backfills the Brand Correct Spelling field against the BQ Brands table.

The enrichment worker is a single file (`src/workers/loop.js`). Everything — cost resolution, UPC historical matching, gender/size extraction, Claude calls, field writes, validation, logging — lives in one monolith.

## Flow (per record)

Each record in queue follows this path:

```
1. COST CHECK
   - If cost > $1.00 already present → proceed
   - Else → queue for Inventory Values report batch (5-min timer or 500-item trigger)
   - If report returns no cost → mark Cost Fix = No Data, park

2. UPC RESOLUTION
   - Read UPC Code field, or extract from item number string

3. UPC HISTORICAL MATCH
   - Query Airtable for record with same Variant Barcode AND Shopify Product ID populated
   - If match: copy all enrichment fields (title, description, images, etc.) + size fields
   - Condition text re-derived per current record

4. GENDER/SIZE EXTRACTION (SDO/Rebound only)
   - Cascade: existing field → UPC match → Keepa → GoFlow → parser → fallback
   - Write to correct SDO_* fields based on Gender (Male/Female/Unisex/Kids) and Age Range
   - Adult+Unisex writes BOTH Men and Women fields
   - Width defaults to "M"
   - Park if no size found

5. SKIP CLAUDE CHECK
   - If all required enrichment fields already populated for this website → skip Claude
   - Else → continue to Claude

6. EXTERNAL SOURCES
   - GoFlow /products (for ASIN lookup)
   - Keepa by ASIN → UPC → text search fallback

7. CLAUDE ENRICHMENT
   - Single structured JSON response with Title, Description, SEO Description, Category, Images, etc.
   - Per-website prompt variants handle SDO/Rebound vs LTV vs RTV differences

8. WRITE + VALIDATE
   - Write all fields to Airtable
   - Re-fetch to check PIV + VIV formulas
   - If both YES: mark Done + PD Ready Hold = true
   - Else retry (up to 5 attempts) or park with reason
```

## Environment Variables

Required:

- `AIRTABLE_API_KEY`
- `AIRTABLE_BASE_ID` — `app0KyG9OA1w7aZJQ` (Deals Outlet)
- `AIRTABLE_TABLE_ID` — `tbllHd4Hfe156drDD` (Product Data)
- `ANTHROPIC_API_KEY`
- `GOFLOW_SUBDOMAIN` — `lyb`
- `GOFLOW_API_TOKEN`
- `GOFLOW_BETA_CONTACT` — `admin@dealsoutletbrands.com`
- `KEEPA_API_KEY`

Optional:

- `MIN_COST_THRESHOLD` (default `1.00`) — records with cost ≤ this get queued for cost-recovery
- `AIRTABLE_RATE_DELAY_MS` (default `250`) — delay between Airtable writes

Dry-run / operational:

- `DRY_RUN_LIMIT` — when set, worker processes at most N records then exits. Use `20` for initial verification.
- `RESET_PARKED` — when `"true"`, on startup reset Loop Status + Enrichment Attempts on ALL records currently parked as Needs VA. Clears them back into the queue.

## Cost-Recovery Mechanism

Uses GoFlow's Inventory Values report API. Records missing cost accumulate in an in-memory pending list. Batch submits when list reaches 500 records OR 5 minutes elapsed.

Rate limit: 6 reports/hour (GoFlow enforces). Our batching keeps us well under.
Quota: 200,000 records/day (GoFlow enforces). Not a concern for our volume.

Report column we care about: `available_value_average` — the average per-unit cost of available inventory. Matches the "Avg Cost" shown in GoFlow's product UI.

## Airtable Fields We Write

**Loop state:**
- `Loop Status` (singleSelect): Pending | Done | Needs VA
- `Enrichment Attempts` (number): tries so far (max 5)
- `VA Needed` (text): reason if parked
- `PD Ready Hold` (checkbox): flipped true when PIV + VIV both YES

**Enrichment output (varies by website):**
- Title, Description, SEO Description, Shopify Category, Google Category, Material
- Option 1 Value (color), Option 2 Custom Value (size), Option 3 Custom Value (condition for RTV)
- SDO Color, SDO Gender, SDO Age Range, SDO_Men_Size, SDO_Men_Width, (etc.)
- Product Images, Variant Image Index, Price
- AI Status (Complete | Partial | Not Found), AI Missing

**Cost resolution:**
- item_cost (number)
- AI Cost Check (Good | Found | Missing)
- Cost Fix (Inputted | No Data)

## Deploy

1. Make Airtable changes:
   - Edit PIV formula — remove `{SEO Title}` reference
   - Edit PIIV formula — remove SEO Title line
   - Create `Enrichment Path` singleSelect field on AI Logs table with values:
     `upc_match`, `skip_claude_existing`, `claude_full`, `cost_recovery_pending`, `cost_no_data`, `parked`
   - Apply the logger.js patch (see logger-patch.txt) with the field ID

2. Deploy code:
   - Merge branch to `main`
   - Railway auto-deploys
   - Set `DRY_RUN_LIMIT=20` and `RESET_PARKED=true` env vars
   - Inspect 20 processed records in Airtable
   - Remove both env vars → next cycle drains full backlog

## Troubleshooting

- **Cost batches never fire** — check GoFlow API token is valid and `X-Beta-Contact` header is set
- **UPC match never hits** — check `findMatchByUPC` filter; Variant Barcode is a formula field so case-sensitivity matters
- **Every record parks as "Size not extractable"** — check `extractAttributes` cascade; inspect `parseItemNumber` output for your data pattern
- **Claude returns invalid JSON** — prompt may need a structured output retry; current behavior sets AI Status = Not Found and retries next pass

## Files

```
src/
  workers/
    loop.js              ← monolith (enrichment)
    brandResolution.js   ← brand cron
  config/
    fields.js            ← field IDs and enums
    taxonomy.js          ← Shopify → Google category map (generated)
  lib/
    airtable.js          ← shared Airtable helpers (used by brandResolution)
    parser.js            ← item-number parsing (used by both workers)
    logger.js            ← AI Logs writer
    retry.js             ← 429 retry wrapper
    lock.js              ← pipeline semaphore (legacy, always unlocked now)
  prompts/
    enrichmentPrompt.js  ← Claude prompts per website
scripts/
  fetch-taxonomy.js      ← build-time: regenerate taxonomy.js from Shopify
package.json
railway.toml
```
