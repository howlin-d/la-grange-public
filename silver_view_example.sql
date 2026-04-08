-- silver_distributor_digital.sql  [REDACTED]
-- ══════════════════════════════════════════════════════════════════════════
-- Standardized distributor digital royalties (dbt view).
--
-- Source:  Bronze.distributor_digital  (~1.4M rows)
--
-- Key patterns demonstrated:
--   1. Multi-pass SKU resolution (product ID → catalog number → canonical)
--   2. GL account derivation (format + transaction type → P&L account)
--   3. Quantity sign sync (align quantity sign to net amount)
--   4. Date architecture (payout / statement / consumption)
--   5. QB transaction type classification (Sales Receipt vs Refund)
--
-- Output: 28-column standard schema consumed by Gold layer.
-- ══════════════════════════════════════════════════════════════════════════

{{
  config(
    materialized = 'view',
    schema       = 'Silver',
    tags         = ['silver', 'royalties', 'distributor']
  )
}}

WITH bronze AS (
    SELECT *
    FROM {{ source('bronze_royalties', 'distributor_digital') }}
),

-- ═══════════════════════════════════════════════════════════════════════
-- SKU RESOLUTION
-- Distributors use their own product IDs. We need to resolve each to
-- the label's canonical SKU via a multi-source lookup table.
-- Sources: distributor catalog export + label metadata + manual overrides
-- ═══════════════════════════════════════════════════════════════════════
sku_lookup AS (
    -- Union of all known SKU mappings (distributor IDs → canonical SKUs)
    SELECT DISTINCT [Original_SKU], [SKU]
    FROM {{ ref('silver_distributor_metadata') }}
    WHERE [Original_SKU] IS NOT NULL AND [SKU] IS NOT NULL

    UNION

    SELECT DISTINCT [Original_SKU], [SKU]
    FROM {{ ref('silver_label_metadata') }}
    WHERE [Original_SKU] IS NOT NULL AND [SKU] IS NOT NULL

    UNION

    -- Manual overrides from admin UI (for items that can't be auto-resolved)
    SELECT DISTINCT [Original_SKU], [Canonical_SKU] AS [SKU]
    FROM {{ source('bronze_admin', 'manual_sku_overrides') }}
    WHERE [Original_SKU] IS NOT NULL AND [Canonical_SKU] IS NOT NULL
),

-- ── Pass 1: exact match on distributor's Product_ID ──────────────────
pid_match AS (
    SELECT
        b.[row_key],
        lk.[SKU] AS canonical_sku
    FROM bronze b
    INNER JOIN sku_lookup lk
        ON UPPER(LTRIM(RTRIM(b.[Product_ID]))) = UPPER(LTRIM(RTRIM(lk.[Original_SKU])))
    WHERE b.[Product_ID] IS NOT NULL
      AND LTRIM(RTRIM(b.[Product_ID])) != ''
),

-- ── Pass 2: fallback match on Catalog_Number (rows not matched above)
cat_match AS (
    SELECT
        b.[row_key],
        lk.[SKU] AS canonical_sku
    FROM bronze b
    INNER JOIN sku_lookup lk
        ON UPPER(LTRIM(RTRIM(b.[Catalog_Number]))) = UPPER(LTRIM(RTRIM(lk.[Original_SKU])))
    WHERE b.[row_key] NOT IN (SELECT [row_key] FROM pid_match)
      AND b.[Catalog_Number] IS NOT NULL
      AND LTRIM(RTRIM(b.[Catalog_Number])) != ''
),

-- ── Resolved: COALESCE pass 1 → pass 2, tag which arm matched ────────
resolved AS (
    SELECT
        b.[row_key],
        COALESCE(pm.canonical_sku, cm.canonical_sku)  AS canonical_sku,
        CASE
            WHEN pm.canonical_sku IS NOT NULL THEN 'product_id'
            WHEN cm.canonical_sku IS NOT NULL THEN 'catalog_number'
            ELSE NULL
        END                                            AS match_arm
    FROM bronze b
    LEFT JOIN pid_match pm  ON b.[row_key] = pm.[row_key]
    LEFT JOIN cat_match cm  ON b.[row_key] = cm.[row_key]
),

-- ── Label metadata enrichment (one row per canonical SKU) ────────────
label_meta AS (
    SELECT DISTINCT
        [SKU], [Artist], [Item], [CAT_Num], [Format], [Class]
    FROM {{ ref('silver_label_metadata') }}
    WHERE [SKU] IS NOT NULL
)


-- ═══════════════════════════════════════════════════════════════════════
-- FINAL SELECT — 28-column standard output
-- ═══════════════════════════════════════════════════════════════════════
SELECT
    b.[row_key],
    CAST('distributor_digital' AS NVARCHAR(50))  AS data_source,

    -- ── DATE ARCHITECTURE ─────────────────────────────────────────────
    -- Three dates per row:
    --   payout_date:      when money was received (drives accounting period)
    --   statement_date:   distributor reporting period end
    --   consumption_date: when the listen/purchase actually happened
    TRY_CONVERT(DATE, b.[datePaidlast])          AS payout_date,
    EOMONTH(TRY_CONVERT(DATE, b.[statement_period]))
                                                  AS statement_date,
    CASE
        WHEN b.[Invoice_Description] IS NOT NULL
             AND PATINDEX('[0-9][0-9][0-9][0-9].[0-9][0-9]%', b.[Invoice_Description]) = 1
        THEN TRY_CONVERT(DATETIME, LEFT(b.[Invoice_Description], 7) + '.01')
        ELSE NULL
    END                                           AS consumption_date,

    -- ── SKU RESOLUTION RESULTS ────────────────────────────────────────
    r.canonical_sku,
    r.match_arm,
    m.[Artist]                                    AS label_artist,
    m.[Item]                                      AS label_item,
    m.[CAT_Num]                                   AS label_cat_num,
    m.[Format]                                    AS label_format,
    m.[Class]                                     AS label_class,

    -- ── QUANTITY SIGN SYNC ────────────────────────────────────────────
    -- Distributors sometimes report absolute quantities regardless of
    -- refund/credit status. We sync the quantity sign to net_amount
    -- so debits and credits are consistent downstream.
    CASE
        WHEN b.[Label_Net_Total] < 0  THEN -ABS(b.[Quantity])
        WHEN b.[Label_Net_Total] >= 0 THEN  ABS(b.[Quantity])
        ELSE b.[Quantity]
    END                                           AS quantity,

    -- ── FINANCIALS ────────────────────────────────────────────────────
    b.[Total_Gross_Income]                        AS gross_amount,
    CAST(NULL AS DECIMAL(18,2))                   AS discount_amount,
    b.[Label_Net_Total]                           AS net_amount,
    CAST(NULL AS DECIMAL(18,2))                   AS tax_amount,
    CAST(NULL AS DECIMAL(18,2))                   AS shipping_amount,
    b.[Distribution_Fee_Total]                    AS fees,

    b.[Country_Code]                              AS country_code,
    CAST(NULL AS NVARCHAR(100))                   AS region,

    -- ── SOURCE IDENTIFIERS (preserved for audit trail) ────────────────
    b.[Catalog_Number]                            AS catalog_number,
    b.[UPC]                                       AS upc,
    CAST(NULL AS NVARCHAR(50))                    AS isrc,

    -- ── GL ACCOUNT DERIVATION ─────────────────────────────────────────
    -- Format-driven routing to P&L accounts, with a streaming override:
    --   Transaction_Type 'Track'/'Album' = downloads → format-based account
    --   Everything else (Stream, Cloud, Radio, etc.) = streaming → override
    -- This is the critical accounting decision — gets it wrong and the
    -- entire P&L is misstated.
    CASE
        WHEN b.[Transaction_Type] IN ('Track', 'Album')
            THEN fs.[Income_Account]              -- format-based (e.g. Income from Digital Sales)
        WHEN b.[Transaction_Type] IS NOT NULL
            THEN 'Income from Streaming'          -- streaming override
        ELSE fs.[Income_Account]                  -- fallback to format default
    END                                           AS gl_account,

    b.[Title_Description]                         AS memo_description,
    b.[Internal_Invoice_Number]                   AS invoice_number,
    b.[Transaction_Type]                          AS transaction_type,
    b.[Customer]                                  AS customer_name,
    CAST(NULL AS NVARCHAR(255))                   AS customer_email,

    -- ── QB TRANSACTION TYPE ───────────────────────────────────────────
    -- Negative net = refund/credit; positive = income.
    -- Determines how row flows to general ledger.
    CASE
        WHEN b.[Label_Net_Total] < 0 THEN 'Refund Receipt'
        ELSE 'Sales Receipt'
    END                                           AS qb_txn_type

FROM bronze b
LEFT JOIN resolved r    ON b.[row_key] = r.[row_key]
LEFT JOIN label_meta m  ON r.canonical_sku = m.[SKU]
LEFT JOIN {{ source('bronze_reference', 'ref_format_specs') }} fs
    ON COALESCE(m.[Format], 'UNKNOWN') = fs.[Format]

-- Exclude neighboring rights rows embedded in pre-2022 digital files
-- (these are routed to a separate Silver view for NR reporting)
WHERE (b.[Transaction_Type] != 'Radio (Broadcasting Statutory)'
       OR b.[Transaction_Type] IS NULL)
