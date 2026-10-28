-- ============================================================================================================================
-- FILE        : UC15_market_penetration_by_region.sql
-- USE CASE    : UC-15 | Sales Domain | Priority: P2
-- PROJECT     : NordLeaf BI Implementation
-- LAYER       : MGMT (Management / Reporting Layer)
-- DESCRIPTION : Market Penetration Analysis by EU Region
-- AUTHOR      : NordLeaf Data Engineering Team
-- CREATED     : 2024-01-15
-- UPDATED     : 2024-01-15
-- VERSION     : 1.0
-- ============================================================================================================================
--
-- ARCHITECTURE DIAGRAM
-- ============================================================================================================================
--
--   nordleaf_dwh.fact_orders          nordleaf_dwh.dim_customer
--   ┌─────────────────────────┐       ┌──────────────────────────┐
--   │ order_key               │       │ customer_key             │
--   │ customer_key (FK)  ─────┼───────│ customer_name            │
--   │ product_key (FK)        │       │ country                  │
--   │ date_key (FK)           │       │ region                   │
--   │ quantity_tonnes         │       │  - Nordic                │
--   │ unit_price_eur          │       │  - DACH                  │
--   │ total_value_eur         │       │  - UK & Ireland          │
--   │ order_status            │       │  - France & Benelux      │
--   └─────────────────────────┘       │  - Baltics & Eastern EU  │
--              │                      └──────────────────────────┘
--              │                                  │
--   nordleaf_dwh.dim_product          nordleaf_dwh.dim_date
--   ┌─────────────────────────┐       ┌──────────────────────────┐
--   │ product_key (FK)   ─────┼─      │ date_key (FK)            │
--   │ product_type            │       │ year_num                 │
--   │ product_name            │       │ quarter_num              │
--   │  - Newsprint            │       │ month_num                │
--   │  - Kraft Paper          │       │ quarter_label            │
--   │  - Cardboard            │       └──────────────────────────┘
--   │  - Specialty Paper      │
--   │  - Tissue               │
--   └─────────────────────────┘
--              │
--              ▼
--   ┌──────────────────────────────────────────────────────────┐
--   │        nordleaf_mgmt.v_market_penetration                │
--   │            Market Penetration View [MGMT]                │
--   │                                                          │
--   │  Revenue by Region | YoY Growth | Market Share          │
--   │  Top Product per Region | Untapped Potential Flags       │
--   └──────────────────────────────────────────────────────────┘
--              │
--              ▼
--   ┌──────────────────────────┐
--   │   Looker Studio          │
--   │   Sales Map Dashboard    │
--   │   (Sales Leadership)     │
--   └──────────────────────────┘
--
-- ============================================================================================================================
--
-- BUSINESS PROBLEM
-- ============================================================================================================================
--
--   NordLeaf sells paper products (Newsprint, Kraft Paper, Cardboard, Specialty Paper, Tissue) across the EU through
--   18 enterprise customers. Sales leadership needs to understand:
--
--     1. Which EU regions are growing in revenue and volume (YoY)?
--     2. Which products dominate in which markets (product-region affinity)?
--     3. Where does untapped revenue potential exist (low customer count + growing demand)?
--     4. How does NordLeaf's regional revenue share shift over time?
--     5. Which regions deliver the highest revenue per customer (productivity)?
--
--   Customer Regions:
--     - Nordic         : Finland, Sweden, Norway, Denmark
--     - DACH           : Germany, Austria
--     - UK & Ireland   : United Kingdom, Ireland
--     - France & Benelux: France, Belgium, Netherlands, Luxembourg
--     - Baltics & Eastern Europe: Estonia, Latvia, Lithuania, Poland
--
-- ============================================================================================================================
--
-- KEY PERFORMANCE INDICATORS (KPIs)
-- ============================================================================================================================
--
--   KPI 1  │ Total Revenue by Region (EUR)         │ Annual and quarterly breakdown
--   KPI 2  │ YoY Revenue Growth %                  │ Year-over-year revenue change per region
--   KPI 3  │ YoY Volume Growth %                   │ Year-over-year tonnes change per region
--   KPI 4  │ Market Share by Region x Product      │ % of total NordLeaf revenue by region/product
--   KPI 5  │ Revenue per Customer by Region        │ Avg EUR value per active customer in each region
--   KPI 6  │ Top Product per Region                │ Highest-revenue product type in each region/year
--   KPI 7  │ Quarterly Momentum                    │ STRONG GROWTH / GROWING / STABLE / SLOWING / DECLINING
--   KPI 8  │ Untapped Potential Flag               │ Regions with <3 customers but YoY growth > 5%
--
-- ============================================================================================================================
--
-- IMPLEMENTATION STEPS
-- ============================================================================================================================
--
--   Step 1  │ Base CTE         │ Join fact_orders + dim_customer + dim_product + dim_date, filter Delivered last 2 years
--   Step 2  │ Annual Summary   │ GROUP BY year, region, product_type → revenue, tonnes, avg price, customers, orders
--   Step 3  │ YoY Growth       │ LAG(revenue,1) / LAG(tonnes,1) OVER(PARTITION BY region, product_type ORDER BY year)
--   Step 4  │ Market Share     │ SUM(revenue) OVER(PARTITION BY year) → region % of total NordLeaf annual revenue
--   Step 5  │ Top Product      │ RANK() OVER(PARTITION BY year, region ORDER BY revenue DESC), QUALIFY = 1
--   Step 6  │ Quarterly Momentum│ Rolling 4-quarter revenue; acceleration vs prior 3 quarter avg; momentum label
--   Step 7  │ Revenue/Customer │ Revenue per customer by region; ranked for high-value region identification
--   Step 8  │ Untapped Markets │ Regions with customer_count < 3 and yoy_growth > 5% → expansion signals
--   Step 9  │ MGMT View        │ CREATE OR REPLACE VIEW nordleaf_mgmt.v_market_penetration
--
-- ============================================================================================================================


-- ============================================================================================================================
-- SECTION 1: BASE — ORDERS BY CUSTOMER REGION AND PRODUCT (LAST 2 YEARS, DELIVERED ONLY)
-- ============================================================================================================================

WITH regional_orders AS (

  SELECT
    fo.order_key,
    dc.customer_key,
    dc.customer_name,
    dc.country,
    dc.region,                        -- Nordic | DACH | UK & Ireland | France & Benelux | Baltics & Eastern Europe
    dp.product_type,
    dp.product_name,
    dd.year_num,
    dd.quarter_num,
    dd.month_num,
    dd.quarter_label,                 -- e.g. '2023-Q3'
    fo.quantity_tonnes,
    fo.unit_price_eur,
    fo.total_value_eur,
    fo.order_status

  FROM nordleaf_dwh.fact_orders fo
  JOIN nordleaf_dwh.dim_customer dc  ON fo.customer_key = dc.customer_key
  JOIN nordleaf_dwh.dim_product  dp  ON fo.product_key  = dp.product_key
  JOIN nordleaf_dwh.dim_date     dd  ON fo.date_key     = dd.date_key

  WHERE fo.order_status = 'Delivered'
    AND dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)

),


-- ============================================================================================================================
-- SECTION 2: ANNUAL REVENUE SUMMARY BY REGION x PRODUCT TYPE
-- ============================================================================================================================
--   Aggregates delivered orders to annual grain by region and product_type.
--   Provides: total revenue, total tonnes, avg unit price, distinct customer count, order count.

annual_regional_summary AS (

  SELECT
    year_num,
    region,
    product_type,
    SUM(total_value_eur)                            AS total_revenue_eur,
    SUM(quantity_tonnes)                            AS total_tonnes,
    ROUND(AVG(unit_price_eur), 2)                   AS avg_unit_price_eur,
    COUNT(DISTINCT customer_key)                    AS customer_count,
    COUNT(DISTINCT order_key)                       AS order_count,
    ROUND(SUM(total_value_eur) / NULLIF(SUM(quantity_tonnes), 0), 2) AS revenue_per_tonne_eur

  FROM regional_orders

  GROUP BY
    year_num,
    region,
    product_type

),


-- ============================================================================================================================
-- SECTION 3: YEAR-OVER-YEAR GROWTH USING LAG WINDOW FUNCTION
-- ============================================================================================================================
--   For each region + product_type, computes prior year revenue/tonnes using LAG.
--   Derives: yoy_revenue_growth_pct, yoy_tonnes_growth_pct.
--   NULL for the earliest year in the dataset (no prior year to compare against).

yoy_growth AS (

  SELECT
    year_num,
    region,
    product_type,
    total_revenue_eur,
    total_tonnes,
    avg_unit_price_eur,
    customer_count,
    order_count,
    revenue_per_tonne_eur,

    -- Prior year values via LAG
    LAG(total_revenue_eur, 1) OVER (
      PARTITION BY region, product_type
      ORDER BY year_num
    ) AS prior_year_revenue_eur,

    LAG(total_tonnes, 1) OVER (
      PARTITION BY region, product_type
      ORDER BY year_num
    ) AS prior_year_tonnes,

    -- YoY revenue growth %
    ROUND(
      SAFE_DIVIDE(
        total_revenue_eur - LAG(total_revenue_eur, 1) OVER (
          PARTITION BY region, product_type ORDER BY year_num
        ),
        LAG(total_revenue_eur, 1) OVER (
          PARTITION BY region, product_type ORDER BY year_num
        )
      ) * 100,
      2
    ) AS yoy_revenue_growth_pct,

    -- YoY volume (tonnes) growth %
    ROUND(
      SAFE_DIVIDE(
        total_tonnes - LAG(total_tonnes, 1) OVER (
          PARTITION BY region, product_type ORDER BY year_num
        ),
        LAG(total_tonnes, 1) OVER (
          PARTITION BY region, product_type ORDER BY year_num
        )
      ) * 100,
      2
    ) AS yoy_tonnes_growth_pct

  FROM annual_regional_summary

),


-- ============================================================================================================================
-- SECTION 4: REGIONAL MARKET SHARE — REGION % OF TOTAL NORDLEAF ANNUAL REVENUE
-- ============================================================================================================================
--   Uses a window function over the full year to compute each region+product's share
--   of total NordLeaf revenue. Helps leadership identify dominant and underrepresented regions.

market_share AS (

  SELECT
    year_num,
    region,
    product_type,
    total_revenue_eur,
    total_tonnes,
    avg_unit_price_eur,
    customer_count,
    order_count,
    revenue_per_tonne_eur,
    prior_year_revenue_eur,
    prior_year_tonnes,
    yoy_revenue_growth_pct,
    yoy_tonnes_growth_pct,

    -- Total NordLeaf revenue for that year (all regions, all products)
    SUM(total_revenue_eur) OVER (
      PARTITION BY year_num
    ) AS total_nordleaf_revenue_eur,

    -- This region+product's share of total annual revenue
    ROUND(
      SAFE_DIVIDE(total_revenue_eur,
        SUM(total_revenue_eur) OVER (PARTITION BY year_num)
      ) * 100,
      2
    ) AS market_share_pct,

    -- Region-level total revenue (all products combined) for region share calc
    SUM(total_revenue_eur) OVER (
      PARTITION BY year_num, region
    ) AS region_total_revenue_eur,

    -- Region share of annual NordLeaf total (across all products in region)
    ROUND(
      SAFE_DIVIDE(
        SUM(total_revenue_eur) OVER (PARTITION BY year_num, region),
        SUM(total_revenue_eur) OVER (PARTITION BY year_num)
      ) * 100,
      2
    ) AS region_revenue_share_pct

  FROM yoy_growth

),


-- ============================================================================================================================
-- SECTION 5: TOP PRODUCT PER REGION PER YEAR (USING RANK + QUALIFY)
-- ============================================================================================================================
--   Identifies the single highest-revenue product type in each region each year.
--   Uses RANK() window function; QUALIFY filters to rank = 1 (top product only).
--   Joins back to market_share CTE to attach the top_product label.

top_product_per_region AS (

  SELECT
    year_num,
    region,
    product_type                AS top_product_type,
    total_revenue_eur           AS top_product_revenue_eur,
    market_share_pct            AS top_product_market_share_pct,

    RANK() OVER (
      PARTITION BY year_num, region
      ORDER BY total_revenue_eur DESC
    ) AS product_rank_in_region

  FROM market_share

  QUALIFY
    RANK() OVER (
      PARTITION BY year_num, region
      ORDER BY total_revenue_eur DESC
    ) = 1

),


-- ============================================================================================================================
-- SECTION 6: QUARTERLY MOMENTUM — ROLLING 4-QUARTER REVENUE AND ACCELERATION SIGNAL
-- ============================================================================================================================
--   Groups orders to quarterly grain per region. Computes rolling 4Q revenue and
--   acceleration = current Q vs average of prior 3 Qs.
--   Momentum labels: STRONG GROWTH (>10% above avg) | GROWING (5-10%) | STABLE (±5%)
--                    SLOWING (<-5%) | DECLINING (<-10%)

quarterly_base AS (

  SELECT
    year_num,
    quarter_num,
    quarter_label,
    region,
    SUM(total_value_eur)     AS quarterly_revenue_eur,
    SUM(quantity_tonnes)     AS quarterly_tonnes,
    COUNT(DISTINCT order_key) AS quarterly_order_count

  FROM regional_orders
  GROUP BY year_num, quarter_num, quarter_label, region

),

quarterly_momentum AS (

  SELECT
    year_num,
    quarter_num,
    quarter_label,
    region,
    quarterly_revenue_eur,
    quarterly_tonnes,
    quarterly_order_count,

    -- Rolling 4-quarter sum of revenue
    SUM(quarterly_revenue_eur) OVER (
      PARTITION BY region
      ORDER BY year_num, quarter_num
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ) AS rolling_4q_revenue_eur,

    -- Average of the prior 3 quarters
    AVG(quarterly_revenue_eur) OVER (
      PARTITION BY region
      ORDER BY year_num, quarter_num
      ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
    ) AS prior_3q_avg_revenue_eur,

    -- Acceleration: current Q vs prior 3Q avg
    ROUND(
      SAFE_DIVIDE(
        quarterly_revenue_eur - AVG(quarterly_revenue_eur) OVER (
          PARTITION BY region
          ORDER BY year_num, quarter_num
          ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        ),
        AVG(quarterly_revenue_eur) OVER (
          PARTITION BY region
          ORDER BY year_num, quarter_num
          ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
        )
      ) * 100,
      2
    ) AS quarterly_acceleration_pct,

    -- Momentum label based on acceleration vs prior 3Q average
    CASE
      WHEN SAFE_DIVIDE(
             quarterly_revenue_eur - AVG(quarterly_revenue_eur) OVER (
               PARTITION BY region
               ORDER BY year_num, quarter_num
               ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
             ),
             AVG(quarterly_revenue_eur) OVER (
               PARTITION BY region
               ORDER BY year_num, quarter_num
               ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
             )
           ) * 100 > 10  THEN 'STRONG GROWTH'
      WHEN SAFE_DIVIDE(
             quarterly_revenue_eur - AVG(quarterly_revenue_eur) OVER (
               PARTITION BY region
               ORDER BY year_num, quarter_num
               ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
             ),
             AVG(quarterly_revenue_eur) OVER (
               PARTITION BY region
               ORDER BY year_num, quarter_num
               ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
             )
           ) * 100 BETWEEN 5 AND 10 THEN 'GROWING'
      WHEN SAFE_DIVIDE(
             quarterly_revenue_eur - AVG(quarterly_revenue_eur) OVER (
               PARTITION BY region
               ORDER BY year_num, quarter_num
               ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
             ),
             AVG(quarterly_revenue_eur) OVER (
               PARTITION BY region
               ORDER BY year_num, quarter_num
               ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
             )
           ) * 100 BETWEEN -5 AND 5  THEN 'STABLE'
      WHEN SAFE_DIVIDE(
             quarterly_revenue_eur - AVG(quarterly_revenue_eur) OVER (
               PARTITION BY region
               ORDER BY year_num, quarter_num
               ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
             ),
             AVG(quarterly_revenue_eur) OVER (
               PARTITION BY region
               ORDER BY year_num, quarter_num
               ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING
             )
           ) * 100 BETWEEN -10 AND -5 THEN 'SLOWING'
      ELSE 'DECLINING'
    END AS momentum_label,

    -- Latest quarter flag for filtering in dashboards
    CASE
      WHEN ROW_NUMBER() OVER (PARTITION BY region ORDER BY year_num DESC, quarter_num DESC) = 1
      THEN TRUE ELSE FALSE
    END AS is_latest_quarter

  FROM quarterly_base

),


-- ============================================================================================================================
-- SECTION 7: REVENUE PER CUSTOMER BY REGION (CUSTOMER PRODUCTIVITY METRIC)
-- ============================================================================================================================
--   Measures how much revenue each region generates per active customer.
--   Ranks regions from most to least productive to guide sales resource allocation.

customer_productivity AS (

  SELECT
    year_num,
    region,
    SUM(total_revenue_eur)                                          AS region_total_revenue_eur,
    SUM(customer_count)                                             AS region_customer_count,
    ROUND(
      SAFE_DIVIDE(SUM(total_revenue_eur), MAX(customer_count)),
      2
    )                                                               AS revenue_per_customer_eur,

    -- Rank regions by revenue per customer (1 = most productive region)
    RANK() OVER (
      PARTITION BY year_num
      ORDER BY
        SAFE_DIVIDE(SUM(total_revenue_eur), MAX(customer_count)) DESC
    )                                                               AS productivity_rank,

    -- Classify region productivity tier
    CASE
      WHEN SAFE_DIVIDE(SUM(total_revenue_eur), MAX(customer_count)) >=
           PERCENTILE_CONT(
             SAFE_DIVIDE(total_revenue_eur, customer_count), 0.75
           ) OVER (PARTITION BY year_num)
      THEN 'HIGH PRODUCTIVITY'
      WHEN SAFE_DIVIDE(SUM(total_revenue_eur), MAX(customer_count)) >=
           PERCENTILE_CONT(
             SAFE_DIVIDE(total_revenue_eur, customer_count), 0.50
           ) OVER (PARTITION BY year_num)
      THEN 'MEDIUM PRODUCTIVITY'
      ELSE 'LOW PRODUCTIVITY'
    END                                                             AS productivity_tier

  FROM market_share
  GROUP BY year_num, region, total_revenue_eur, customer_count

),


-- ============================================================================================================================
-- SECTION 8: UNTAPPED MARKET POTENTIAL
-- ============================================================================================================================
--   Identifies regions where NordLeaf has fewer than 3 customers (low penetration)
--   but where delivered order volume is growing (YoY growth > 5%).
--   These represent expansion opportunities that Sales leadership should prioritise.

untapped_potential AS (

  SELECT
    ms.year_num,
    ms.region,
    ms.product_type,
    ms.customer_count,
    ms.total_revenue_eur,
    ms.yoy_revenue_growth_pct,
    ms.yoy_tonnes_growth_pct,
    ms.region_revenue_share_pct,

    -- Classify the opportunity level
    CASE
      WHEN ms.customer_count < 3 AND ms.yoy_revenue_growth_pct > 20
        THEN 'HIGH PRIORITY EXPANSION'
      WHEN ms.customer_count < 3 AND ms.yoy_revenue_growth_pct BETWEEN 5 AND 20
        THEN 'MODERATE EXPANSION SIGNAL'
      WHEN ms.customer_count < 3 AND ms.yoy_revenue_growth_pct BETWEEN 0 AND 5
        THEN 'MONITOR - LOW GROWTH'
      ELSE 'ESTABLISHED MARKET'
    END AS expansion_opportunity_label,

    -- Flag for dashboard filtering
    CASE WHEN ms.customer_count < 3 AND ms.yoy_revenue_growth_pct > 5
         THEN TRUE ELSE FALSE
    END AS untapped_potential_flag

  FROM market_share ms

  WHERE ms.customer_count < 3
    AND ms.yoy_revenue_growth_pct > 5

),


-- ============================================================================================================================
-- SECTION 9: FINAL MGMT VIEW ASSEMBLY — COMBINING ALL SECTIONS
-- ============================================================================================================================
--   Joins market_share (primary) with top_product, customer_productivity, and untapped potential.
--   This is the full enriched dataset exposed via the MGMT view.

final_market_penetration AS (

  SELECT
    -- Identifiers and time dimensions
    ms.year_num,
    ms.region,
    ms.product_type,

    -- Volume and revenue metrics
    ms.total_revenue_eur,
    ms.total_tonnes,
    ms.avg_unit_price_eur,
    ms.revenue_per_tonne_eur,
    ms.customer_count,
    ms.order_count,

    -- Prior year comparisons
    ms.prior_year_revenue_eur,
    ms.prior_year_tonnes,

    -- YoY growth metrics
    ms.yoy_revenue_growth_pct,
    ms.yoy_tonnes_growth_pct,

    -- Market share metrics
    ms.total_nordleaf_revenue_eur,
    ms.market_share_pct,
    ms.region_total_revenue_eur,
    ms.region_revenue_share_pct,

    -- Top product per region/year (from top_product_per_region CTE)
    tp.top_product_type,
    tp.top_product_revenue_eur,
    tp.top_product_market_share_pct,

    -- Customer productivity
    cp.revenue_per_customer_eur,
    cp.productivity_rank,
    cp.productivity_tier,

    -- Untapped market flags (NULLs for established markets)
    up.expansion_opportunity_label,
    COALESCE(up.untapped_potential_flag, FALSE)  AS untapped_potential_flag,

    -- Metadata
    CURRENT_TIMESTAMP()                          AS record_created_at,
    'UC-15'                                      AS use_case_id,
    'nordleaf_dwh'                               AS source_schema

  FROM market_share ms

  LEFT JOIN top_product_per_region tp
    ON ms.year_num   = tp.year_num
   AND ms.region     = tp.region

  LEFT JOIN customer_productivity cp
    ON ms.year_num   = cp.year_num
   AND ms.region     = cp.region

  LEFT JOIN untapped_potential up
    ON ms.year_num   = up.year_num
   AND ms.region     = up.region
   AND ms.product_type = up.product_type

)


-- ============================================================================================================================
-- SECTION 9 (CONT.): CREATE MANAGEMENT LAYER VIEW
-- ============================================================================================================================
--   Materialises the final CTE output as a BigQuery view in the nordleaf_mgmt dataset.
--   This view is the source for the Looker Studio Sales Map dashboard.
--
--   Refresh cadence : Daily (driven by scheduled query or Dataform workflow)
--   Access          : nordleaf_mgmt.v_market_penetration
--   Dashboard       : Looker Studio → Sales Map → Regional Revenue & Penetration Tab
--   Consumers       : Sales Leadership, Regional Sales Managers, Finance (read access)

CREATE OR REPLACE VIEW nordleaf_mgmt.v_market_penetration AS (

  WITH regional_orders AS (

    SELECT
      fo.order_key,
      dc.customer_key,
      dc.customer_name,
      dc.country,
      dc.region,
      dp.product_type,
      dp.product_name,
      dd.year_num,
      dd.quarter_num,
      dd.month_num,
      dd.quarter_label,
      fo.quantity_tonnes,
      fo.unit_price_eur,
      fo.total_value_eur,
      fo.order_status

    FROM nordleaf_dwh.fact_orders fo
    JOIN nordleaf_dwh.dim_customer dc  ON fo.customer_key = dc.customer_key
    JOIN nordleaf_dwh.dim_product  dp  ON fo.product_key  = dp.product_key
    JOIN nordleaf_dwh.dim_date     dd  ON fo.date_key     = dd.date_key

    WHERE fo.order_status = 'Delivered'
      AND dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)

  ),

  annual_regional_summary AS (

    SELECT
      year_num,
      region,
      product_type,
      SUM(total_value_eur)                                               AS total_revenue_eur,
      SUM(quantity_tonnes)                                               AS total_tonnes,
      ROUND(AVG(unit_price_eur), 2)                                      AS avg_unit_price_eur,
      COUNT(DISTINCT customer_key)                                       AS customer_count,
      COUNT(DISTINCT order_key)                                          AS order_count,
      ROUND(SUM(total_value_eur) / NULLIF(SUM(quantity_tonnes), 0), 2)  AS revenue_per_tonne_eur

    FROM regional_orders
    GROUP BY year_num, region, product_type

  ),

  yoy_growth AS (

    SELECT
      year_num,
      region,
      product_type,
      total_revenue_eur,
      total_tonnes,
      avg_unit_price_eur,
      customer_count,
      order_count,
      revenue_per_tonne_eur,

      LAG(total_revenue_eur, 1) OVER (
        PARTITION BY region, product_type ORDER BY year_num
      ) AS prior_year_revenue_eur,

      LAG(total_tonnes, 1) OVER (
        PARTITION BY region, product_type ORDER BY year_num
      ) AS prior_year_tonnes,

      ROUND(
        SAFE_DIVIDE(
          total_revenue_eur - LAG(total_revenue_eur, 1) OVER (
            PARTITION BY region, product_type ORDER BY year_num
          ),
          LAG(total_revenue_eur, 1) OVER (
            PARTITION BY region, product_type ORDER BY year_num
          )
        ) * 100, 2
      ) AS yoy_revenue_growth_pct,

      ROUND(
        SAFE_DIVIDE(
          total_tonnes - LAG(total_tonnes, 1) OVER (
            PARTITION BY region, product_type ORDER BY year_num
          ),
          LAG(total_tonnes, 1) OVER (
            PARTITION BY region, product_type ORDER BY year_num
          )
        ) * 100, 2
      ) AS yoy_tonnes_growth_pct

    FROM annual_regional_summary

  ),

  market_share AS (

    SELECT
      *,
      SUM(total_revenue_eur) OVER (PARTITION BY year_num)              AS total_nordleaf_revenue_eur,
      ROUND(
        SAFE_DIVIDE(total_revenue_eur,
          SUM(total_revenue_eur) OVER (PARTITION BY year_num)
        ) * 100, 2
      )                                                                  AS market_share_pct,
      SUM(total_revenue_eur) OVER (PARTITION BY year_num, region)      AS region_total_revenue_eur,
      ROUND(
        SAFE_DIVIDE(
          SUM(total_revenue_eur) OVER (PARTITION BY year_num, region),
          SUM(total_revenue_eur) OVER (PARTITION BY year_num)
        ) * 100, 2
      )                                                                  AS region_revenue_share_pct

    FROM yoy_growth

  ),

  top_product_per_region AS (

    SELECT
      year_num,
      region,
      product_type           AS top_product_type,
      total_revenue_eur      AS top_product_revenue_eur,
      market_share_pct       AS top_product_market_share_pct

    FROM market_share

    QUALIFY
      RANK() OVER (
        PARTITION BY year_num, region
        ORDER BY total_revenue_eur DESC
      ) = 1

  ),

  customer_productivity AS (

    SELECT
      year_num,
      region,
      ROUND(
        SAFE_DIVIDE(SUM(total_revenue_eur), MAX(customer_count)), 2
      )                                                                  AS revenue_per_customer_eur,
      RANK() OVER (
        PARTITION BY year_num
        ORDER BY SAFE_DIVIDE(SUM(total_revenue_eur), MAX(customer_count)) DESC
      )                                                                  AS productivity_rank,
      CASE
        WHEN SAFE_DIVIDE(SUM(total_revenue_eur), MAX(customer_count)) >=
             AVG(SAFE_DIVIDE(total_revenue_eur, customer_count)) OVER (PARTITION BY year_num) * 1.25
        THEN 'HIGH PRODUCTIVITY'
        WHEN SAFE_DIVIDE(SUM(total_revenue_eur), MAX(customer_count)) >=
             AVG(SAFE_DIVIDE(total_revenue_eur, customer_count)) OVER (PARTITION BY year_num)
        THEN 'MEDIUM PRODUCTIVITY'
        ELSE 'LOW PRODUCTIVITY'
      END                                                                AS productivity_tier

    FROM market_share
    GROUP BY year_num, region, total_revenue_eur, customer_count

  )

  SELECT
    ms.year_num,
    ms.region,
    ms.product_type,
    ms.total_revenue_eur,
    ms.total_tonnes,
    ms.avg_unit_price_eur,
    ms.revenue_per_tonne_eur,
    ms.customer_count,
    ms.order_count,
    ms.prior_year_revenue_eur,
    ms.prior_year_tonnes,
    ms.yoy_revenue_growth_pct,
    ms.yoy_tonnes_growth_pct,
    ms.total_nordleaf_revenue_eur,
    ms.market_share_pct,
    ms.region_total_revenue_eur,
    ms.region_revenue_share_pct,
    tp.top_product_type,
    tp.top_product_revenue_eur,
    tp.top_product_market_share_pct,
    cp.revenue_per_customer_eur,
    cp.productivity_rank,
    cp.productivity_tier,
    CASE
      WHEN ms.customer_count < 3 AND ms.yoy_revenue_growth_pct > 20 THEN 'HIGH PRIORITY EXPANSION'
      WHEN ms.customer_count < 3 AND ms.yoy_revenue_growth_pct BETWEEN 5 AND 20 THEN 'MODERATE EXPANSION SIGNAL'
      WHEN ms.customer_count < 3 THEN 'MONITOR - LOW GROWTH'
      ELSE 'ESTABLISHED MARKET'
    END                                                                  AS expansion_opportunity_label,
    CASE WHEN ms.customer_count < 3 AND ms.yoy_revenue_growth_pct > 5
         THEN TRUE ELSE FALSE END                                        AS untapped_potential_flag,
    CURRENT_TIMESTAMP()                                                  AS record_created_at,
    'UC-15'                                                              AS use_case_id

  FROM market_share ms

  LEFT JOIN top_product_per_region tp
    ON ms.year_num = tp.year_num AND ms.region = tp.region

  LEFT JOIN customer_productivity cp
    ON ms.year_num = cp.year_num AND ms.region = cp.region

  ORDER BY
    ms.year_num     DESC,
    ms.region       ASC,
    ms.product_type ASC

);

-- ============================================================================================================================
-- END OF FILE: UC15_market_penetration_by_region.sql
-- Use Case : UC-15 | Sales Domain | Priority P2
-- View     : nordleaf_mgmt.v_market_penetration
-- Dashboard: Looker Studio → Sales Map
-- ============================================================================================================================
