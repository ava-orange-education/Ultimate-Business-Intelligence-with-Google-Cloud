-- =============================================================================
-- USE CASE: UC-09 | Supplier Delivery Reliability & Lead Time Analysis
-- Domain   : Supply Chain
-- Priority : P2 (High Business Value)
-- Author   : NordLeaf Data Engineering Team
-- Created  : 2026-02-23
-- Platform : Google BigQuery
-- =============================================================================
--
-- ARCHITECTURE DIAGRAM
-- ---------------------
--
--   GCS Raw Deliveries
--   (gs://nordleaf-raw/deliveries/)
--        |
--        v  (Dataflow / Pub/Sub ingest)
--   STG Layer
--   nordleaf_stg.stg_deliveries
--        |
--        v  (dbt / scheduled transform)
--   DWH Layer
--   nordleaf_dwh.fact_deliveries
--   nordleaf_dwh.dim_supplier
--   nordleaf_dwh.dim_date
--        |
--        v  (this SQL)
--   Supplier Delivery KPI View
--   nordleaf_mgmt.v_supplier_delivery_kpi
--        |
--        v
--   Looker Studio
--   Supplier Performance Dashboard
--   (Procurement, Mill Supply Planning, Finance)
--
-- =============================================================================
--
-- BUSINESS PROBLEM
-- ----------------
-- NordLeaf procures from 12 active suppliers across 4 material categories:
--   - Pulp       : Primary raw material; 60% of input cost
--   - Chemicals  : Bleaching agents, coatings; safety-critical SLAs
--   - Energy     : Contracted biomass and gas; production continuity
--   - Packaging  : Reel wrapping, pallets; downstream dispatch impact
--
-- The current procurement team tracks deliveries manually in spreadsheets,
-- resulting in a 3-day lag before late deliveries are reported to mill
-- planning. A single late pulp delivery can idle an entire paper machine for
-- 4-8 hours, costing NordLeaf approximately EUR 18,000–35,000 per incident.
--
-- UC-09 automates delivery reliability tracking with:
--   - Real-time SLA breach detection per material category
--   - Quarterly on-time delivery (OTD) rates per supplier
--   - Lead time distribution statistics (mean, stddev, P90)
--   - Quarter-over-quarter trend analysis
--   - Automated procurement action flags (URGENT / WATCH / STANDARD)
--
-- =============================================================================
--
-- SLA TARGETS BY MATERIAL TYPE
-- ----------------------------
--
--  Material Type | SLA Days | Rationale
--  --------------|----------|------------------------------------------
--  Pulp          | 7 days   | Mill buffer stock covers max 7 days
--  Chemicals     | 5 days   | Safety reorder point at 5-day inventory
--  Energy        | 3 days   | Contracted delivery window; grid fallback
--  Packaging     | 10 days  | Lower criticality; longer planning horizon
--  Other         | 14 days  | Default for miscellaneous / project items
--
-- =============================================================================
--
-- KEY PERFORMANCE INDICATORS
-- --------------------------
--
--  KPI                    | Calculation                          | Target
--  -----------------------|--------------------------------------|----------
--  OTD %                  | on_time_deliveries / total * 100     | ≥ 95%
--  Avg Lead Time (days)   | AVG(actual - order date)             | <= SLA
--  Lead Time Std Dev      | STDDEV(lead_time_days)               | < 2 days
--  Lead Time P90 Buffer   | 90th percentile lead time            | Benchmark
--  Breach Count           | COUNT where lead_time > sla_days     | 0 target
--  Minor Delay Count      | 1-2 days over SLA                    | < 5%
--  Severe Delay Count     | 6-10 days over SLA                   | 0
--  Critical Delay Count   | > 10 days over SLA                   | 0
--  QoQ OTD Trend          | current_q OTD - prior_q OTD          | Positive
--
-- =============================================================================
--
-- IMPLEMENTATION STEPS
-- --------------------
--  Step 1 : Join fact_deliveries to dim_supplier and dim_date for the
--           rolling 12-month analytical window
--  Step 2 : Compute lead_time_days and SLA benchmark per material type
--           using a CASE expression (avoids a separate SLA lookup table)
--  Step 3 : Flag each delivery as on-time or late; compute late_by_days
--           and classify delay severity into 5 buckets
--  Step 4 : Aggregate to quarterly KPIs per supplier: OTD rate, lead time
--           distribution statistics, breach counts by severity band
--  Step 5 : Classify supplier into performance rating (EXCELLENT → POOR)
--  Step 6 : Apply LAG() over quarterly partitions for QoQ trend detection
--           and compute 3-quarter rolling OTD average
--  Step 7 : Derive procurement action flag from combined (rating, trend)
--  Step 8 : Analyse delivery reliability at mill-receiving level to expose
--           which mills are disproportionately affected by poor service
--  Step 9 : Build Top 5 / Bottom 5 supplier leaderboard for executive view
--  Step 10: Persist as MGMT view for Looker Studio Supplier Dashboard
--
-- EXPECTED OUTPUT
-- ---------------
--  Primary result set: one row per supplier × quarter with full KPI suite.
--  Supporting queries expose mill-level reliability and leaderboard views.
--  The MGMT view is materialized daily; row count ≈ 12 suppliers × 4 qtrs
--  = ~48 rows per rolling year. Looker Studio uses the view as a blended
--  source with the supplier scorecard report page.
--
-- =============================================================================


-- =============================================================================
-- SECTION 1 - Delivery Base with SLA Lookup
-- Granularity: one row per individual delivery event
-- Window: rolling 12 months from current date
-- SLA days injected inline via CASE to avoid a separate reference table
-- =============================================================================

WITH delivery_base AS (

  SELECT
    fd.delivery_key,
    fd.supplier_key,
    ds.supplier_name,
    ds.country,
    ds.material_type,

    -- Date fields
    fd.order_date,
    fd.actual_delivery_date,
    dd.year_num,
    dd.quarter_num,
    dd.month_num,
    dd.full_date,

    -- Lead time: calendar days from order placement to physical receipt
    DATE_DIFF(fd.actual_delivery_date, fd.order_date, DAY)
                                                  AS lead_time_days,

    -- SLA benchmark derived from material category
    CASE ds.material_type
      WHEN 'Pulp'      THEN 7
      WHEN 'Chemicals' THEN 5
      WHEN 'Energy'    THEN 3
      WHEN 'Packaging' THEN 10
      ELSE                  14          -- Default SLA for Other / unclassified
    END                                           AS sla_days,

    -- Volume and cost
    fd.quantity_tonnes,
    fd.unit_cost_eur,
    ROUND(fd.quantity_tonnes * fd.unit_cost_eur, 2)
                                                  AS total_cost_eur,

    -- Quality grading of delivered material (A/B/C, mirrors production grade)
    fd.quality_grade,

    -- Mill that received the delivery (links via fact_deliveries.mill_key)
    fd.mill_key

  FROM nordleaf_dwh.fact_deliveries   fd
  JOIN nordleaf_dwh.dim_supplier      ds  ON fd.supplier_key = ds.supplier_key
  JOIN nordleaf_dwh.dim_date          dd  ON fd.date_key     = dd.date_key

  WHERE
    dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    AND fd.actual_delivery_date IS NOT NULL        -- Exclude in-transit (no receipt yet)
    AND fd.quantity_tonnes > 0                     -- Exclude cancelled / zero-qty lines

),


-- =============================================================================
-- SECTION 2 - On-Time Flag, Delay Days, and Severity Classification
-- on_time_flag = 1 if delivered within SLA, else 0
-- late_by_days capped at 0 for on-time deliveries (no negative lateness)
-- delay_severity provides 5-bucket categorical label for dashboards
-- =============================================================================

delivery_flagged AS (

  SELECT
    *,

    -- Binary on-time flag (1 = met SLA, 0 = breached SLA)
    IF(lead_time_days <= sla_days, 1, 0)          AS on_time_flag,

    -- Days late (0 if on time; positive integer if breached)
    GREATEST(lead_time_days - sla_days, 0)        AS late_by_days,

    -- Lead time headroom (negative = late; positive = delivered early)
    sla_days - lead_time_days                     AS sla_buffer_days,

    -- Delay severity classification for breach analysis
    CASE
      WHEN lead_time_days <= sla_days                                THEN 'ON TIME'
      WHEN (lead_time_days - sla_days) BETWEEN 1 AND 2              THEN 'MINOR DELAY'
      WHEN (lead_time_days - sla_days) BETWEEN 3 AND 5              THEN 'MODERATE DELAY'
      WHEN (lead_time_days - sla_days) BETWEEN 6 AND 10             THEN 'SEVERE DELAY'
      WHEN (lead_time_days - sla_days) > 10                         THEN 'CRITICAL DELAY'
      ELSE 'UNCLASSIFIED'
    END                                           AS delay_severity,

    -- Numeric severity for ordering (5 = worst)
    CASE
      WHEN lead_time_days <= sla_days                                THEN 1
      WHEN (lead_time_days - sla_days) BETWEEN 1 AND 2              THEN 2
      WHEN (lead_time_days - sla_days) BETWEEN 3 AND 5              THEN 3
      WHEN (lead_time_days - sla_days) BETWEEN 6 AND 10             THEN 4
      WHEN (lead_time_days - sla_days) > 10                         THEN 5
      ELSE 0
    END                                           AS delay_severity_rank

  FROM delivery_base

),


-- =============================================================================
-- SECTION 3 - Quarterly KPIs Per Supplier
-- Aggregates to supplier × year × quarter granularity
-- Includes statistical distribution metrics for lead time
-- =============================================================================

supplier_quarterly_kpis AS (

  SELECT
    supplier_key,
    supplier_name,
    country,
    material_type,
    year_num,
    quarter_num,

    -- Formatted quarter label for display
    CONCAT('Q', CAST(quarter_num AS STRING), '-', CAST(year_num AS STRING))
                                                  AS quarter_label,

    -- Volume
    COUNT(delivery_key)                           AS total_deliveries,
    SUM(quantity_tonnes)                          AS total_tonnes,
    ROUND(SUM(total_cost_eur), 2)                 AS total_cost_eur,

    -- On-time delivery rate (core KPI)
    SUM(on_time_flag)                             AS on_time_count,
    COUNT(delivery_key) - SUM(on_time_flag)       AS late_count,
    ROUND(
      SAFE_DIVIDE(SUM(on_time_flag), COUNT(delivery_key)) * 100,
      2
    )                                             AS on_time_rate_pct,

    -- Lead time distribution statistics
    ROUND(AVG(lead_time_days),    2)              AS avg_lead_time_days,
    ROUND(STDDEV(lead_time_days), 2)              AS stddev_lead_time_days,
    MIN(lead_time_days)                           AS min_lead_time_days,
    MAX(lead_time_days)                           AS max_lead_time_days,

    -- Median lead time using approximate quantiles (bucket 5 of 10 = P50)
    APPROX_QUANTILES(lead_time_days, 10)[OFFSET(5)]
                                                  AS median_lead_time_days,

    -- P90 buffer: 90th percentile lead time (use for safety stock planning)
    APPROX_QUANTILES(lead_time_days, 10)[OFFSET(9)]
                                                  AS p90_lead_time_days,

    -- Breach counts by severity band
    COUNTIF(delay_severity = 'ON TIME')           AS cnt_on_time,
    COUNTIF(delay_severity = 'MINOR DELAY')       AS cnt_minor_delay,
    COUNTIF(delay_severity = 'MODERATE DELAY')    AS cnt_moderate_delay,
    COUNTIF(delay_severity = 'SEVERE DELAY')      AS cnt_severe_delay,
    COUNTIF(delay_severity = 'CRITICAL DELAY')    AS cnt_critical_delay,

    -- SLA days reference for this supplier's material type
    MAX(sla_days)                                 AS sla_days,

    -- Total cost of late deliveries (for financial impact reporting)
    ROUND(
      SUM(CASE WHEN on_time_flag = 0 THEN total_cost_eur ELSE 0 END),
      2
    )                                             AS late_delivery_cost_eur,

    -- Average quality grade of deliveries (A=3, B=2, C=1 for scoring)
    ROUND(
      AVG(
        CASE quality_grade
          WHEN 'A' THEN 3
          WHEN 'B' THEN 2
          WHEN 'C' THEN 1
          ELSE          2   -- Default B if not graded
        END
      ), 2
    )                                             AS avg_quality_score

  FROM delivery_flagged

  GROUP BY
    supplier_key,
    supplier_name,
    country,
    material_type,
    year_num,
    quarter_num

),


-- =============================================================================
-- SECTION 4 - Supplier Performance Rating
-- Derived from quarterly OTD rate against 4-tier scale
-- Used as input to procurement action flag in Section 6
-- =============================================================================

supplier_rated AS (

  SELECT
    *,

    CASE
      WHEN on_time_rate_pct >= 95 THEN 'EXCELLENT'
      WHEN on_time_rate_pct >= 85 THEN 'GOOD'
      WHEN on_time_rate_pct >= 75 THEN 'ACCEPTABLE'
      ELSE                             'POOR'
    END                                           AS performance_rating,

    -- Numeric rating for window function ordering
    CASE
      WHEN on_time_rate_pct >= 95 THEN 4
      WHEN on_time_rate_pct >= 85 THEN 3
      WHEN on_time_rate_pct >= 75 THEN 2
      ELSE                             1
    END                                           AS performance_rating_rank

  FROM supplier_quarterly_kpis

),


-- =============================================================================
-- SECTION 5 - Quarter-over-Quarter Trend Analysis
-- LAG(1) = immediately preceding quarter
-- LAG(2) = two quarters back (used for acceleration check)
-- Rolling 3-quarter average smooths seasonal variation in delivery patterns
-- Trend direction classified as IMPROVING / DECLINING / STABLE
-- =============================================================================

supplier_qoq_trend AS (

  SELECT
    *,

    -- Prior quarter OTD rate
    LAG(on_time_rate_pct, 1) OVER (
      PARTITION BY supplier_key
      ORDER BY year_num, quarter_num
    )                                             AS prior_q_on_time_rate_pct,

    -- Two quarters back OTD rate (for acceleration)
    LAG(on_time_rate_pct, 2) OVER (
      PARTITION BY supplier_key
      ORDER BY year_num, quarter_num
    )                                             AS two_q_prior_on_time_rate_pct,

    -- Prior quarter avg lead time
    LAG(avg_lead_time_days, 1) OVER (
      PARTITION BY supplier_key
      ORDER BY year_num, quarter_num
    )                                             AS prior_q_avg_lead_time,

    -- QoQ delta in OTD rate (positive = improvement)
    ROUND(
      on_time_rate_pct - LAG(on_time_rate_pct, 1) OVER (
        PARTITION BY supplier_key
        ORDER BY year_num, quarter_num
      ),
      2
    )                                             AS qoq_otd_delta_pct,

    -- Rolling 3-quarter average OTD rate
    ROUND(
      AVG(on_time_rate_pct) OVER (
        PARTITION BY supplier_key
        ORDER BY year_num, quarter_num
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ),
      2
    )                                             AS rolling_3q_avg_otd_pct,

    -- Rolling 3-quarter average lead time
    ROUND(
      AVG(avg_lead_time_days) OVER (
        PARTITION BY supplier_key
        ORDER BY year_num, quarter_num
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
      ),
      2
    )                                             AS rolling_3q_avg_lead_time,

    -- Trend direction based on QoQ delta thresholds
    CASE
      WHEN (on_time_rate_pct - LAG(on_time_rate_pct, 1) OVER (
              PARTITION BY supplier_key ORDER BY year_num, quarter_num
            )) > 2  THEN 'IMPROVING'
      WHEN (on_time_rate_pct - LAG(on_time_rate_pct, 1) OVER (
              PARTITION BY supplier_key ORDER BY year_num, quarter_num
            )) < -2 THEN 'DECLINING'
      ELSE               'STABLE'
    END                                           AS trend_direction,

    -- Rank supplier within its material type for current and prior quarter
    RANK() OVER (
      PARTITION BY material_type, year_num, quarter_num
      ORDER BY on_time_rate_pct DESC
    )                                             AS rank_within_material_type,

    -- Overall rank across all suppliers for the quarter
    RANK() OVER (
      PARTITION BY year_num, quarter_num
      ORDER BY on_time_rate_pct DESC
    )                                             AS overall_rank_this_quarter

  FROM supplier_rated

),


-- =============================================================================
-- SECTION 6 - Procurement Action Flag
-- Combines performance rating and trend direction to prescribe action:
--   URGENT  : POOR rating AND DECLINING trend      → schedule supplier review
--   WATCH   : ACCEPTABLE rating AND DECLINING       → monthly check-in
--   STANDARD: GOOD or EXCELLENT in any trend        → routine monitoring
-- This flag drives the action column in Looker Studio's supplier scorecard
-- =============================================================================

supplier_action_flagged AS (

  SELECT
    *,

    CASE
      -- Highest severity: poor performance that is getting worse
      WHEN performance_rating = 'POOR'
        AND trend_direction = 'DECLINING'
        THEN 'URGENT - SCHEDULE SUPPLIER REVIEW'

      -- High severity: acceptable performance deteriorating
      WHEN performance_rating = 'ACCEPTABLE'
        AND trend_direction = 'DECLINING'
        THEN 'WATCH - MONTHLY CHECK-IN'

      -- Moderate: poor but at least stabilising
      WHEN performance_rating = 'POOR'
        AND trend_direction = 'STABLE'
        THEN 'WATCH - MONTHLY CHECK-IN'

      -- Critical delays present regardless of overall OTD
      WHEN cnt_critical_delay >= 2
        THEN 'WATCH - REVIEW CRITICAL DELAY ROOT CAUSE'

      -- Good performance trending down (early warning)
      WHEN performance_rating = 'GOOD'
        AND trend_direction = 'DECLINING'
        AND qoq_otd_delta_pct < -5
        THEN 'WATCH - MONITOR CLOSELY'

      -- Strong performance
      WHEN performance_rating IN ('GOOD', 'EXCELLENT')
        THEN 'STANDARD MONITORING'

      -- Default
      ELSE 'STANDARD MONITORING'
    END                                           AS procurement_action,

    -- Numeric priority for sorting action items (1 = most urgent)
    CASE
      WHEN performance_rating = 'POOR'       AND trend_direction = 'DECLINING' THEN 1
      WHEN performance_rating = 'ACCEPTABLE' AND trend_direction = 'DECLINING' THEN 2
      WHEN performance_rating = 'POOR'       AND trend_direction = 'STABLE'    THEN 2
      WHEN cnt_critical_delay >= 2                                              THEN 3
      WHEN performance_rating = 'GOOD'       AND trend_direction = 'DECLINING' THEN 4
      ELSE                                                                           5
    END                                           AS action_priority

  FROM supplier_qoq_trend

)


-- =============================================================================
-- SECTION 7 - Per-Mill Delivery Reliability
-- Identifies which NordLeaf mills receive the worst delivery service.
-- Mill planners use this to adjust safety stock thresholds and buffer
-- lead times in their MRP settings.
-- =============================================================================

SELECT
  df.mill_key,
  dm.mill_name,
  df.material_type,
  df.year_num,
  df.quarter_num,

  COUNT(df.delivery_key)                          AS total_deliveries,
  SUM(df.on_time_flag)                            AS on_time_deliveries,
  ROUND(
    SAFE_DIVIDE(SUM(df.on_time_flag), COUNT(df.delivery_key)) * 100,
    2
  )                                               AS mill_on_time_rate_pct,

  ROUND(AVG(df.lead_time_days), 2)                AS avg_lead_time_days,
  COUNTIF(df.delay_severity IN ('SEVERE DELAY', 'CRITICAL DELAY'))
                                                  AS severe_critical_breaches,

  -- Rank mills by on-time rate (1 = best-served mill)
  RANK() OVER (
    PARTITION BY df.year_num, df.quarter_num, df.material_type
    ORDER BY SAFE_DIVIDE(SUM(df.on_time_flag), COUNT(df.delivery_key)) DESC
  )                                               AS rank_by_otd_rate

FROM delivery_flagged df
JOIN nordleaf_dwh.dim_mill dm ON df.mill_key = dm.mill_key

GROUP BY
  df.mill_key,
  dm.mill_name,
  df.material_type,
  df.year_num,
  df.quarter_num

ORDER BY
  df.year_num DESC,
  df.quarter_num DESC,
  mill_on_time_rate_pct ASC;


-- =============================================================================
-- SECTION 8 - Top 5 / Bottom 5 Supplier Leaderboard
-- Snapshot for the most recently completed quarter.
-- Surfaces champions and underperformers for the executive summary page
-- of the Looker Studio Supplier Performance Dashboard.
-- =============================================================================

-- Top 5 Suppliers (Best On-Time Delivery)
SELECT
  'TOP 5'                                         AS leaderboard_tier,
  supplier_name,
  country,
  material_type,
  quarter_label,
  total_deliveries,
  on_time_rate_pct,
  avg_lead_time_days,
  stddev_lead_time_days,
  p90_lead_time_days,
  performance_rating,
  trend_direction,
  procurement_action,
  overall_rank_this_quarter

FROM supplier_action_flagged

WHERE
  year_num    = EXTRACT(YEAR    FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 QUARTER))
  AND quarter_num = EXTRACT(QUARTER FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 QUARTER))

QUALIFY
  RANK() OVER (ORDER BY on_time_rate_pct DESC, avg_lead_time_days ASC) <= 5

UNION ALL

-- Bottom 5 Suppliers (Worst On-Time Delivery)
SELECT
  'BOTTOM 5'                                      AS leaderboard_tier,
  supplier_name,
  country,
  material_type,
  quarter_label,
  total_deliveries,
  on_time_rate_pct,
  avg_lead_time_days,
  stddev_lead_time_days,
  p90_lead_time_days,
  performance_rating,
  trend_direction,
  procurement_action,
  overall_rank_this_quarter

FROM supplier_action_flagged

WHERE
  year_num    = EXTRACT(YEAR    FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 QUARTER))
  AND quarter_num = EXTRACT(QUARTER FROM DATE_SUB(CURRENT_DATE(), INTERVAL 1 QUARTER))

QUALIFY
  RANK() OVER (ORDER BY on_time_rate_pct ASC, avg_lead_time_days DESC) <= 5

ORDER BY
  leaderboard_tier,
  on_time_rate_pct DESC;


-- =============================================================================
-- SECTION 9 - MGMT View Creation
-- nordleaf_mgmt.v_supplier_delivery_kpi is the canonical supplier KPI view
-- consumed by Looker Studio, procurement reports, and finance chargebacks.
-- Refresh: Daily at 06:00 EET via BigQuery Scheduled Query
-- Owner  : nordleaf-procurement-analytics@nordleaf.fi
-- =============================================================================

CREATE OR REPLACE VIEW nordleaf_mgmt.v_supplier_delivery_kpi AS (

  /*
   * v_supplier_delivery_kpi
   * ---------------------------------------------------------
   * Quarterly supplier delivery KPI fact at
   * supplier × year × quarter granularity.
   *
   * Includes: OTD rate, lead time distribution, breach counts
   * by severity, QoQ trend, performance rating, and procurement
   * action flag.
   *
   * Row count: ~12 suppliers × up to 5 rolling quarters = ~60 rows
   * Refresh  : Daily 06:00 EET
   * Owner    : nordleaf-data-engineering@nordleaf.fi
   */

  WITH base_v AS (

    SELECT
      fd.delivery_key,
      fd.supplier_key,
      ds.supplier_name,
      ds.country,
      ds.material_type,
      fd.order_date,
      fd.actual_delivery_date,
      DATE_DIFF(fd.actual_delivery_date, fd.order_date, DAY)  AS lead_time_days,
      CASE ds.material_type
        WHEN 'Pulp'      THEN 7
        WHEN 'Chemicals' THEN 5
        WHEN 'Energy'    THEN 3
        WHEN 'Packaging' THEN 10
        ELSE                  14
      END                                                      AS sla_days,
      fd.quantity_tonnes,
      fd.unit_cost_eur,
      fd.quality_grade,
      fd.mill_key,
      dd.year_num,
      dd.quarter_num,
      CONCAT('Q', CAST(dd.quarter_num AS STRING), '-', CAST(dd.year_num AS STRING))
                                                               AS quarter_label

    FROM nordleaf_dwh.fact_deliveries   fd
    JOIN nordleaf_dwh.dim_supplier      ds  ON fd.supplier_key = ds.supplier_key
    JOIN nordleaf_dwh.dim_date          dd  ON fd.date_key     = dd.date_key

    WHERE
      dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
      AND fd.actual_delivery_date IS NOT NULL
      AND fd.quantity_tonnes > 0

  ),

  flagged_v AS (

    SELECT
      *,
      IF(lead_time_days <= sla_days, 1, 0)        AS on_time_flag,
      GREATEST(lead_time_days - sla_days, 0)      AS late_by_days,
      CASE
        WHEN lead_time_days <= sla_days                           THEN 'ON TIME'
        WHEN (lead_time_days - sla_days) BETWEEN 1 AND 2         THEN 'MINOR DELAY'
        WHEN (lead_time_days - sla_days) BETWEEN 3 AND 5         THEN 'MODERATE DELAY'
        WHEN (lead_time_days - sla_days) BETWEEN 6 AND 10        THEN 'SEVERE DELAY'
        WHEN (lead_time_days - sla_days) > 10                    THEN 'CRITICAL DELAY'
        ELSE 'UNCLASSIFIED'
      END                                         AS delay_severity

    FROM base_v

  ),

  agg_v AS (

    SELECT
      supplier_key,
      supplier_name,
      country,
      material_type,
      year_num,
      quarter_num,
      quarter_label,
      MAX(sla_days)                               AS sla_days,
      COUNT(delivery_key)                         AS total_deliveries,
      SUM(quantity_tonnes)                        AS total_tonnes,
      ROUND(SUM(quantity_tonnes * unit_cost_eur), 2)
                                                  AS total_cost_eur,
      SUM(on_time_flag)                           AS on_time_count,
      ROUND(SAFE_DIVIDE(SUM(on_time_flag), COUNT(delivery_key)) * 100, 2)
                                                  AS on_time_rate_pct,
      ROUND(AVG(lead_time_days),    2)            AS avg_lead_time_days,
      ROUND(STDDEV(lead_time_days), 2)            AS stddev_lead_time_days,
      APPROX_QUANTILES(lead_time_days, 10)[OFFSET(5)]
                                                  AS median_lead_time_days,
      APPROX_QUANTILES(lead_time_days, 10)[OFFSET(9)]
                                                  AS p90_lead_time_days,
      COUNTIF(delay_severity = 'MINOR DELAY')     AS cnt_minor_delay,
      COUNTIF(delay_severity = 'MODERATE DELAY')  AS cnt_moderate_delay,
      COUNTIF(delay_severity = 'SEVERE DELAY')    AS cnt_severe_delay,
      COUNTIF(delay_severity = 'CRITICAL DELAY')  AS cnt_critical_delay

    FROM flagged_v

    GROUP BY
      supplier_key, supplier_name, country, material_type,
      year_num, quarter_num, quarter_label

  ),

  rated_v AS (

    SELECT
      *,
      CASE
        WHEN on_time_rate_pct >= 95 THEN 'EXCELLENT'
        WHEN on_time_rate_pct >= 85 THEN 'GOOD'
        WHEN on_time_rate_pct >= 75 THEN 'ACCEPTABLE'
        ELSE                             'POOR'
      END                                         AS performance_rating,

      LAG(on_time_rate_pct, 1) OVER (
        PARTITION BY supplier_key ORDER BY year_num, quarter_num
      )                                           AS prior_q_otd_pct,

      ROUND(
        on_time_rate_pct - LAG(on_time_rate_pct, 1) OVER (
          PARTITION BY supplier_key ORDER BY year_num, quarter_num
        ), 2
      )                                           AS qoq_otd_delta_pct,

      ROUND(
        AVG(on_time_rate_pct) OVER (
          PARTITION BY supplier_key
          ORDER BY year_num, quarter_num
          ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ), 2
      )                                           AS rolling_3q_avg_otd_pct,

      CASE
        WHEN (on_time_rate_pct - LAG(on_time_rate_pct, 1) OVER (
                PARTITION BY supplier_key ORDER BY year_num, quarter_num
              )) > 2  THEN 'IMPROVING'
        WHEN (on_time_rate_pct - LAG(on_time_rate_pct, 1) OVER (
                PARTITION BY supplier_key ORDER BY year_num, quarter_num
              )) < -2 THEN 'DECLINING'
        ELSE               'STABLE'
      END                                         AS trend_direction,

      RANK() OVER (
        PARTITION BY year_num, quarter_num
        ORDER BY on_time_rate_pct DESC
      )                                           AS overall_rank_this_quarter

    FROM agg_v

  )

  SELECT
    supplier_key,
    supplier_name,
    country,
    material_type,
    year_num,
    quarter_num,
    quarter_label,
    sla_days,
    total_deliveries,
    total_tonnes,
    total_cost_eur,
    on_time_count,
    on_time_rate_pct,
    avg_lead_time_days,
    stddev_lead_time_days,
    median_lead_time_days,
    p90_lead_time_days,
    cnt_minor_delay,
    cnt_moderate_delay,
    cnt_severe_delay,
    cnt_critical_delay,
    performance_rating,
    prior_q_otd_pct,
    qoq_otd_delta_pct,
    rolling_3q_avg_otd_pct,
    trend_direction,
    overall_rank_this_quarter,

    -- Procurement action flag (reproduced inline for view self-containment)
    CASE
      WHEN performance_rating = 'POOR'       AND trend_direction = 'DECLINING'
        THEN 'URGENT - SCHEDULE SUPPLIER REVIEW'
      WHEN performance_rating = 'ACCEPTABLE' AND trend_direction = 'DECLINING'
        THEN 'WATCH - MONTHLY CHECK-IN'
      WHEN performance_rating = 'POOR'       AND trend_direction = 'STABLE'
        THEN 'WATCH - MONTHLY CHECK-IN'
      WHEN cnt_critical_delay >= 2
        THEN 'WATCH - REVIEW CRITICAL DELAY ROOT CAUSE'
      WHEN performance_rating = 'GOOD'       AND trend_direction = 'DECLINING'
        AND qoq_otd_delta_pct < -5
        THEN 'WATCH - MONITOR CLOSELY'
      ELSE 'STANDARD MONITORING'
    END                                           AS procurement_action,

    CURRENT_TIMESTAMP()                           AS last_refreshed_at

  FROM rated_v

);

-- =============================================================================
-- END OF FILE: UC09_delivery_reliability_lead_time.sql
-- =============================================================================
