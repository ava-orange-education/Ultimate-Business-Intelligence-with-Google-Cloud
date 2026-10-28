-- =============================================================================
-- USE CASE: UC-05 | Shift Performance & Workforce Productivity
-- Domain   : Production
-- Priority : P2 (High Business Value)
-- Author   : NordLeaf Data Engineering Team
-- Created  : 2026-02-23
-- Platform : Google BigQuery
-- =============================================================================
--
-- ARCHITECTURE DIAGRAM
-- ---------------------
--
--   fact_production
--   (nordleaf_dwh)
--        |
--        v
--   [BigQuery DWH]
--   Shift aggregation CTEs
--   (weekly_shift_base → mill_week_shift_rollup → shift_rankings
--    → shift_wow_comparison → shift_final)
--        |
--        v
--   MGMT Views
--   nordleaf_mgmt.v_shift_performance_weekly
--        |
--        v
--   Looker Studio
--   Shift Performance Dashboard
--   (Mill Operations Centre, Production Planning)
--
-- =============================================================================
--
-- BUSINESS PROBLEM
-- ----------------
-- NordLeaf operates 3 shifts across all 5 mills (Tampere, Oulu, Turku,
-- Espoo, Jyväskylä):
--   Morning   : 06:00 – 14:00
--   Afternoon : 14:00 – 22:00
--   Night     : 22:00 – 06:00 (following calendar day)
--
-- Mill operations management needs to identify which shifts consistently
-- underperform in terms of output volume and product quality, so they can
-- investigate root causes including:
--   - Staffing levels and operator skill mix per shift
--   - Machine warm-up / cool-down impact at shift changeover
--   - Raw material delivery timing (pulp bale arrivals, chemical dosing)
--   - Supervisor coverage and experience on Night shifts
--
-- Without this visibility, underperforming shifts go undetected until
-- monthly mill reviews, costing NordLeaf an estimated 2-4% of monthly
-- capacity in preventable waste and rework.
--
-- =============================================================================
--
-- KEY PERFORMANCE INDICATORS
-- --------------------------
--
--  KPI                   | Calculation                     | Target
--  ----------------------|---------------------------------|---------------
--  Tonnes / Shift        | SUM(tonnes_produced) per shift  | ≥ plan
--  Shift Efficiency %    | actual_tons / planned_tons * 100| ≥ 85%
--  Grade A Yield %       | grade_a_batches / total_batches | ≥ 80%
--  Best Shift Rank       | RANK by efficiency_pct (ASC=1)  | Rank 1 = Best
--  Worst Shift Rank      | RANK by efficiency_pct (DESC=1) | Monitor rank 3
--  WoW Delta Tonnes      | actual - prior week actual      | Positive trend
--  WoW Delta %           | (delta / prior week) * 100      | > -5% threshold
--  Rolling 4-Wk Avg      | AVG(actual) last 4 ISO weeks    | Smoothed trend
--
-- =============================================================================
--
-- IMPLEMENTATION STEPS
-- --------------------
--  Step 1 : Extract 12-week rolling window from fact_production
--           joined to dim_mill, dim_product, dim_date
--  Step 2 : Aggregate at mill × week × shift × product level
--           to capture per-product contribution per shift
--  Step 3 : Roll up to mill × week × shift (collapse product dimension)
--           and compute core KPIs (efficiency, quality, capacity util)
--  Step 4 : Apply RANK() window functions to rank all 3 shifts within
--           each mill for each ISO week
--  Step 5 : Apply LAG() window functions for week-over-week delta
--           and 4-week rolling average
--  Step 6 : Classify each shift-week into a performance tier
--           (ELITE → CRITICAL) based on efficiency_pct
--  Step 7 : Surface best shift per mill for current reporting week
--           using QUALIFY to eliminate subquery boilerplate
--  Step 8 : Dedicated Night shift monitoring with grade-C alert logic
--  Step 9 : Persist results as MGMT view for Looker Studio consumption
--
-- EXPECTED OUTPUT
-- ---------------
--  The final result set returns one row per mill × ISO week × shift.
--  Each row contains volume KPIs, efficiency metrics, quality ratios,
--  shift rankings, WoW deltas, rolling averages, and a performance tier
--  label. The MGMT view is scheduled to refresh daily via BigQuery
--  Scheduled Queries at 07:00 EET so morning shift supervisors see
--  the previous day's Night shift results before the 08:00 stand-up.
--
-- =============================================================================


-- =============================================================================
-- SECTION 1 - Weekly Shift Base Aggregation
-- Granularity: mill × shift × product × ISO-week
-- Source: fact_production (12-week rolling window)
-- =============================================================================

WITH weekly_shift_base AS (

  SELECT
    fp.mill_key,
    dm.mill_name,
    fp.shift,                                     -- 'Morning' | 'Afternoon' | 'Night'
    dp.product_type,
    dd.iso_year,
    dd.iso_week,
    dd.week_start_date,

    -- Volume metrics
    COUNT(fp.batch_id)                            AS total_batches,
    SUM(fp.tonnes_produced)                       AS actual_tons,
    AVG(fp.planned_tonnes)                        AS avg_planned_tons,
    SUM(fp.planned_tonnes)                        AS total_planned_tons,

    -- Quality metrics
    AVG(fp.quality_score)                         AS avg_quality_score,
    COUNTIF(fp.quality_score >= 90)               AS grade_a_batches,    -- Premium
    COUNTIF(fp.quality_score >= 70
        AND fp.quality_score < 90)                AS grade_b_batches,    -- Standard
    COUNTIF(fp.quality_score < 70)                AS grade_c_batches,    -- Reject / rework

    -- Downtime capture (minutes lost per shift per product per week)
    SUM(fp.downtime_minutes)                      AS total_downtime_mins,
    AVG(fp.machine_speed_pct)                     AS avg_machine_speed_pct

  FROM nordleaf_dwh.fact_production   fp
  JOIN nordleaf_dwh.dim_mill          dm  ON fp.mill_key     = dm.mill_key
  JOIN nordleaf_dwh.dim_product       dp  ON fp.product_key  = dp.product_key
  JOIN nordleaf_dwh.dim_date          dd  ON fp.date_key     = dd.date_key

  WHERE
    dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
    AND fp.shift IN ('Morning', 'Afternoon', 'Night')   -- Exclude unclassified records
    AND fp.tonnes_produced IS NOT NULL                  -- Exclude incomplete batches

  GROUP BY
    fp.mill_key,
    dm.mill_name,
    fp.shift,
    dp.product_type,
    dd.iso_year,
    dd.iso_week,
    dd.week_start_date

),


-- =============================================================================
-- SECTION 2 - Mill × Week × Shift Rollup
-- Collapse product dimension; compute shift-level KPIs
-- Granularity: mill × shift × ISO-week
-- =============================================================================

mill_week_shift_rollup AS (

  SELECT
    mill_key,
    mill_name,
    shift,
    iso_year,
    iso_week,
    week_start_date,

    -- Consolidated volume
    SUM(total_batches)                            AS total_batches,
    SUM(actual_tons)                              AS actual_tons,
    SUM(total_planned_tons)                       AS total_planned_tons,

    -- Core efficiency KPI: actual output vs planned output
    ROUND(
      SAFE_DIVIDE(SUM(actual_tons), SUM(total_planned_tons)) * 100,
      2
    )                                             AS efficiency_pct,

    -- Quality KPIs
    SUM(grade_a_batches)                          AS grade_a_batches,
    SUM(grade_b_batches)                          AS grade_b_batches,
    SUM(grade_c_batches)                          AS grade_c_batches,

    ROUND(
      SAFE_DIVIDE(SUM(grade_a_batches), SUM(total_batches)) * 100,
      2
    )                                             AS grade_a_pct,

    ROUND(
      SAFE_DIVIDE(SUM(grade_c_batches), SUM(total_batches)) * 100,
      2
    )                                             AS grade_c_pct,

    ROUND(AVG(avg_quality_score), 2)              AS avg_quality_score,

    -- Capacity utilisation: machine speed proxy
    ROUND(AVG(avg_machine_speed_pct), 2)          AS capacity_util_pct,

    -- Downtime aggregation
    SUM(total_downtime_mins)                      AS total_downtime_mins,
    ROUND(
      SAFE_DIVIDE(SUM(total_downtime_mins), (8 * 60)) * 100,
      2
    )                                             AS downtime_pct_of_shift,

    -- Tonnes per batch (productivity density)
    ROUND(
      SAFE_DIVIDE(SUM(actual_tons), NULLIF(SUM(total_batches), 0)),
      3
    )                                             AS tonnes_per_batch

  FROM weekly_shift_base

  GROUP BY
    mill_key,
    mill_name,
    shift,
    iso_year,
    iso_week,
    week_start_date

),


-- =============================================================================
-- SECTION 3 - Shift Rankings Per Mill Per ISO Week
-- Three independent ranking dimensions:
--   (a) rank_by_tonnes      – absolute volume output
--   (b) rank_by_efficiency  – planned vs actual ratio
--   (c) rank_by_quality     – grade_a_pct
-- Rank = 1 means best-performing shift for that mill-week
-- =============================================================================

shift_rankings AS (

  SELECT
    *,

    -- Rank all 3 shifts within each mill for each ISO week
    RANK() OVER (
      PARTITION BY mill_key, iso_year, iso_week
      ORDER BY actual_tons DESC
    )                                             AS rank_by_tonnes,

    RANK() OVER (
      PARTITION BY mill_key, iso_year, iso_week
      ORDER BY efficiency_pct DESC
    )                                             AS rank_by_efficiency,

    RANK() OVER (
      PARTITION BY mill_key, iso_year, iso_week
      ORDER BY grade_a_pct DESC
    )                                             AS rank_by_quality,

    -- Composite score rank: equal-weight average of the three dimensions
    -- Used as a tiebreaker for overall shift champion
    RANK() OVER (
      PARTITION BY mill_key, iso_year, iso_week
      ORDER BY (
        COALESCE(efficiency_pct, 0)
        + COALESCE(grade_a_pct,  0)
        + COALESCE(capacity_util_pct, 0)
      ) DESC
    )                                             AS rank_composite

  FROM mill_week_shift_rollup

),


-- =============================================================================
-- SECTION 4 - Week-over-Week Comparison & Rolling Averages
-- LAG(1) gives the immediately preceding ISO week for the same mill+shift
-- LAG(2) gives two weeks back (used for acceleration detection)
-- Rolling 4-week average smooths out single-week anomalies
-- =============================================================================

shift_wow_comparison AS (

  SELECT
    *,

    -- Prior week actuals (same mill, same shift)
    LAG(actual_tons, 1) OVER (
      PARTITION BY mill_key, shift
      ORDER BY iso_year, iso_week
    )                                             AS prior_week_actual_tons,

    LAG(efficiency_pct, 1) OVER (
      PARTITION BY mill_key, shift
      ORDER BY iso_year, iso_week
    )                                             AS prior_week_efficiency_pct,

    LAG(grade_a_pct, 1) OVER (
      PARTITION BY mill_key, shift
      ORDER BY iso_year, iso_week
    )                                             AS prior_week_grade_a_pct,

    -- Two weeks back for acceleration measurement
    LAG(actual_tons, 2) OVER (
      PARTITION BY mill_key, shift
      ORDER BY iso_year, iso_week
    )                                             AS two_week_prior_actual_tons,

    -- WoW absolute delta tonnes
    ROUND(
      actual_tons - LAG(actual_tons, 1) OVER (
        PARTITION BY mill_key, shift
        ORDER BY iso_year, iso_week
      ),
      2
    )                                             AS wow_delta_tons,

    -- WoW percentage delta
    ROUND(
      SAFE_DIVIDE(
        actual_tons - LAG(actual_tons, 1) OVER (
          PARTITION BY mill_key, shift
          ORDER BY iso_year, iso_week
        ),
        NULLIF(
          LAG(actual_tons, 1) OVER (
            PARTITION BY mill_key, shift
            ORDER BY iso_year, iso_week
          ), 0
        )
      ) * 100,
      2
    )                                             AS wow_delta_pct,

    -- Rolling 4-week average actual tonnes
    ROUND(
      AVG(actual_tons) OVER (
        PARTITION BY mill_key, shift
        ORDER BY iso_year, iso_week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ),
      2
    )                                             AS rolling_4wk_avg_tons,

    -- Rolling 4-week average efficiency
    ROUND(
      AVG(efficiency_pct) OVER (
        PARTITION BY mill_key, shift
        ORDER BY iso_year, iso_week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ),
      2
    )                                             AS rolling_4wk_avg_efficiency,

    -- Trend direction vs rolling average
    CASE
      WHEN actual_tons > AVG(actual_tons) OVER (
        PARTITION BY mill_key, shift
        ORDER BY iso_year, iso_week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ) * 1.03 THEN 'ABOVE TREND'
      WHEN actual_tons < AVG(actual_tons) OVER (
        PARTITION BY mill_key, shift
        ORDER BY iso_year, iso_week
        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
      ) * 0.97 THEN 'BELOW TREND'
      ELSE 'ON TREND'
    END                                           AS vs_rolling_avg_flag

  FROM shift_rankings

),


-- =============================================================================
-- SECTION 5 - Final Output with Performance Tier Classification
-- Tiers derived from efficiency_pct (actual vs planned tonnes)
-- =============================================================================

shift_final AS (

  SELECT
    -- Identity dimensions
    mill_key,
    mill_name,
    shift,
    iso_year,
    iso_week,
    week_start_date,

    -- Volume KPIs
    total_batches,
    ROUND(actual_tons,          2)                AS actual_tons,
    ROUND(total_planned_tons,   2)                AS total_planned_tons,
    efficiency_pct,
    capacity_util_pct,
    tonnes_per_batch,

    -- Quality KPIs
    grade_a_batches,
    grade_b_batches,
    grade_c_batches,
    grade_a_pct,
    grade_c_pct,
    avg_quality_score,

    -- Downtime
    total_downtime_mins,
    downtime_pct_of_shift,

    -- Rankings
    rank_by_tonnes,
    rank_by_efficiency,
    rank_by_quality,
    rank_composite,

    -- WoW metrics
    prior_week_actual_tons,
    prior_week_efficiency_pct,
    prior_week_grade_a_pct,
    wow_delta_tons,
    wow_delta_pct,
    rolling_4wk_avg_tons,
    rolling_4wk_avg_efficiency,
    vs_rolling_avg_flag,

    -- Performance tier classification based on efficiency
    CASE
      WHEN efficiency_pct >= 95 THEN 'ELITE'
      WHEN efficiency_pct >= 85 THEN 'HIGH PERFORMER'
      WHEN efficiency_pct >= 75 THEN 'MEETS TARGET'
      WHEN efficiency_pct >= 60 THEN 'BELOW TARGET'
      ELSE                           'CRITICAL'
    END                                           AS performance_tier,

    -- Numeric tier for sorting / colouring in Looker Studio
    CASE
      WHEN efficiency_pct >= 95 THEN 5
      WHEN efficiency_pct >= 85 THEN 4
      WHEN efficiency_pct >= 75 THEN 3
      WHEN efficiency_pct >= 60 THEN 2
      ELSE                           1
    END                                           AS performance_tier_rank,

    -- WoW alert flag: drop >5% triggers amber; drop >10% triggers red
    CASE
      WHEN wow_delta_pct <= -10 THEN 'RED - SIGNIFICANT DROP'
      WHEN wow_delta_pct <= -5  THEN 'AMBER - MONITOR'
      WHEN wow_delta_pct >= 5   THEN 'GREEN - IMPROVING'
      ELSE                           'NEUTRAL'
    END                                           AS wow_alert_flag,

    -- Metadata
    CURRENT_TIMESTAMP()                           AS last_refreshed_at

  FROM shift_wow_comparison

)


-- =============================================================================
-- SECTION 6 - Best Shift Per Mill This (Reporting) Week
-- Uses QUALIFY to return only rank-1 shift (top performer) per mill
-- Reporting week = last completed ISO week
-- =============================================================================

SELECT
  mill_name,
  shift                                           AS best_shift,
  iso_year,
  iso_week,
  week_start_date,
  actual_tons,
  efficiency_pct,
  grade_a_pct,
  performance_tier,
  rank_composite                                  AS overall_rank

FROM shift_final

WHERE
  iso_year = EXTRACT(YEAR     FROM CURRENT_DATE())
  AND iso_week = EXTRACT(ISOWEEK FROM CURRENT_DATE()) - 1

QUALIFY
  RANK() OVER (PARTITION BY mill_name ORDER BY efficiency_pct DESC) = 1

ORDER BY
  mill_name;


-- =============================================================================
-- SECTION 7 - Night Shift Monitoring
-- Night shift (22:00-06:00) is operationally challenging due to reduced
-- supervisor coverage and fatigue effects. This query surfaces Night shift
-- grade-C trends and raises an alert when grade_c_pct exceeds 20% threshold.
-- =============================================================================

SELECT
  mill_name,
  shift,
  iso_year,
  iso_week,
  week_start_date,
  total_batches,
  actual_tons,
  efficiency_pct,
  grade_a_pct,
  grade_c_pct,
  avg_quality_score,
  total_downtime_mins,
  wow_delta_pct,
  rolling_4wk_avg_tons,
  performance_tier,

  -- Alert flag for quality deterioration
  CASE
    WHEN grade_c_pct > 20 THEN 'ALERT: Grade-C exceeds 20% threshold - Escalate to Mill Manager'
    WHEN grade_c_pct > 15 THEN 'WARNING: Grade-C approaching threshold - Review operator logs'
    WHEN grade_c_pct > 10 THEN 'WATCH: Grade-C elevated - Monitor next 2 shifts'
    ELSE                        'OK'
  END                                             AS quality_alert,

  -- Trend in grade-C over last 4 weeks for this shift
  ROUND(
    grade_c_pct - LAG(grade_c_pct, 3) OVER (
      PARTITION BY mill_key, shift
      ORDER BY iso_year, iso_week
    ),
    2
  )                                               AS grade_c_trend_4wk

FROM shift_final

WHERE
  shift = 'Night'
  AND iso_year = EXTRACT(YEAR FROM CURRENT_DATE())

ORDER BY
  mill_name,
  iso_week;


-- =============================================================================
-- SECTION 8 - MGMT View Creation
-- This view is the single source of truth for Looker Studio dashboards.
-- Refresh: Daily at 07:00 EET via BigQuery Scheduled Query
-- Consumers: Production Manager, Mill Operations Centre, CI Team
-- =============================================================================

CREATE OR REPLACE VIEW nordleaf_mgmt.v_shift_performance_weekly AS (

  /*
   * v_shift_performance_weekly
   * ---------------------------------------------------------
   * Full shift performance fact at mill × ISO-week × shift
   * granularity. Exposes all KPIs computed in the CTE chain
   * above. Looker Studio connects directly to this view;
   * no further transformation required in the BI layer.
   *
   * Row count: ~(5 mills × 3 shifts × 12 weeks) = ~180 rows
   * Refresh cadence: Daily, 07:00 EET
   * Owner: nordleaf-data-engineering@nordleaf.fi
   */

  WITH weekly_shift_base_v AS (

    SELECT
      fp.mill_key,
      dm.mill_name,
      fp.shift,
      dp.product_type,
      dd.iso_year,
      dd.iso_week,
      dd.week_start_date,
      COUNT(fp.batch_id)                          AS total_batches,
      SUM(fp.tonnes_produced)                     AS actual_tons,
      SUM(fp.planned_tonnes)                      AS total_planned_tons,
      AVG(fp.quality_score)                       AS avg_quality_score,
      COUNTIF(fp.quality_score >= 90)             AS grade_a_batches,
      COUNTIF(fp.quality_score >= 70
          AND fp.quality_score < 90)              AS grade_b_batches,
      COUNTIF(fp.quality_score < 70)              AS grade_c_batches,
      SUM(fp.downtime_minutes)                    AS total_downtime_mins,
      AVG(fp.machine_speed_pct)                   AS avg_machine_speed_pct

    FROM nordleaf_dwh.fact_production   fp
    JOIN nordleaf_dwh.dim_mill          dm  ON fp.mill_key    = dm.mill_key
    JOIN nordleaf_dwh.dim_product       dp  ON fp.product_key = dp.product_key
    JOIN nordleaf_dwh.dim_date          dd  ON fp.date_key    = dd.date_key

    WHERE
      dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 WEEK)
      AND fp.shift IN ('Morning', 'Afternoon', 'Night')
      AND fp.tonnes_produced IS NOT NULL

    GROUP BY
      fp.mill_key, dm.mill_name, fp.shift, dp.product_type,
      dd.iso_year, dd.iso_week, dd.week_start_date

  ),

  rollup_v AS (

    SELECT
      mill_key,
      mill_name,
      shift,
      iso_year,
      iso_week,
      week_start_date,
      SUM(total_batches)                          AS total_batches,
      SUM(actual_tons)                            AS actual_tons,
      SUM(total_planned_tons)                     AS total_planned_tons,
      ROUND(SAFE_DIVIDE(SUM(actual_tons), SUM(total_planned_tons)) * 100, 2)
                                                  AS efficiency_pct,
      SUM(grade_a_batches)                        AS grade_a_batches,
      SUM(grade_b_batches)                        AS grade_b_batches,
      SUM(grade_c_batches)                        AS grade_c_batches,
      ROUND(SAFE_DIVIDE(SUM(grade_a_batches), SUM(total_batches)) * 100, 2)
                                                  AS grade_a_pct,
      ROUND(SAFE_DIVIDE(SUM(grade_c_batches), SUM(total_batches)) * 100, 2)
                                                  AS grade_c_pct,
      ROUND(AVG(avg_quality_score), 2)            AS avg_quality_score,
      ROUND(AVG(avg_machine_speed_pct), 2)        AS capacity_util_pct,
      SUM(total_downtime_mins)                    AS total_downtime_mins,
      ROUND(SAFE_DIVIDE(SUM(total_downtime_mins), (8 * 60)) * 100, 2)
                                                  AS downtime_pct_of_shift,
      ROUND(SAFE_DIVIDE(SUM(actual_tons), NULLIF(SUM(total_batches), 0)), 3)
                                                  AS tonnes_per_batch

    FROM weekly_shift_base_v
    GROUP BY
      mill_key, mill_name, shift, iso_year, iso_week, week_start_date

  ),

  ranked_v AS (

    SELECT
      *,
      RANK() OVER (PARTITION BY mill_key, iso_year, iso_week ORDER BY actual_tons    DESC) AS rank_by_tonnes,
      RANK() OVER (PARTITION BY mill_key, iso_year, iso_week ORDER BY efficiency_pct DESC) AS rank_by_efficiency,
      RANK() OVER (PARTITION BY mill_key, iso_year, iso_week ORDER BY grade_a_pct    DESC) AS rank_by_quality,
      LAG(actual_tons,   1) OVER (PARTITION BY mill_key, shift ORDER BY iso_year, iso_week) AS prior_week_actual_tons,
      LAG(efficiency_pct,1) OVER (PARTITION BY mill_key, shift ORDER BY iso_year, iso_week) AS prior_week_efficiency_pct,
      ROUND(
        actual_tons - LAG(actual_tons, 1) OVER (PARTITION BY mill_key, shift ORDER BY iso_year, iso_week),
        2
      )                                           AS wow_delta_tons,
      ROUND(
        SAFE_DIVIDE(
          actual_tons - LAG(actual_tons, 1) OVER (PARTITION BY mill_key, shift ORDER BY iso_year, iso_week),
          NULLIF(LAG(actual_tons, 1) OVER (PARTITION BY mill_key, shift ORDER BY iso_year, iso_week), 0)
        ) * 100, 2
      )                                           AS wow_delta_pct,
      ROUND(
        AVG(actual_tons) OVER (
          PARTITION BY mill_key, shift
          ORDER BY iso_year, iso_week
          ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ), 2
      )                                           AS rolling_4wk_avg_tons

    FROM rollup_v

  )

  SELECT
    mill_key,
    mill_name,
    shift,
    iso_year,
    iso_week,
    week_start_date,
    total_batches,
    ROUND(actual_tons,        2)                  AS actual_tons,
    ROUND(total_planned_tons, 2)                  AS total_planned_tons,
    efficiency_pct,
    capacity_util_pct,
    tonnes_per_batch,
    grade_a_batches,
    grade_b_batches,
    grade_c_batches,
    grade_a_pct,
    grade_c_pct,
    avg_quality_score,
    total_downtime_mins,
    downtime_pct_of_shift,
    rank_by_tonnes,
    rank_by_efficiency,
    rank_by_quality,
    prior_week_actual_tons,
    prior_week_efficiency_pct,
    wow_delta_tons,
    wow_delta_pct,
    rolling_4wk_avg_tons,

    CASE
      WHEN efficiency_pct >= 95 THEN 'ELITE'
      WHEN efficiency_pct >= 85 THEN 'HIGH PERFORMER'
      WHEN efficiency_pct >= 75 THEN 'MEETS TARGET'
      WHEN efficiency_pct >= 60 THEN 'BELOW TARGET'
      ELSE                           'CRITICAL'
    END                                           AS performance_tier,

    CASE
      WHEN wow_delta_pct <= -10 THEN 'RED - SIGNIFICANT DROP'
      WHEN wow_delta_pct <= -5  THEN 'AMBER - MONITOR'
      WHEN wow_delta_pct >= 5   THEN 'GREEN - IMPROVING'
      ELSE                           'NEUTRAL'
    END                                           AS wow_alert_flag,

    -- Night shift quality alert (useful as a Looker Studio calculated field)
    CASE
      WHEN shift = 'Night' AND grade_c_pct > 20
        THEN 'ALERT: Escalate to Mill Manager'
      WHEN shift = 'Night' AND grade_c_pct > 15
        THEN 'WARNING: Review operator logs'
      ELSE NULL
    END                                           AS night_shift_alert,

    CURRENT_TIMESTAMP()                           AS last_refreshed_at

  FROM ranked_v

);

-- =============================================================================
-- END OF FILE: UC05_shift_performance_workforce.sql
-- =============================================================================
