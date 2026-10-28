-- ============================================================
-- UC-03: Mill Efficiency & Overall Equipment Effectiveness (OEE)
-- Domain: Production Operations
-- Business Question: How efficiently are each of our 5 mills
--   running? Which mill/shift/product combination underperforms?
-- Priority: P1 | Users: Mill Managers, COO, Plant Engineers
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  fact_production (batch_id, mill_id, production_date, shift,
--                   product_type, planned_tons, actual_tons,
--                   efficiency_pct, quality_grade)
--         |
--         | Aggregated by: mill × month × product × shift
--         v
--  OEE Components:
--    Availability = actual batches / planned batches (proxy)
--    Performance  = actual_tons / planned_tons (efficiency_pct)
--    Quality      = Grade-A batches / total batches
--    OEE          = Availability × Performance × Quality
--         |
--         v
--  nordleaf_mgmt.v_mill_oee_monthly
--  Alert: Cloud Monitoring if OEE < 75% for any mill
--
-- STEPS TO IMPLEMENT:
--  1. Schedule this query daily via Composer (nordleaf_dq_dag)
--  2. Write to nordleaf_mgmt.mill_oee_daily table
--  3. Surface on "Mill Operations" Looker Studio dashboard
--  4. Set Cloud Monitoring alert: mill OEE < 75% → PagerDuty
-- ============================================================

WITH daily_mill_metrics AS (
  SELECT
    fp.mill_id,
    dm.mill_name,
    dm.city,
    dm.capacity_tons_day,
    fp.production_date,
    DATE_TRUNC(fp.production_date, MONTH)         AS production_month,
    fp.product_type,
    fp.shift,
    -- Production counts
    COUNT(fp.batch_id)                            AS batch_count,
    -- Volume metrics
    SUM(fp.planned_tons)                          AS total_planned_tons,
    SUM(fp.actual_tons)                           AS total_actual_tons,
    -- OEE components
    ROUND(AVG(fp.efficiency_pct), 2)              AS avg_efficiency_pct,
    -- Quality rate (Grade A = good output)
    COUNTIF(fp.quality_grade = 'A')               AS grade_a_batches,
    COUNTIF(fp.quality_grade = 'B')               AS grade_b_batches,
    COUNTIF(fp.quality_grade = 'C')               AS grade_c_batches,
    ROUND(COUNTIF(fp.quality_grade = 'A') / NULLIF(COUNT(*), 0) * 100, 1) AS quality_rate_pct,
    -- Variance
    ROUND(SUM(fp.actual_tons) - SUM(fp.planned_tons), 1) AS production_variance_tons
  FROM `nordleaf_dwh.fact_production` fp
  JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

oee_calculation AS (
  SELECT
    *,
    -- OEE = Performance × Quality (Availability proxy = 1, no downtime data yet)
    ROUND(
      (avg_efficiency_pct / 100) * (quality_rate_pct / 100) * 100
    , 1) AS oee_pct,
    -- Capacity utilisation vs nameplate capacity
    ROUND(total_actual_tons / NULLIF(capacity_tons_day, 0) * 100, 1) AS capacity_utilisation_pct
  FROM daily_mill_metrics
)

SELECT
  mill_name,
  city,
  production_month,
  product_type,
  shift,
  SUM(batch_count)                           AS total_batches,
  ROUND(SUM(total_planned_tons), 0)          AS planned_tons,
  ROUND(SUM(total_actual_tons), 0)           AS actual_tons,
  ROUND(SUM(total_actual_tons)
    - SUM(total_planned_tons), 0)            AS variance_tons,
  ROUND(AVG(avg_efficiency_pct), 1)          AS avg_performance_pct,
  ROUND(AVG(quality_rate_pct), 1)            AS avg_quality_rate_pct,
  ROUND(AVG(oee_pct), 1)                     AS avg_oee_pct,
  ROUND(AVG(capacity_utilisation_pct), 1)    AS avg_capacity_utilisation_pct,
  SUM(grade_a_batches)                       AS grade_a_count,
  SUM(grade_b_batches)                       AS grade_b_count,
  SUM(grade_c_batches)                       AS grade_c_count,
  -- OEE benchmarks (paper industry world-class = 85%)
  CASE
    WHEN AVG(oee_pct) >= 85 THEN 'WORLD CLASS'
    WHEN AVG(oee_pct) >= 75 THEN 'GOOD'
    WHEN AVG(oee_pct) >= 65 THEN 'AVERAGE'
    WHEN AVG(oee_pct) >= 50 THEN 'POOR'
    ELSE 'CRITICAL'
  END AS oee_benchmark
FROM oee_calculation
GROUP BY 1, 2, 3, 4, 5
ORDER BY production_month DESC, avg_oee_pct ASC;

-- Monthly OEE KPI View (for Executive Dashboard)
CREATE OR REPLACE VIEW `nordleaf_mgmt.v_mill_oee_monthly` AS
SELECT
  dm.mill_name,
  DATE_TRUNC(fp.production_date, MONTH)         AS month,
  COUNT(fp.batch_id)                            AS batches,
  ROUND(SUM(fp.planned_tons), 0)                AS planned_tons,
  ROUND(SUM(fp.actual_tons), 0)                 AS actual_tons,
  ROUND(AVG(fp.efficiency_pct), 1)              AS performance_pct,
  ROUND(COUNTIF(fp.quality_grade='A') / COUNT(*) * 100, 1) AS quality_rate_pct,
  ROUND(AVG(fp.efficiency_pct) / 100
    * COUNTIF(fp.quality_grade='A') / COUNT(*) * 100, 1)   AS oee_pct
FROM `nordleaf_dwh.fact_production` fp
JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
GROUP BY 1, 2;
