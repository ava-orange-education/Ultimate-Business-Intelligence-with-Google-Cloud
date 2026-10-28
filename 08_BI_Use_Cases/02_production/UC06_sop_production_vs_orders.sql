-- ============================================================
-- UC-06: Sales & Operations Planning (S&OP) — Production vs Demand
-- Domain: Production Planning
-- Business Question: Is what we produce aligned with what customers
--   are ordering? Which products are we over/under producing?
-- Priority: P1 | Users: S&OP Team, Supply Chain Director, CFO
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  fact_orders (demand signal: quantity_tons per product per month)
--         |
--         | COMPARE
--         v
--  fact_production (supply: actual_tons per product per month)
--         |
--         | Gap analysis: Demand - Supply = Surplus or Deficit
--         v
--  S&OP Balance Report
--    Positive gap = OVERSUPPLY (risk of inventory cost)
--    Negative gap = UNDERSUPPLY (risk of lost sales, stockout)
--         |
--         v
--  demand_forecast (ML 30-day forward view for planning)
-- ============================================================

-- S&OP alignment: production output vs customer orders
WITH monthly_demand AS (
  SELECT
    dp.product_type,
    dd.year,
    dd.month,
    dd.month_name,
    SUM(fo.quantity_tons)           AS ordered_tons,
    COUNT(fo.order_id)              AS order_count,
    SUM(fo.total_value_eur)         AS order_value_eur,
    COUNTIF(fo.status='CANCELLED')  AS cancelled_orders
  FROM `nordleaf_dwh.fact_orders` fo
  JOIN `nordleaf_dwh.dim_date`    dd ON fo.order_date_id = dd.date_id
  JOIN `nordleaf_dwh.dim_product` dp ON fo.product_key   = dp.product_key
  WHERE fo.status != 'CANCELLED'
  GROUP BY 1, 2, 3, 4
),

monthly_supply AS (
  SELECT
    fp.product_type,
    EXTRACT(YEAR FROM fp.production_date)  AS year,
    EXTRACT(MONTH FROM fp.production_date) AS month,
    SUM(fp.actual_tons)                    AS produced_tons,
    SUM(fp.planned_tons)                   AS planned_tons,
    ROUND(AVG(fp.efficiency_pct), 1)       AS avg_efficiency_pct,
    COUNTIF(fp.quality_grade = 'A')        AS grade_a_batches,
    COUNT(fp.batch_id)                     AS total_batches
  FROM `nordleaf_dwh.fact_production` fp
  GROUP BY 1, 2, 3
)

SELECT
  COALESCE(d.product_type, s.product_type) AS product_type,
  COALESCE(d.year, s.year)                 AS year,
  COALESCE(d.month, s.month)               AS month,
  COALESCE(d.month_name, '')               AS month_name,
  ROUND(COALESCE(d.ordered_tons, 0), 1)    AS ordered_tons,
  ROUND(COALESCE(s.produced_tons, 0), 1)   AS produced_tons,
  ROUND(COALESCE(s.produced_tons, 0)
    - COALESCE(d.ordered_tons, 0), 1)      AS supply_demand_gap_tons,
  COALESCE(d.order_count, 0)               AS order_count,
  ROUND(COALESCE(d.order_value_eur, 0), 0) AS order_value_eur,
  ROUND(COALESCE(s.avg_efficiency_pct, 0), 1) AS production_efficiency_pct,
  -- S&OP status
  CASE
    WHEN COALESCE(s.produced_tons,0) - COALESCE(d.ordered_tons,0) > 200
      THEN 'OVERSUPPLY — reduce production or find buyers'
    WHEN COALESCE(s.produced_tons,0) - COALESCE(d.ordered_tons,0) < -200
      THEN 'UNDERSUPPLY — risk of stockout, accelerate production'
    WHEN COALESCE(d.ordered_tons,0) = 0
      THEN 'NO DEMAND — review product strategy'
    ELSE 'BALANCED'
  END AS sop_status,
  ROUND(
    COALESCE(d.cancelled_orders, 0)
    / NULLIF(COALESCE(d.order_count, 0), 0) * 100
  , 1) AS cancellation_rate_pct
FROM monthly_demand d
FULL OUTER JOIN monthly_supply s
  ON d.product_type = s.product_type
  AND d.year = s.year
  AND d.month = s.month
ORDER BY year DESC, month DESC, product_type;
