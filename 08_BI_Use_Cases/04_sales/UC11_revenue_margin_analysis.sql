-- ============================================================
-- UC-11: Revenue & Margin Analysis by Product, Mill, Region
-- Domain: Sales / Finance
-- Business Question: Which product × mill combination is most
--   profitable? How does pricing vary by customer region?
--   What is our gross margin after material costs?
-- Priority: P1 | Users: CEO, CFO, Sales Director
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  fact_orders (revenue signal)
--    + fact_deliveries (material cost signal)
--    + fact_production (production cost proxy)
--         |
--         | Joined on product_type + mill + month
--         v
--  Gross Margin = Revenue - Material Cost
--  Contribution Margin = Gross Margin - Production Cost estimate
--         |
--         v
--  nordleaf_mgmt.v_revenue_margin
--  Executive Dashboard → CFO Monthly P&L view
-- ============================================================

WITH revenue_by_product_mill AS (
  SELECT
    dp.product_type,
    dp.product_category,
    dm.mill_name,
    dm.city                                   AS mill_city,
    dc.region                                 AS customer_region,
    dc.country                                AS customer_country,
    dc.customer_type,
    dd.year,
    dd.quarter,
    dd.month,
    dd.month_name,
    dd.is_holiday,
    -- Revenue
    COUNT(fo.order_id)                        AS order_count,
    SUM(fo.quantity_tons)                     AS sold_tons,
    SUM(fo.total_value_eur)                   AS revenue_eur,
    AVG(fo.unit_price_eur)                    AS avg_price_per_ton,
    MIN(fo.unit_price_eur)                    AS min_price_per_ton,
    MAX(fo.unit_price_eur)                    AS max_price_per_ton,
    -- Lead time
    AVG(fo.lead_time_days)                    AS avg_lead_time_days,
    -- Order status
    COUNTIF(fo.status = 'CANCELLED')          AS cancelled_count,
    COUNTIF(fo.status = 'DELIVERED')          AS delivered_count
  FROM `nordleaf_dwh.fact_orders` fo
  JOIN `nordleaf_dwh.dim_product`  dp ON fo.product_key   = dp.product_key
  JOIN `nordleaf_dwh.dim_mill`     dm ON fo.mill_key       = dm.mill_key
  JOIN `nordleaf_dwh.dim_customer` dc ON fo.customer_key   = dc.customer_key
  JOIN `nordleaf_dwh.dim_date`     dd ON fo.order_date_id  = dd.date_id
  WHERE dc.is_current = TRUE
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12
),

material_cost_by_mill_month AS (
  SELECT
    d.mill_destination                        AS mill_name,
    EXTRACT(YEAR FROM d.delivery_date)        AS year,
    EXTRACT(MONTH FROM d.delivery_date)       AS month,
    SUM(d.invoice_eur)                        AS total_material_cost_eur,
    SUM(d.quantity_tons)                      AS raw_material_tons
  FROM `nordleaf_stg.deliveries` d
  GROUP BY 1,2,3
),

production_efficiency AS (
  SELECT
    dm.mill_name,
    EXTRACT(YEAR FROM fp.production_date)     AS year,
    EXTRACT(MONTH FROM fp.production_date)    AS month,
    fp.product_type,
    SUM(fp.actual_tons)                       AS produced_tons,
    AVG(fp.efficiency_pct)                    AS avg_efficiency_pct
  FROM `nordleaf_dwh.fact_production` fp
  JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
  GROUP BY 1,2,3,4
)

SELECT
  r.product_type,
  r.product_category,
  r.mill_name,
  r.mill_city,
  r.customer_region,
  r.customer_type,
  r.year,
  r.quarter,
  r.month,
  r.month_name,
  r.order_count,
  ROUND(r.sold_tons, 1)                       AS sold_tons,
  ROUND(r.revenue_eur, 0)                     AS revenue_eur,
  ROUND(r.avg_price_per_ton, 2)               AS avg_price_per_ton_eur,
  -- Estimated material cost allocation (proportional to sold tons)
  ROUND(
    COALESCE(mc.total_material_cost_eur, 0)
    * SAFE_DIVIDE(r.sold_tons, NULLIF(pe.produced_tons, 0))
  , 0)                                        AS est_material_cost_eur,
  -- Gross margin estimate
  ROUND(
    r.revenue_eur
    - COALESCE(mc.total_material_cost_eur, 0)
      * SAFE_DIVIDE(r.sold_tons, NULLIF(pe.produced_tons, 0))
  , 0)                                        AS est_gross_margin_eur,
  ROUND(
    (r.revenue_eur
     - COALESCE(mc.total_material_cost_eur, 0)
       * SAFE_DIVIDE(r.sold_tons, NULLIF(pe.produced_tons, 0)))
    / NULLIF(r.revenue_eur, 0) * 100
  , 1)                                        AS est_gross_margin_pct,
  ROUND(r.avg_lead_time_days, 1)              AS avg_lead_time_days,
  ROUND(pe.avg_efficiency_pct, 1)             AS mill_efficiency_pct,
  ROUND(r.cancelled_count / NULLIF(r.order_count,0) * 100, 1) AS cancellation_rate_pct
FROM revenue_by_product_mill r
LEFT JOIN material_cost_by_mill_month mc
  ON r.mill_name = mc.mill_name AND r.year = mc.year AND r.month = mc.month
LEFT JOIN production_efficiency pe
  ON r.mill_name = pe.mill_name AND r.year = pe.year
  AND r.month = pe.month AND r.product_type = pe.product_type
ORDER BY r.year DESC, r.month DESC, revenue_eur DESC;

-- Executive Revenue KPI View
CREATE OR REPLACE VIEW `nordleaf_mgmt.v_revenue_kpi` AS
SELECT
  dd.year,
  dd.quarter,
  dp.product_type,
  dm.mill_name,
  dc.region,
  COUNT(fo.order_id)          AS orders,
  ROUND(SUM(fo.quantity_tons),0) AS tons_sold,
  ROUND(SUM(fo.total_value_eur),0) AS revenue_eur,
  ROUND(AVG(fo.unit_price_eur),2)  AS avg_price_per_ton
FROM `nordleaf_dwh.fact_orders` fo
JOIN `nordleaf_dwh.dim_date` dd     ON fo.order_date_id = dd.date_id
JOIN `nordleaf_dwh.dim_product` dp  ON fo.product_key = dp.product_key
JOIN `nordleaf_dwh.dim_mill` dm     ON fo.mill_key = dm.mill_key
JOIN `nordleaf_dwh.dim_customer` dc ON fo.customer_key = dc.customer_key
WHERE fo.status = 'DELIVERED' AND dc.is_current
GROUP BY 1,2,3,4,5;
