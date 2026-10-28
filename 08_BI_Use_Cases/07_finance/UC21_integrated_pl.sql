-- ============================================================
-- UC-21: Integrated P&L — Revenue vs Raw Material Cost
-- Domain: Finance / Executive
-- Business Question: What is NordLeaf's estimated gross profit?
--   Revenue (sales) vs Cost of Goods Sold (raw materials)?
--   Which month / mill was most profitable?
-- Priority: P1 | Users: CFO, CEO, Board
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  fact_orders   → Revenue line (top line)
--  fact_deliveries → COGS line (raw material input costs)
--  fact_production → Efficiency adjustment
--         |
--         | Joined by: mill_name + YEAR + MONTH
--         v
--  Integrated P&L View:
--    Revenue
--    - Raw Material Cost        (deliveries invoices)
--    = Gross Profit
--    Gross Margin %
--    Est. EBITDA (assuming 30% fixed cost ratio)
-- ============================================================

WITH monthly_revenue AS (
  SELECT
    dm.mill_name,
    dd.year,
    dd.month,
    dd.month_name,
    dp.product_category,
    SUM(fo.total_value_eur)   AS revenue_eur,
    SUM(fo.quantity_tons)     AS sold_tons,
    COUNT(fo.order_id)        AS order_count
  FROM `nordleaf_dwh.fact_orders` fo
  JOIN `nordleaf_dwh.dim_date`    dd ON fo.order_date_id = dd.date_id
  JOIN `nordleaf_dwh.dim_mill`    dm ON fo.mill_key      = dm.mill_key
  JOIN `nordleaf_dwh.dim_product` dp ON fo.product_key   = dp.product_key
  WHERE fo.status = 'DELIVERED'
  GROUP BY 1,2,3,4,5
),

monthly_raw_cost AS (
  SELECT
    d.mill_destination        AS mill_name,
    EXTRACT(YEAR  FROM d.delivery_date) AS year,
    EXTRACT(MONTH FROM d.delivery_date) AS month,
    SUM(d.invoice_eur)        AS raw_material_cost_eur,
    SUM(d.quantity_tons)      AS material_received_tons,
    COUNT(d.delivery_id)      AS delivery_count
  FROM `nordleaf_stg.deliveries` d
  GROUP BY 1,2,3
),

production_facts AS (
  SELECT
    dm.mill_name,
    EXTRACT(YEAR  FROM fp.production_date) AS year,
    EXTRACT(MONTH FROM fp.production_date) AS month,
    SUM(fp.actual_tons)       AS produced_tons,
    AVG(fp.efficiency_pct)    AS avg_efficiency_pct,
    COUNTIF(fp.quality_grade='A') AS grade_a_batches,
    COUNT(*)                  AS total_batches
  FROM `nordleaf_dwh.fact_production` fp
  JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
  GROUP BY 1,2,3
)

SELECT
  r.mill_name,
  r.year,
  r.month,
  r.month_name,
  r.product_category,
  ROUND(r.revenue_eur, 0)                   AS revenue_eur,
  ROUND(r.sold_tons, 1)                     AS sold_tons,
  r.order_count,
  ROUND(COALESCE(mc.raw_material_cost_eur,0), 0) AS raw_material_cost_eur,
  -- Gross Profit
  ROUND(r.revenue_eur - COALESCE(mc.raw_material_cost_eur,0), 0) AS gross_profit_eur,
  -- Gross Margin %
  ROUND(
    (r.revenue_eur - COALESCE(mc.raw_material_cost_eur,0))
    / NULLIF(r.revenue_eur,0) * 100
  , 1)                                       AS gross_margin_pct,
  -- Estimated EBITDA (assuming fixed costs = 30% of revenue)
  ROUND(
    r.revenue_eur - COALESCE(mc.raw_material_cost_eur,0)
    - (r.revenue_eur * 0.30)
  , 0)                                       AS est_ebitda_eur,
  ROUND(
    (r.revenue_eur - COALESCE(mc.raw_material_cost_eur,0)
     - r.revenue_eur * 0.30)
    / NULLIF(r.revenue_eur,0) * 100
  , 1)                                       AS est_ebitda_margin_pct,
  -- Production context
  ROUND(COALESCE(pf.avg_efficiency_pct,0),1) AS mill_efficiency_pct,
  ROUND(COALESCE(pf.produced_tons,0),0)      AS produced_tons,
  -- Revenue per ton
  ROUND(r.revenue_eur / NULLIF(r.sold_tons,0), 2) AS revenue_per_ton_eur,
  ROUND(COALESCE(mc.raw_material_cost_eur,0)
    / NULLIF(r.sold_tons,0), 2)              AS material_cost_per_ton_eur
FROM monthly_revenue r
LEFT JOIN monthly_raw_cost mc
  ON r.mill_name = mc.mill_name AND r.year = mc.year AND r.month = mc.month
LEFT JOIN production_facts pf
  ON r.mill_name = pf.mill_name AND r.year = pf.year AND r.month = pf.month
ORDER BY r.year DESC, r.month DESC, gross_profit_eur DESC;
