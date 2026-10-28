-- ============================================================
-- UC-08: Raw Material Cost & Spend Analytics
-- Domain: Supply Chain / Finance
-- Business Question: How much are we spending on raw materials?
--   What drives cost volatility? How does material cost relate
--   to product margin?
-- Priority: P1 | Users: CFO, Procurement, Controllers
-- ============================================================

WITH monthly_material_cost AS (
  SELECT
    DATE_TRUNC(d.delivery_date, MONTH) AS month,
    d.material_type,
    s.supplier_type,
    d.mill_destination,
    COUNT(d.delivery_id)               AS deliveries,
    ROUND(SUM(d.quantity_tons), 1)     AS total_tons,
    ROUND(SUM(d.invoice_eur), 0)       AS total_cost_eur,
    ROUND(AVG(d.invoice_eur / NULLIF(d.quantity_tons,0)), 2) AS cost_per_ton,
    ROUND(MIN(d.invoice_eur / NULLIF(d.quantity_tons,0)), 2) AS min_cost_per_ton,
    ROUND(MAX(d.invoice_eur / NULLIF(d.quantity_tons,0)), 2) AS max_cost_per_ton
  FROM `nordleaf_stg.deliveries` d
  LEFT JOIN `nordleaf_stg.suppliers` s USING (supplier_id)
  GROUP BY 1,2,3,4
),

monthly_revenue AS (
  SELECT
    DATE_TRUNC(dd.full_date, MONTH) AS month,
    dm.mill_name,
    SUM(fo.total_value_eur)         AS revenue_eur,
    SUM(fo.quantity_tons)           AS sold_tons
  FROM `nordleaf_dwh.fact_orders` fo
  JOIN `nordleaf_dwh.dim_date` dd ON fo.order_date_id = dd.date_id
  JOIN `nordleaf_dwh.dim_mill` dm ON fo.mill_key = dm.mill_key
  WHERE fo.status = 'DELIVERED'
  GROUP BY 1,2
)

SELECT
  c.month,
  c.material_type,
  c.supplier_type,
  c.mill_destination,
  c.total_tons,
  c.total_cost_eur,
  c.cost_per_ton,
  c.min_cost_per_ton,
  c.max_cost_per_ton,
  ROUND(c.max_cost_per_ton - c.min_cost_per_ton, 2) AS cost_spread_eur,
  r.revenue_eur,
  -- Material cost as % of revenue (cost ratio)
  ROUND(c.total_cost_eur / NULLIF(r.revenue_eur, 0) * 100, 1) AS material_cost_pct_revenue,
  -- Cost trend (MoM change — requires LAG)
  ROUND(c.cost_per_ton - LAG(c.cost_per_ton) OVER (
    PARTITION BY c.material_type, c.mill_destination
    ORDER BY c.month
  ), 2) AS mom_cost_change_eur,
  -- Budget flag (energy typically 30-40% of operating costs)
  CASE c.supplier_type
    WHEN 'Energy'    THEN ROUND(c.total_cost_eur / NULLIF(r.revenue_eur,0)*100,1)
    WHEN 'Timber'    THEN ROUND(c.total_cost_eur / NULLIF(r.revenue_eur,0)*100,1)
    WHEN 'Chemicals' THEN ROUND(c.total_cost_eur / NULLIF(r.revenue_eur,0)*100,1)
    ELSE NULL
  END AS category_cost_pct_revenue
FROM monthly_material_cost c
LEFT JOIN monthly_revenue r
  ON c.month = r.month AND c.mill_destination = r.mill_name
ORDER BY c.month DESC, c.total_cost_eur DESC;
