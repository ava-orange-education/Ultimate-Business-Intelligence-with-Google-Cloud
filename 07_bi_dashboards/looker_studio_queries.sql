-- ============================================================
-- NordLeaf Looker Studio Dashboard SQL Queries
-- Connect Looker Studio to BigQuery with these queries
-- ============================================================

-- ============================================================
-- DASHBOARD 1: Executive Revenue Overview
-- ============================================================

-- KPI: Total Revenue (current month)
SELECT
  SUM(total_value_eur) AS total_revenue_eur,
  COUNT(*) AS order_count,
  SUM(quantity_tons) AS total_tons
FROM `nordleaf_dwh.fact_orders` fo
JOIN `nordleaf_dwh.dim_date` dd ON fo.order_date_id = dd.date_id
WHERE dd.year = EXTRACT(YEAR FROM CURRENT_DATE())
  AND dd.month = EXTRACT(MONTH FROM CURRENT_DATE())
  AND fo.status = 'DELIVERED';

-- Revenue trend (monthly, last 12 months)
SELECT
  dd.year,
  dd.month,
  dd.month_name,
  dp.product_type,
  dm.mill_name,
  SUM(fo.total_value_eur)  AS revenue_eur,
  SUM(fo.quantity_tons)    AS tons_sold,
  COUNT(fo.order_id)       AS orders,
  AVG(fo.lead_time_days)   AS avg_lead_days
FROM `nordleaf_dwh.fact_orders` fo
JOIN `nordleaf_dwh.dim_date`    dd ON fo.order_date_id = dd.date_id
JOIN `nordleaf_dwh.dim_product` dp ON fo.product_key   = dp.product_key
JOIN `nordleaf_dwh.dim_mill`    dm ON fo.mill_key       = dm.mill_key
WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
  AND fo.status = 'DELIVERED'
GROUP BY 1,2,3,4,5
ORDER BY 1,2,4,5;

-- ============================================================
-- DASHBOARD 2: Mill Operations & Production KPIs
-- ============================================================

-- Mill efficiency heatmap (month x mill)
SELECT
  dd.month_name,
  dm.mill_name,
  dm.city,
  COUNT(fp.batch_id)           AS batch_count,
  SUM(fp.planned_tons)         AS planned_tons,
  SUM(fp.actual_tons)          AS actual_tons,
  ROUND(AVG(fp.efficiency_pct),1) AS avg_efficiency_pct,
  COUNTIF(fp.quality_grade='A') AS grade_a_count,
  COUNTIF(fp.quality_grade='B') AS grade_b_count,
  COUNTIF(fp.quality_grade='C') AS grade_c_count,
  ROUND(COUNTIF(fp.quality_grade='A') / COUNT(*) * 100, 1) AS grade_a_pct
FROM `nordleaf_dwh.fact_production` fp
JOIN `nordleaf_dwh.dim_date`    dd ON fp.production_date = dd.full_date
JOIN `nordleaf_dwh.dim_mill`    dm ON fp.mill_id         = dm.mill_id
WHERE fp.production_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY 1,2,3
ORDER BY avg_efficiency_pct DESC;

-- Shift performance comparison
SELECT
  dm.mill_name,
  fp.shift,
  ROUND(AVG(fp.efficiency_pct), 1) AS avg_efficiency,
  ROUND(AVG(fp.actual_tons), 0)    AS avg_actual_tons,
  COUNT(*)                          AS batch_count
FROM `nordleaf_dwh.fact_production` fp
JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
WHERE fp.production_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
GROUP BY 1,2
ORDER BY 1,2;

-- ============================================================
-- DASHBOARD 3: Supplier Scorecard
-- ============================================================
SELECT
  s.supplier_name,
  s.country,
  s.supplier_type,
  COUNT(d.delivery_id)                  AS deliveries,
  SUM(d.quantity_tons)                  AS total_tons,
  SUM(d.invoice_eur)                    AS total_spend_eur,
  COUNTIF(d.quality_grade='A')          AS grade_a,
  ROUND(COUNTIF(d.quality_grade='A') / COUNT(*) * 100, 1) AS quality_score_pct,
  AVG(d.quantity_tons)                  AS avg_delivery_tons
FROM `nordleaf_stg.suppliers` s
LEFT JOIN `nordleaf_stg.deliveries` d USING (supplier_id)
WHERE d.delivery_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY 1,2,3
ORDER BY quality_score_pct DESC;

-- ============================================================
-- DASHBOARD 4: Customer Analytics
-- ============================================================
SELECT
  dc.customer_name,
  dc.country,
  dc.region,
  dc.customer_type,
  dc.credit_rating,
  COUNT(fo.order_id)         AS order_count,
  SUM(fo.total_value_eur)    AS total_revenue_eur,
  AVG(fo.quantity_tons)      AS avg_order_size_tons,
  AVG(fo.lead_time_days)     AS avg_lead_days,
  COUNTIF(fo.status='CANCELLED') AS cancelled_orders,
  ROUND(COUNTIF(fo.status='CANCELLED') / COUNT(*) * 100, 1) AS cancellation_rate_pct,
  MAX(dd.full_date)           AS last_order_date,
  DATE_DIFF(CURRENT_DATE(), MAX(dd.full_date), DAY) AS days_since_last_order
FROM `nordleaf_dwh.dim_customer` dc
LEFT JOIN `nordleaf_dwh.fact_orders` fo ON dc.customer_key = fo.customer_key
LEFT JOIN `nordleaf_dwh.dim_date`    dd ON fo.order_date_id = dd.date_id
WHERE dc.is_current = TRUE
GROUP BY 1,2,3,4,5
ORDER BY total_revenue_eur DESC;

-- ============================================================
-- DASHBOARD 5: Data Quality Status
-- ============================================================
SELECT
  DATE(run_timestamp) AS check_date,
  table_name,
  check_type,
  COUNT(*) AS checks_run,
  COUNTIF(status='PASS') AS passed,
  COUNTIF(status='FAIL') AS failed,
  ROUND(COUNTIF(status='PASS') / COUNT(*) * 100, 1) AS pass_rate_pct
FROM `nordleaf_mgmt.dq_results`
WHERE run_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY 1,2,3
ORDER BY check_date DESC, failed DESC;
