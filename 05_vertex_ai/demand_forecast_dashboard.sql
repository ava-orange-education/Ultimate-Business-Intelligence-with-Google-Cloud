-- ============================================================
-- NordLeaf Demand Forecast Dashboard Queries
-- Use these in Looker Studio / BigQuery connected sheets
-- ============================================================

-- 1. 30-Day Demand Forecast by Product and Mill
SELECT
  forecast_week,
  product_type,
  mill_name,
  forecast_tons,
  lower_bound,
  upper_bound,
  ROUND(forecast_tons * CASE product_type
    WHEN 'Newsprint'       THEN 680
    WHEN 'Kraft Paper'     THEN 520
    WHEN 'Cardboard'       THEN 750
    WHEN 'Specialty Paper' THEN 1200
    WHEN 'Tissue'          THEN 920 ELSE 700 END, 0) AS forecast_revenue_eur
FROM `nordleaf_dwh.demand_forecast`
ORDER BY forecast_week, product_type;

-- 2. Forecast vs Actual (model performance view)
SELECT
  dd.full_date AS order_week,
  dp.product_type,
  dm.mill_name,
  SUM(fo.quantity_tons)    AS actual_tons,
  f.forecast_tons,
  ABS(SUM(fo.quantity_tons) - f.forecast_tons) / NULLIF(SUM(fo.quantity_tons), 0) AS abs_pct_error
FROM `nordleaf_dwh.fact_orders` fo
JOIN `nordleaf_dwh.dim_date`    dd ON fo.order_date_id = dd.date_id
JOIN `nordleaf_dwh.dim_product` dp ON fo.product_key   = dp.product_key
JOIN `nordleaf_dwh.dim_mill`    dm ON fo.mill_key       = dm.mill_key
LEFT JOIN `nordleaf_dwh.demand_forecast` f
  ON DATE_TRUNC(dd.full_date, WEEK) = f.forecast_week
  AND dp.product_type = f.product_type
  AND dm.mill_name = f.mill_name
WHERE fo.status = 'DELIVERED'
  AND dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY 1,2,3,5
ORDER BY 1,2,3;

-- 3. Customer Churn Risk
SELECT
  customer_name,
  predicted_is_churned_probs[OFFSET(1)].prob AS churn_probability,
  days_since_last_order,
  order_count_12m,
  total_revenue_12m,
  CASE
    WHEN predicted_is_churned_probs[OFFSET(1)].prob > 0.7 THEN 'HIGH RISK'
    WHEN predicted_is_churned_probs[OFFSET(1)].prob > 0.4 THEN 'MEDIUM RISK'
    ELSE 'LOW RISK'
  END AS churn_risk_category
FROM ML.PREDICT(
  MODEL `nordleaf_dwh.customer_churn_model`,
  (SELECT * FROM `nordleaf_dwh.dim_customer` c
   LEFT JOIN (
     SELECT customer_key, COUNT(*) AS order_count_12m, SUM(total_value_eur) AS total_revenue_12m,
            AVG(quantity_tons) AS avg_order_size
     FROM `nordleaf_dwh.fact_orders`
     WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
     GROUP BY 1
   ) fo USING (customer_key)
   WHERE c.is_current = TRUE)
)
ORDER BY churn_probability DESC;
