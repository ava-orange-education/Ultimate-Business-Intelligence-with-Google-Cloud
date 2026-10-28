-- ============================================================
-- UC-12: Customer Profitability & Customer Lifetime Value (CLV)
-- Domain: Sales / CRM
-- Business Question: Who are our most valuable customers?
--   Who is at risk of churning? Where should we focus retention?
-- Priority: P1 | Users: Sales Director, Account Managers, CFO
-- ============================================================

WITH customer_orders AS (
  SELECT
    dc.customer_key,
    dc.customer_name,
    dc.country,
    dc.region,
    dc.customer_type,
    dc.credit_rating,
    -- Revenue metrics
    COUNT(fo.order_id)                        AS total_orders,
    COUNTIF(fo.status = 'DELIVERED')          AS delivered_orders,
    COUNTIF(fo.status = 'CANCELLED')          AS cancelled_orders,
    SUM(fo.total_value_eur)                   AS total_revenue_eur,
    AVG(fo.total_value_eur)                   AS avg_order_value_eur,
    SUM(fo.quantity_tons)                     AS total_tons,
    AVG(fo.unit_price_eur)                    AS avg_price_per_ton,
    -- Recency
    MAX(dd.full_date)                         AS last_order_date,
    MIN(dd.full_date)                         AS first_order_date,
    DATE_DIFF(MAX(dd.full_date), MIN(dd.full_date), DAY) AS customer_tenure_days,
    DATE_DIFF(CURRENT_DATE(), MAX(dd.full_date), DAY)    AS days_since_last_order,
    -- Frequency
    COUNT(DISTINCT DATE_TRUNC(dd.full_date, MONTH))      AS active_months,
    -- Favourite product
    (SELECT product_type FROM (
      SELECT fo2.order_id, dp2.product_type,
             ROW_NUMBER() OVER (
               PARTITION BY fo2.customer_key
               ORDER BY SUM(fo2.total_value_eur) DESC
             ) rn
      FROM `nordleaf_dwh.fact_orders` fo2
      JOIN `nordleaf_dwh.dim_product` dp2 ON fo2.product_key = dp2.product_key
      WHERE fo2.customer_key = dc.customer_key
      GROUP BY fo2.order_id, dp2.product_type
    ) t WHERE rn = 1 LIMIT 1)                             AS top_product
  FROM `nordleaf_dwh.dim_customer` dc
  LEFT JOIN `nordleaf_dwh.fact_orders`     fo ON dc.customer_key   = fo.customer_key
  LEFT JOIN `nordleaf_dwh.dim_date`        dd ON fo.order_date_id  = dd.date_id
  WHERE dc.is_current = TRUE
  GROUP BY 1,2,3,4,5,6
),

feedback_summary AS (
  SELECT
    customer_name,
    ROUND(AVG(rating), 2)  AS avg_satisfaction_rating,
    COUNT(*)               AS feedback_count,
    COUNTIF(rating <= 2)   AS low_rating_count
  FROM `nordleaf_stg.customer_feedback`
  GROUP BY 1
),

clv_scoring AS (
  SELECT
    co.*,
    fs.avg_satisfaction_rating,
    fs.feedback_count,
    fs.low_rating_count,
    -- RFM Score (Recency 1-5, Frequency 1-5, Monetary 1-5)
    CASE
      WHEN days_since_last_order <= 30  THEN 5
      WHEN days_since_last_order <= 60  THEN 4
      WHEN days_since_last_order <= 90  THEN 3
      WHEN days_since_last_order <= 180 THEN 2
      ELSE 1
    END AS recency_score,
    CASE
      WHEN active_months >= 6  THEN 5
      WHEN active_months >= 4  THEN 4
      WHEN active_months >= 3  THEN 3
      WHEN active_months >= 2  THEN 2
      ELSE 1
    END AS frequency_score,
    CASE
      WHEN total_revenue_eur >= 500000  THEN 5
      WHEN total_revenue_eur >= 200000  THEN 4
      WHEN total_revenue_eur >= 100000  THEN 3
      WHEN total_revenue_eur >= 50000   THEN 2
      ELSE 1
    END AS monetary_score
  FROM customer_orders co
  LEFT JOIN feedback_summary fs ON co.customer_name = fs.customer_name
)

SELECT
  customer_name,
  country,
  region,
  customer_type,
  credit_rating,
  total_orders,
  ROUND(total_revenue_eur, 0)              AS total_revenue_eur,
  ROUND(avg_order_value_eur, 0)            AS avg_order_value_eur,
  ROUND(total_tons, 1)                     AS total_tons,
  ROUND(avg_price_per_ton, 2)              AS avg_price_per_ton,
  days_since_last_order,
  customer_tenure_days,
  active_months,
  top_product,
  ROUND(avg_satisfaction_rating, 1)        AS avg_satisfaction,
  -- RFM composite
  recency_score,
  frequency_score,
  monetary_score,
  recency_score + frequency_score + monetary_score AS rfm_total_score,
  -- CLV Segment
  CASE
    WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4
      THEN 'CHAMPION — protect & grow'
    WHEN recency_score >= 3 AND monetary_score >= 4
      THEN 'LOYAL — reward & upsell'
    WHEN recency_score >= 4 AND monetary_score <= 2
      THEN 'PROMISING — develop'
    WHEN recency_score <= 2 AND monetary_score >= 4
      THEN 'AT RISK — win back'
    WHEN recency_score <= 2 AND monetary_score <= 2
      THEN 'LOST — reactivate or exit'
    ELSE 'NEEDS ATTENTION'
  END AS clv_segment,
  -- Cancellation risk
  ROUND(cancelled_orders / NULLIF(total_orders, 0) * 100, 1) AS cancellation_rate_pct,
  IF(days_since_last_order > 90 OR COALESCE(avg_satisfaction_rating,3) < 3, TRUE, FALSE) AS churn_risk_flag
FROM clv_scoring
ORDER BY rfm_total_score DESC, total_revenue_eur DESC;
