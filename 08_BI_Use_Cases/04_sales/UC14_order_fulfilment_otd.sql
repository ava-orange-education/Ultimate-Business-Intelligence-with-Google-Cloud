-- ============================================================
-- UC-14: Order Fulfilment & On-Time Delivery (OTD)
-- Domain: Sales / Customer Service / Logistics
-- Business Question: Are we delivering on time? What causes
--   delays? Which customers / mills have worst OTD?
-- Priority: P1 | Users: Customer Service, Logistics, Sales
-- ============================================================

WITH order_delivery_analysis AS (
  SELECT
    fo.order_id,
    dc.customer_name,
    dc.country                            AS customer_country,
    dc.region                             AS customer_region,
    dc.customer_type,
    dp.product_type,
    dm.mill_name,
    dd_ord.full_date                      AS order_date,
    -- The promised delivery date (surrogate key decoded)
    DATE(CAST(
      CONCAT(
        SUBSTR(CAST(fo.delivery_date_id AS STRING),1,4),'-',
        SUBSTR(CAST(fo.delivery_date_id AS STRING),5,2),'-',
        SUBSTR(CAST(fo.delivery_date_id AS STRING),7,2)
      ) AS TIMESTAMP)
    )                                     AS promised_delivery_date,
    fo.lead_time_days                     AS promised_lead_days,
    fo.quantity_tons,
    fo.total_value_eur,
    fo.status,
    -- Industry SLA: Newsprint ≤10 days, others ≤14 days
    CASE dp.product_type
      WHEN 'Newsprint' THEN 10
      ELSE 14
    END                                   AS sla_lead_days,
    -- On-time check
    CASE
      WHEN fo.status = 'CANCELLED'              THEN 'CANCELLED'
      WHEN fo.lead_time_days <= CASE dp.product_type WHEN 'Newsprint' THEN 10 ELSE 14 END
        AND fo.status = 'DELIVERED'             THEN 'ON TIME'
      WHEN fo.status = 'DELIVERED'              THEN 'LATE'
      WHEN fo.status = 'IN_TRANSIT'             THEN 'IN TRANSIT'
      ELSE 'PENDING'
    END                                   AS delivery_status,
    -- Days early / late
    CASE dp.product_type WHEN 'Newsprint' THEN 10 ELSE 14 END
      - fo.lead_time_days                 AS days_vs_sla
  FROM `nordleaf_dwh.fact_orders` fo
  JOIN `nordleaf_dwh.dim_customer` dc ON fo.customer_key  = dc.customer_key AND dc.is_current
  JOIN `nordleaf_dwh.dim_product`  dp ON fo.product_key   = dp.product_key
  JOIN `nordleaf_dwh.dim_mill`     dm ON fo.mill_key       = dm.mill_key
  JOIN `nordleaf_dwh.dim_date`     dd_ord ON fo.order_date_id = dd_ord.date_id
)

-- OTD Summary by Mill and Product
SELECT
  mill_name,
  product_type,
  customer_region,
  COUNT(*)                                  AS total_orders,
  COUNTIF(delivery_status = 'ON TIME')      AS on_time,
  COUNTIF(delivery_status = 'LATE')         AS late,
  COUNTIF(delivery_status = 'CANCELLED')    AS cancelled,
  ROUND(COUNTIF(delivery_status='ON TIME')
    / NULLIF(COUNTIF(status IN ('DELIVERED')), 0) * 100, 1) AS otd_pct,
  ROUND(AVG(CASE WHEN delivery_status IN ('ON TIME','LATE')
    THEN promised_lead_days END), 1)         AS avg_lead_days,
  ROUND(AVG(days_vs_sla), 1)                AS avg_days_vs_sla,
  -- SLA compliance (target: >95% OTD)
  CASE
    WHEN COUNTIF(delivery_status='ON TIME')
      / NULLIF(COUNTIF(delivery_status IN ('ON TIME','LATE')),0) >= 0.95
      THEN 'SLA MET'
    WHEN COUNTIF(delivery_status='ON TIME')
      / NULLIF(COUNTIF(delivery_status IN ('ON TIME','LATE')),0) >= 0.85
      THEN 'SLA AT RISK'
    ELSE 'SLA BREACH'
  END AS sla_status
FROM order_delivery_analysis
GROUP BY 1,2,3
ORDER BY otd_pct ASC;
