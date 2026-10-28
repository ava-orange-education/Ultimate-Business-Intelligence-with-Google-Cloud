-- ============================================================
-- UC-07: Supplier Performance Scorecard
-- Domain: Supply Chain / Procurement
-- Business Question: Which suppliers are performing well on
--   quality, reliability, and cost? Where should we renegotiate?
-- Priority: P1 | Users: Procurement Director, Supply Chain
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  fact_deliveries (delivery_id, supplier_id, delivery_date,
--                   material_type, quantity_tons, quality_grade,
--                   invoice_eur)
--         |
--         | JOIN dim_supplier
--         v
--  Supplier KPIs:
--    Quality Score   = % Grade A deliveries
--    Volume Score    = total tons vs contract expectation
--    Cost Efficiency = actual invoice vs estimated market rate
--    Reliability     = deliveries within expected frequency
--         |
--         v
--  nordleaf_mgmt.v_supplier_scorecard
--  "Procurement" Looker Studio dashboard
-- ============================================================

WITH supplier_kpis AS (
  SELECT
    s.supplier_id,
    s.supplier_name,
    s.country,
    s.supplier_type,
    s.rating                                AS contract_rating,
    s.annual_contract_eur,
    s.payment_terms,
    -- Delivery volume metrics
    COUNT(d.delivery_id)                    AS delivery_count,
    ROUND(SUM(d.quantity_tons), 1)          AS total_tons_delivered,
    ROUND(AVG(d.quantity_tons), 1)          AS avg_delivery_size_tons,
    ROUND(MIN(d.quantity_tons), 1)          AS min_delivery_tons,
    ROUND(MAX(d.quantity_tons), 1)          AS max_delivery_tons,
    -- Quality metrics
    COUNTIF(d.quality_grade = 'A')          AS grade_a_deliveries,
    COUNTIF(d.quality_grade = 'B')          AS grade_b_deliveries,
    COUNTIF(d.quality_grade = 'C')          AS grade_c_deliveries,
    ROUND(COUNTIF(d.quality_grade = 'A') / NULLIF(COUNT(*),0) * 100, 1) AS quality_score_pct,
    -- Cost metrics
    ROUND(SUM(d.invoice_eur), 0)            AS total_spend_eur,
    ROUND(AVG(d.invoice_eur / NULLIF(d.quantity_tons,0)), 2) AS avg_cost_per_ton_eur,
    -- Delivery spread (reliability proxy: std dev of quantities)
    ROUND(STDDEV(d.quantity_tons), 1)       AS delivery_consistency_stddev,
    -- Days between deliveries (frequency reliability)
    ROUND(DATE_DIFF(MAX(d.delivery_date), MIN(d.delivery_date), DAY)
      / NULLIF(COUNT(d.delivery_id) - 1, 0), 0) AS avg_days_between_deliveries,
    -- Mills served
    COUNT(DISTINCT d.mill_destination)      AS mills_served
  FROM `nordleaf_stg.suppliers` s
  LEFT JOIN `nordleaf_stg.deliveries` d USING (supplier_id)
  GROUP BY 1,2,3,4,5,6,7
),

scored AS (
  SELECT
    *,
    -- Composite score (0-100)
    ROUND(
      quality_score_pct * 0.40
      + LEAST(delivery_count * 5, 30) * 0.30    -- reliability proxy
      + CASE contract_rating WHEN 'A' THEN 30 ELSE 15 END * 0.30
    , 1) AS composite_supplier_score
  FROM supplier_kpis
)

SELECT
  supplier_name,
  country,
  supplier_type,
  contract_rating,
  ROUND(annual_contract_eur / 1000000, 2) AS contract_value_meur,
  payment_terms,
  delivery_count,
  ROUND(total_tons_delivered, 0)          AS total_tons,
  quality_score_pct,
  ROUND(total_spend_eur / 1000, 0)        AS spend_keur,
  ROUND(avg_cost_per_ton_eur, 2)          AS cost_per_ton_eur,
  avg_days_between_deliveries,
  mills_served,
  composite_supplier_score,
  CASE
    WHEN composite_supplier_score >= 85 THEN 'PREFERRED — renew & expand'
    WHEN composite_supplier_score >= 70 THEN 'APPROVED — maintain'
    WHEN composite_supplier_score >= 50 THEN 'MONITOR — improvement plan'
    ELSE 'AT RISK — renegotiate or replace'
  END AS supplier_recommendation,
  -- Flag for procurement action
  IF(quality_score_pct < 70 OR grade_c_deliveries > 2, TRUE, FALSE) AS quality_flag,
  IF(delivery_count < 3, TRUE, FALSE) AS low_volume_flag
FROM scored
ORDER BY composite_supplier_score DESC;
