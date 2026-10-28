-- ============================================================
-- UC-10: Supply Risk Dashboard
-- Domain: Supply Chain Risk
-- Business Question: Which raw materials are we most exposed
--   to supply disruption? Single-supplier dependency? Low
--   inventory buffer? Low quality streak?
-- Priority: P1 | Users: COO, Supply Chain Director, Board
-- ============================================================

WITH supplier_concentration AS (
  -- Single-source risk: if 1 supplier provides >70% of a material
  SELECT
    d.material_type,
    d.mill_destination,
    s.supplier_name,
    SUM(d.quantity_tons) AS supplier_tons,
    SUM(SUM(d.quantity_tons)) OVER (PARTITION BY d.material_type, d.mill_destination) AS total_tons
  FROM `nordleaf_stg.deliveries` d
  JOIN `nordleaf_stg.suppliers` s USING (supplier_id)
  WHERE d.delivery_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
  GROUP BY 1,2,3
),

risk_flags AS (
  SELECT
    material_type,
    mill_destination,
    supplier_name,
    ROUND(supplier_tons, 0)              AS supplier_tons,
    ROUND(total_tons, 0)                 AS total_tons,
    ROUND(supplier_tons / NULLIF(total_tons,0) * 100, 1) AS concentration_pct,
    CASE
      WHEN supplier_tons / NULLIF(total_tons,0) >= 0.80 THEN 'CRITICAL — single source dependency'
      WHEN supplier_tons / NULLIF(total_tons,0) >= 0.60 THEN 'HIGH — concentrated supply'
      WHEN supplier_tons / NULLIF(total_tons,0) >= 0.40 THEN 'MEDIUM — some concentration'
      ELSE 'LOW — diversified'
    END AS concentration_risk
  FROM supplier_concentration
),

quality_streak AS (
  -- Risk: supplier with 2+ consecutive Grade C deliveries
  SELECT
    supplier_id,
    COUNT(*) AS recent_grade_c_count
  FROM `nordleaf_stg.deliveries`
  WHERE delivery_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    AND quality_grade = 'C'
  GROUP BY 1
)

SELECT
  r.material_type,
  r.mill_destination,
  r.supplier_name,
  r.concentration_pct,
  r.concentration_risk,
  COALESCE(qs.recent_grade_c_count, 0) AS grade_c_deliveries_last_30d,
  CASE
    WHEN COALESCE(qs.recent_grade_c_count, 0) >= 2 THEN 'QUALITY ALERT'
    ELSE 'OK'
  END AS quality_risk,
  -- Overall risk score
  CASE
    WHEN r.concentration_pct >= 80 OR COALESCE(qs.recent_grade_c_count,0) >= 2
      THEN 'HIGH RISK — immediate action'
    WHEN r.concentration_pct >= 60
      THEN 'MEDIUM RISK — develop alternatives'
    ELSE 'LOW RISK — monitor'
  END AS overall_supply_risk
FROM risk_flags r
LEFT JOIN (
  SELECT d.supplier_id, s.supplier_name AS sname, qs.recent_grade_c_count
  FROM `nordleaf_stg.deliveries` d
  JOIN `nordleaf_stg.suppliers` s USING (supplier_id)
  JOIN quality_streak qs USING (supplier_id)
  GROUP BY 1,2,3
) rqs ON r.supplier_name = rqs.sname
LEFT JOIN quality_streak qs ON qs.supplier_id = (
  SELECT supplier_id FROM `nordleaf_stg.suppliers` WHERE supplier_name = r.supplier_name LIMIT 1
)
ORDER BY r.concentration_pct DESC;
