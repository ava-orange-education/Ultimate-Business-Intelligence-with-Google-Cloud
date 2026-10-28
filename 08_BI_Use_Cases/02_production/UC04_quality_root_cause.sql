-- ============================================================
-- UC-04: Quality Grade Root Cause Analysis
-- Domain: Production Quality
-- Business Question: Why are certain batches getting Grade B/C?
--   Is it the supplier material, the shift, the mill, or the
--   product type causing quality degradation?
-- Priority: P1 | Users: Quality Directors, Mill Managers
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  fact_production (quality_grade, mill_id, product_type, shift)
--         |              |
--         |              | JOIN delivery window
--         |              v
--         |       fact_deliveries (quality_grade of input material)
--         |              |
--         v              v
--  Quality Grade (output) vs Material Grade (input)
--  Correlation analysis: which input factors predict Grade C output?
--         |
--         v
--  nordleaf_mgmt.v_quality_root_cause
--  "Quality Management" dashboard in Looker Studio
-- ============================================================

-- Grade distribution by all dimensions (multi-way breakdown)
WITH quality_breakdown AS (
  SELECT
    dm.mill_name,
    fp.product_type,
    fp.shift,
    DATE_TRUNC(fp.production_date, MONTH)  AS month,
    fp.quality_grade                       AS output_grade,
    COUNT(*)                               AS batch_count,
    ROUND(AVG(fp.efficiency_pct), 1)       AS avg_efficiency,
    ROUND(SUM(fp.actual_tons), 0)          AS total_tons,
    ROUND(SUM(fp.planned_tons) - SUM(fp.actual_tons), 1) AS tonnes_lost
  FROM `nordleaf_dwh.fact_production` fp
  JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
  GROUP BY 1, 2, 3, 4, 5
),

-- Input material quality received in the same time window
input_quality AS (
  SELECT
    d.mill_destination                     AS mill_name,
    DATE_TRUNC(d.delivery_date, MONTH)     AS month,
    d.material_type,
    ROUND(AVG(
      CASE d.quality_grade WHEN 'A' THEN 3 WHEN 'B' THEN 2 ELSE 1 END
    ), 2)                                  AS avg_input_quality_score,
    ROUND(COUNTIF(d.quality_grade = 'A') / COUNT(*) * 100, 1) AS pct_a_input
  FROM `nordleaf_stg.deliveries` d
  WHERE d.material_type IN ('Softwood Pulp','Hardwood Pulp','Softwood Logs')
  GROUP BY 1, 2, 3
)

SELECT
  q.mill_name,
  q.product_type,
  q.shift,
  q.month,
  q.output_grade,
  q.batch_count,
  q.avg_efficiency,
  q.total_tons,
  q.tonnes_lost,
  ROUND(q.batch_count / SUM(q.batch_count)
    OVER (PARTITION BY q.mill_name, q.product_type, q.month) * 100, 1) AS pct_of_batches,
  iq.avg_input_quality_score,
  iq.pct_a_input                           AS pct_grade_a_input_material,
  -- Root cause hypothesis
  CASE
    WHEN q.output_grade IN ('B','C') AND iq.pct_a_input < 60
      THEN 'INPUT MATERIAL QUALITY'
    WHEN q.output_grade IN ('B','C') AND q.shift = 'NIGHT'
      AND q.avg_efficiency < 88
      THEN 'NIGHT SHIFT PERFORMANCE'
    WHEN q.output_grade IN ('B','C') AND q.avg_efficiency < 85
      THEN 'PRODUCTION EFFICIENCY'
    WHEN q.output_grade IN ('B','C') AND q.product_type = 'Cardboard'
      THEN 'PRODUCT COMPLEXITY'
    WHEN q.output_grade = 'C'
      THEN 'MULTI-FACTOR — INVESTIGATE'
    ELSE 'NORMAL'
  END AS root_cause_hypothesis
FROM quality_breakdown q
LEFT JOIN input_quality iq
  ON q.mill_name = iq.mill_name AND q.month = iq.month
ORDER BY q.month DESC, q.mill_name, q.output_grade DESC;

-- Shift quality comparison view
CREATE OR REPLACE VIEW `nordleaf_mgmt.v_shift_quality_comparison` AS
SELECT
  dm.mill_name,
  fp.shift,
  fp.product_type,
  COUNT(*)                               AS batches,
  ROUND(AVG(fp.efficiency_pct), 1)       AS avg_efficiency_pct,
  ROUND(COUNTIF(fp.quality_grade='A') / COUNT(*) * 100, 1) AS grade_a_pct,
  ROUND(COUNTIF(fp.quality_grade='C') / COUNT(*) * 100, 1) AS grade_c_pct,
  ROUND(SUM(fp.planned_tons - fp.actual_tons), 0)           AS total_tonnes_lost
FROM `nordleaf_dwh.fact_production` fp
JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
GROUP BY 1, 2, 3;
