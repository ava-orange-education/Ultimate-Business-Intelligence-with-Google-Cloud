-- ============================================================
-- UC-24: Carbon Footprint & ESG Reporting
-- Domain: Sustainability / ESG
-- Business Question: What is NordLeaf's carbon footprint per
--   ton of paper produced? Are we on track for EU Green Deal
--   targets? Which mill is most energy intensive?
-- Priority: P1 | Users: Sustainability, Board, Investors
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  fact_deliveries (energy supplier = Fortum Oyj)
--    material_type = 'Electricity (MWh)'
--    → Energy consumption per mill per month
--         |
--         | × Emission factor (Finland grid: 0.079 kg CO2/kWh in 2023)
--         v
--  Scope 2 emissions (purchased electricity)
--
--  fact_deliveries (timber suppliers)
--    → Timber transport emissions proxy
--         |
--         | × transport emission factor
--         v
--  Scope 3 upstream emissions
--
--  fact_production (actual_tons per mill)
--    → Carbon intensity = tonnes CO2 / tonne paper
--
-- EMISSION FACTORS (Finland 2023):
--   Grid electricity:   0.079 kg CO2e/kWh
--   Diesel logistics:   0.168 kg CO2e/tonne-km
--   Pulp production:    0.800 kg CO2e/kg pulp (kraft process)
-- ============================================================

WITH energy_consumption AS (
  SELECT
    d.mill_destination                       AS mill_name,
    DATE_TRUNC(d.delivery_date, MONTH)       AS month,
    SUM(d.quantity_tons)                     AS energy_mwh,
    -- Scope 2: purchased electricity emissions (Finland grid 2023)
    ROUND(SUM(d.quantity_tons) * 1000 * 0.079 / 1000, 2) AS scope2_tonnes_co2e
  FROM `nordleaf_stg.deliveries` d
  WHERE d.material_type LIKE '%Electricity%'
  GROUP BY 1,2
),

logistics_emissions AS (
  SELECT
    d.mill_destination                       AS mill_name,
    DATE_TRUNC(d.delivery_date, MONTH)       AS month,
    SUM(d.quantity_tons)                     AS freight_tons,
    -- Scope 3: supplier logistics (assumed avg 250km per delivery, diesel truck)
    ROUND(SUM(d.quantity_tons) * 250 * 0.168 / 1000, 2) AS scope3_logistics_co2e
  FROM `nordleaf_stg.deliveries` d
  WHERE d.material_type IN ('Softwood Pulp','Hardwood Pulp','Softwood Logs')
  GROUP BY 1,2
),

production_output AS (
  SELECT
    dm.mill_name,
    DATE_TRUNC(fp.production_date, MONTH)    AS month,
    SUM(fp.actual_tons)                      AS produced_paper_tons,
    -- Scope 1 proxy: process heat (assume 0.4 t CO2e per t paper, biomass boilers)
    ROUND(SUM(fp.actual_tons) * 0.40, 2)     AS scope1_process_co2e
  FROM `nordleaf_dwh.fact_production` fp
  JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
  GROUP BY 1,2
)

SELECT
  po.mill_name,
  po.month,
  ROUND(po.produced_paper_tons, 0)           AS produced_paper_tons,
  -- Emissions by scope
  ROUND(COALESCE(po.scope1_process_co2e,0),1)       AS scope1_co2e_tonnes,
  ROUND(COALESCE(ec.scope2_tonnes_co2e,0),1)         AS scope2_co2e_tonnes,
  ROUND(COALESCE(le.scope3_logistics_co2e,0),1)      AS scope3_logistics_co2e_tonnes,
  ROUND(
    COALESCE(po.scope1_process_co2e,0)
    + COALESCE(ec.scope2_tonnes_co2e,0)
    + COALESCE(le.scope3_logistics_co2e,0), 1
  )                                          AS total_co2e_tonnes,
  -- Carbon intensity (key ESG KPI)
  ROUND(
    (COALESCE(po.scope1_process_co2e,0)
     + COALESCE(ec.scope2_tonnes_co2e,0)
     + COALESCE(le.scope3_logistics_co2e,0))
    / NULLIF(po.produced_paper_tons, 0), 3
  )                                          AS carbon_intensity_t_co2_per_t_paper,
  COALESCE(ec.energy_mwh, 0)                AS energy_consumed_mwh,
  ROUND(COALESCE(ec.energy_mwh,0)
    / NULLIF(po.produced_paper_tons,0), 3)   AS energy_intensity_mwh_per_t_paper,
  -- EU Green Deal target: 55% reduction by 2030 vs 2023 baseline
  CASE
    WHEN (COALESCE(po.scope1_process_co2e,0)
          + COALESCE(ec.scope2_tonnes_co2e,0))
         / NULLIF(po.produced_paper_tons,0) < 0.35  THEN 'ON TRACK — Green Deal'
    WHEN (COALESCE(po.scope1_process_co2e,0)
          + COALESCE(ec.scope2_tonnes_co2e,0))
         / NULLIF(po.produced_paper_tons,0) < 0.55  THEN 'IMPROVEMENT NEEDED'
    ELSE 'OFF TRACK — needs investment'
  END AS eu_green_deal_status
FROM production_output po
LEFT JOIN energy_consumption ec USING (mill_name, month)
LEFT JOIN logistics_emissions  le USING (mill_name, month)
ORDER BY po.month DESC, carbon_intensity_t_co2_per_t_paper DESC;

-- ESG Summary View for Sustainability Report
CREATE OR REPLACE VIEW `nordleaf_mgmt.v_esg_summary` AS
SELECT
  EXTRACT(YEAR FROM po.production_date)    AS year,
  dm.mill_name,
  ROUND(SUM(po.actual_tons), 0)            AS paper_produced_tons,
  ROUND(SUM(po.actual_tons * 0.40), 0)     AS scope1_co2e_est,
  COUNTIF(po.quality_grade = 'A')          AS grade_a_batches,
  ROUND(AVG(po.efficiency_pct), 1)         AS avg_efficiency_pct,
  ROUND(COUNTIF(po.quality_grade='A') / COUNT(*) * 100, 1) AS grade_a_pct
FROM `nordleaf_dwh.fact_production` po
JOIN `nordleaf_dwh.dim_mill` dm ON po.mill_id = dm.mill_id
GROUP BY 1, 2;
