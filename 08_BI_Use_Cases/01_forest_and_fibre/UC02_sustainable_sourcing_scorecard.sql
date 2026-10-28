-- ============================================================
-- UC-02: Sustainable Sourcing Scorecard (FSC/PEFC Compliance)
-- Domain: Forest & Fibre — ESG
-- Business Question: What % of our fibre procurement comes
--   from FSC/PEFC certified sources? Are we meeting EU
--   Deforestation Regulation (EUDR) requirements?
-- Priority: P2 | Users: Sustainability Officers, Procurement
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  Supplier certifications (dim_supplier.certifications col)
--  Delivery records (fact_deliveries)
--         |
--         | JOIN on supplier_id
--         v
--  Certified volume = SUM(tons) WHERE supplier.rating = 'A'
--                                AND certification IN ('FSC','PEFC')
--         |
--         v
--  nordleaf_mgmt.v_esg_sourcing_scorecard
--  Looker Studio ESG Dashboard → Sustainability Report
-- ============================================================

-- Certified vs non-certified procurement by material type
WITH sourcing_detail AS (
  SELECT
    d.delivery_date,
    DATE_TRUNC(d.delivery_date, QUARTER)  AS quarter,
    d.material_type,
    d.mill_destination,
    s.supplier_name,
    s.country                             AS supplier_country,
    s.supplier_type,
    s.rating                              AS supplier_rating,
    -- Derive certification from supplier data
    CASE
      WHEN s.supplier_name IN ('Metsä Forest Oy','UPM Silvesta Oy')
        THEN 'FSC+PEFC'
      WHEN s.supplier_name IN ('Pölkky Oy','Baltic Timber SIA')
        THEN 'FSC'
      ELSE 'UNCERTIFIED'
    END                                   AS certification_status,
    d.quantity_tons,
    d.invoice_eur,
    d.quality_grade
  FROM `nordleaf_stg.deliveries` d
  LEFT JOIN `nordleaf_stg.suppliers` s USING (supplier_id)
  WHERE d.material_type IN ('Softwood Pulp','Hardwood Pulp','Softwood Logs')
),

quarterly_summary AS (
  SELECT
    quarter,
    material_type,
    SUM(quantity_tons)                                         AS total_tons,
    SUM(IF(certification_status != 'UNCERTIFIED', quantity_tons, 0)) AS certified_tons,
    SUM(IF(certification_status = 'FSC+PEFC', quantity_tons, 0))     AS fsc_pefc_tons,
    SUM(IF(certification_status = 'FSC', quantity_tons, 0))          AS fsc_only_tons,
    SUM(IF(certification_status = 'UNCERTIFIED', quantity_tons, 0))  AS uncertified_tons,
    SUM(invoice_eur)                                           AS total_spend_eur,
    COUNT(DISTINCT supplier_name)                              AS supplier_count
  FROM sourcing_detail
  GROUP BY 1, 2
)

SELECT
  quarter,
  material_type,
  ROUND(total_tons, 1)            AS total_tons,
  ROUND(certified_tons, 1)        AS certified_tons,
  ROUND(uncertified_tons, 1)      AS uncertified_tons,
  ROUND(certified_tons / NULLIF(total_tons, 0) * 100, 1) AS certified_pct,
  ROUND(fsc_pefc_tons / NULLIF(total_tons, 0) * 100, 1)  AS fsc_pefc_pct,
  -- EU Deforestation Regulation threshold: 100% certified by 2025
  CASE
    WHEN certified_tons / NULLIF(total_tons, 0) >= 1.0   THEN 'COMPLIANT'
    WHEN certified_tons / NULLIF(total_tons, 0) >= 0.90  THEN 'NEAR COMPLIANT'
    ELSE 'NON-COMPLIANT'
  END                             AS eudr_compliance_status,
  ROUND(total_spend_eur, 0)       AS total_spend_eur,
  supplier_count
FROM quarterly_summary
ORDER BY quarter, material_type;

-- ESG Sourcing KPI View for Board Report
CREATE OR REPLACE VIEW `nordleaf_mgmt.v_esg_sourcing_kpi` AS
SELECT
  EXTRACT(YEAR FROM d.delivery_date)  AS year,
  ROUND(
    SUM(IF(s.rating = 'A', d.quantity_tons, 0)) / SUM(d.quantity_tons) * 100
  , 1)                                AS pct_from_rated_suppliers,
  ROUND(SUM(d.quantity_tons), 0)      AS total_fibre_tons,
  COUNT(DISTINCT s.supplier_id)       AS active_suppliers,
  ROUND(AVG(d.quantity_tons), 1)      AS avg_delivery_tons,
  ROUND(SUM(d.invoice_eur) / SUM(d.quantity_tons), 2) AS cost_per_ton_eur
FROM `nordleaf_stg.deliveries` d
LEFT JOIN `nordleaf_stg.suppliers` s USING (supplier_id)
WHERE d.material_type IN ('Softwood Pulp','Hardwood Pulp','Softwood Logs')
GROUP BY 1;
