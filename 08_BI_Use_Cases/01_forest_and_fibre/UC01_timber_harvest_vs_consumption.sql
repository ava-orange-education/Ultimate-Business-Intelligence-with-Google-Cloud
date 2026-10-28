-- ============================================================
-- UC-01: Timber Harvest vs Mill Consumption Balance
-- Domain: Forest & Fibre Supply
-- Business Question: Are our forest assets supplying enough
--   raw fibre to sustain all 5 mills? Where are the gaps?
-- Priority: P1 | Users: Forest Directors, Mill Managers, COO
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  Forest Assets (SUP-001 Metsä Forest, SUP-002 UPM Silvesta,
--                 SUP-003 Pölkky, SUP-008 Baltic Timber)
--         |
--         | Delivery records (fact_deliveries)
--         | material_type IN ('Softwood Pulp','Hardwood Pulp','Softwood Logs')
--         v
--  nordleaf_stg.deliveries  ──────────────────────────────────────────┐
--                                                                      |
--  nordleaf_dwh.fact_production                                       |
--    (actual_tons consumed per mill per product)                      |
--         |                                                            |
--         v                                                            v
--  Mill Consumption Estimate          Timber/Pulp Receipts
--  (actual_tons × fibre_ratio)        (SUM quantity_tons by mill)
--         |                                                            |
--         └──────────────────────────────────────────────────────────┘
--                         BALANCE = RECEIPTS - CONSUMPTION
--                         Negative = SHORTFALL risk
--
-- STEPS TO IMPLEMENT:
--  1. Run this query monthly in BigQuery Scheduled Queries
--  2. Write output to nordleaf_mgmt.timber_balance_report
--  3. Connect to Looker Studio "Forest Operations" dashboard
--  4. Alert via Cloud Monitoring when balance < 500 tons
-- ============================================================

-- Step 1: Timber/Pulp receipts per mill per month
WITH timber_receipts AS (
  SELECT
    d.mill_destination                                AS mill_name,
    DATE_TRUNC(d.delivery_date, MONTH)                AS delivery_month,
    d.material_type,
    SUM(d.quantity_tons)                              AS received_tons,
    SUM(d.invoice_eur)                                AS receipt_cost_eur,
    COUNT(d.delivery_id)                              AS delivery_count,
    ROUND(AVG(d.quantity_tons), 1)                    AS avg_delivery_size,
    COUNTIF(d.quality_grade = 'A')                    AS grade_a_count,
    ROUND(COUNTIF(d.quality_grade = 'A') / COUNT(*) * 100, 1) AS grade_a_pct
  FROM `nordleaf_stg.deliveries` d
  WHERE d.material_type IN (
    'Softwood Pulp', 'Hardwood Pulp', 'Softwood Logs', 'Recycled Fibre'
  )
  GROUP BY 1, 2, 3
),

-- Step 2: Mill consumption of fibre (approximate from production output)
-- Paper production fibre yield ratios (industry standard):
--   Newsprint:       2.3 tons fibre per ton paper
--   Kraft Paper:     2.5 tons fibre per ton paper
--   Cardboard:       1.8 tons fibre per ton paper (recycled content)
--   Specialty Paper: 2.8 tons fibre per ton paper
--   Tissue:          1.5 tons fibre per ton paper (recycled content)
mill_consumption AS (
  SELECT
    dm.mill_name,
    DATE_TRUNC(fp.production_date, MONTH)             AS production_month,
    SUM(fp.actual_tons * CASE fp.product_type
      WHEN 'Newsprint'       THEN 2.3
      WHEN 'Kraft Paper'     THEN 2.5
      WHEN 'Cardboard'       THEN 1.8
      WHEN 'Specialty Paper' THEN 2.8
      WHEN 'Tissue'          THEN 1.5
      ELSE 2.0 END)                                   AS estimated_fibre_consumed_tons,
    SUM(fp.actual_tons)                               AS paper_produced_tons,
    ROUND(AVG(fp.efficiency_pct), 1)                  AS avg_efficiency_pct
  FROM `nordleaf_dwh.fact_production` fp
  JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
  GROUP BY 1, 2
),

-- Step 3: Balance calculation
balance AS (
  SELECT
    COALESCE(r.mill_name, c.mill_name)                AS mill_name,
    COALESCE(r.delivery_month, c.production_month)    AS month,
    COALESCE(SUM(r.received_tons), 0)                 AS total_received_tons,
    COALESCE(c.estimated_fibre_consumed_tons, 0)      AS estimated_consumed_tons,
    COALESCE(SUM(r.receipt_cost_eur), 0)              AS fibre_cost_eur,
    COALESCE(c.paper_produced_tons, 0)                AS paper_produced_tons,
    COALESCE(SUM(r.received_tons), 0)
      - COALESCE(c.estimated_fibre_consumed_tons, 0)  AS fibre_balance_tons,
    COALESCE(c.avg_efficiency_pct, 0)                 AS production_efficiency_pct
  FROM timber_receipts r
  FULL OUTER JOIN mill_consumption c
    ON r.mill_name = c.mill_name
    AND r.delivery_month = c.production_month
  GROUP BY 1, 2, 4, 5, 7, 8
)

-- Step 4: Final report with risk flag
SELECT
  mill_name,
  month,
  ROUND(total_received_tons, 1)       AS fibre_received_tons,
  ROUND(estimated_consumed_tons, 1)   AS fibre_consumed_tons,
  ROUND(fibre_balance_tons, 1)        AS fibre_balance_tons,
  ROUND(fibre_cost_eur, 0)            AS fibre_procurement_cost_eur,
  ROUND(fibre_cost_eur
    / NULLIF(paper_produced_tons, 0), 2) AS fibre_cost_per_ton_paper_eur,
  production_efficiency_pct,
  CASE
    WHEN fibre_balance_tons < -500   THEN 'CRITICAL SHORTFALL'
    WHEN fibre_balance_tons < 0      THEN 'SHORTFALL'
    WHEN fibre_balance_tons < 200    THEN 'LOW BUFFER'
    WHEN fibre_balance_tons < 1000   THEN 'ADEQUATE'
    ELSE                                  'SURPLUS'
  END AS supply_status,
  CASE
    WHEN fibre_balance_tons < 0     THEN TRUE
    ELSE                                 FALSE
  END AS requires_action
FROM balance
ORDER BY month DESC, mill_name;

-- ============================================================
-- Management View: Monthly Timber Balance Summary
-- ============================================================
CREATE OR REPLACE VIEW `nordleaf_mgmt.v_timber_balance_monthly` AS
WITH receipts AS (
  SELECT mill_destination AS mill_name,
    DATE_TRUNC(delivery_date, MONTH) AS month,
    SUM(quantity_tons) AS received_tons,
    SUM(invoice_eur)   AS cost_eur
  FROM `nordleaf_stg.deliveries`
  WHERE material_type IN ('Softwood Pulp','Hardwood Pulp','Softwood Logs')
  GROUP BY 1, 2
),
consumption AS (
  SELECT dm.mill_name,
    DATE_TRUNC(fp.production_date, MONTH) AS month,
    SUM(fp.actual_tons * CASE fp.product_type
      WHEN 'Newsprint' THEN 2.3 WHEN 'Kraft Paper' THEN 2.5
      WHEN 'Cardboard' THEN 1.8 WHEN 'Specialty Paper' THEN 2.8
      WHEN 'Tissue' THEN 1.5 ELSE 2.0 END) AS consumed_tons
  FROM `nordleaf_dwh.fact_production` fp
  JOIN `nordleaf_dwh.dim_mill` dm ON fp.mill_id = dm.mill_id
  GROUP BY 1, 2
)
SELECT
  r.mill_name, r.month,
  ROUND(r.received_tons, 1)           AS received_tons,
  ROUND(c.consumed_tons, 1)           AS consumed_tons,
  ROUND(r.received_tons - COALESCE(c.consumed_tons,0), 1) AS balance_tons,
  ROUND(r.cost_eur, 0)                AS cost_eur,
  IF(r.received_tons < COALESCE(c.consumed_tons,0), 'SHORTFALL', 'OK') AS status
FROM receipts r
LEFT JOIN consumption c USING (mill_name, month);
