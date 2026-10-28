-- ============================================================================================================================
-- FILE        : UC22_working_capital_payment_terms.sql
-- USE CASE    : UC-22  |  Finance Domain  |  Priority: P2
-- PROJECT     : NordLeaf BI Implementation — Finnish Paper Manufacturer
-- LAYER       : DWH → MGMT (4-layer: EXT → STG → DWH → MGMT)
-- DATASET     : nordleaf_dwh (source), nordleaf_mgmt (output view)
-- AUTHOR      : NordLeaf Data Engineering Team
-- CREATED     : 2026-02-23
-- LAST UPDATE : 2026-02-23
-- ============================================================================================================================
--
-- ARCHITECTURE DIAGRAM
-- ============================================================================================================================
--
--   nordleaf_dwh                               nordleaf_mgmt                   Consumers
--   ─────────────────────────────────          ─────────────────────────────   ──────────────────────────
--
--   fact_orders (payment data)  ──────────────►                                Finance Dashboard (Looker)
--        │                                     Working Capital View  ────────►
--   dim_customer (terms, country) ────────────►  [v_working_capital]           CFO Report (Scheduled)
--        │                                         │
--   dim_date (year/month/quarter) ────────────►    │                           Treasury Alerts
--        │                                         │                           (overdue flagging)
--   fact_deliveries (payables) ───────────────►────┘
--
-- ============================================================================================================================
--
-- BUSINESS PROBLEM
-- ============================================================================================================================
--
--   NordLeaf's CFO needs real-time visibility into working capital health across all 5 mills:
--   Tampere, Oulu, Turku, Espoo, and Jyväskylä.
--
--   Key challenges:
--     • 18 enterprise customers (paper mills, publishers, packaging firms) often operate on Net-60
--       or Net-90 payment terms, locking up significant receivables for extended periods.
--     • Seasonal pulp procurement from 12 suppliers creates payables spikes in Q1/Q3.
--     • The treasury team lacks a single dashboard to reconcile customer DSO vs. supplier DPO
--       and compute the net cash conversion cycle.
--     • Overdue receivables are tracked in spreadsheets — no automated aging or risk scoring.
--
--   This UC delivers:
--     1. Days Sales Outstanding (DSO) per customer and in aggregate
--     2. Days Payable Outstanding (DPO) across all 12 suppliers
--     3. Cash Conversion Cycle (CCC) at monthly granularity
--     4. Working capital ratio trend with MoM change
--     5. Top-5 customer receivables with aging buckets
--     6. 30-day forward cash flow projection
--
-- ============================================================================================================================
--
-- FORMULAS
-- ============================================================================================================================
--
--   DSO  = (Total Outstanding Receivables / Total Revenue in Period) × Days in Period
--          Weighted variant: SUM(invoice_value × days_to_pay) / SUM(collected_value)
--
--   DPO  = (Total Outstanding Payables / Total COGS in Period) × Days in Period
--          Higher DPO is better — NordLeaf retains cash longer.
--
--   Cash Conversion Cycle (CCC) = DSO − DPO
--          Simplified: excludes Days Inventory Outstanding (DIO).
--          Target: CCC < 15 days.
--
--   Working Capital       = Current Receivables − Current Payables
--   Working Capital Ratio = Current Receivables / Current Payables  (target > 1.5)
--
-- ============================================================================================================================
--
-- KPI TARGETS
-- ============================================================================================================================
--
--   | KPI                    | Target        | Alert Threshold  |
--   |------------------------|---------------|------------------|
--   | DSO                    | < 45 days     | > 60 days        |
--   | DPO                    | > 30 days     | < 20 days        |
--   | Cash Conversion Cycle  | < 15 days     | > 30 days        |
--   | Working Capital Ratio  | > 1.5         | < 1.2            |
--   | Overdue Receivables %  | < 5%          | > 15%            |
--
-- ============================================================================================================================
--
-- IMPLEMENTATION STEPS
-- ============================================================================================================================
--
--   Step 1 : Pull 12-month rolling receivables from fact_orders joined to dim_customer + dim_date
--   Step 2 : Pull 12-month rolling payables from fact_deliveries joined to dim_supplier + dim_date
--   Step 3 : Compute DSO — weighted and simple variants — flag HIGH DSO RISK customers
--   Step 4 : Compute DPO per supplier and in aggregate
--   Step 5 : Derive CCC = DSO − DPO with target banding
--   Step 6 : Compute monthly working capital and WC ratio with MoM LAG()
--   Step 7 : Rank top-5 customers by outstanding receivables, produce aging bucket breakdown
--   Step 8 : Project 30-day cash inflows based on payment_terms_days + delivery_date
--   Step 9 : Materialise everything into nordleaf_mgmt.v_working_capital view
--
-- ============================================================================================================================


-- ============================================================================================================================
-- SECTION 1 — MONTHLY RECEIVABLES (What customers owe NordLeaf)
-- ============================================================================================================================
--
--   Source  : nordleaf_dwh.fact_orders
--   Joins   : dim_customer (payment terms, country, segment), dim_date (year/month/quarter)
--   Grain   : One row per customer per calendar month
--   Scope   : Rolling 12 months from current date

WITH monthly_receivables AS (
  SELECT
    dd.year_num,
    dd.month_num,
    dd.month_name,
    dd.quarter_label,
    dc.customer_key,
    dc.customer_name,
    dc.country,
    dc.customer_segment,
    dc.payment_terms_days,

    -- Volume metrics
    COUNT(fo.order_key)                                          AS invoice_count,
    SUM(fo.total_value_eur)                                      AS invoiced_eur,

    -- Collected: invoices that have been paid
    SUM(IF(fo.payment_date IS NOT NULL,
           fo.total_value_eur, 0))                               AS collected_eur,

    -- Outstanding: delivered but not yet paid (regardless of due date)
    SUM(IF(fo.payment_date IS NULL
           AND fo.order_status = 'Delivered',
           fo.total_value_eur, 0))                               AS outstanding_eur,

    -- Overdue: outstanding AND past the agreed payment terms window
    SUM(IF(fo.payment_date IS NULL
           AND fo.order_status = 'Delivered'
           AND DATE_DIFF(CURRENT_DATE(), fo.delivery_date, DAY) > dc.payment_terms_days,
           fo.total_value_eur, 0))                               AS overdue_eur,

    -- Weighted DSO numerator: invoice_value × actual_days_to_payment (only for paid invoices)
    SUM(IF(fo.payment_date IS NOT NULL,
           fo.total_value_eur * DATE_DIFF(fo.payment_date, fo.delivery_date, DAY),
           NULL))                                                AS dso_numerator,

    -- Largest single overdue invoice (for prioritisation)
    MAX(IF(fo.payment_date IS NULL
           AND fo.order_status = 'Delivered'
           AND DATE_DIFF(CURRENT_DATE(), fo.delivery_date, DAY) > dc.payment_terms_days,
           fo.total_value_eur, NULL))                            AS max_single_overdue_eur,

    -- Oldest outstanding delivery date (earliest unresolved invoice)
    MIN(IF(fo.payment_date IS NULL
           AND fo.order_status = 'Delivered',
           fo.delivery_date, NULL))                              AS oldest_outstanding_date

  FROM nordleaf_dwh.fact_orders fo
  JOIN nordleaf_dwh.dim_customer dc ON fo.customer_key = dc.customer_key
  JOIN nordleaf_dwh.dim_date    dd ON fo.date_key       = dd.date_key
  WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
  GROUP BY
    dd.year_num, dd.month_num, dd.month_name, dd.quarter_label,
    dc.customer_key, dc.customer_name, dc.country,
    dc.customer_segment, dc.payment_terms_days
),

-- ============================================================================================================================
-- SECTION 2 — MONTHLY PAYABLES (What NordLeaf owes suppliers)
-- ============================================================================================================================
--
--   Source  : nordleaf_dwh.fact_deliveries  (inbound supplier deliveries = NordLeaf's payables)
--   Joins   : dim_supplier, dim_date
--   Grain   : One row per supplier per calendar month
--   Note    : fact_deliveries.delivery_cost_eur is used as the payable amount (COGS proxy)

monthly_payables AS (
  SELECT
    dd.year_num,
    dd.month_num,
    dd.quarter_label,
    ds.supplier_key,
    ds.supplier_name,
    ds.supplier_country,
    ds.payment_terms_days                                        AS supplier_payment_terms,

    COUNT(fd.delivery_key)                                       AS supplier_invoice_count,
    SUM(fd.delivery_cost_eur)                                    AS supplier_invoiced_eur,

    -- Paid to supplier
    SUM(IF(fd.payment_date IS NOT NULL,
           fd.delivery_cost_eur, 0))                             AS supplier_paid_eur,

    -- Still owed to supplier
    SUM(IF(fd.payment_date IS NULL
           AND fd.delivery_status = 'Received',
           fd.delivery_cost_eur, 0))                             AS supplier_outstanding_eur,

    -- Overdue payables (NordLeaf is late paying supplier — reputational risk)
    SUM(IF(fd.payment_date IS NULL
           AND fd.delivery_status = 'Received'
           AND DATE_DIFF(CURRENT_DATE(), fd.received_date, DAY) > ds.payment_terms_days,
           fd.delivery_cost_eur, 0))                             AS supplier_overdue_eur,

    -- DPO numerator: weighted days NordLeaf took to pay supplier
    SUM(IF(fd.payment_date IS NOT NULL,
           fd.delivery_cost_eur * DATE_DIFF(fd.payment_date, fd.received_date, DAY),
           NULL))                                                AS dpo_numerator,

    -- COGS proxy for DPO denominator (all received supplier goods this month)
    SUM(fd.delivery_cost_eur)                                    AS cogs_eur

  FROM nordleaf_dwh.fact_deliveries fd
  JOIN nordleaf_dwh.dim_supplier ds ON fd.supplier_key = ds.supplier_key
  JOIN nordleaf_dwh.dim_date     dd ON fd.date_key      = dd.date_key
  WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
  GROUP BY
    dd.year_num, dd.month_num, dd.quarter_label,
    ds.supplier_key, ds.supplier_name, ds.supplier_country,
    ds.payment_terms_days
),

-- ============================================================================================================================
-- SECTION 3 — DSO CALCULATION PER CUSTOMER PER MONTH
-- ============================================================================================================================
--
--   Two DSO variants:
--     Simple DSO   = (outstanding_eur / invoiced_eur) × 30
--     Weighted DSO = SUM(invoice_value × days_paid) / SUM(collected_value)
--                    (More accurate — reflects true collection speed)
--
--   DSO Risk Flag:
--     HIGH       — DSO > payment_terms_days × 1.5  (customer chronically slow)
--     ELEVATED   — DSO > payment_terms_days × 1.2
--     NORMAL     — DSO <= payment_terms_days
--     AHEAD      — DSO < payment_terms_days × 0.8  (paying early)

dso_by_customer AS (
  SELECT
    year_num,
    month_num,
    month_name,
    quarter_label,
    customer_key,
    customer_name,
    country,
    customer_segment,
    payment_terms_days,
    invoice_count,
    invoiced_eur,
    collected_eur,
    outstanding_eur,
    overdue_eur,
    max_single_overdue_eur,
    oldest_outstanding_date,

    -- Simple DSO: outstanding as a fraction of monthly revenue, scaled to 30-day month
    ROUND(SAFE_DIVIDE(outstanding_eur, invoiced_eur) * 30, 1)    AS dso_simple_days,

    -- Weighted DSO: reflects actual speed of collection on paid invoices
    ROUND(SAFE_DIVIDE(dso_numerator, collected_eur), 1)           AS dso_weighted_days,

    -- Overdue rate: what fraction of invoiced amount is currently overdue
    ROUND(SAFE_DIVIDE(overdue_eur, invoiced_eur) * 100, 2)        AS overdue_rate_pct,

    -- DSO Risk Classification
    CASE
      WHEN SAFE_DIVIDE(outstanding_eur, invoiced_eur) * 30
           > payment_terms_days * 1.5                            THEN 'HIGH DSO RISK'
      WHEN SAFE_DIVIDE(outstanding_eur, invoiced_eur) * 30
           > payment_terms_days * 1.2                            THEN 'ELEVATED DSO RISK'
      WHEN SAFE_DIVIDE(outstanding_eur, invoiced_eur) * 30
           <= payment_terms_days * 0.8                           THEN 'AHEAD OF TERMS'
      ELSE                                                            'NORMAL'
    END                                                          AS dso_risk_flag

  FROM monthly_receivables
),

-- ============================================================================================================================
-- SECTION 4 — DPO CALCULATION (Aggregate — NordLeaf's payable position)
-- ============================================================================================================================
--
--   DPO = (Outstanding Payables / Monthly COGS) × 30
--   Higher DPO = NordLeaf holds cash longer = better for treasury.
--   Target: DPO > 30 days. Alert: DPO < 20 days (paying too fast, suboptimal cash usage).

dpo_monthly AS (
  SELECT
    year_num,
    month_num,
    quarter_label,

    -- Aggregate across all 12 suppliers
    SUM(supplier_invoiced_eur)                                   AS total_cogs_eur,
    SUM(supplier_paid_eur)                                       AS total_supplier_paid_eur,
    SUM(supplier_outstanding_eur)                                AS total_payables_outstanding_eur,
    SUM(supplier_overdue_eur)                                    AS total_overdue_payables_eur,
    SUM(dpo_numerator)                                           AS dpo_numerator_agg,
    SUM(supplier_invoiced_eur)                                   AS dpo_denominator_agg,

    -- Simple DPO
    ROUND(SAFE_DIVIDE(SUM(supplier_outstanding_eur),
                      SUM(supplier_invoiced_eur)) * 30, 1)       AS dpo_simple_days,

    -- Weighted DPO
    ROUND(SAFE_DIVIDE(SUM(dpo_numerator),
                      SUM(supplier_paid_eur)), 1)                 AS dpo_weighted_days,

    -- DPO health flag
    CASE
      WHEN SAFE_DIVIDE(SUM(supplier_outstanding_eur),
                       SUM(supplier_invoiced_eur)) * 30 >= 45    THEN 'STRONG DPO'
      WHEN SAFE_DIVIDE(SUM(supplier_outstanding_eur),
                       SUM(supplier_invoiced_eur)) * 30 >= 30    THEN 'ADEQUATE DPO'
      WHEN SAFE_DIVIDE(SUM(supplier_outstanding_eur),
                       SUM(supplier_invoiced_eur)) * 30 >= 20    THEN 'WEAK DPO'
      ELSE                                                            'CRITICAL DPO'
    END                                                          AS dpo_health_flag

  FROM monthly_payables
  GROUP BY year_num, month_num, quarter_label
),

-- ============================================================================================================================
-- SECTION 5 — CASH CONVERSION CYCLE (CCC = DSO − DPO)
-- ============================================================================================================================
--
--   Computed at monthly level by joining aggregate DSO (all customers) with aggregate DPO (all suppliers).
--   Lower CCC = less time cash is tied up outside NordLeaf.
--   Target: CCC < 15 days. Alert: CCC > 30 days.

monthly_dso_aggregate AS (
  SELECT
    year_num,
    month_num,
    quarter_label,
    SUM(invoiced_eur)                                            AS total_revenue_eur,
    SUM(outstanding_eur)                                         AS total_receivables_eur,
    SUM(overdue_eur)                                             AS total_overdue_receivables_eur,
    ROUND(SAFE_DIVIDE(SUM(outstanding_eur),
                      SUM(invoiced_eur)) * 30, 1)                AS dso_aggregate_days,
    ROUND(SAFE_DIVIDE(SUM(overdue_eur),
                      SUM(invoiced_eur)) * 100, 2)               AS overdue_receivables_rate_pct
  FROM monthly_receivables
  GROUP BY year_num, month_num, quarter_label
),

cash_conversion_cycle AS (
  SELECT
    r.year_num,
    r.month_num,
    r.quarter_label,
    r.total_revenue_eur,
    r.total_receivables_eur,
    r.total_overdue_receivables_eur,
    r.overdue_receivables_rate_pct,
    p.total_cogs_eur,
    p.total_payables_outstanding_eur,
    r.dso_aggregate_days,
    p.dpo_simple_days,
    p.dpo_health_flag,

    -- CCC = DSO - DPO (simplified, no inventory turn component)
    ROUND(r.dso_aggregate_days - p.dpo_simple_days, 1)           AS ccc_days,

    -- CCC health banding
    CASE
      WHEN (r.dso_aggregate_days - p.dpo_simple_days) < 15       THEN 'HEALTHY CCC'
      WHEN (r.dso_aggregate_days - p.dpo_simple_days) < 30       THEN 'MODERATE CCC'
      WHEN (r.dso_aggregate_days - p.dpo_simple_days) < 45       THEN 'ELEVATED CCC'
      ELSE                                                            'CRITICAL CCC'
    END                                                          AS ccc_health_flag

  FROM monthly_dso_aggregate r
  JOIN dpo_monthly            p
    ON r.year_num   = p.year_num
   AND r.month_num  = p.month_num
),

-- ============================================================================================================================
-- SECTION 6 — WORKING CAPITAL BY MONTH (with MoM trend)
-- ============================================================================================================================
--
--   Working Capital       = total_receivables − total_payables
--   Working Capital Ratio = total_receivables / total_payables  (target > 1.5)
--   MoM delta computed via LAG() to detect deteriorating trends for CFO alert.

working_capital_monthly AS (
  SELECT
    ccc.year_num,
    ccc.month_num,
    ccc.quarter_label,
    ccc.total_revenue_eur,
    ccc.total_cogs_eur,
    ccc.total_receivables_eur,
    ccc.total_payables_outstanding_eur,
    ccc.total_overdue_receivables_eur,
    ccc.overdue_receivables_rate_pct,
    ccc.dso_aggregate_days,
    ccc.dpo_simple_days,
    ccc.ccc_days,
    ccc.ccc_health_flag,

    -- Working Capital (absolute, EUR)
    ROUND(ccc.total_receivables_eur - ccc.total_payables_outstanding_eur, 0)   AS working_capital_eur,

    -- Working Capital Ratio
    ROUND(SAFE_DIVIDE(ccc.total_receivables_eur,
                      ccc.total_payables_outstanding_eur), 2)    AS working_capital_ratio,

    -- MoM working capital change (EUR)
    ROUND((ccc.total_receivables_eur - ccc.total_payables_outstanding_eur)
          - LAG(ccc.total_receivables_eur - ccc.total_payables_outstanding_eur)
              OVER (ORDER BY ccc.year_num, ccc.month_num), 0)    AS wc_mom_change_eur,

    -- MoM WC ratio change
    ROUND(SAFE_DIVIDE(ccc.total_receivables_eur, ccc.total_payables_outstanding_eur)
          - LAG(SAFE_DIVIDE(ccc.total_receivables_eur, ccc.total_payables_outstanding_eur))
              OVER (ORDER BY ccc.year_num, ccc.month_num), 3)    AS wc_ratio_mom_change,

    -- WC ratio health flag
    CASE
      WHEN SAFE_DIVIDE(ccc.total_receivables_eur,
                       ccc.total_payables_outstanding_eur) >= 1.5 THEN 'HEALTHY'
      WHEN SAFE_DIVIDE(ccc.total_receivables_eur,
                       ccc.total_payables_outstanding_eur) >= 1.2 THEN 'ADEQUATE'
      WHEN SAFE_DIVIDE(ccc.total_receivables_eur,
                       ccc.total_payables_outstanding_eur) >= 1.0 THEN 'BORDERLINE'
      ELSE                                                             'BELOW TARGET'
    END                                                          AS wc_ratio_flag

  FROM cash_conversion_cycle ccc
),

-- ============================================================================================================================
-- SECTION 7 — TOP 5 CUSTOMERS BY OUTSTANDING RECEIVABLES (with Aging Buckets)
-- ============================================================================================================================
--
--   Aging buckets mirror standard AR aging report:
--     0-30 days   — Current (within payment terms)
--     31-60 days  — Slightly overdue
--     61-90 days  — Moderately overdue (escalation warranted)
--     90+ days    — Severely overdue (collections / credit hold consideration)

customer_aging AS (
  SELECT
    dc.customer_key,
    dc.customer_name,
    dc.country,
    dc.customer_segment,
    dc.payment_terms_days,
    dc.credit_limit_eur,
    fo.order_key,
    fo.order_number,
    fo.delivery_date,
    fo.total_value_eur                                           AS invoice_amount_eur,
    DATE_DIFF(CURRENT_DATE(), fo.delivery_date, DAY)             AS days_outstanding,

    -- Aging bucket
    CASE
      WHEN DATE_DIFF(CURRENT_DATE(), fo.delivery_date, DAY) <= 30  THEN '00_Current_0_30'
      WHEN DATE_DIFF(CURRENT_DATE(), fo.delivery_date, DAY) <= 60  THEN '01_Overdue_31_60'
      WHEN DATE_DIFF(CURRENT_DATE(), fo.delivery_date, DAY) <= 90  THEN '02_Overdue_61_90'
      ELSE                                                              '03_Overdue_90plus'
    END                                                          AS aging_bucket

  FROM nordleaf_dwh.fact_orders fo
  JOIN nordleaf_dwh.dim_customer dc ON fo.customer_key = dc.customer_key
  WHERE fo.payment_date IS NULL
    AND fo.order_status  = 'Delivered'
),

top5_customers_outstanding AS (
  SELECT
    customer_key,
    customer_name,
    country,
    customer_segment,
    payment_terms_days,
    credit_limit_eur,
    SUM(invoice_amount_eur)                                      AS total_outstanding_eur,
    COUNT(order_key)                                             AS open_invoice_count,
    AVG(days_outstanding)                                        AS avg_days_outstanding,
    MAX(days_outstanding)                                        AS max_days_outstanding,

    -- Aging bucket sums
    SUM(IF(aging_bucket = '00_Current_0_30',   invoice_amount_eur, 0))  AS bucket_0_30_eur,
    SUM(IF(aging_bucket = '01_Overdue_31_60',  invoice_amount_eur, 0))  AS bucket_31_60_eur,
    SUM(IF(aging_bucket = '02_Overdue_61_90',  invoice_amount_eur, 0))  AS bucket_61_90_eur,
    SUM(IF(aging_bucket = '03_Overdue_90plus', invoice_amount_eur, 0))  AS bucket_90plus_eur,

    -- Credit utilisation: outstanding vs. credit limit
    ROUND(SAFE_DIVIDE(SUM(invoice_amount_eur), credit_limit_eur) * 100, 1) AS credit_utilisation_pct,

    -- Dense rank — limit to TOP 5 in outer query
    DENSE_RANK() OVER (ORDER BY SUM(invoice_amount_eur) DESC)    AS receivables_rank

  FROM customer_aging
  GROUP BY
    customer_key, customer_name, country, customer_segment,
    payment_terms_days, credit_limit_eur
),

-- ============================================================================================================================
-- SECTION 8 — 30-DAY CASH FLOW PROJECTION (Expected Inflows)
-- ============================================================================================================================
--
--   For each outstanding invoice, derive the expected payment date:
--     expected_payment_date = delivery_date + payment_terms_days
--   Group by week to produce a cash inflow forecast for the treasury team.
--   Only includes invoices not yet overdue (reasonable collection assumption).

cash_flow_projection AS (
  SELECT
    DATE_ADD(fo.delivery_date, INTERVAL dc.payment_terms_days DAY) AS expected_payment_date,
    DATE_TRUNC(DATE_ADD(fo.delivery_date, INTERVAL dc.payment_terms_days DAY), WEEK(MONDAY))
                                                                 AS expected_week_start,
    dc.customer_name,
    dc.payment_terms_days,
    fo.order_key,
    fo.order_number,
    fo.total_value_eur                                           AS expected_inflow_eur,
    fo.delivery_date,
    DATE_DIFF(
      DATE_ADD(fo.delivery_date, INTERVAL dc.payment_terms_days DAY),
      CURRENT_DATE(), DAY)                                       AS days_until_due

  FROM nordleaf_dwh.fact_orders fo
  JOIN nordleaf_dwh.dim_customer dc ON fo.customer_key = dc.customer_key
  WHERE fo.payment_date IS NULL
    AND fo.order_status  = 'Delivered'
    -- Only include invoices expected within next 30 days
    AND DATE_ADD(fo.delivery_date, INTERVAL dc.payment_terms_days DAY)
        BETWEEN CURRENT_DATE() AND DATE_ADD(CURRENT_DATE(), INTERVAL 30 DAY)
),

cash_flow_by_week AS (
  SELECT
    expected_week_start,
    COUNT(order_key)                                             AS invoices_due,
    ROUND(SUM(expected_inflow_eur), 0)                           AS expected_inflow_eur,
    STRING_AGG(DISTINCT customer_name ORDER BY customer_name)    AS customers_paying
  FROM cash_flow_projection
  GROUP BY expected_week_start
  ORDER BY expected_week_start
),

-- ============================================================================================================================
-- SECTION 9 — FINAL WORKING CAPITAL SUMMARY (for MGMT view)
-- ============================================================================================================================
--
--   Produces the canonical output for nordleaf_mgmt.v_working_capital.
--   Combines: WC monthly trend + current DSO customer risk summary + top-5 AR aging.

current_month_snapshot AS (
  SELECT
    wm.year_num,
    wm.month_num,
    wm.quarter_label,
    wm.total_revenue_eur,
    wm.total_cogs_eur,
    wm.total_receivables_eur,
    wm.total_payables_outstanding_eur,
    wm.total_overdue_receivables_eur,
    wm.overdue_receivables_rate_pct,
    wm.dso_aggregate_days,
    wm.dpo_simple_days,
    wm.ccc_days,
    wm.ccc_health_flag,
    wm.working_capital_eur,
    wm.working_capital_ratio,
    wm.wc_mom_change_eur,
    wm.wc_ratio_mom_change,
    wm.wc_ratio_flag,

    -- DSO KPI assessment
    CASE
      WHEN wm.dso_aggregate_days < 45                            THEN 'ON TARGET'
      WHEN wm.dso_aggregate_days < 60                            THEN 'MONITOR'
      ELSE                                                            'ALERT — ABOVE THRESHOLD'
    END                                                          AS dso_kpi_status,

    -- WC ratio KPI assessment
    CASE
      WHEN wm.working_capital_ratio >= 1.5                       THEN 'ON TARGET'
      WHEN wm.working_capital_ratio >= 1.2                       THEN 'MONITOR'
      ELSE                                                            'ALERT — BELOW THRESHOLD'
    END                                                          AS wc_ratio_kpi_status,

    CURRENT_TIMESTAMP()                                          AS report_generated_at

  FROM working_capital_monthly wm
)

-- ============================================================================================================================
-- FINAL SELECT — Working Capital Monthly Trend (primary output)
-- ============================================================================================================================

SELECT * FROM current_month_snapshot
ORDER BY year_num DESC, month_num DESC;

-- ============================================================================================================================
-- SUPPLEMENTAL QUERY A — DSO Risk by Customer (current rolling 12 months)
-- ============================================================================================================================

-- SELECT
--   customer_name,
--   country,
--   customer_segment,
--   payment_terms_days,
--   ROUND(SUM(invoiced_eur),0)                  AS total_invoiced_eur,
--   ROUND(SUM(outstanding_eur),0)               AS total_outstanding_eur,
--   ROUND(SUM(overdue_eur),0)                   AS total_overdue_eur,
--   ROUND(SAFE_DIVIDE(SUM(outstanding_eur), SUM(invoiced_eur)) * 30, 1) AS dso_simple_days,
--   dso_risk_flag
-- FROM dso_by_customer
-- GROUP BY customer_name, country, customer_segment, payment_terms_days, dso_risk_flag
-- ORDER BY total_outstanding_eur DESC;

-- ============================================================================================================================
-- SUPPLEMENTAL QUERY B — Top-5 Customers by Outstanding AR with Aging
-- ============================================================================================================================

-- SELECT
--   receivables_rank,
--   customer_name,
--   country,
--   payment_terms_days,
--   ROUND(total_outstanding_eur,0)              AS total_outstanding_eur,
--   open_invoice_count,
--   ROUND(avg_days_outstanding,0)               AS avg_days_outstanding,
--   max_days_outstanding,
--   ROUND(bucket_0_30_eur,0)                    AS bucket_0_30_eur,
--   ROUND(bucket_31_60_eur,0)                   AS bucket_31_60_eur,
--   ROUND(bucket_61_90_eur,0)                   AS bucket_61_90_eur,
--   ROUND(bucket_90plus_eur,0)                  AS bucket_90plus_eur,
--   credit_utilisation_pct
-- FROM top5_customers_outstanding
-- WHERE receivables_rank <= 5
-- ORDER BY receivables_rank;

-- ============================================================================================================================
-- SUPPLEMENTAL QUERY C — 30-Day Cash Inflow Projection by Week
-- ============================================================================================================================

-- SELECT
--   expected_week_start,
--   invoices_due,
--   expected_inflow_eur,
--   customers_paying
-- FROM cash_flow_by_week
-- ORDER BY expected_week_start;

-- ============================================================================================================================
-- SECTION 9 — MGMT VIEW DEFINITION
-- ============================================================================================================================
--
--   Creates a persistent view in nordleaf_mgmt for Looker / Finance Dashboard consumption.
--   The view encapsulates the full CTE chain so downstream consumers need no SQL knowledge.

CREATE OR REPLACE VIEW nordleaf_mgmt.v_working_capital AS
WITH monthly_receivables AS (
  SELECT
    dd.year_num, dd.month_num, dd.month_name, dd.quarter_label,
    dc.customer_key, dc.customer_name, dc.country, dc.customer_segment,
    dc.payment_terms_days,
    COUNT(fo.order_key)                                           AS invoice_count,
    SUM(fo.total_value_eur)                                       AS invoiced_eur,
    SUM(IF(fo.payment_date IS NOT NULL, fo.total_value_eur, 0))   AS collected_eur,
    SUM(IF(fo.payment_date IS NULL AND fo.order_status = 'Delivered',
           fo.total_value_eur, 0))                                AS outstanding_eur,
    SUM(IF(fo.payment_date IS NULL AND fo.order_status = 'Delivered'
           AND DATE_DIFF(CURRENT_DATE(), fo.delivery_date, DAY) > dc.payment_terms_days,
           fo.total_value_eur, 0))                                AS overdue_eur,
    SUM(IF(fo.payment_date IS NOT NULL,
           fo.total_value_eur * DATE_DIFF(fo.payment_date, fo.delivery_date, DAY),
           NULL))                                                 AS dso_numerator
  FROM nordleaf_dwh.fact_orders     fo
  JOIN nordleaf_dwh.dim_customer    dc ON fo.customer_key = dc.customer_key
  JOIN nordleaf_dwh.dim_date        dd ON fo.date_key     = dd.date_key
  WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
  GROUP BY dd.year_num, dd.month_num, dd.month_name, dd.quarter_label,
           dc.customer_key, dc.customer_name, dc.country,
           dc.customer_segment, dc.payment_terms_days
),
monthly_payables AS (
  SELECT
    dd.year_num, dd.month_num, dd.quarter_label,
    SUM(fd.delivery_cost_eur)                                     AS total_cogs_eur,
    SUM(IF(fd.payment_date IS NULL AND fd.delivery_status = 'Received',
           fd.delivery_cost_eur, 0))                              AS total_payables_outstanding_eur,
    SUM(IF(fd.payment_date IS NOT NULL,
           fd.delivery_cost_eur * DATE_DIFF(fd.payment_date, fd.received_date, DAY),
           NULL))                                                 AS dpo_numerator,
    SUM(IF(fd.payment_date IS NOT NULL, fd.delivery_cost_eur, 0)) AS supplier_paid_eur
  FROM nordleaf_dwh.fact_deliveries fd
  JOIN nordleaf_dwh.dim_supplier    ds ON fd.supplier_key = ds.supplier_key
  JOIN nordleaf_dwh.dim_date        dd ON fd.date_key     = dd.date_key
  WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
  GROUP BY dd.year_num, dd.month_num, dd.quarter_label
),
monthly_agg AS (
  SELECT
    r.year_num, r.month_num, r.quarter_label,
    SUM(r.invoiced_eur)                                           AS total_revenue_eur,
    SUM(r.outstanding_eur)                                        AS total_receivables_eur,
    SUM(r.overdue_eur)                                            AS total_overdue_receivables_eur,
    ROUND(SAFE_DIVIDE(SUM(r.outstanding_eur), SUM(r.invoiced_eur)) * 30, 1) AS dso_days,
    p.total_cogs_eur,
    p.total_payables_outstanding_eur,
    ROUND(SAFE_DIVIDE(p.total_payables_outstanding_eur, p.total_cogs_eur) * 30, 1) AS dpo_days
  FROM monthly_receivables r
  JOIN monthly_payables    p ON r.year_num = p.year_num AND r.month_num = p.month_num
  GROUP BY r.year_num, r.month_num, r.quarter_label,
           p.total_cogs_eur, p.total_payables_outstanding_eur
)
SELECT
  year_num, month_num, quarter_label,
  total_revenue_eur, total_cogs_eur,
  total_receivables_eur, total_payables_outstanding_eur,
  total_overdue_receivables_eur,
  ROUND(SAFE_DIVIDE(total_overdue_receivables_eur, total_revenue_eur) * 100, 2) AS overdue_rate_pct,
  dso_days, dpo_days,
  ROUND(dso_days - dpo_days, 1)                                   AS ccc_days,
  ROUND(total_receivables_eur - total_payables_outstanding_eur, 0) AS working_capital_eur,
  ROUND(SAFE_DIVIDE(total_receivables_eur, total_payables_outstanding_eur), 2) AS working_capital_ratio,
  LAG(ROUND(total_receivables_eur - total_payables_outstanding_eur, 0))
      OVER (ORDER BY year_num, month_num)                         AS prev_month_wc_eur,
  ROUND((total_receivables_eur - total_payables_outstanding_eur)
        - LAG(total_receivables_eur - total_payables_outstanding_eur)
              OVER (ORDER BY year_num, month_num), 0)             AS wc_mom_change_eur,
  CASE
    WHEN dso_days < 45                                            THEN 'DSO: ON TARGET'
    WHEN dso_days < 60                                            THEN 'DSO: MONITOR'
    ELSE                                                               'DSO: ALERT'
  END                                                             AS dso_kpi_status,
  CASE
    WHEN SAFE_DIVIDE(total_receivables_eur, total_payables_outstanding_eur) >= 1.5 THEN 'WC: ON TARGET'
    WHEN SAFE_DIVIDE(total_receivables_eur, total_payables_outstanding_eur) >= 1.2 THEN 'WC: MONITOR'
    ELSE                                                               'WC: ALERT'
  END                                                             AS wc_kpi_status,
  CASE
    WHEN (dso_days - dpo_days) < 15                               THEN 'CCC: HEALTHY'
    WHEN (dso_days - dpo_days) < 30                               THEN 'CCC: MODERATE'
    ELSE                                                               'CCC: CRITICAL'
  END                                                             AS ccc_health_flag,
  CURRENT_TIMESTAMP()                                             AS view_refreshed_at
FROM monthly_agg
ORDER BY year_num DESC, month_num DESC;

-- ============================================================================================================================
-- END OF FILE: UC22_working_capital_payment_terms.sql
-- ============================================================================================================================
