-- ============================================================================================================================
-- FILE        : UC18_customer_segmentation_credit_risk.sql
-- USE CASE    : UC-18 | Customer Domain | Priority: P2
-- PROJECT     : NordLeaf BI Implementation
-- LAYER       : MGMT (Management / Reporting Layer)
-- DESCRIPTION : Customer Segmentation by Credit Risk and Payment Behaviour
-- AUTHOR      : NordLeaf Data Engineering Team
-- CREATED     : 2024-01-15
-- UPDATED     : 2024-01-15
-- VERSION     : 1.0
-- ============================================================================================================================
--
-- ARCHITECTURE DIAGRAM
-- ============================================================================================================================
--
--   nordleaf_dwh.fact_orders          nordleaf_dwh.dim_customer
--   ┌─────────────────────────┐       ┌──────────────────────────────┐
--   │ order_key               │       │ customer_key                 │
--   │ customer_key (FK)  ─────┼───────│ customer_name                │
--   │ total_value_eur         │       │ country                      │
--   │ order_date              │       │ region                       │
--   │ delivery_date           │       │ credit_limit_eur             │
--   │ payment_date            │       │ payment_terms_days (30/60)   │
--   │ order_status            │       └──────────────────────────────┘
--   └─────────────────────────┘
--              │                  ─── days_to_pay = payment_date - delivery_date
--              │                  ─── late_payment_flag = days_to_pay > payment_terms_days
--              │                  ─── days_overdue = MAX(days_to_pay - payment_terms_days, 0)
--              ▼
--   ┌────────────────────────────────────────────────────────────┐
--   │         Customer Risk Segmentation Engine                  │
--   │                                                            │
--   │  DSO Calculation | Late Payment Rate | Dispute Rate        │
--   │  Risk Classification | Credit Utilization                  │
--   │  Trend Analysis (Improving / Stable / Deteriorating)       │
--   └────────────────────────────────────────────────────────────┘
--              │
--              ▼
--   ┌──────────────────────────────────────────────────────────┐
--   │         nordleaf_mgmt.v_customer_credit_risk             │
--   │              Customer Risk View [MGMT]                    │
--   │                                                          │
--   │  Customer Risk Tier | Recommended Credit Action          │
--   │  Country Risk Aggregation | CFO Escalation Flags         │
--   └──────────────────────────────────────────────────────────┘
--              │
--              ▼
--   ┌──────────────────────────┐
--   │   Finance Dashboard      │
--   │   (Looker Studio)        │
--   │   CFO / Finance Team     │
--   └──────────────────────────┘
--
-- ============================================================================================================================
--
-- BUSINESS PROBLEM
-- ============================================================================================================================
--
--   NordLeaf extends trade credit to 18 EU enterprise customers with payment terms ranging from Net-30 to Net-60.
--   Paper manufacturing involves long production cycles, meaning unpaid invoices directly affect working capital.
--
--   Finance needs to:
--     1. Segment all customers by payment behaviour and assign an appropriate risk tier.
--     2. Identify customers at risk of default before it materialises.
--     3. Set or adjust credit limits based on observed payment discipline.
--     4. Monitor whether customer risk profiles are improving or deteriorating over time.
--     5. Provide country-level risk exposure reports for CFO and credit committee review.
--
--   Customer Credit Risk Segment Definitions:
--     - LOW RISK      : avg days-to-pay <= 30, zero late payments in last 6 months
--     - MEDIUM RISK   : avg days-to-pay 31–45, up to 2 late payments in trailing 12 months
--     - HIGH RISK     : avg days-to-pay 46–60, more than 2 late payments in trailing 12 months
--     - CRITICAL RISK : avg days-to-pay > 60 OR any disputed invoices present
--
--   NordLeaf operates across 5 EU regions:
--     - Nordic (Finland, Sweden, Norway, Denmark)
--     - DACH (Germany, Austria)
--     - UK & Ireland
--     - France & Benelux (France, Belgium, Netherlands, Luxembourg)
--     - Baltics & Eastern Europe (Estonia, Latvia, Lithuania, Poland)
--
-- ============================================================================================================================
--
-- KEY PERFORMANCE INDICATORS (KPIs)
-- ============================================================================================================================
--
--   KPI 1  │ Days Sales Outstanding (DSO)     │ Weighted avg: SUM(value × days_to_pay) / SUM(value)
--   KPI 2  │ Late Payment Rate %              │ late_payment_count / total_orders * 100
--   KPI 3  │ Dispute Rate %                   │ dispute_count / total_orders * 100
--   KPI 4  │ Credit Utilization %             │ open_receivables / credit_limit * 100
--   KPI 5  │ Recommended Credit Limit         │ Adjusted based on risk tier (see Section 6)
--   KPI 6  │ Risk Trend                       │ IMPROVING / STABLE / DETERIORATING (6M vs prior 6M DSO)
--   KPI 7  │ Max Days Overdue                 │ Worst-case overdue days per customer
--   KPI 8  │ Country Risk Exposure EUR        │ Total open receivables aggregated at country level
--
-- ============================================================================================================================
--
-- IMPLEMENTATION STEPS
-- ============================================================================================================================
--
--   Step 1  │ Payment Base CTE     │ Join fact_orders + dim_customer, compute days_to_pay, late_payment_flag, days_overdue
--   Step 2  │ Payment KPIs         │ Per-customer: DSO (weighted), late count, rate, total revenue, open value, max overdue
--   Step 3  │ 6M Trend Analysis    │ Split trailing 12M into two 6M windows; compare DSO improvement/deterioration
--   Step 4  │ Risk Classification  │ CASE logic: CRITICAL / HIGH / MEDIUM / LOW based on DSO and late payment count
--   Step 5  │ Credit Utilization   │ open_value / credit_limit * 100; flag if > 80% (CREDIT LIMIT ALERT)
--   Step 6  │ Credit Adjustment    │ LOW → +20% | MEDIUM → maintain | HIGH → -15% + review | CRITICAL → hold + CFO
--   Step 7  │ Risk Trend           │ recent_6m_dso vs prior_6m_dso → IMPROVING / STABLE / DETERIORATING
--   Step 8  │ Country Aggregation  │ GROUP BY country: avg risk score, total exposure, % HIGH + CRITICAL customers
--   Step 9  │ MGMT View            │ CREATE OR REPLACE VIEW nordleaf_mgmt.v_customer_credit_risk
--
-- ============================================================================================================================


-- ============================================================================================================================
-- SECTION 1: PAYMENT BEHAVIOUR BASE — ORDER-LEVEL PAYMENT METRICS
-- ============================================================================================================================
--   Computes payment latency at order grain. Only includes orders with a recorded payment_date
--   and within the trailing 12 months. Joins dim_customer to get payment terms and credit limit.

WITH payment_base AS (

  SELECT
    fo.order_key,
    dc.customer_key,
    dc.customer_name,
    dc.country,
    dc.region,
    dc.credit_limit_eur,
    dc.payment_terms_days,                          -- contractual terms: 30 or 60 days

    fo.total_value_eur,
    fo.order_date,
    fo.delivery_date,
    fo.payment_date,
    fo.order_status,

    -- Core payment latency metric
    DATE_DIFF(fo.payment_date, fo.delivery_date, DAY)          AS days_to_pay,

    -- Binary flag: was payment made after the agreed terms window?
    IF(
      DATE_DIFF(fo.payment_date, fo.delivery_date, DAY) > dc.payment_terms_days,
      1,
      0
    )                                                           AS late_payment_flag,

    -- How many days beyond contractual terms (floored at 0 for on-time payments)
    GREATEST(
      DATE_DIFF(fo.payment_date, fo.delivery_date, DAY) - dc.payment_terms_days,
      0
    )                                                           AS days_overdue,

    -- Dispute indicator (disputed orders have a specific status value)
    IF(fo.order_status = 'Disputed', 1, 0)                     AS dispute_flag,

    -- 6-month window flags for trend analysis
    CASE
      WHEN fo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6  MONTH) THEN 'RECENT_6M'
      WHEN fo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN 'PRIOR_6M'
      ELSE 'OUTSIDE_WINDOW'
    END                                                         AS analysis_window,

    -- Whether this order is paid or still open (affects utilization calc)
    IF(fo.order_status IN ('Paid'), 0, fo.total_value_eur)     AS open_receivable_eur

  FROM nordleaf_dwh.fact_orders   fo
  JOIN nordleaf_dwh.dim_customer  dc ON fo.customer_key = dc.customer_key

  WHERE fo.order_status IN ('Delivered', 'Paid', 'Disputed')
    AND fo.payment_date IS NOT NULL
    AND fo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)

),


-- ============================================================================================================================
-- SECTION 2: CUSTOMER PAYMENT KPIs — AGGREGATE TO CUSTOMER GRAIN (TRAILING 12 MONTHS)
-- ============================================================================================================================
--   Aggregates payment_base to one row per customer. Computes DSO as a weighted average
--   (weighted by invoice value) — this is the standard Finance-grade DSO calculation.

customer_payment_kpis AS (

  SELECT
    customer_key,
    customer_name,
    country,
    region,
    credit_limit_eur,
    payment_terms_days,

    -- Total and open receivables
    SUM(total_value_eur)                                               AS total_revenue_eur,
    SUM(open_receivable_eur)                                           AS total_open_receivable_eur,
    COUNT(DISTINCT order_key)                                          AS total_orders,

    -- Weighted average Days Sales Outstanding (Finance standard)
    ROUND(
      SAFE_DIVIDE(
        SUM(total_value_eur * days_to_pay),
        SUM(total_value_eur)
      ),
      1
    )                                                                  AS dso_days,

    -- Simple unweighted avg for reference
    ROUND(AVG(days_to_pay), 1)                                        AS avg_days_to_pay,

    -- Late payment metrics
    SUM(late_payment_flag)                                             AS late_payment_count,
    ROUND(
      SAFE_DIVIDE(SUM(late_payment_flag), COUNT(DISTINCT order_key)) * 100,
      1
    )                                                                  AS late_payment_rate_pct,

    -- Worst-case overdue
    MAX(days_overdue)                                                  AS max_days_overdue,
    ROUND(AVG(days_overdue), 1)                                       AS avg_days_overdue,

    -- Dispute metrics
    SUM(dispute_flag)                                                  AS dispute_count,
    ROUND(
      SAFE_DIVIDE(SUM(dispute_flag), COUNT(DISTINCT order_key)) * 100,
      1
    )                                                                  AS dispute_rate_pct

  FROM payment_base
  GROUP BY
    customer_key,
    customer_name,
    country,
    region,
    credit_limit_eur,
    payment_terms_days

),


-- ============================================================================================================================
-- SECTION 3: 6-MONTH TREND ANALYSIS — RECENT vs PRIOR WINDOW BEHAVIOUR
-- ============================================================================================================================
--   Splits the 12-month window into two 6-month halves.
--   Computes DSO for each half to detect whether a customer is improving or deteriorating.

six_month_windows AS (

  SELECT
    customer_key,
    analysis_window,

    -- DSO per window (weighted average)
    ROUND(
      SAFE_DIVIDE(
        SUM(total_value_eur * days_to_pay),
        SUM(total_value_eur)
      ),
      1
    ) AS window_dso,

    SUM(late_payment_flag)   AS window_late_count,
    SUM(total_value_eur)     AS window_revenue_eur

  FROM payment_base
  WHERE analysis_window IN ('RECENT_6M', 'PRIOR_6M')
  GROUP BY customer_key, analysis_window

),

-- Pivot the two windows into a single row per customer for comparison
customer_dso_trend AS (

  SELECT
    customer_key,

    MAX(CASE WHEN analysis_window = 'RECENT_6M' THEN window_dso         END) AS recent_6m_dso,
    MAX(CASE WHEN analysis_window = 'PRIOR_6M'  THEN window_dso         END) AS prior_6m_dso,
    MAX(CASE WHEN analysis_window = 'RECENT_6M' THEN window_late_count  END) AS recent_6m_late_count,
    MAX(CASE WHEN analysis_window = 'PRIOR_6M'  THEN window_late_count  END) AS prior_6m_late_count,
    MAX(CASE WHEN analysis_window = 'RECENT_6M' THEN window_revenue_eur END) AS recent_6m_revenue_eur,
    MAX(CASE WHEN analysis_window = 'PRIOR_6M'  THEN window_revenue_eur END) AS prior_6m_revenue_eur,

    -- DSO delta: negative = improving (paying faster), positive = deteriorating (paying slower)
    ROUND(
      MAX(CASE WHEN analysis_window = 'RECENT_6M' THEN window_dso END) -
      MAX(CASE WHEN analysis_window = 'PRIOR_6M'  THEN window_dso END),
      1
    ) AS dso_delta_days

  FROM six_month_windows
  GROUP BY customer_key

),


-- ============================================================================================================================
-- SECTION 4: RISK CLASSIFICATION — SEGMENT EACH CUSTOMER INTO RISK TIER
-- ============================================================================================================================
--   Applies NordLeaf's defined risk segmentation rules:
--     CRITICAL RISK : avg_days_to_pay > 60 OR any disputed invoices
--     HIGH RISK     : avg_days_to_pay > 45 OR more than 2 late payments
--     MEDIUM RISK   : avg_days_to_pay > 30 OR at least 1 late payment
--     LOW RISK      : avg_days_to_pay <= 30 AND no late payments (or all on time)

risk_classification AS (

  SELECT
    kpi.*,
    tr.recent_6m_dso,
    tr.prior_6m_dso,
    tr.dso_delta_days,
    tr.recent_6m_late_count,
    tr.prior_6m_late_count,
    tr.recent_6m_revenue_eur,
    tr.prior_6m_revenue_eur,

    -- Primary risk tier assignment
    CASE
      WHEN kpi.avg_days_to_pay > 60 OR kpi.dispute_count > 0
        THEN 'CRITICAL RISK'
      WHEN kpi.avg_days_to_pay > 45 OR kpi.late_payment_count > 2
        THEN 'HIGH RISK'
      WHEN kpi.avg_days_to_pay > 30 OR kpi.late_payment_count > 0
        THEN 'MEDIUM RISK'
      ELSE
        'LOW RISK'
    END AS risk_tier,

    -- Numeric risk score for ranking and aggregation (4 = worst)
    CASE
      WHEN kpi.avg_days_to_pay > 60 OR kpi.dispute_count > 0 THEN 4
      WHEN kpi.avg_days_to_pay > 45 OR kpi.late_payment_count > 2 THEN 3
      WHEN kpi.avg_days_to_pay > 30 OR kpi.late_payment_count > 0 THEN 2
      ELSE 1
    END AS risk_score,

    -- RAG status for dashboard traffic lights
    CASE
      WHEN kpi.avg_days_to_pay > 60 OR kpi.dispute_count > 0 THEN 'RED'
      WHEN kpi.avg_days_to_pay > 45 OR kpi.late_payment_count > 2 THEN 'AMBER'
      WHEN kpi.avg_days_to_pay > 30 OR kpi.late_payment_count > 0 THEN 'YELLOW'
      ELSE 'GREEN'
    END AS risk_rag_status

  FROM customer_payment_kpis kpi
  LEFT JOIN customer_dso_trend tr ON kpi.customer_key = tr.customer_key

),


-- ============================================================================================================================
-- SECTION 5: CREDIT UTILIZATION — OPEN RECEIVABLES VS CREDIT LIMIT
-- ============================================================================================================================
--   Measures how much of each customer's credit limit is currently utilised.
--   A utilisation > 80% triggers a CREDIT LIMIT ALERT regardless of risk tier.
--   Provides Finance with proactive visibility before limits are breached.

credit_utilization AS (

  SELECT
    customer_key,
    customer_name,
    country,
    region,
    credit_limit_eur,
    payment_terms_days,
    total_revenue_eur,
    total_open_receivable_eur,
    dso_days,
    avg_days_to_pay,
    late_payment_count,
    late_payment_rate_pct,
    max_days_overdue,
    avg_days_overdue,
    dispute_count,
    dispute_rate_pct,
    total_orders,
    risk_tier,
    risk_score,
    risk_rag_status,
    recent_6m_dso,
    prior_6m_dso,
    dso_delta_days,
    recent_6m_late_count,
    prior_6m_late_count,
    recent_6m_revenue_eur,
    prior_6m_revenue_eur,

    -- Credit utilization percentage
    ROUND(
      SAFE_DIVIDE(total_open_receivable_eur, credit_limit_eur) * 100,
      1
    ) AS credit_utilization_pct,

    -- Credit limit alert flag
    CASE
      WHEN SAFE_DIVIDE(total_open_receivable_eur, credit_limit_eur) >= 0.80
        THEN 'CREDIT LIMIT ALERT'
      WHEN SAFE_DIVIDE(total_open_receivable_eur, credit_limit_eur) >= 0.60
        THEN 'MONITOR UTILIZATION'
      ELSE 'WITHIN LIMITS'
    END AS credit_utilization_status,

    -- Headroom remaining on credit limit
    GREATEST(credit_limit_eur - total_open_receivable_eur, 0) AS credit_headroom_eur

  FROM risk_classification

),


-- ============================================================================================================================
-- SECTION 6: RECOMMENDED CREDIT ADJUSTMENT — FINANCE ACTION PER RISK TIER
-- ============================================================================================================================
--   Applies NordLeaf's credit policy framework to recommend a specific action and
--   recalculated credit limit for each customer based on their risk tier.
--     LOW RISK      → Eligible to increase limit by 20% (reward good payers)
--     MEDIUM RISK   → Maintain current limit; no change
--     HIGH RISK     → Reduce limit by 15%; mandate monthly account review
--     CRITICAL RISK → Suspend credit; put on hold; escalate to CFO for decision

credit_recommendations AS (

  SELECT
    *,

    -- Recommended new credit limit based on risk tier
    ROUND(
      CASE
        WHEN risk_tier = 'LOW RISK'      THEN credit_limit_eur * 1.20
        WHEN risk_tier = 'MEDIUM RISK'   THEN credit_limit_eur * 1.00
        WHEN risk_tier = 'HIGH RISK'     THEN credit_limit_eur * 0.85
        WHEN risk_tier = 'CRITICAL RISK' THEN 0
      END,
      2
    ) AS recommended_credit_limit_eur,

    -- Limit change amount (EUR) for finance reporting
    ROUND(
      CASE
        WHEN risk_tier = 'LOW RISK'      THEN credit_limit_eur * 0.20
        WHEN risk_tier = 'MEDIUM RISK'   THEN 0
        WHEN risk_tier = 'HIGH RISK'     THEN credit_limit_eur * -0.15
        WHEN risk_tier = 'CRITICAL RISK' THEN -credit_limit_eur
      END,
      2
    ) AS credit_limit_change_eur,

    -- Human-readable action for credit committee
    CASE
      WHEN risk_tier = 'LOW RISK'
        THEN 'INCREASE LIMIT +20% — Strong payment history; reward reliability'
      WHEN risk_tier = 'MEDIUM RISK'
        THEN 'MAINTAIN LIMIT — Acceptable behaviour; continue standard monitoring'
      WHEN risk_tier = 'HIGH RISK'
        THEN 'REDUCE LIMIT -15% — Elevated risk; enforce monthly account review'
      WHEN risk_tier = 'CRITICAL RISK'
        THEN 'SUSPEND CREDIT — Place on hold; escalate immediately to CFO'
    END AS recommended_action,

    -- Escalation flag for CFO-level intervention
    CASE
      WHEN risk_tier = 'CRITICAL RISK' THEN TRUE
      WHEN risk_tier = 'HIGH RISK' AND credit_utilization_pct > 80 THEN TRUE
      ELSE FALSE
    END AS cfo_escalation_required

  FROM credit_utilization

),


-- ============================================================================================================================
-- SECTION 7: RISK TREND — IS CUSTOMER IMPROVING, STABLE OR DETERIORATING?
-- ============================================================================================================================
--   Compares the recent 6-month DSO vs the prior 6-month DSO to determine direction of travel.
--   This is the most forward-looking signal: a HIGH RISK customer who is improving may not need
--   immediate limit reduction, while a LOW RISK customer who is deteriorating warrants a watch flag.

risk_trend_analysis AS (

  SELECT
    *,

    -- Risk trend label based on DSO delta (negative = paying faster = improving)
    CASE
      WHEN dso_delta_days <= -5
        THEN 'IMPROVING'
      WHEN dso_delta_days BETWEEN -5 AND 5
        THEN 'STABLE'
      WHEN dso_delta_days > 5
        THEN 'DETERIORATING'
      ELSE 'INSUFFICIENT DATA'
    END AS risk_trend,

    -- Trend severity for nuanced flagging
    CASE
      WHEN dso_delta_days > 15  THEN 'RAPID DETERIORATION — Immediate review required'
      WHEN dso_delta_days > 5   THEN 'GRADUAL DETERIORATION — Schedule account review'
      WHEN dso_delta_days < -15 THEN 'RAPID IMPROVEMENT — Consider limit increase'
      WHEN dso_delta_days < -5  THEN 'GRADUAL IMPROVEMENT — Monitor positive trend'
      ELSE 'STABLE — No significant DSO movement'
    END AS risk_trend_detail,

    -- Combined risk signal: risk tier + trend direction
    CONCAT(risk_tier, ' | ', CASE
      WHEN dso_delta_days <= -5 THEN 'IMPROVING'
      WHEN dso_delta_days BETWEEN -5 AND 5 THEN 'STABLE'
      WHEN dso_delta_days > 5 THEN 'DETERIORATING'
      ELSE 'INSUFFICIENT DATA'
    END) AS combined_risk_signal

  FROM credit_recommendations

),


-- ============================================================================================================================
-- SECTION 8: COUNTRY-LEVEL CREDIT RISK AGGREGATION
-- ============================================================================================================================
--   Rolls up individual customer risk to country level for CFO and credit committee.
--   Provides: average risk score, total exposure EUR, count and % of HIGH + CRITICAL customers.
--   This is useful for identifying geographic concentration of credit risk in NordLeaf's portfolio.

country_risk_aggregation AS (

  SELECT
    country,
    region,

    -- Customer counts by tier
    COUNT(DISTINCT customer_key)                                             AS total_customers,
    COUNTIF(risk_tier = 'LOW RISK')                                          AS low_risk_count,
    COUNTIF(risk_tier = 'MEDIUM RISK')                                       AS medium_risk_count,
    COUNTIF(risk_tier = 'HIGH RISK')                                         AS high_risk_count,
    COUNTIF(risk_tier = 'CRITICAL RISK')                                     AS critical_risk_count,

    -- % of customers in elevated risk tiers (HIGH + CRITICAL)
    ROUND(
      SAFE_DIVIDE(
        COUNTIF(risk_tier IN ('HIGH RISK', 'CRITICAL RISK')),
        COUNT(DISTINCT customer_key)
      ) * 100,
      1
    )                                                                         AS pct_elevated_risk,

    -- Average numeric risk score for the country (1 = low, 4 = critical)
    ROUND(AVG(risk_score), 2)                                                AS avg_risk_score,

    -- Total financial exposure in that country
    SUM(total_revenue_eur)                                                   AS country_total_revenue_eur,
    SUM(total_open_receivable_eur)                                           AS country_total_exposure_eur,
    SUM(credit_limit_eur)                                                    AS country_total_credit_limit_eur,

    -- Country-level average credit utilization
    ROUND(
      SAFE_DIVIDE(
        SUM(total_open_receivable_eur),
        SUM(credit_limit_eur)
      ) * 100,
      1
    )                                                                         AS country_credit_utilization_pct,

    -- CFO escalation count for that country
    COUNTIF(cfo_escalation_required = TRUE)                                  AS cfo_escalation_count,

    -- Country-level risk rating for executive summary
    CASE
      WHEN AVG(risk_score) >= 3.5 THEN 'CRITICAL EXPOSURE'
      WHEN AVG(risk_score) >= 2.5 THEN 'HIGH EXPOSURE'
      WHEN AVG(risk_score) >= 1.5 THEN 'MODERATE EXPOSURE'
      ELSE 'LOW EXPOSURE'
    END                                                                       AS country_risk_rating

  FROM risk_trend_analysis
  GROUP BY country, region

)


-- ============================================================================================================================
-- SECTION 9: CREATE MANAGEMENT LAYER VIEW
-- ============================================================================================================================
--   Materialises the full customer credit risk model as a BigQuery view in nordleaf_mgmt.
--   Finance and the CFO use this view via the Looker Studio Finance Dashboard.
--
--   NOTE: country_risk_aggregation is surfaced as a separate CTE output joined back to customer
--   rows so each customer row also carries their country-level context metrics.
--
--   Refresh cadence : Daily
--   Access          : nordleaf_mgmt.v_customer_credit_risk
--   Dashboard       : Looker Studio → Finance Dashboard → Credit Risk Tab
--   Consumers       : Finance Team, CFO, Credit Committee, Regional Sales Managers (read-only)

CREATE OR REPLACE VIEW nordleaf_mgmt.v_customer_credit_risk AS (

  WITH payment_base AS (

    SELECT
      fo.order_key,
      dc.customer_key,
      dc.customer_name,
      dc.country,
      dc.region,
      dc.credit_limit_eur,
      dc.payment_terms_days,
      fo.total_value_eur,
      fo.order_date,
      fo.delivery_date,
      fo.payment_date,
      fo.order_status,
      DATE_DIFF(fo.payment_date, fo.delivery_date, DAY)                   AS days_to_pay,
      IF(DATE_DIFF(fo.payment_date, fo.delivery_date, DAY) > dc.payment_terms_days, 1, 0)
                                                                           AS late_payment_flag,
      GREATEST(
        DATE_DIFF(fo.payment_date, fo.delivery_date, DAY) - dc.payment_terms_days,
        0
      )                                                                    AS days_overdue,
      IF(fo.order_status = 'Disputed', 1, 0)                              AS dispute_flag,
      CASE
        WHEN fo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6  MONTH) THEN 'RECENT_6M'
        WHEN fo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH) THEN 'PRIOR_6M'
        ELSE 'OUTSIDE_WINDOW'
      END                                                                  AS analysis_window,
      IF(fo.order_status IN ('Paid'), 0, fo.total_value_eur)              AS open_receivable_eur

    FROM nordleaf_dwh.fact_orders  fo
    JOIN nordleaf_dwh.dim_customer dc ON fo.customer_key = dc.customer_key

    WHERE fo.order_status IN ('Delivered', 'Paid', 'Disputed')
      AND fo.payment_date IS NOT NULL
      AND fo.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)

  ),

  customer_kpis AS (

    SELECT
      customer_key, customer_name, country, region,
      credit_limit_eur, payment_terms_days,
      SUM(total_value_eur)                                                 AS total_revenue_eur,
      SUM(open_receivable_eur)                                             AS total_open_receivable_eur,
      COUNT(DISTINCT order_key)                                            AS total_orders,
      ROUND(SAFE_DIVIDE(SUM(total_value_eur * days_to_pay), SUM(total_value_eur)), 1)
                                                                           AS dso_days,
      ROUND(AVG(days_to_pay), 1)                                          AS avg_days_to_pay,
      SUM(late_payment_flag)                                               AS late_payment_count,
      ROUND(SAFE_DIVIDE(SUM(late_payment_flag), COUNT(DISTINCT order_key)) * 100, 1)
                                                                           AS late_payment_rate_pct,
      MAX(days_overdue)                                                    AS max_days_overdue,
      ROUND(AVG(days_overdue), 1)                                         AS avg_days_overdue,
      SUM(dispute_flag)                                                    AS dispute_count,
      ROUND(SAFE_DIVIDE(SUM(dispute_flag), COUNT(DISTINCT order_key)) * 100, 1)
                                                                           AS dispute_rate_pct

    FROM payment_base
    GROUP BY customer_key, customer_name, country, region, credit_limit_eur, payment_terms_days

  ),

  six_month_windows AS (

    SELECT
      customer_key, analysis_window,
      ROUND(SAFE_DIVIDE(SUM(total_value_eur * days_to_pay), SUM(total_value_eur)), 1) AS window_dso,
      SUM(late_payment_flag) AS window_late_count

    FROM payment_base
    WHERE analysis_window IN ('RECENT_6M', 'PRIOR_6M')
    GROUP BY customer_key, analysis_window

  ),

  dso_trend AS (

    SELECT
      customer_key,
      MAX(CASE WHEN analysis_window = 'RECENT_6M' THEN window_dso        END) AS recent_6m_dso,
      MAX(CASE WHEN analysis_window = 'PRIOR_6M'  THEN window_dso        END) AS prior_6m_dso,
      MAX(CASE WHEN analysis_window = 'RECENT_6M' THEN window_late_count END) AS recent_6m_late_count,
      MAX(CASE WHEN analysis_window = 'PRIOR_6M'  THEN window_late_count END) AS prior_6m_late_count,
      ROUND(
        MAX(CASE WHEN analysis_window = 'RECENT_6M' THEN window_dso END) -
        MAX(CASE WHEN analysis_window = 'PRIOR_6M'  THEN window_dso END),
        1
      )                                                                    AS dso_delta_days

    FROM six_month_windows
    GROUP BY customer_key

  )

  SELECT
    -- Customer identifiers
    k.customer_key,
    k.customer_name,
    k.country,
    k.region,

    -- Credit terms
    k.credit_limit_eur,
    k.payment_terms_days,

    -- Payment KPIs (trailing 12M)
    k.total_revenue_eur,
    k.total_open_receivable_eur,
    k.total_orders,
    k.dso_days,
    k.avg_days_to_pay,
    k.late_payment_count,
    k.late_payment_rate_pct,
    k.max_days_overdue,
    k.avg_days_overdue,
    k.dispute_count,
    k.dispute_rate_pct,

    -- Risk tier and scoring
    CASE
      WHEN k.avg_days_to_pay > 60 OR k.dispute_count > 0 THEN 'CRITICAL RISK'
      WHEN k.avg_days_to_pay > 45 OR k.late_payment_count > 2 THEN 'HIGH RISK'
      WHEN k.avg_days_to_pay > 30 OR k.late_payment_count > 0 THEN 'MEDIUM RISK'
      ELSE 'LOW RISK'
    END                                                                    AS risk_tier,

    CASE
      WHEN k.avg_days_to_pay > 60 OR k.dispute_count > 0 THEN 4
      WHEN k.avg_days_to_pay > 45 OR k.late_payment_count > 2 THEN 3
      WHEN k.avg_days_to_pay > 30 OR k.late_payment_count > 0 THEN 2
      ELSE 1
    END                                                                    AS risk_score,

    CASE
      WHEN k.avg_days_to_pay > 60 OR k.dispute_count > 0 THEN 'RED'
      WHEN k.avg_days_to_pay > 45 OR k.late_payment_count > 2 THEN 'AMBER'
      WHEN k.avg_days_to_pay > 30 OR k.late_payment_count > 0 THEN 'YELLOW'
      ELSE 'GREEN'
    END                                                                    AS risk_rag_status,

    -- Credit utilization
    ROUND(SAFE_DIVIDE(k.total_open_receivable_eur, k.credit_limit_eur) * 100, 1)
                                                                           AS credit_utilization_pct,
    GREATEST(k.credit_limit_eur - k.total_open_receivable_eur, 0)        AS credit_headroom_eur,
    CASE
      WHEN SAFE_DIVIDE(k.total_open_receivable_eur, k.credit_limit_eur) >= 0.80 THEN 'CREDIT LIMIT ALERT'
      WHEN SAFE_DIVIDE(k.total_open_receivable_eur, k.credit_limit_eur) >= 0.60 THEN 'MONITOR UTILIZATION'
      ELSE 'WITHIN LIMITS'
    END                                                                    AS credit_utilization_status,

    -- Recommended credit actions
    ROUND(
      CASE
        WHEN k.avg_days_to_pay > 60 OR k.dispute_count > 0 THEN 0
        WHEN k.avg_days_to_pay > 45 OR k.late_payment_count > 2 THEN k.credit_limit_eur * 0.85
        WHEN k.avg_days_to_pay > 30 OR k.late_payment_count > 0 THEN k.credit_limit_eur
        ELSE k.credit_limit_eur * 1.20
      END,
      2
    )                                                                      AS recommended_credit_limit_eur,

    CASE
      WHEN k.avg_days_to_pay > 60 OR k.dispute_count > 0
        THEN 'SUSPEND CREDIT — Place on hold; escalate immediately to CFO'
      WHEN k.avg_days_to_pay > 45 OR k.late_payment_count > 2
        THEN 'REDUCE LIMIT -15% — Elevated risk; enforce monthly account review'
      WHEN k.avg_days_to_pay > 30 OR k.late_payment_count > 0
        THEN 'MAINTAIN LIMIT — Acceptable behaviour; continue standard monitoring'
      ELSE
        'INCREASE LIMIT +20% — Strong payment history; reward reliability'
    END                                                                    AS recommended_action,

    CASE
      WHEN k.avg_days_to_pay > 60 OR k.dispute_count > 0 THEN TRUE
      WHEN (k.avg_days_to_pay > 45 OR k.late_payment_count > 2)
           AND SAFE_DIVIDE(k.total_open_receivable_eur, k.credit_limit_eur) > 0.80 THEN TRUE
      ELSE FALSE
    END                                                                    AS cfo_escalation_required,

    -- 6-month trend metrics
    t.recent_6m_dso,
    t.prior_6m_dso,
    t.dso_delta_days,
    t.recent_6m_late_count,
    t.prior_6m_late_count,

    -- Risk trend classification
    CASE
      WHEN t.dso_delta_days <= -5 THEN 'IMPROVING'
      WHEN t.dso_delta_days BETWEEN -5 AND 5 THEN 'STABLE'
      WHEN t.dso_delta_days > 5 THEN 'DETERIORATING'
      ELSE 'INSUFFICIENT DATA'
    END                                                                    AS risk_trend,

    CASE
      WHEN t.dso_delta_days > 15  THEN 'RAPID DETERIORATION — Immediate review required'
      WHEN t.dso_delta_days > 5   THEN 'GRADUAL DETERIORATION — Schedule account review'
      WHEN t.dso_delta_days < -15 THEN 'RAPID IMPROVEMENT — Consider limit increase'
      WHEN t.dso_delta_days < -5  THEN 'GRADUAL IMPROVEMENT — Monitor positive trend'
      ELSE 'STABLE — No significant DSO movement'
    END                                                                    AS risk_trend_detail,

    -- Country-level risk context (from aggregation)
    ca.avg_risk_score                                                      AS country_avg_risk_score,
    ca.country_total_exposure_eur,
    ca.country_credit_utilization_pct,
    ca.pct_elevated_risk                                                   AS country_pct_elevated_risk,
    ca.country_risk_rating,

    -- Metadata
    CURRENT_TIMESTAMP()                                                    AS record_created_at,
    'UC-18'                                                                AS use_case_id,
    'nordleaf_dwh'                                                         AS source_schema

  FROM customer_kpis k

  LEFT JOIN dso_trend t
    ON k.customer_key = t.customer_key

  -- Self-join for country aggregation context (subquery to avoid circular reference)
  LEFT JOIN (
    SELECT
      c2.country,
      ROUND(AVG(
        CASE
          WHEN c2.avg_days_to_pay > 60 THEN 4
          WHEN c2.avg_days_to_pay > 45 THEN 3
          WHEN c2.avg_days_to_pay > 30 THEN 2
          ELSE 1
        END
      ), 2)                                                                AS avg_risk_score,
      SUM(c2.total_open_receivable_eur)                                   AS country_total_exposure_eur,
      ROUND(SAFE_DIVIDE(
        SUM(c2.total_open_receivable_eur),
        SUM(c2.credit_limit_eur)
      ) * 100, 1)                                                          AS country_credit_utilization_pct,
      ROUND(SAFE_DIVIDE(
        COUNTIF(c2.avg_days_to_pay > 45 OR c2.late_payment_count > 2),
        COUNT(DISTINCT c2.customer_key)
      ) * 100, 1)                                                          AS pct_elevated_risk,
      CASE
        WHEN AVG(CASE WHEN c2.avg_days_to_pay > 60 THEN 4
                      WHEN c2.avg_days_to_pay > 45 THEN 3
                      WHEN c2.avg_days_to_pay > 30 THEN 2
                      ELSE 1 END) >= 3.5 THEN 'CRITICAL EXPOSURE'
        WHEN AVG(CASE WHEN c2.avg_days_to_pay > 60 THEN 4
                      WHEN c2.avg_days_to_pay > 45 THEN 3
                      WHEN c2.avg_days_to_pay > 30 THEN 2
                      ELSE 1 END) >= 2.5 THEN 'HIGH EXPOSURE'
        WHEN AVG(CASE WHEN c2.avg_days_to_pay > 60 THEN 4
                      WHEN c2.avg_days_to_pay > 45 THEN 3
                      WHEN c2.avg_days_to_pay > 30 THEN 2
                      ELSE 1 END) >= 1.5 THEN 'MODERATE EXPOSURE'
        ELSE 'LOW EXPOSURE'
      END                                                                  AS country_risk_rating

    FROM (
      SELECT
        dc2.customer_key,
        dc2.country,
        dc2.credit_limit_eur,
        ROUND(AVG(DATE_DIFF(fo2.payment_date, fo2.delivery_date, DAY)), 1) AS avg_days_to_pay,
        SUM(IF(fo2.order_status IN ('Paid'), 0, fo2.total_value_eur))      AS total_open_receivable_eur,
        SUM(IF(DATE_DIFF(fo2.payment_date, fo2.delivery_date, DAY) > dc2.payment_terms_days, 1, 0))
                                                                            AS late_payment_count
      FROM nordleaf_dwh.fact_orders  fo2
      JOIN nordleaf_dwh.dim_customer dc2 ON fo2.customer_key = dc2.customer_key
      WHERE fo2.order_status IN ('Delivered', 'Paid', 'Disputed')
        AND fo2.payment_date IS NOT NULL
        AND fo2.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
      GROUP BY dc2.customer_key, dc2.country, dc2.credit_limit_eur, dc2.payment_terms_days
    ) c2
    GROUP BY c2.country
  ) ca ON k.country = ca.country

  ORDER BY
    risk_score       DESC,
    dso_days         DESC,
    k.customer_name  ASC

);

-- ============================================================================================================================
-- END OF FILE: UC18_customer_segmentation_credit_risk.sql
-- Use Case : UC-18 | Customer Domain | Priority P2
-- View     : nordleaf_mgmt.v_customer_credit_risk
-- Dashboard: Looker Studio → Finance Dashboard → Credit Risk Tab
-- ============================================================================================================================
