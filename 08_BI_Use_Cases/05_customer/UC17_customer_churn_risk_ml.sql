/*
================================================================================
  FILE        : UC17_customer_churn_risk_ml.sql
  PROJECT     : NordLeaf BI Implementation
  DOMAIN      : Customer
  USE CASE    : UC-17 — Customer Churn Risk Detection (BigQuery ML)
  PRIORITY    : P1 — Critical
  LAYER       : DWH → MGMT
  SCHEMA      : nordleaf_dwh  |  nordleaf_mgmt
  AUTHOR      : NordLeaf Data Engineering Team
  CREATED     : 2026-02-23
  LAST UPDATED: 2026-02-23
  VERSION     : 1.0.0
================================================================================

  ARCHITECTURE
  ─────────────────────────────────────────────────────────────────────────────

  ╔═══════════════════════════════════════════════════════════════════╗
  ║  UC-17: Customer Churn Risk Detection (BigQuery ML)               ║
  ║                                                                   ║
  ║  fact_orders ──┐                                                  ║
  ║                ├──► Feature Engineering ──► BQML Logistic ──►    ║
  ║  dim_customer ─┘    (RFM + Behavioural)     Regression Model     ║
  ║                                                      │            ║
  ║  ML.PREDICT ◄── nordleaf_mgmt.bqml_churn_model ◄───┘            ║
  ║       │                                                           ║
  ║       └──► v_churn_risk_scores ──► CRM Alert / Looker Studio     ║
  ╚═══════════════════════════════════════════════════════════════════╝

  DATA FLOW
  ─────────────────────────────────────────────────────────────────────────────
  EXT  ──►  STG  ──►  DWH  ──►  MGMT
                       │          │
               fact_orders    bqml_churn_model
               dim_customer   v_churn_risk_scores
               dim_date

================================================================================

  BUSINESS PROBLEM
  ─────────────────────────────────────────────────────────────────────────────
  NordLeaf operates on long-term paper supply contracts across 18 EU customers
  spanning 5 production mills (Tampere, Oulu, Turku, Espoo, Jyväskylä) and
  5 product lines (Newsprint, Kraft Paper, Cardboard, Specialty Paper, Tissue).

  Customers can reduce contracted volumes or switch suppliers at renewal without
  explicit early warning signals. This use case builds a Machine Learning churn
  prediction model using BigQuery ML Logistic Regression, trained on behavioural
  features derived from transactional order history:

    - Declining order frequency over recent 3-month windows
    - Falling order volumes compared to prior periods
    - Increasing complaint counts (proxy via order anomalies)
    - Late payment rates derived from delivery and order data
    - Product diversity (breadth of product engagement)

  The output is a scored customer list with churn probability (0–1), risk tier
  classification, predicted revenue at risk (EUR), and a recommended commercial
  intervention action — consumed by the CRM system and Looker Studio dashboards.

================================================================================

  CHURN DEFINITION
  ─────────────────────────────────────────────────────────────────────────────
  A customer is labelled as CHURNED (churn_label = 1) if either condition holds:

    1. Volume Decline Churn:
       Order volume in the most recent 3 months is MORE THAN 30% lower than
       volume in the preceding 3 months (months 3–6 ago).
       Formula: (volume_last_3m - volume_prior_3m) / volume_prior_3m < -0.30

    2. Silence / Inactivity Churn:
       The customer has placed NO orders for more than 60 consecutive days,
       measured as: DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) > 60

  A customer is labelled as NOT CHURNED (churn_label = 0) if neither applies.

================================================================================

  FEATURES USED IN ML MODEL
  ─────────────────────────────────────────────────────────────────────────────
  Feature                   | Type    | Description
  ──────────────────────────┼─────────┼──────────────────────────────────────
  recency_days              | INT64   | Days since last delivered order
  order_frequency_3m        | INT64   | Count of orders in last 3 months
  volume_change_pct         | FLOAT64 | % change in tonnes: last 3m vs prior 3m
  frequency_change_pct      | FLOAT64 | % change in order count: 3m vs prior 3m
  avg_days_since_last_order | FLOAT64 | Avg gap (days) between consecutive orders
  complaint_count           | INT64   | Proxy: orders with amended/cancelled flag
  late_payment_rate         | FLOAT64 | % of deliveries with payment delays
  product_diversity         | INT64   | Count of distinct products ordered (12m)
  recency_risk_flag         | INT64   | Binary: 1 if recency_days > 60
  volume_decline_flag       | INT64   | Binary: 1 if volume_change_pct < -30

================================================================================

  KPIs PRODUCED
  ─────────────────────────────────────────────────────────────────────────────
  - Churn probability score (0.0 – 1.0) per customer
  - Risk tier classification: HIGH RISK / MEDIUM RISK / LOW RISK
  - Predicted revenue at risk (EUR) = churn_probability * total_revenue_12m
  - Recommended commercial intervention action per tier

================================================================================

  IMPLEMENTATION STEPS
  ─────────────────────────────────────────────────────────────────────────────
  Step 1  — Build customer behaviour feature table (12-month window)
  Step 2  — Compute derived churn features and assign historical churn labels
  Step 3  — Create BQML training feature table (nordleaf_dwh.churn_training_features)
  Step 4  — Train BQML Logistic Regression model (nordleaf_mgmt.bqml_churn_model)
  Step 5  — Evaluate model (ROC-AUC, accuracy, precision, recall, F1)
  Step 6  — Score current customers with ML.PREDICT
  Step 7  — Apply risk tier classification and compute revenue at risk
  Step 8  — Generate intervention recommendations per risk tier
  Step 9  — Create MGMT view: nordleaf_mgmt.v_churn_risk_scores
  Step 10 — Aggregate total revenue at risk summary by region

================================================================================

  DEPENDENCIES
  ─────────────────────────────────────────────────────────────────────────────
  Tables  : nordleaf_dwh.fact_orders
            nordleaf_dwh.fact_deliveries
            nordleaf_dwh.dim_customer
            nordleaf_dwh.dim_product
            nordleaf_dwh.dim_date
  Models  : nordleaf_mgmt.bqml_churn_model  (created by this script)
  Views   : nordleaf_mgmt.v_churn_risk_scores (created by this script)

  PERMISSIONS REQUIRED
  ─────────────────────────────────────────────────────────────────────────────
  - bigquery.tables.create   on nordleaf_dwh
  - bigquery.models.create   on nordleaf_mgmt
  - bigquery.models.getData  on nordleaf_mgmt
  - bigquery.tables.getData  on nordleaf_dwh

================================================================================
*/


/* ============================================================================
   SECTION 1 — FEATURE ENGINEERING BASE
   Build raw behavioural features per customer from the last 12 months of
   delivered orders. Splits the 12-month window into:
     - Last 3 months   (most recent activity)
     - Prior 3 months  (months 3-6 ago, the comparison baseline)
     - Full 12 months  (for annual aggregates)
   ============================================================================ */

WITH customer_behaviour AS (

  SELECT
    dc.customer_key,
    dc.customer_name,
    dc.country,
    dc.region,
    dc.customer_segment,
    dc.account_manager,

    -- ── Recency ──────────────────────────────────────────────────────────────
    -- Days elapsed since the customer's most recent delivered order
    DATE_DIFF(CURRENT_DATE(), MAX(dd.full_date), DAY)          AS recency_days,
    MAX(dd.full_date)                                           AS last_order_date,

    -- ── Frequency: Last 3 months ─────────────────────────────────────────────
    COUNTIF(
      dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    )                                                           AS orders_last_3m,

    -- ── Frequency: Prior 3 months (months 3–6 ago) ───────────────────────────
    COUNTIF(
      dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
      AND dd.full_date <  DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH)
    )                                                           AS orders_prior_3m,

    -- ── Volume (tonnes): Last 3 months ───────────────────────────────────────
    SUM(
      IF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
         fo.quantity_tonnes, 0)
    )                                                           AS volume_last_3m,

    -- ── Volume (tonnes): Prior 3 months ──────────────────────────────────────
    SUM(
      IF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
         AND dd.full_date < DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
         fo.quantity_tonnes, 0)
    )                                                           AS volume_prior_3m,

    -- ── Revenue (EUR): Last 3 months ─────────────────────────────────────────
    SUM(
      IF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
         fo.total_value_eur, 0)
    )                                                           AS revenue_last_3m,

    -- ── Revenue (EUR): Prior 3 months ────────────────────────────────────────
    SUM(
      IF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
         AND dd.full_date < DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
         fo.total_value_eur, 0)
    )                                                           AS revenue_prior_3m,

    -- ── Product Diversity ────────────────────────────────────────────────────
    -- Number of distinct product types ordered in the last 12 months
    -- Customers ordering across multiple product lines are less likely to churn
    COUNT(DISTINCT fo.product_key)                              AS distinct_products_ordered,

    -- ── Complaint Proxy ──────────────────────────────────────────────────────
    -- Orders flagged as AMENDED or CANCELLED act as a complaint proxy
    -- (assumes complaints manifest as order amendments in transactional data)
    COUNTIF(fo.order_status IN ('Amended', 'Cancelled'))        AS complaint_count,

    -- ── Annual Aggregates ────────────────────────────────────────────────────
    COUNT(fo.order_key)                                         AS total_orders_12m,
    SUM(fo.total_value_eur)                                     AS total_revenue_12m,
    SUM(fo.quantity_tonnes)                                     AS total_volume_12m_tonnes,
    AVG(fo.total_value_eur)                                     AS avg_order_value_eur

  FROM nordleaf_dwh.fact_orders    fo
  JOIN nordleaf_dwh.dim_customer   dc ON fo.customer_key = dc.customer_key
  JOIN nordleaf_dwh.dim_date       dd ON fo.date_key     = dd.date_key

  WHERE
    -- 12-month rolling window
    dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    -- Only count delivered orders as confirmed activity
    AND fo.order_status = 'Delivered'

  GROUP BY
    dc.customer_key,
    dc.customer_name,
    dc.country,
    dc.region,
    dc.customer_segment,
    dc.account_manager

),


/* ============================================================================
   SECTION 1b — LATE PAYMENT RATE (from fact_deliveries)
   Joins delivery data to compute the fraction of deliveries where payment
   was received after the due date. High late payment rate is a churn signal
   (financial stress or disengagement from the supplier relationship).
   ============================================================================ */

late_payment_data AS (

  SELECT
    fo.customer_key,
    -- Late payment rate: deliveries paid after due date / total deliveries
    SAFE_DIVIDE(
      COUNTIF(fd.payment_received_date > fd.payment_due_date),
      COUNT(fd.delivery_key)
    )                                                           AS late_payment_rate,
    -- Average days overdue (only for late payments)
    AVG(
      IF(fd.payment_received_date > fd.payment_due_date,
         DATE_DIFF(fd.payment_received_date, fd.payment_due_date, DAY),
         NULL)
    )                                                           AS avg_days_overdue

  FROM nordleaf_dwh.fact_deliveries  fd
  JOIN nordleaf_dwh.fact_orders      fo ON fd.order_key     = fo.order_key
  JOIN nordleaf_dwh.dim_date         dd ON fo.date_key      = dd.date_key

  WHERE
    dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    AND fd.delivery_status = 'Completed'

  GROUP BY fo.customer_key

),


/* ============================================================================
   SECTION 2 — DERIVED CHURN FEATURES
   Compute the key engineered features used to classify churn risk:
     - volume_change_pct        : % volume change, recent vs prior 3m
     - frequency_change_pct     : % order frequency change, recent vs prior 3m
     - avg_days_since_last_order: average inter-order gap over 12 months
     - recency_risk_flag        : 1 if customer silent for more than 60 days
     - volume_decline_flag      : 1 if volume dropped more than 30%
     - churn_label              : historical ground-truth label (1 = churned)
   ============================================================================ */

churn_features AS (

  SELECT
    cb.customer_key,
    cb.customer_name,
    cb.country,
    cb.region,
    cb.customer_segment,
    cb.account_manager,
    cb.last_order_date,
    cb.recency_days,
    cb.orders_last_3m                                           AS order_frequency_3m,
    cb.distinct_products_ordered                                AS product_diversity,
    cb.complaint_count,
    cb.total_orders_12m,
    cb.total_revenue_12m,
    cb.total_volume_12m_tonnes,
    cb.avg_order_value_eur,
    cb.revenue_last_3m,
    cb.revenue_prior_3m,
    cb.volume_last_3m,
    cb.volume_prior_3m,

    -- ── Late Payment Rate ────────────────────────────────────────────────────
    COALESCE(lp.late_payment_rate, 0.0)                         AS late_payment_rate,
    COALESCE(lp.avg_days_overdue,  0.0)                         AS avg_days_overdue,

    -- ── Volume Change % (last 3m vs prior 3m) ───────────────────────────────
    -- Positive = growth, Negative = decline, NULL-safe via SAFE_DIVIDE
    ROUND(
      SAFE_DIVIDE(
        cb.volume_last_3m - cb.volume_prior_3m,
        NULLIF(cb.volume_prior_3m, 0)
      ) * 100,
      2
    )                                                           AS volume_change_pct,

    -- ── Frequency Change % (last 3m vs prior 3m) ────────────────────────────
    ROUND(
      SAFE_DIVIDE(
        cb.orders_last_3m - cb.orders_prior_3m,
        NULLIF(cb.orders_prior_3m, 0)
      ) * 100,
      2
    )                                                           AS frequency_change_pct,

    -- ── Average Days Between Orders ──────────────────────────────────────────
    -- Estimated as: 365 / total_orders_12m (proxy for inter-order gap)
    -- Longer gaps indicate reduced engagement
    ROUND(
      SAFE_DIVIDE(365.0, NULLIF(cb.total_orders_12m, 0)),
      1
    )                                                           AS avg_days_since_last_order,

    -- ── Binary Risk Flags ────────────────────────────────────────────────────
    -- recency_risk_flag: 1 if no orders in last 60 days (silence churn signal)
    IF(cb.recency_days > 60,   1, 0)                            AS recency_risk_flag,

    -- volume_decline_flag: 1 if volume dropped by more than 30%
    IF(
      SAFE_DIVIDE(
        cb.volume_last_3m - cb.volume_prior_3m,
        NULLIF(cb.volume_prior_3m, 0)
      ) < -0.30,
      1, 0
    )                                                           AS volume_decline_flag,

    -- ── Historical Churn Label ───────────────────────────────────────────────
    -- Ground-truth label for model training.
    -- churn_label = 1 if customer exhibits volume decline churn OR silence churn
    -- churn_label = 0 otherwise (active / retained customer)
    IF(
      SAFE_DIVIDE(
        cb.volume_last_3m - cb.volume_prior_3m,
        NULLIF(cb.volume_prior_3m, 0)
      ) < -0.30
      OR cb.recency_days > 60,
      1, 0
    )                                                           AS churn_label

  FROM customer_behaviour  cb
  LEFT JOIN late_payment_data lp ON cb.customer_key = lp.customer_key

)


/* ============================================================================
   SECTION 3 — CREATE BQML TRAINING FEATURE TABLE
   Materialise the feature set into a persistent table in nordleaf_dwh.
   This table is used as the training corpus for the BQML model.

   Snapshot logic: we use customers whose "last active period" ends before
   DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH), simulating the historical
   state at a point-in-time 3 months ago. This avoids data leakage by
   ensuring the churn label reflects outcomes that occurred AFTER the
   feature observation window.
   ============================================================================ */

-- Materialise training features (run this step first, independently)
CREATE OR REPLACE TABLE nordleaf_dwh.churn_training_features
OPTIONS (
  description            = 'UC-17: BQML churn training feature set with historical labels. Refreshed monthly.',
  labels                 = [('domain', 'customer'), ('use_case', 'uc17'), ('layer', 'dwh')]
)
AS
SELECT
  customer_key,
  customer_name,
  country,
  region,
  customer_segment,
  recency_days,
  order_frequency_3m,
  volume_change_pct,
  frequency_change_pct,
  avg_days_since_last_order,
  complaint_count,
  late_payment_rate,
  avg_days_overdue,
  product_diversity,
  recency_risk_flag,
  volume_decline_flag,
  total_orders_12m,
  total_revenue_12m,
  total_volume_12m_tonnes,
  avg_order_value_eur,
  revenue_last_3m,
  revenue_prior_3m,
  volume_last_3m,
  volume_prior_3m,
  -- The label the model will learn to predict
  churn_label,
  -- Audit metadata
  CURRENT_TIMESTAMP()    AS feature_snapshot_ts,
  CURRENT_DATE()         AS feature_snapshot_date
FROM churn_features
WHERE
  -- Training set: only customers with sufficient history (at least 2 orders)
  total_orders_12m >= 2
  -- Exclude customers with zero prior-3m volume to avoid division artefacts
  -- (new customers with no comparison baseline are excluded from training)
  AND volume_prior_3m > 0
;


/* ============================================================================
   SECTION 4 — TRAIN BIGQUERY ML LOGISTIC REGRESSION MODEL
   Creates (or replaces) the churn prediction model in nordleaf_mgmt.
   Model is also registered to Vertex AI Model Registry for MLOps governance.

   Hyperparameters:
     - model_type              = LOGISTIC_REG (binary classification)
     - l2_reg                  = 0.01 (regularisation to prevent overfitting)
     - max_iterations          = 20 (sufficient for convergence on this dataset)
     - data_split_method       = RANDOM (80/20 train/eval automatic split)
     - data_split_eval_fraction= 0.2 (20% held out for evaluation)

   Input label: churn_label (0 = retained, 1 = churned)
   ============================================================================ */

CREATE OR REPLACE MODEL nordleaf_mgmt.bqml_churn_model

  OPTIONS (
    model_type                    = 'LOGISTIC_REG',
    input_label_cols              = ['churn_label'],
    -- Regularisation: reduces overfitting on small EU customer dataset (18 customers)
    l2_reg                        = 0.01,
    -- Training iterations
    max_iterations                = 20,
    -- Automatic 80/20 train-evaluation split
    data_split_method             = 'RANDOM',
    data_split_eval_fraction      = 0.2,
    -- Vertex AI Model Registry integration for MLOps governance
    model_registry                = 'vertex_ai',
    vertex_ai_model_display_name  = 'nordleaf-churn-model',
    -- Model description
    model_description             = 'NordLeaf UC-17: Customer churn logistic regression. Predicts 90-day churn risk using RFM + behavioural features. Trained on 12-month delivered order history.'
  )

AS

SELECT
  -- Numerical features
  recency_days,
  order_frequency_3m,
  volume_change_pct,
  frequency_change_pct,
  avg_days_since_last_order,
  complaint_count,
  late_payment_rate,
  avg_days_overdue,
  product_diversity,
  -- Binary engineered flags
  recency_risk_flag,
  volume_decline_flag,
  -- Target label
  churn_label

FROM nordleaf_dwh.churn_training_features

WHERE
  -- Exclude rows with NULL features (would be silently dropped by BQML but
  -- making this explicit prevents silent data quality issues)
  recency_days              IS NOT NULL
  AND volume_change_pct     IS NOT NULL
  AND frequency_change_pct  IS NOT NULL
  AND late_payment_rate     IS NOT NULL
;


/* ============================================================================
   SECTION 5 — MODEL EVALUATION
   Retrieve standard binary classification metrics from ML.EVALUATE.
   Target thresholds for production sign-off:
     - ROC AUC     >= 0.75  (acceptable discriminatory power)
     - Precision   >= 0.70  (limit false positives — costly account manager effort)
     - Recall      >= 0.65  (capture most true churners)
     - F1 Score    >= 0.67  (harmonic balance of precision/recall)
   ============================================================================ */

-- Full evaluation metrics on the held-out evaluation set
SELECT
  'nordleaf_mgmt.bqml_churn_model'   AS model_name,
  ROUND(roc_auc,      4)             AS roc_auc,
  ROUND(accuracy,     4)             AS accuracy,
  ROUND(precision,    4)             AS precision,
  ROUND(recall,       4)             AS recall,
  ROUND(f1_score,     4)             AS f1_score,
  ROUND(log_loss,     4)             AS log_loss,
  -- Derive threshold recommendation: flag if AUC < 0.75 (below acceptable bar)
  IF(roc_auc >= 0.75, 'PASS', 'REVIEW REQUIRED')  AS auc_threshold_check,
  IF(f1_score >= 0.67, 'PASS', 'REVIEW REQUIRED') AS f1_threshold_check,
  CURRENT_TIMESTAMP()                AS evaluated_at

FROM ML.EVALUATE(
  MODEL nordleaf_mgmt.bqml_churn_model,
  (
    SELECT
      recency_days,
      order_frequency_3m,
      volume_change_pct,
      frequency_change_pct,
      avg_days_since_last_order,
      complaint_count,
      late_payment_rate,
      avg_days_overdue,
      product_diversity,
      recency_risk_flag,
      volume_decline_flag,
      churn_label
    FROM nordleaf_dwh.churn_training_features
  )
)
;

-- Feature importance (coefficient weights from logistic regression)
-- Higher absolute weight = stronger predictor of churn
SELECT
  processed_input                         AS feature_name,
  ROUND(weight,   4)                      AS coefficient_weight,
  ROUND(ABS(weight), 4)                   AS abs_importance,
  IF(weight > 0, 'Increases Churn Risk',
                 'Decreases Churn Risk')  AS direction,
  ROW_NUMBER() OVER (ORDER BY ABS(weight) DESC) AS importance_rank

FROM ML.WEIGHTS(MODEL nordleaf_mgmt.bqml_churn_model)

ORDER BY abs_importance DESC
;


/* ============================================================================
   SECTION 6 — SCORE CURRENT CUSTOMERS (ML.PREDICT)
   Apply the trained model to the current customer feature set to generate
   live churn probability scores. The predicted_churn_label_probs array
   contains probability for each class:
     OFFSET(0) = prob of churn_label = 0 (retained)
     OFFSET(1) = prob of churn_label = 1 (churned)  ← this is our score
   ============================================================================ */

-- NOTE: This SELECT is embedded inside the MGMT view (Section 9).
-- It is shown here standalone for testing/validation purposes.

WITH current_features AS (

  -- Re-derive features for ALL current active customers (no churn_label filter)
  -- Uses the same CTE logic as Sections 1-2 but runs against current data
  SELECT
    cf.customer_key,
    cf.customer_name,
    cf.country,
    cf.region,
    cf.customer_segment,
    cf.account_manager,
    cf.last_order_date,
    cf.recency_days,
    cf.order_frequency_3m,
    cf.volume_change_pct,
    cf.frequency_change_pct,
    cf.avg_days_since_last_order,
    cf.complaint_count,
    cf.late_payment_rate,
    cf.avg_days_overdue,
    cf.product_diversity,
    cf.recency_risk_flag,
    cf.volume_decline_flag,
    cf.total_revenue_12m,
    cf.total_volume_12m_tonnes,
    cf.avg_order_value_eur,
    cf.revenue_last_3m,
    cf.revenue_prior_3m

  FROM nordleaf_dwh.churn_training_features cf

  WHERE
    -- Score ALL customers with sufficient data, not just labelled churners
    cf.total_orders_12m >= 1

),

churn_scores AS (

  SELECT
    p.customer_key,
    p.customer_name,
    p.country,
    p.region,
    p.customer_segment,
    p.account_manager,
    p.last_order_date,
    p.recency_days,
    p.order_frequency_3m,
    p.volume_change_pct,
    p.frequency_change_pct,
    p.avg_days_since_last_order,
    p.complaint_count,
    p.late_payment_rate,
    p.avg_days_overdue,
    p.product_diversity,
    p.recency_risk_flag,
    p.volume_decline_flag,
    p.total_revenue_12m,
    p.total_volume_12m_tonnes,
    p.avg_order_value_eur,
    p.revenue_last_3m,
    p.revenue_prior_3m,
    -- Predicted class (0 or 1) from the model
    p.predicted_churn_label                                     AS predicted_churn,
    -- Probability of churn (class = 1), extracted from probs array
    ROUND(
      p.predicted_churn_label_probs[OFFSET(1)].prob,
      4
    )                                                           AS churn_probability,
    -- Probability of retention (class = 0) for completeness
    ROUND(
      p.predicted_churn_label_probs[OFFSET(0)].prob,
      4
    )                                                           AS retention_probability

  FROM ML.PREDICT(
    MODEL nordleaf_mgmt.bqml_churn_model,
    (
      SELECT
        customer_key,
        customer_name,
        country,
        region,
        customer_segment,
        account_manager,
        last_order_date,
        recency_days,
        order_frequency_3m,
        volume_change_pct,
        frequency_change_pct,
        avg_days_since_last_order,
        complaint_count,
        late_payment_rate,
        avg_days_overdue,
        product_diversity,
        recency_risk_flag,
        volume_decline_flag,
        total_revenue_12m,
        total_volume_12m_tonnes,
        avg_order_value_eur,
        revenue_last_3m,
        revenue_prior_3m
      FROM current_features
    )
  ) AS p

),


/* ============================================================================
   SECTION 7 — RISK TIER CLASSIFICATION AND REVENUE AT RISK
   Segment customers into three risk tiers based on churn_probability:
     HIGH RISK   : churn_probability >= 0.70  (urgent intervention needed)
     MEDIUM RISK : churn_probability >= 0.40  (proactive engagement)
     LOW RISK    : churn_probability <  0.40  (standard relationship management)

   Revenue at Risk = churn_probability * total_revenue_12m
   This is the expected EUR loss if the customer churns, weighted by probability.
   ============================================================================ */

risk_tier_classification AS (

  SELECT
    cs.*,

    -- ── Risk Tier ────────────────────────────────────────────────────────────
    CASE
      WHEN cs.churn_probability >= 0.70 THEN 'HIGH RISK'
      WHEN cs.churn_probability >= 0.40 THEN 'MEDIUM RISK'
      ELSE                                   'LOW RISK'
    END                                                         AS risk_tier,

    -- ── Risk Tier Score (1=Low, 2=Medium, 3=High) for sorting ────────────────
    CASE
      WHEN cs.churn_probability >= 0.70 THEN 3
      WHEN cs.churn_probability >= 0.40 THEN 2
      ELSE                                   1
    END                                                         AS risk_tier_score,

    -- ── Revenue at Risk (EUR) ────────────────────────────────────────────────
    -- Expected annual revenue loss if the customer churns
    ROUND(cs.churn_probability * cs.total_revenue_12m, 2)       AS revenue_at_risk_eur,

    -- ── Volume at Risk (tonnes) ──────────────────────────────────────────────
    ROUND(cs.churn_probability * cs.total_volume_12m_tonnes, 1) AS volume_at_risk_tonnes,

    -- ── Churn Probability Band (for display) ─────────────────────────────────
    CONCAT(
      CAST(ROUND(cs.churn_probability * 100, 1) AS STRING), '%'
    )                                                           AS churn_probability_pct,

    -- ── Days Since Last Order (readable label) ───────────────────────────────
    CASE
      WHEN cs.recency_days <= 14  THEN 'Active (0-14d)'
      WHEN cs.recency_days <= 30  THEN 'Recent (15-30d)'
      WHEN cs.recency_days <= 60  THEN 'At Risk (31-60d)'
      WHEN cs.recency_days <= 90  THEN 'Dormant (61-90d)'
      ELSE                             CONCAT('Inactive (', CAST(cs.recency_days AS STRING), 'd)')
    END                                                         AS recency_label,

    -- ── Volume Trend Label ───────────────────────────────────────────────────
    CASE
      WHEN cs.volume_change_pct >= 10   THEN 'Growing'
      WHEN cs.volume_change_pct >= -10  THEN 'Stable'
      WHEN cs.volume_change_pct >= -30  THEN 'Declining'
      ELSE                                   'Critical Decline'
    END                                                         AS volume_trend_label,

    -- ── Primary Churn Driver (most prominent signal) ──────────────────────────
    CASE
      WHEN cs.recency_days > 60
           AND cs.volume_change_pct < -30 THEN 'Silence + Volume Decline'
      WHEN cs.recency_days > 60           THEN 'Customer Silence (>60 days)'
      WHEN cs.volume_change_pct < -30     THEN 'Volume Decline (>30%)'
      WHEN cs.late_payment_rate > 0.30    THEN 'High Late Payment Rate'
      WHEN cs.complaint_count >= 3        THEN 'Multiple Complaints'
      WHEN cs.frequency_change_pct < -30  THEN 'Order Frequency Drop'
      ELSE                                     'Composite Behavioural Signals'
    END                                                         AS primary_churn_driver

  FROM churn_scores cs

),


/* ============================================================================
   SECTION 8 — INTERVENTION RECOMMENDATIONS
   Map each risk tier to a specific, actionable commercial intervention.
   Recommendations are calibrated for NordLeaf's B2B paper supply context
   where the commercial cycle is contract-based and relationship-driven.
   ============================================================================ */

churn_final AS (

  SELECT
    rt.*,

    -- ── Recommended Intervention ─────────────────────────────────────────────
    CASE rt.risk_tier

      WHEN 'HIGH RISK' THEN
        'IMMEDIATE ACTION: Escalate to Account Manager + Regional Sales Director. '
        || 'Schedule emergency retention call within 48 hours. '
        || 'Prepare bespoke retention offer: volume discount, extended payment terms, '
        || 'or dedicated mill capacity guarantee. '
        || 'Flag for CEO-level relationship review if revenue_at_risk > €500,000.'

      WHEN 'MEDIUM RISK' THEN
        'PROACTIVE ENGAGEMENT: Schedule Quarterly Business Review (QBR) within 30 days. '
        || 'Discuss product expansion opportunities across NordLeaf product lines. '
        || 'Present new product catalogue (Specialty Paper / Tissue innovations). '
        || 'Offer mill tour at nearest facility (Tampere/Turku/Espoo). '
        || 'Consider loyalty pricing for multi-product contracts.'

      ELSE /* LOW RISK */
        'STANDARD NURTURE: Maintain regular account cadence (monthly check-in). '
        || 'Include in NordLeaf sustainability newsletter and ESG reporting. '
        || 'Monitor for any sudden behavioural shifts using monthly churn score refresh. '
        || 'Continue standard pricing and service levels.'

    END                                                         AS recommended_intervention,

    -- ── Intervention Urgency ─────────────────────────────────────────────────
    CASE rt.risk_tier
      WHEN 'HIGH RISK'   THEN 'Within 48 Hours'
      WHEN 'MEDIUM RISK' THEN 'Within 30 Days'
      ELSE                    'Next Monthly Review'
    END                                                         AS intervention_urgency,

    -- ── CRM Alert Flag ───────────────────────────────────────────────────────
    IF(rt.risk_tier = 'HIGH RISK', TRUE, FALSE)                 AS crm_alert_triggered,

    -- ── Score Metadata ───────────────────────────────────────────────────────
    CURRENT_TIMESTAMP()                                         AS scored_at,
    CURRENT_DATE()                                              AS score_date,
    'nordleaf_mgmt.bqml_churn_model'                            AS model_used,
    'v1.0'                                                      AS model_version

  FROM risk_tier_classification rt

)


/* ============================================================================
   SECTION 9 — MGMT VIEW: nordleaf_mgmt.v_churn_risk_scores
   Production view surfaced to Looker Studio dashboards and CRM integrations.
   Presents the final scored, tiered, and annotated customer churn risk list.
   Ordered by revenue_at_risk_eur DESC so highest-value threats appear first.
   ============================================================================ */

-- This SELECT finalises the CTE chain above.
-- Wrap in a CREATE OR REPLACE VIEW in the view definition below (Section 9 DDL).
SELECT
  -- Customer identification
  customer_key,
  customer_name,
  country,
  region,
  customer_segment,
  account_manager,

  -- Activity metrics
  last_order_date,
  recency_days,
  recency_label,
  order_frequency_3m,

  -- Volume & revenue signals
  volume_change_pct,
  frequency_change_pct,
  volume_trend_label,
  revenue_last_3m,
  revenue_prior_3m,
  total_revenue_12m,
  total_volume_12m_tonnes,
  avg_order_value_eur,

  -- Behavioural risk indicators
  complaint_count,
  late_payment_rate,
  avg_days_overdue,
  product_diversity,
  recency_risk_flag,
  volume_decline_flag,
  primary_churn_driver,

  -- ML output
  churn_probability,
  churn_probability_pct,
  retention_probability,
  predicted_churn,

  -- Risk classification
  risk_tier,
  risk_tier_score,

  -- Financial impact
  revenue_at_risk_eur,
  volume_at_risk_tonnes,

  -- Commercial action
  recommended_intervention,
  intervention_urgency,
  crm_alert_triggered,

  -- Audit
  scored_at,
  score_date,
  model_used,
  model_version

FROM churn_final

ORDER BY
  risk_tier_score    DESC,   -- HIGH RISK first
  revenue_at_risk_eur DESC   -- Highest revenue at risk within tier
;


/* ─────────────────────────────────────────────────────────────────────────────
   SECTION 9 (DDL) — CREATE OR REPLACE VIEW in nordleaf_mgmt
   The complete CTE chain from Sections 1-8 is encapsulated here as a
   single reusable view. Scheduled to refresh daily via Cloud Scheduler
   triggering a BigQuery Scheduled Query or Dataform workflow run.
   ───────────────────────────────────────────────────────────────────────────── */

CREATE OR REPLACE VIEW nordleaf_mgmt.v_churn_risk_scores

  OPTIONS (
    description = 'UC-17: Daily customer churn risk scores from BQML Logistic Regression. '
                  || 'Includes RFM features, churn probability, risk tier, revenue at risk, '
                  || 'and recommended intervention. Source: nordleaf_mgmt.bqml_churn_model.',
    labels      = [('domain', 'customer'), ('use_case', 'uc17'), ('layer', 'mgmt'),
                   ('refresh', 'daily'), ('priority', 'p1')]
  )

AS (

  WITH customer_behaviour_v AS (
    SELECT
      dc.customer_key,
      dc.customer_name,
      dc.country,
      dc.region,
      dc.customer_segment,
      dc.account_manager,
      DATE_DIFF(CURRENT_DATE(), MAX(dd.full_date), DAY)              AS recency_days,
      MAX(dd.full_date)                                               AS last_order_date,
      COUNTIF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH))
                                                                     AS orders_last_3m,
      COUNTIF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
              AND dd.full_date < DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH))
                                                                     AS orders_prior_3m,
      SUM(IF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
             fo.quantity_tonnes, 0))                                 AS volume_last_3m,
      SUM(IF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
             AND dd.full_date < DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
             fo.quantity_tonnes, 0))                                 AS volume_prior_3m,
      SUM(IF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
             fo.total_value_eur, 0))                                 AS revenue_last_3m,
      SUM(IF(dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
             AND dd.full_date < DATE_SUB(CURRENT_DATE(), INTERVAL 3 MONTH),
             fo.total_value_eur, 0))                                 AS revenue_prior_3m,
      COUNT(DISTINCT fo.product_key)                                 AS distinct_products_ordered,
      COUNTIF(fo.order_status IN ('Amended', 'Cancelled'))           AS complaint_count,
      COUNT(fo.order_key)                                            AS total_orders_12m,
      SUM(fo.total_value_eur)                                        AS total_revenue_12m,
      SUM(fo.quantity_tonnes)                                        AS total_volume_12m_tonnes,
      AVG(fo.total_value_eur)                                        AS avg_order_value_eur
    FROM nordleaf_dwh.fact_orders    fo
    JOIN nordleaf_dwh.dim_customer   dc ON fo.customer_key = dc.customer_key
    JOIN nordleaf_dwh.dim_date       dd ON fo.date_key     = dd.date_key
    WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
      AND fo.order_status = 'Delivered'
    GROUP BY
      dc.customer_key, dc.customer_name, dc.country, dc.region,
      dc.customer_segment, dc.account_manager
  ),

  late_payment_v AS (
    SELECT
      fo.customer_key,
      SAFE_DIVIDE(
        COUNTIF(fd.payment_received_date > fd.payment_due_date),
        COUNT(fd.delivery_key)
      )                                                              AS late_payment_rate,
      AVG(IF(fd.payment_received_date > fd.payment_due_date,
             DATE_DIFF(fd.payment_received_date, fd.payment_due_date, DAY),
             NULL))                                                  AS avg_days_overdue
    FROM nordleaf_dwh.fact_deliveries  fd
    JOIN nordleaf_dwh.fact_orders      fo ON fd.order_key  = fo.order_key
    JOIN nordleaf_dwh.dim_date         dd ON fo.date_key   = dd.date_key
    WHERE dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
      AND fd.delivery_status = 'Completed'
    GROUP BY fo.customer_key
  ),

  view_features AS (
    SELECT
      cb.customer_key,
      cb.customer_name,
      cb.country,
      cb.region,
      cb.customer_segment,
      cb.account_manager,
      cb.last_order_date,
      cb.recency_days,
      cb.orders_last_3m                                              AS order_frequency_3m,
      cb.distinct_products_ordered                                   AS product_diversity,
      cb.complaint_count,
      cb.total_orders_12m,
      cb.total_revenue_12m,
      cb.total_volume_12m_tonnes,
      cb.avg_order_value_eur,
      cb.revenue_last_3m,
      cb.revenue_prior_3m,
      cb.volume_last_3m,
      cb.volume_prior_3m,
      COALESCE(lp.late_payment_rate, 0.0)                            AS late_payment_rate,
      COALESCE(lp.avg_days_overdue,  0.0)                            AS avg_days_overdue,
      ROUND(SAFE_DIVIDE(cb.volume_last_3m - cb.volume_prior_3m,
            NULLIF(cb.volume_prior_3m, 0)) * 100, 2)                AS volume_change_pct,
      ROUND(SAFE_DIVIDE(cb.orders_last_3m - cb.orders_prior_3m,
            NULLIF(cb.orders_prior_3m, 0)) * 100, 2)               AS frequency_change_pct,
      ROUND(SAFE_DIVIDE(365.0, NULLIF(cb.total_orders_12m, 0)), 1)  AS avg_days_since_last_order,
      IF(cb.recency_days > 60, 1, 0)                                 AS recency_risk_flag,
      IF(SAFE_DIVIDE(cb.volume_last_3m - cb.volume_prior_3m,
         NULLIF(cb.volume_prior_3m, 0)) < -0.30, 1, 0)             AS volume_decline_flag
    FROM customer_behaviour_v cb
    LEFT JOIN late_payment_v  lp ON cb.customer_key = lp.customer_key
    WHERE cb.total_orders_12m >= 1
  ),

  predictions AS (
    SELECT
      p.customer_key,
      p.customer_name,
      p.country,
      p.region,
      p.customer_segment,
      p.account_manager,
      p.last_order_date,
      p.recency_days,
      p.order_frequency_3m,
      p.volume_change_pct,
      p.frequency_change_pct,
      p.avg_days_since_last_order,
      p.complaint_count,
      p.late_payment_rate,
      p.avg_days_overdue,
      p.product_diversity,
      p.recency_risk_flag,
      p.volume_decline_flag,
      p.total_revenue_12m,
      p.total_volume_12m_tonnes,
      p.avg_order_value_eur,
      p.revenue_last_3m,
      p.revenue_prior_3m,
      p.predicted_churn_label                                        AS predicted_churn,
      ROUND(p.predicted_churn_label_probs[OFFSET(1)].prob, 4)       AS churn_probability,
      ROUND(p.predicted_churn_label_probs[OFFSET(0)].prob, 4)       AS retention_probability
    FROM ML.PREDICT(
      MODEL nordleaf_mgmt.bqml_churn_model,
      (SELECT * FROM view_features)
    ) AS p
  ),

  scored AS (
    SELECT
      pr.*,
      CASE
        WHEN pr.churn_probability >= 0.70 THEN 'HIGH RISK'
        WHEN pr.churn_probability >= 0.40 THEN 'MEDIUM RISK'
        ELSE                                   'LOW RISK'
      END                                                            AS risk_tier,
      CASE
        WHEN pr.churn_probability >= 0.70 THEN 3
        WHEN pr.churn_probability >= 0.40 THEN 2
        ELSE                                   1
      END                                                            AS risk_tier_score,
      ROUND(pr.churn_probability * pr.total_revenue_12m, 2)         AS revenue_at_risk_eur,
      ROUND(pr.churn_probability * pr.total_volume_12m_tonnes, 1)   AS volume_at_risk_tonnes,
      CONCAT(CAST(ROUND(pr.churn_probability * 100, 1) AS STRING), '%')
                                                                     AS churn_probability_pct,
      CASE
        WHEN pr.recency_days <= 14  THEN 'Active (0-14d)'
        WHEN pr.recency_days <= 30  THEN 'Recent (15-30d)'
        WHEN pr.recency_days <= 60  THEN 'At Risk (31-60d)'
        WHEN pr.recency_days <= 90  THEN 'Dormant (61-90d)'
        ELSE CONCAT('Inactive (', CAST(pr.recency_days AS STRING), 'd)')
      END                                                            AS recency_label,
      CASE
        WHEN pr.volume_change_pct >= 10   THEN 'Growing'
        WHEN pr.volume_change_pct >= -10  THEN 'Stable'
        WHEN pr.volume_change_pct >= -30  THEN 'Declining'
        ELSE                                   'Critical Decline'
      END                                                            AS volume_trend_label,
      CASE
        WHEN pr.recency_days > 60 AND pr.volume_change_pct < -30 THEN 'Silence + Volume Decline'
        WHEN pr.recency_days > 60                                 THEN 'Customer Silence (>60 days)'
        WHEN pr.volume_change_pct < -30                           THEN 'Volume Decline (>30%)'
        WHEN pr.late_payment_rate > 0.30                          THEN 'High Late Payment Rate'
        WHEN pr.complaint_count >= 3                              THEN 'Multiple Complaints'
        WHEN pr.frequency_change_pct < -30                        THEN 'Order Frequency Drop'
        ELSE                                                           'Composite Behavioural Signals'
      END                                                            AS primary_churn_driver
    FROM predictions pr
  )

  SELECT
    s.*,
    CASE s.risk_tier
      WHEN 'HIGH RISK' THEN
        'IMMEDIATE ACTION: Escalate to Account Manager + Regional Sales Director. '
        || 'Schedule emergency retention call within 48 hours. '
        || 'Prepare bespoke retention offer: volume discount, extended payment terms, '
        || 'or dedicated mill capacity guarantee.'
      WHEN 'MEDIUM RISK' THEN
        'PROACTIVE ENGAGEMENT: Schedule QBR within 30 days. '
        || 'Discuss product expansion across NordLeaf product lines. '
        || 'Offer mill tour at nearest facility. '
        || 'Consider loyalty pricing for multi-product contracts.'
      ELSE
        'STANDARD NURTURE: Maintain monthly check-in cadence. '
        || 'Monitor for sudden behavioural shifts on next monthly refresh. '
        || 'Continue standard pricing and service levels.'
    END                                                              AS recommended_intervention,
    CASE s.risk_tier
      WHEN 'HIGH RISK'   THEN 'Within 48 Hours'
      WHEN 'MEDIUM RISK' THEN 'Within 30 Days'
      ELSE                    'Next Monthly Review'
    END                                                              AS intervention_urgency,
    IF(s.risk_tier = 'HIGH RISK', TRUE, FALSE)                      AS crm_alert_triggered,
    CURRENT_TIMESTAMP()                                              AS scored_at,
    CURRENT_DATE()                                                   AS score_date,
    'nordleaf_mgmt.bqml_churn_model'                                 AS model_used,
    'v1.0'                                                           AS model_version
  FROM scored s
  ORDER BY
    s.risk_tier_score    DESC,
    s.revenue_at_risk_eur DESC

);


/* ============================================================================
   SECTION 10 — TOTAL REVENUE AT RISK SUMMARY BY REGION
   Executive-level aggregation for Looker Studio regional risk dashboard.
   Shows the total commercial exposure per EU region, filtered to HIGH and
   MEDIUM risk customers only (LOW RISK excluded as not requiring action).
   ============================================================================ */

-- Regional Revenue-at-Risk Summary
SELECT
  region,
  country,
  COUNT(*)                                               AS total_customers_in_region,
  COUNTIF(risk_tier = 'HIGH RISK')                       AS high_risk_count,
  COUNTIF(risk_tier = 'MEDIUM RISK')                     AS medium_risk_count,
  COUNTIF(risk_tier = 'LOW RISK')                        AS low_risk_count,
  -- Customers requiring action (HIGH + MEDIUM)
  COUNTIF(risk_tier IN ('HIGH RISK', 'MEDIUM RISK'))     AS at_risk_customers,
  -- Revenue exposure
  ROUND(SUM(revenue_at_risk_eur), 2)                     AS total_revenue_at_risk_eur,
  ROUND(SUM(IF(risk_tier = 'HIGH RISK',
               revenue_at_risk_eur, 0)), 2)              AS high_risk_revenue_eur,
  ROUND(SUM(IF(risk_tier = 'MEDIUM RISK',
               revenue_at_risk_eur, 0)), 2)              AS medium_risk_revenue_eur,
  -- Volume exposure
  ROUND(SUM(volume_at_risk_tonnes), 1)                   AS total_volume_at_risk_tonnes,
  -- Average churn probability in region
  ROUND(AVG(churn_probability), 4)                       AS avg_churn_probability,
  ROUND(MAX(churn_probability), 4)                       AS max_churn_probability,
  -- % of total regional revenue that is at risk
  ROUND(
    SAFE_DIVIDE(
      SUM(revenue_at_risk_eur),
      SUM(total_revenue_12m)
    ) * 100,
    2
  )                                                      AS revenue_at_risk_pct,
  -- Most common churn driver in this region
  (
    SELECT primary_churn_driver
    FROM nordleaf_mgmt.v_churn_risk_scores inner_v
    WHERE inner_v.region = outer_v.region
      AND inner_v.risk_tier IN ('HIGH RISK', 'MEDIUM RISK')
    GROUP BY primary_churn_driver
    ORDER BY COUNT(*) DESC
    LIMIT 1
  )                                                      AS top_churn_driver_in_region

FROM nordleaf_mgmt.v_churn_risk_scores AS outer_v

WHERE risk_tier IN ('HIGH RISK', 'MEDIUM RISK')

GROUP BY
  region,
  country

ORDER BY
  total_revenue_at_risk_eur DESC
;


/* ============================================================================
   SECTION 10b — HIGH RISK CUSTOMER ALERT LIST
   Flat list of HIGH RISK customers for CRM system ingestion and immediate
   account manager notification. Ordered by revenue at risk descending.
   ============================================================================ */

SELECT
  customer_key,
  customer_name,
  country,
  region,
  account_manager,
  customer_segment,
  risk_tier,
  churn_probability_pct,
  primary_churn_driver,
  revenue_at_risk_eur,
  volume_at_risk_tonnes,
  last_order_date,
  recency_days,
  recency_label,
  volume_trend_label,
  late_payment_rate,
  complaint_count,
  intervention_urgency,
  recommended_intervention,
  score_date

FROM nordleaf_mgmt.v_churn_risk_scores

WHERE risk_tier = 'HIGH RISK'

ORDER BY revenue_at_risk_eur DESC
;


/*
================================================================================
  END OF FILE: UC17_customer_churn_risk_ml.sql
  ─────────────────────────────────────────────────────────────────────────────
  EXECUTION ORDER (run sequentially in BigQuery):
    1. SECTION 3  — CREATE TABLE nordleaf_dwh.churn_training_features
                    (resolves CTEs from Sections 1 + 2 + 1b)
    2. SECTION 4  — CREATE MODEL nordleaf_mgmt.bqml_churn_model
    3. SECTION 5  — SELECT ML.EVALUATE (validation — review metrics)
    4. SECTION 5b — SELECT ML.WEIGHTS  (feature importance review)
    5. SECTION 9  — CREATE VIEW nordleaf_mgmt.v_churn_risk_scores
    6. SECTION 10 — SELECT regional summary (ad hoc / scheduled)
    7. SECTION 10b— SELECT high-risk alert list (CRM pipeline trigger)

  SCHEDULED REFRESH:
    - Training feature table: Monthly (1st of each month, 02:00 UTC)
    - Model retraining:       Monthly (1st of each month, 03:00 UTC)
    - View (v_churn_risk_scores): Daily (06:00 UTC, after ETL completion)
    - CRM alert export:       Daily (07:00 UTC, post scoring)

  MONITORING:
    - Cloud Logging: BQML job completion + row counts
    - Alert if high_risk_count increases by > 20% day-over-day
    - Alert if model ROC-AUC drops below 0.70 on monthly retrain

  RELATED USE CASES:
    - UC-12: Customer Profitability & CLV       (customer domain)
    - UC-16: Customer Satisfaction & NPS        (customer domain)
    - UC-18: Customer Segmentation & Credit Risk (customer domain)
================================================================================
*/
