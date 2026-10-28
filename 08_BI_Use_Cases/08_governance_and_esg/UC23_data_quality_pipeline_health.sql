-- ============================================================================================================================
-- FILE        : UC23_data_quality_pipeline_health.sql
-- USE CASE    : UC-23  |  Governance Domain  |  Priority: P2
-- PROJECT     : NordLeaf BI Implementation — Finnish Paper Manufacturer
-- LAYER       : MGMT (monitoring layer — reads DWH + MGMT control tables)
-- DATASET     : nordleaf_mgmt (source: dq_results, pipeline_log), nordleaf_dwh (freshness checks)
-- AUTHOR      : NordLeaf Data Engineering Team
-- CREATED     : 2026-02-23
-- LAST UPDATE : 2026-02-23
-- ============================================================================================================================
--
-- ARCHITECTURE DIAGRAM
-- ============================================================================================================================
--
--   Orchestration                  Data Platform                  Monitoring Layer
--   ────────────────────           ──────────────────────────     ──────────────────────────────────
--
--   Cloud Composer DAG ──────────► nordleaf_dwh Tables ──────────► DQ Check Queries
--         │                               │                               │
--         │                               │                               ▼
--         │                        Dataplex Auto DQ ──────────► nordleaf_mgmt.dq_results
--         │                                                               │
--         └────────────────────────────────────────────────────► nordleaf_mgmt.pipeline_log
--                                                                         │
--                                                     ┌───────────────────┘
--                                                     ▼
--                                   nordleaf_mgmt.v_pipeline_health_dashboard
--                                                     │
--                                      ┌──────────────┴──────────────┐
--                                      ▼                             ▼
--                               Looker Dashboard            PagerDuty / Slack Alert
--                             (Data Engineering)            (CRITICAL pipelines)
--
-- ============================================================================================================================
--
-- BUSINESS PROBLEM
-- ============================================================================================================================
--
--   NordLeaf operates a 4-layer data platform (EXT → STG → DWH → MGMT) processing data from
--   5 mills (Tampere, Oulu, Turku, Espoo, Jyväskylä), 18 customers, and 12 suppliers daily.
--   Multiple Cloud Composer DAGs ingest, transform, and load fact/dimension tables overnight.
--
--   Key pain points:
--     • Silent data failures: pipelines "succeed" but rows are dropped or corrupted; BI reports
--       show wrong numbers the next morning and nobody knows until a business user calls.
--     • No single view to answer: "Is today's data good?" — the data engineering team checks
--       3 separate dashboards and several Slack channels to assess platform health.
--     • DQ rules are defined in Dataplex but results are not surfaced in a human-readable KPI view.
--     • Pipeline SLA breaches (e.g., fact_orders not loaded by 06:00 CET) are detected reactively.
--
--   This UC delivers:
--     1. Pipeline execution health summary per DAG/job (30-day window)
--     2. Pipeline status tiers: HEALTHY / DEGRADED / AT RISK / CRITICAL
--     3. Data freshness check — hours since last record for each core table
--     4. DQ rule pass/fail summary from dq_results (7-day window)
--     5. Per-table DQ score with dimension breakdown (Completeness, Uniqueness, Validity, Freshness)
--     6. WoW DQ trend to detect quality degradation early
--     7. Failed rules with business impact classification
--     8. INFORMATION_SCHEMA anomaly detection (table size drop = data loss alert)
--     9. Composite health dashboard — one-stop view for the data engineering team
--    10. Persistent MGMT view for Looker
--
-- ============================================================================================================================
--
-- DQ DIMENSIONS TRACKED
-- ============================================================================================================================
--
--   Completeness       — Are all expected rows/columns present? No unexpected NULLs?
--   Uniqueness         — No duplicate primary keys or business keys?
--   Validity           — Values within expected domain / range / format?
--   Freshness          — Data loaded within the SLA window (< 6 hours)?
--   Referential Integrity — FK relationships intact between fact and dimension tables?
--
-- ============================================================================================================================
--
-- KPI TARGETS
-- ============================================================================================================================
--
--   | KPI                        | Target       | Alert Threshold  |
--   |----------------------------|--------------|------------------|
--   | Overall DQ Score           | > 95%        | < 90%            |
--   | Pipeline Success Rate      | > 98%        | < 90%            |
--   | Data Freshness             | < 6 hours    | > 12 hours       |
--   | Completeness Score         | > 99%        | < 95%            |
--   | Uniqueness Score           | > 99.9%      | < 99%            |
--   | Validity Score             | > 98%        | < 95%            |
--   | Referential Integrity      | 100%         | < 99%            |
--
-- ============================================================================================================================
--
-- IMPLEMENTATION STEPS
-- ============================================================================================================================
--
--   Step  1 : Pull 30-day pipeline execution history from nordleaf_mgmt.pipeline_log
--   Step  2 : Compute per-pipeline success rate, avg runtime, days since last success
--   Step  3 : Classify each pipeline into HEALTHY / DEGRADED / AT RISK / CRITICAL tier
--   Step  4 : Query INFORMATION_SCHEMA.TABLES for each core table to check row-count anomalies
--   Step  5 : Check data freshness by querying MAX(created_at) on each core DWH table
--   Step  6 : Classify freshness as FRESH / STALE / VERY STALE / NO DATA TODAY
--   Step  7 : Summarise DQ rule results from nordleaf_mgmt.dq_results (7-day window)
--   Step  8 : Compute per-table DQ score and dimension-level breakdown
--   Step  9 : Compute WoW DQ score trend using LAG() over weekly aggregation
--   Step 10 : List all FAIL rules with row impact and business impact classification
--   Step 11 : Join all sections into composite pipeline health dashboard CTE
--   Step 12 : Materialise as nordleaf_mgmt.v_pipeline_health_dashboard view
--
-- ============================================================================================================================


-- ============================================================================================================================
-- SECTION 1 — PIPELINE EXECUTION HEALTH (from nordleaf_mgmt.pipeline_log)
-- ============================================================================================================================
--
--   Source  : nordleaf_mgmt.pipeline_log
--   Grain   : One row per pipeline run
--   Scope   : Rolling 30 days
--   Fields  : pipeline_name, run_date, start_time, end_time, status,
--             rows_processed, rows_failed, error_message, triggered_by

WITH pipeline_health AS (
  SELECT
    pl.pipeline_name,
    pl.run_date,
    pl.start_time,
    pl.end_time,
    pl.status,                                -- 'SUCCESS', 'FAILED', 'RUNNING', 'SKIPPED'
    pl.rows_processed,
    pl.rows_failed,
    pl.layer,                                 -- 'EXT', 'STG', 'DWH', 'MGMT'
    pl.target_table,
    TIMESTAMP_DIFF(pl.end_time, pl.start_time, MINUTE)    AS runtime_minutes,
    pl.error_message,
    pl.triggered_by,                          -- 'COMPOSER', 'MANUAL', 'API'

    -- Row failure rate for this run
    ROUND(SAFE_DIVIDE(pl.rows_failed, pl.rows_processed) * 100, 2) AS row_failure_rate_pct,

    -- Was this run within SLA? (assumption: pipelines should complete within 90 minutes)
    IF(TIMESTAMP_DIFF(pl.end_time, pl.start_time, MINUTE) <= 90
       AND pl.status = 'SUCCESS', TRUE, FALSE)             AS within_sla

  FROM nordleaf_mgmt.pipeline_log pl
  WHERE pl.run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
),

-- ============================================================================================================================
-- SECTION 2 — PIPELINE STATISTICS (aggregated per pipeline, rolling 30 days)
-- ============================================================================================================================
--
--   Computes summary statistics per pipeline name.
--   days_since_last_success drives the health tier classification in Section 3.

pipeline_stats AS (
  SELECT
    pipeline_name,
    layer,
    target_table,
    COUNT(*)                                                     AS total_runs,
    COUNTIF(status = 'SUCCESS')                                  AS successful_runs,
    COUNTIF(status = 'FAILED')                                   AS failed_runs,
    COUNTIF(status = 'SKIPPED')                                  AS skipped_runs,
    COUNTIF(status = 'RUNNING')                                  AS currently_running,

    -- Success rate percentage (primary health KPI)
    ROUND(SAFE_DIVIDE(COUNTIF(status = 'SUCCESS'), COUNT(*)) * 100, 2) AS success_rate_pct,

    -- SLA compliance rate
    ROUND(SAFE_DIVIDE(COUNTIF(within_sla = TRUE), COUNTIF(status = 'SUCCESS')) * 100, 2)
                                                                 AS sla_compliance_pct,

    -- Runtime statistics (minutes)
    ROUND(AVG(runtime_minutes), 1)                               AS avg_runtime_min,
    MAX(runtime_minutes)                                         AS max_runtime_min,
    MIN(IF(status = 'SUCCESS', runtime_minutes, NULL))           AS min_successful_runtime_min,

    -- Volume statistics
    ROUND(AVG(rows_processed), 0)                                AS avg_rows_processed,
    MAX(rows_processed)                                          AS max_rows_processed,
    ROUND(AVG(row_failure_rate_pct), 2)                          AS avg_row_failure_rate_pct,

    -- Last run metadata
    MAX(run_date)                                                AS last_run_date,
    MAX(IF(status = 'SUCCESS', run_date, NULL))                  AS last_successful_run_date,

    -- Days since last success (0 = ran successfully today)
    DATE_DIFF(CURRENT_DATE(), MAX(IF(status = 'SUCCESS', run_date, NULL)), DAY)
                                                                 AS days_since_last_success,

    -- Most recent error message (for FAILED pipelines)
    STRING_AGG(IF(status = 'FAILED', error_message, NULL)
               ORDER BY run_date DESC LIMIT 1)                   AS latest_error_message

  FROM pipeline_health
  GROUP BY pipeline_name, layer, target_table
),

-- ============================================================================================================================
-- SECTION 3 — PIPELINE STATUS TIERS
-- ============================================================================================================================
--
--   Tier logic (combines success rate + recency of last successful run):
--
--     HEALTHY   — success_rate >= 98% AND ran successfully today (days_since_last_success = 0)
--     DEGRADED  — success_rate >= 90% AND last success within 1 day
--     AT RISK   — success_rate >= 75% AND last success within 2 days
--     CRITICAL  — success_rate <  75% OR last success > 2 days ago
--
--   Pipelines in CRITICAL state trigger PagerDuty alert (external to SQL).

pipeline_with_tier AS (
  SELECT
    ps.*,
    CASE
      WHEN ps.success_rate_pct >= 98
           AND ps.days_since_last_success = 0                    THEN 'HEALTHY'
      WHEN ps.success_rate_pct >= 90
           AND ps.days_since_last_success <= 1                   THEN 'DEGRADED'
      WHEN ps.success_rate_pct >= 75
           AND ps.days_since_last_success <= 2                   THEN 'AT RISK'
      ELSE                                                            'CRITICAL'
    END                                                          AS pipeline_health_tier,

    -- Priority score for dashboard sorting (CRITICAL = 4 sorts first)
    CASE
      WHEN ps.success_rate_pct >= 98 AND ps.days_since_last_success = 0  THEN 1
      WHEN ps.success_rate_pct >= 90 AND ps.days_since_last_success <= 1 THEN 2
      WHEN ps.success_rate_pct >= 75 AND ps.days_since_last_success <= 2 THEN 3
      ELSE                                                                     4
    END                                                          AS health_priority_score,

    -- Human-readable KPI status for dashboard badge
    CASE
      WHEN ps.success_rate_pct >= 98                             THEN 'SUCCESS RATE: ON TARGET'
      WHEN ps.success_rate_pct >= 90                             THEN 'SUCCESS RATE: MONITOR'
      ELSE                                                            'SUCCESS RATE: ALERT'
    END                                                          AS success_rate_kpi_status,

    CASE
      WHEN ps.sla_compliance_pct >= 95                           THEN 'SLA: ON TARGET'
      WHEN ps.sla_compliance_pct >= 80                           THEN 'SLA: MONITOR'
      ELSE                                                            'SLA: BREACHED'
    END                                                          AS sla_kpi_status

  FROM pipeline_stats ps
),

-- ============================================================================================================================
-- SECTION 4 — INFORMATION_SCHEMA ANOMALY DETECTION (table row-count health check)
-- ============================================================================================================================
--
--   Uses INFORMATION_SCHEMA.TABLES to retrieve row counts for all core DWH tables.
--   A sudden drop in row_count vs. the expected baseline signals a data loss or failed load.
--   Note: INFORMATION_SCHEMA.TABLES row_count is approximate — use for anomaly detection only.
--   For exact counts, schedule a daily COUNT(*) job and store results in pipeline_log.

table_row_count_check AS (
  SELECT
    table_name,
    row_count,
    size_bytes,
    ROUND(size_bytes / 1024 / 1024, 2)                           AS size_mb,
    last_modified_time,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_time, HOUR) AS hours_since_last_modified,

    -- Flag tables that haven't been modified in > 25 hours (should update daily)
    CASE
      WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_time, HOUR) <= 6   THEN 'RECENTLY_UPDATED'
      WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_time, HOUR) <= 25  THEN 'WITHIN_DAILY_SLA'
      WHEN TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), last_modified_time, HOUR) <= 48  THEN 'DELAYED'
      ELSE                                                                            'STALE_TABLE'
    END                                                          AS table_update_status,

    -- Flag suspiciously small tables (row_count = 0 on a fact table = critical alert)
    CASE
      WHEN table_name LIKE 'fact_%' AND row_count = 0            THEN 'CRITICAL — ZERO ROWS IN FACT TABLE'
      WHEN table_name LIKE 'fact_%' AND row_count < 100          THEN 'WARNING — UNUSUALLY FEW ROWS'
      WHEN table_name LIKE 'dim_%'  AND row_count = 0            THEN 'CRITICAL — EMPTY DIMENSION TABLE'
      ELSE                                                            'ROW COUNT NORMAL'
    END                                                          AS row_count_anomaly_flag

  FROM nordleaf_dwh.INFORMATION_SCHEMA.TABLES
  WHERE table_name IN (
    'fact_orders', 'fact_production', 'fact_deliveries',
    'dim_customer', 'dim_product', 'dim_mill',
    'dim_date', 'dim_supplier'
  )
),

-- ============================================================================================================================
-- SECTION 5 — DATA FRESHNESS CHECKS ON CORE DWH TABLES
-- ============================================================================================================================
--
--   Queries each core table for MAX(created_at) and record count today.
--   Freshness SLA: NordLeaf requires all fact tables loaded by 06:00 CET (< 6 hours stale by 12:00).
--
--   Freshness Tiers:
--     FRESH      — hours_since_last_record < 2
--     STALE      — hours_since_last_record 2–6
--     VERY STALE — hours_since_last_record 6–24
--     NO DATA TODAY — > 24 hours or no records today at all

freshness_checks AS (
  SELECT
    'fact_orders'                                                AS table_name,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR)   AS hours_since_last_record,
    COUNT(*)                                                     AS records_today,
    MAX(created_at)                                              AS latest_record_timestamp
  FROM nordleaf_dwh.fact_orders
  WHERE DATE(created_at) = CURRENT_DATE()

  UNION ALL

  SELECT
    'fact_production',
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR),
    COUNT(*),
    MAX(created_at)
  FROM nordleaf_dwh.fact_production
  WHERE DATE(created_at) = CURRENT_DATE()

  UNION ALL

  SELECT
    'fact_deliveries',
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR),
    COUNT(*),
    MAX(created_at)
  FROM nordleaf_dwh.fact_deliveries
  WHERE DATE(created_at) = CURRENT_DATE()

  UNION ALL

  SELECT
    'dim_customer',
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR),
    COUNT(*),
    MAX(created_at)
  FROM nordleaf_dwh.dim_customer
  WHERE DATE(created_at) = CURRENT_DATE()

  UNION ALL

  SELECT
    'dim_product',
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR),
    COUNT(*),
    MAX(created_at)
  FROM nordleaf_dwh.dim_product
  WHERE DATE(created_at) = CURRENT_DATE()

  UNION ALL

  SELECT
    'dim_mill',
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR),
    COUNT(*),
    MAX(created_at)
  FROM nordleaf_dwh.dim_mill
  WHERE DATE(created_at) = CURRENT_DATE()

  UNION ALL

  SELECT
    'dim_supplier',
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR),
    COUNT(*),
    MAX(created_at)
  FROM nordleaf_dwh.dim_supplier
  WHERE DATE(created_at) = CURRENT_DATE()
),

freshness_with_status AS (
  SELECT
    fc.table_name,
    fc.latest_record_timestamp,
    fc.hours_since_last_record,
    fc.records_today,

    -- Freshness tier classification
    CASE
      WHEN fc.records_today = 0 OR fc.hours_since_last_record IS NULL  THEN 'NO DATA TODAY'
      WHEN fc.hours_since_last_record < 2                              THEN 'FRESH'
      WHEN fc.hours_since_last_record < 6                              THEN 'STALE'
      WHEN fc.hours_since_last_record < 24                             THEN 'VERY STALE'
      ELSE                                                                  'NO DATA TODAY'
    END                                                          AS freshness_tier,

    -- Freshness KPI flag (SLA = < 6 hours)
    CASE
      WHEN fc.hours_since_last_record <= 6                             THEN 'FRESHNESS: ON TARGET'
      WHEN fc.hours_since_last_record <= 12                            THEN 'FRESHNESS: MONITOR'
      ELSE                                                                  'FRESHNESS: SLA BREACH'
    END                                                          AS freshness_kpi_status,

    -- Is this a critical fact table? Affects alert priority.
    IF(fc.table_name LIKE 'fact_%', TRUE, FALSE)                 AS is_fact_table

  FROM freshness_checks fc
),

-- ============================================================================================================================
-- SECTION 6 — DQ RULE RESULTS FROM nordleaf_mgmt.dq_results (7-day window)
-- ============================================================================================================================
--
--   Source  : nordleaf_mgmt.dq_results
--   Grain   : One row per DQ rule execution per check_date
--   Scope   : Rolling 7 days
--   Fields  : rule_name, table_name, column_name, dimension, check_date,
--             passed_rows, failed_rows, total_rows

dq_results_summary AS (
  SELECT
    dqr.rule_name,
    dqr.table_name,
    dqr.column_name,
    dqr.dimension,                            -- Completeness, Uniqueness, Validity, Freshness, Referential_Integrity
    dqr.check_date,
    dqr.passed_rows,
    dqr.failed_rows,
    dqr.total_rows,

    -- Pass rate for this rule on this check date
    ROUND(SAFE_DIVIDE(dqr.passed_rows, dqr.total_rows) * 100, 4) AS pass_rate_pct,

    -- Binary PASS / FAIL (threshold: 99% pass rate)
    IF(SAFE_DIVIDE(dqr.passed_rows, dqr.total_rows) * 100 >= 99, 'PASS', 'FAIL') AS result,

    -- Severity of failure (how many rows affected)
    CASE
      WHEN SAFE_DIVIDE(dqr.failed_rows, dqr.total_rows) = 0           THEN 'CLEAN'
      WHEN SAFE_DIVIDE(dqr.failed_rows, dqr.total_rows) < 0.001       THEN 'MINOR'
      WHEN SAFE_DIVIDE(dqr.failed_rows, dqr.total_rows) < 0.01        THEN 'MODERATE'
      WHEN SAFE_DIVIDE(dqr.failed_rows, dqr.total_rows) < 0.05        THEN 'SIGNIFICANT'
      ELSE                                                                  'SEVERE'
    END                                                          AS failure_severity

  FROM nordleaf_mgmt.dq_results dqr
  WHERE dqr.check_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
),

-- ============================================================================================================================
-- SECTION 7 — DQ SCORE PER TABLE (weighted composite with dimension breakdown)
-- ============================================================================================================================
--
--   Computes one DQ score row per table by averaging pass rates across all rules.
--   Dimension breakdown lets DE team see which dimension is causing failures.
--   Critical tables (fact_orders, fact_production) classified as HIGH IMPACT.

dq_score_by_table AS (
  SELECT
    table_name,
    COUNT(DISTINCT rule_name)                                    AS total_rules,
    COUNTIF(result = 'PASS')                                     AS passed_rules,
    COUNTIF(result = 'FAIL')                                     AS failed_rules,

    -- Overall DQ score: average pass rate across all rules for this table
    ROUND(AVG(pass_rate_pct), 2)                                 AS overall_dq_score_pct,

    -- Dimension-level scores (NULL if no rules for that dimension on this table)
    ROUND(AVG(IF(dimension = 'Completeness',        pass_rate_pct, NULL)), 2) AS completeness_score_pct,
    ROUND(AVG(IF(dimension = 'Uniqueness',          pass_rate_pct, NULL)), 2) AS uniqueness_score_pct,
    ROUND(AVG(IF(dimension = 'Validity',            pass_rate_pct, NULL)), 2) AS validity_score_pct,
    ROUND(AVG(IF(dimension = 'Freshness',           pass_rate_pct, NULL)), 2) AS freshness_dq_score_pct,
    ROUND(AVG(IF(dimension = 'Referential_Integrity', pass_rate_pct, NULL)), 2) AS ref_integrity_score_pct,

    -- Total rows affected by failures
    SUM(failed_rows)                                             AS total_failed_rows,
    SUM(total_rows)                                              AS total_rows_checked,

    -- Business impact classification
    CASE
      WHEN table_name IN ('fact_orders', 'fact_production')      THEN 'HIGH IMPACT — Core Revenue'
      WHEN table_name IN ('fact_deliveries')                     THEN 'HIGH IMPACT — Supply Chain'
      WHEN table_name IN ('dim_customer', 'dim_product')         THEN 'MEDIUM IMPACT — Master Data'
      WHEN table_name IN ('dim_mill', 'dim_supplier')            THEN 'MEDIUM IMPACT — Reference Data'
      ELSE                                                            'LOW IMPACT'
    END                                                          AS business_impact,

    -- DQ score health flag
    CASE
      WHEN AVG(pass_rate_pct) >= 99                              THEN 'EXCELLENT'
      WHEN AVG(pass_rate_pct) >= 95                              THEN 'GOOD — ON TARGET'
      WHEN AVG(pass_rate_pct) >= 90                              THEN 'DEGRADED — MONITOR'
      ELSE                                                            'CRITICAL — ALERT'
    END                                                          AS dq_health_flag,

    MAX(check_date)                                              AS last_dq_check_date

  FROM dq_results_summary
  GROUP BY table_name
),

-- ============================================================================================================================
-- SECTION 8 — DQ TREND (WoW change — is quality improving or degrading?)
-- ============================================================================================================================
--
--   Aggregates DQ scores at weekly granularity and computes WoW change using LAG().
--   Alert threshold: > 2% drop WoW triggers a data quality incident.
--   Note: 7-day rolling window only covers 1 full week + current partial week.
--   For robust trend analysis, extend the dq_results query to 30+ days.

dq_weekly_trend AS (
  SELECT
    DATE_TRUNC(check_date, WEEK(MONDAY))                         AS week_start,
    table_name,
    ROUND(AVG(pass_rate_pct), 2)                                 AS weekly_dq_score_pct,
    COUNT(DISTINCT rule_name)                                    AS rules_evaluated,
    COUNTIF(result = 'FAIL')                                     AS failed_rules_this_week
  FROM dq_results_summary
  GROUP BY week_start, table_name
),

dq_trend_with_wow AS (
  SELECT
    week_start,
    table_name,
    weekly_dq_score_pct,
    rules_evaluated,
    failed_rules_this_week,

    -- Previous week score for WoW comparison
    LAG(weekly_dq_score_pct) OVER (
      PARTITION BY table_name
      ORDER BY week_start)                                       AS prev_week_dq_score_pct,

    -- WoW absolute change (negative = degradation)
    ROUND(weekly_dq_score_pct
          - LAG(weekly_dq_score_pct) OVER (
              PARTITION BY table_name
              ORDER BY week_start), 2)                           AS wow_dq_change_pct,

    -- Alert: flag if WoW drop > 2 percentage points
    CASE
      WHEN weekly_dq_score_pct
           - LAG(weekly_dq_score_pct) OVER (
               PARTITION BY table_name
               ORDER BY week_start) < -2.0                      THEN 'DQ DEGRADATION ALERT'
      WHEN weekly_dq_score_pct
           - LAG(weekly_dq_score_pct) OVER (
               PARTITION BY table_name
               ORDER BY week_start) < -0.5                      THEN 'DQ SLIGHT DECLINE'
      WHEN weekly_dq_score_pct
           - LAG(weekly_dq_score_pct) OVER (
               PARTITION BY table_name
               ORDER BY week_start) IS NULL                     THEN 'NO PRIOR WEEK'
      ELSE                                                           'DQ STABLE OR IMPROVING'
    END                                                          AS dq_trend_flag

  FROM dq_weekly_trend
),

-- ============================================================================================================================
-- SECTION 9 — FAILED DQ RULES WITH IMPACT ASSESSMENT
-- ============================================================================================================================
--
--   Lists all FAIL rules from the 7-day window with:
--     - rows affected and percentage of table
--     - failure severity
--     - business impact classification (critical tables = HIGH IMPACT)
--   This section feeds the daily DE stand-up report and incident tickets.

failed_dq_rules AS (
  SELECT
    drs.rule_name,
    drs.table_name,
    drs.column_name,
    drs.dimension,
    drs.check_date,
    drs.failed_rows,
    drs.total_rows,
    drs.pass_rate_pct,
    drs.failure_severity,

    -- Failed rows as a percentage of the table
    ROUND(SAFE_DIVIDE(drs.failed_rows, drs.total_rows) * 100, 4)  AS failed_rows_pct,

    -- Business impact: critical tables touching revenue/production = HIGH
    CASE
      WHEN drs.table_name IN ('fact_orders', 'fact_production')
           AND drs.dimension IN ('Uniqueness', 'Referential_Integrity')  THEN 'HIGH IMPACT — Potential Duplicate / Orphan Records'
      WHEN drs.table_name IN ('fact_orders', 'fact_production')
           AND drs.dimension = 'Completeness'                            THEN 'HIGH IMPACT — Missing Revenue Records'
      WHEN drs.table_name IN ('fact_deliveries')                         THEN 'HIGH IMPACT — Supply Chain Integrity'
      WHEN drs.table_name IN ('dim_customer', 'dim_product')
           AND drs.dimension = 'Uniqueness'                              THEN 'MEDIUM IMPACT — Master Data Duplicates'
      WHEN drs.table_name IN ('dim_mill', 'dim_supplier')                THEN 'MEDIUM IMPACT — Reference Data'
      ELSE                                                                    'LOW IMPACT'
    END                                                          AS business_impact_detail,

    -- Recommended action
    CASE
      WHEN drs.failure_severity IN ('SEVERE', 'SIGNIFICANT')             THEN 'IMMEDIATE — Raise P1 incident, halt downstream pipelines'
      WHEN drs.failure_severity = 'MODERATE'                             THEN 'URGENT — Investigate within 4 hours, notify data owner'
      WHEN drs.failure_severity = 'MINOR'                                THEN 'MONITOR — Log ticket, review at next stand-up'
      ELSE                                                                    'INFORMATIONAL — No action required'
    END                                                          AS recommended_action

  FROM dq_results_summary drs
  WHERE drs.result = 'FAIL'
  ORDER BY drs.check_date DESC, drs.failed_rows DESC
),

-- ============================================================================================================================
-- SECTION 10 — COMPOSITE HEALTH DASHBOARD QUERY
-- ============================================================================================================================
--
--   Joins pipeline health tier, freshness status, and DQ score into a single summary.
--   One row per core table — provides the top-level traffic-light view for the
--   data engineering team's morning dashboard.

composite_health_dashboard AS (
  SELECT
    -- Identifiers
    COALESCE(pt.target_table, fw.table_name, ds.table_name)      AS table_name,

    -- Pipeline health
    pt.pipeline_name,
    pt.pipeline_health_tier,
    pt.success_rate_pct,
    pt.days_since_last_success,
    pt.avg_runtime_min,
    pt.last_successful_run_date,
    pt.latest_error_message,
    pt.health_priority_score,

    -- Freshness
    fw.freshness_tier,
    fw.hours_since_last_record,
    fw.records_today,
    fw.freshness_kpi_status,

    -- DQ score
    ds.overall_dq_score_pct,
    ds.completeness_score_pct,
    ds.uniqueness_score_pct,
    ds.validity_score_pct,
    ds.ref_integrity_score_pct,
    ds.failed_rules                                              AS dq_failed_rules_7d,
    ds.total_failed_rows                                         AS dq_failed_rows_7d,
    ds.business_impact,
    ds.dq_health_flag,

    -- Overall composite score (average of pipeline success, freshness OK, DQ score)
    -- Pipeline: 0–100 (success_rate_pct)
    -- Freshness: FRESH=100, STALE=75, VERY STALE=50, NO DATA=0
    -- DQ: overall_dq_score_pct
    ROUND((
      COALESCE(pt.success_rate_pct, 0) +
      CASE fw.freshness_tier
        WHEN 'FRESH'         THEN 100
        WHEN 'STALE'         THEN 75
        WHEN 'VERY STALE'    THEN 50
        ELSE                      0
      END +
      COALESCE(ds.overall_dq_score_pct, 0)
    ) / 3.0, 2)                                                  AS composite_health_score,

    -- Top-level RAG status
    CASE
      WHEN pt.pipeline_health_tier = 'CRITICAL'
           OR fw.freshness_tier IN ('NO DATA TODAY', 'VERY STALE')
           OR ds.dq_health_flag = 'CRITICAL — ALERT'             THEN 'RED'
      WHEN pt.pipeline_health_tier IN ('AT RISK', 'DEGRADED')
           OR fw.freshness_tier = 'STALE'
           OR ds.dq_health_flag = 'DEGRADED — MONITOR'           THEN 'AMBER'
      ELSE                                                            'GREEN'
    END                                                          AS rag_status,

    CURRENT_TIMESTAMP()                                          AS dashboard_refreshed_at

  FROM pipeline_with_tier      pt
  FULL OUTER JOIN freshness_with_status fw ON pt.target_table = fw.table_name
  FULL OUTER JOIN dq_score_by_table     ds ON pt.target_table = ds.table_name
)

-- ============================================================================================================================
-- FINAL SELECT — Composite Health Dashboard (primary output, sorted by priority)
-- ============================================================================================================================

SELECT * FROM composite_health_dashboard
ORDER BY health_priority_score DESC, composite_health_score ASC;

-- ============================================================================================================================
-- SUPPLEMENTAL QUERY A — Pipeline Execution History (last 30 days, all runs)
-- ============================================================================================================================

-- SELECT
--   pipeline_name, run_date, status, runtime_minutes,
--   rows_processed, rows_failed, row_failure_rate_pct, within_sla, triggered_by
-- FROM pipeline_health
-- ORDER BY run_date DESC, pipeline_name;

-- ============================================================================================================================
-- SUPPLEMENTAL QUERY B — Failed DQ Rules with Impact (DE stand-up report)
-- ============================================================================================================================

-- SELECT
--   rule_name, table_name, column_name, dimension, check_date,
--   failed_rows, failed_rows_pct, failure_severity,
--   business_impact_detail, recommended_action
-- FROM failed_dq_rules
-- ORDER BY check_date DESC, failed_rows DESC;

-- ============================================================================================================================
-- SUPPLEMENTAL QUERY C — WoW DQ Trend (degradation detection)
-- ============================================================================================================================

-- SELECT
--   table_name, week_start, weekly_dq_score_pct,
--   prev_week_dq_score_pct, wow_dq_change_pct, dq_trend_flag
-- FROM dq_trend_with_wow
-- WHERE dq_trend_flag != 'NO PRIOR WEEK'
-- ORDER BY table_name, week_start DESC;

-- ============================================================================================================================
-- SUPPLEMENTAL QUERY D — Table Anomaly Detection via INFORMATION_SCHEMA
-- ============================================================================================================================

-- SELECT
--   table_name, row_count, size_mb, hours_since_last_modified,
--   table_update_status, row_count_anomaly_flag
-- FROM table_row_count_check
-- ORDER BY table_name;

-- ============================================================================================================================
-- SECTION 10 — MGMT VIEW DEFINITION
-- ============================================================================================================================
--
--   Persistent view in nordleaf_mgmt for Looker Data Engineering Health Dashboard.
--   Consolidates pipeline health, freshness, and DQ scores into one authoritative view.
--   Refreshed on every query (BigQuery view, not materialised table).
--   For performance at scale, consider scheduling as a daily CTAS into a physical table.

CREATE OR REPLACE VIEW nordleaf_mgmt.v_pipeline_health_dashboard AS
WITH
-- Pipeline stats: 30-day rolling window
pl_stats AS (
  SELECT
    pl.pipeline_name,
    pl.layer,
    pl.target_table,
    COUNT(*)                                                              AS total_runs,
    COUNTIF(pl.status = 'SUCCESS')                                        AS successful_runs,
    COUNTIF(pl.status = 'FAILED')                                         AS failed_runs,
    ROUND(SAFE_DIVIDE(COUNTIF(pl.status = 'SUCCESS'), COUNT(*)) * 100, 2) AS success_rate_pct,
    ROUND(AVG(TIMESTAMP_DIFF(pl.end_time, pl.start_time, MINUTE)), 1)     AS avg_runtime_min,
    DATE_DIFF(CURRENT_DATE(),
              MAX(IF(pl.status = 'SUCCESS', pl.run_date, NULL)), DAY)     AS days_since_last_success,
    MAX(IF(pl.status = 'SUCCESS', pl.run_date, NULL))                     AS last_successful_run_date,
    STRING_AGG(IF(pl.status = 'FAILED', pl.error_message, NULL)
               ORDER BY pl.run_date DESC LIMIT 1)                         AS latest_error_message
  FROM nordleaf_mgmt.pipeline_log pl
  WHERE pl.run_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
  GROUP BY pl.pipeline_name, pl.layer, pl.target_table
),
-- Pipeline tier classification
pl_tier AS (
  SELECT
    ps.*,
    CASE
      WHEN ps.success_rate_pct >= 98 AND ps.days_since_last_success = 0  THEN 'HEALTHY'
      WHEN ps.success_rate_pct >= 90 AND ps.days_since_last_success <= 1 THEN 'DEGRADED'
      WHEN ps.success_rate_pct >= 75 AND ps.days_since_last_success <= 2 THEN 'AT RISK'
      ELSE                                                                     'CRITICAL'
    END                                                                   AS pipeline_health_tier
  FROM pl_stats ps
),
-- DQ scores: 7-day rolling window, per table
dq_scores AS (
  SELECT
    dqr.table_name,
    ROUND(AVG(SAFE_DIVIDE(dqr.passed_rows, dqr.total_rows) * 100), 2)    AS overall_dq_score_pct,
    ROUND(AVG(IF(dqr.dimension = 'Completeness',
                 SAFE_DIVIDE(dqr.passed_rows, dqr.total_rows) * 100, NULL)), 2) AS completeness_score_pct,
    ROUND(AVG(IF(dqr.dimension = 'Uniqueness',
                 SAFE_DIVIDE(dqr.passed_rows, dqr.total_rows) * 100, NULL)), 2) AS uniqueness_score_pct,
    ROUND(AVG(IF(dqr.dimension = 'Validity',
                 SAFE_DIVIDE(dqr.passed_rows, dqr.total_rows) * 100, NULL)), 2) AS validity_score_pct,
    ROUND(AVG(IF(dqr.dimension = 'Referential_Integrity',
                 SAFE_DIVIDE(dqr.passed_rows, dqr.total_rows) * 100, NULL)), 2) AS ref_integrity_score_pct,
    COUNTIF(SAFE_DIVIDE(dqr.passed_rows, dqr.total_rows) * 100 < 99)     AS failed_rules_7d,
    SUM(dqr.failed_rows)                                                  AS total_failed_rows_7d,
    MAX(dqr.check_date)                                                   AS last_dq_check_date
  FROM nordleaf_mgmt.dq_results dqr
  WHERE dqr.check_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
  GROUP BY dqr.table_name
),
-- Freshness: latest record timestamp per table (fact tables only — most critical)
freshness AS (
  SELECT 'fact_orders' AS table_name,
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR) AS hours_since_last_record
  FROM nordleaf_dwh.fact_orders WHERE DATE(created_at) = CURRENT_DATE()
  UNION ALL
  SELECT 'fact_production',
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR)
  FROM nordleaf_dwh.fact_production WHERE DATE(created_at) = CURRENT_DATE()
  UNION ALL
  SELECT 'fact_deliveries',
    TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR)
  FROM nordleaf_dwh.fact_deliveries WHERE DATE(created_at) = CURRENT_DATE()
)
-- Final composite view
SELECT
  COALESCE(pt.target_table, dq.table_name, fr.table_name)       AS table_name,
  pt.pipeline_name,
  pt.layer,
  pt.pipeline_health_tier,
  pt.success_rate_pct,
  pt.days_since_last_success,
  pt.avg_runtime_min,
  pt.last_successful_run_date,
  pt.latest_error_message,
  fr.hours_since_last_record,
  CASE
    WHEN fr.hours_since_last_record IS NULL                       THEN 'NO DATA TODAY'
    WHEN fr.hours_since_last_record < 2                           THEN 'FRESH'
    WHEN fr.hours_since_last_record < 6                           THEN 'STALE'
    WHEN fr.hours_since_last_record < 24                          THEN 'VERY STALE'
    ELSE                                                               'NO DATA TODAY'
  END                                                            AS freshness_tier,
  dq.overall_dq_score_pct,
  dq.completeness_score_pct,
  dq.uniqueness_score_pct,
  dq.validity_score_pct,
  dq.ref_integrity_score_pct,
  dq.failed_rules_7d,
  dq.total_failed_rows_7d,
  dq.last_dq_check_date,
  -- Overall RAG status
  CASE
    WHEN pt.pipeline_health_tier = 'CRITICAL'
         OR COALESCE(fr.hours_since_last_record, 999) >= 24
         OR COALESCE(dq.overall_dq_score_pct, 0) < 90            THEN 'RED'
    WHEN pt.pipeline_health_tier IN ('AT RISK', 'DEGRADED')
         OR COALESCE(fr.hours_since_last_record, 0) >= 6
         OR COALESCE(dq.overall_dq_score_pct, 100) < 95          THEN 'AMBER'
    ELSE                                                               'GREEN'
  END                                                            AS rag_status,
  CURRENT_TIMESTAMP()                                            AS view_refreshed_at
FROM pl_tier               pt
FULL OUTER JOIN dq_scores  dq ON pt.target_table      = dq.table_name
FULL OUTER JOIN freshness  fr ON COALESCE(pt.target_table, dq.table_name) = fr.table_name
ORDER BY
  CASE rag_status WHEN 'RED' THEN 1 WHEN 'AMBER' THEN 2 ELSE 3 END,
  table_name;

-- ============================================================================================================================
-- END OF FILE: UC23_data_quality_pipeline_health.sql
-- ============================================================================================================================
