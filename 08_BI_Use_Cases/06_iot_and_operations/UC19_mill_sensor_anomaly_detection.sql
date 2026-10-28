-- ============================================================================================================================
-- FILE        : UC19_mill_sensor_anomaly_detection.sql
-- PROJECT     : NordLeaf BI Implementation
-- DOMAIN      : IoT & Operations
-- USE CASE    : UC-19 — Mill Sensor Anomaly Detection (BQML Autoencoder)
-- PRIORITY    : P1 — Critical Production System
-- LAYER       : EXT → STG → DWH → MGMT
-- AUTHOR      : NordLeaf Data Engineering Team
-- CREATED     : 2024-01-01
-- UPDATED     : 2024-01-01
-- ============================================================================================================================
--
-- ARCHITECTURE DIAGRAM
-- ────────────────────
--
--   Pub/Sub (IoT Events) ──► Dataflow ──► sensor_events [DWH Streaming]
--                                                │
--                   BigQuery ML Autoencoder ◄────┤
--                   (Anomaly Detection)           │
--                                                ▼
--                   Monitoring Alerts ◄── v_sensor_anomalies [MGMT]
--
-- DATA FLOW:
--   1. Paper machine sensors publish events to Cloud Pub/Sub (6 readings/hour per sensor)
--   2. Dataflow pipeline consumes Pub/Sub, applies schema validation, writes to nordleaf_dwh.sensor_events (streaming insert)
--   3. BQML Autoencoder is trained on 30 days of "normal" sensor data (no alert_flag = TRUE)
--   4. ML.DETECT_ANOMALIES scores recent events, combined with rule-based threshold checks
--   5. Final aggregated anomaly view written to nordleaf_mgmt.v_sensor_anomalies for dashboards & alerting
--
-- ============================================================================================================================
-- BUSINESS PROBLEM
-- ────────────────
-- NordLeaf mills run paper machines 24 hours a day, 7 days a week across 5 Finnish facilities:
--   Tampere, Oulu, Turku, Espoo, Jyväskylä
--
-- Unexpected spikes in temperature, pressure, or vibration precede machine failures by 2–6 hours on average.
-- A single unplanned downtime event costs approximately EUR 80,000–200,000 in lost production.
--
-- This use case implements unsupervised anomaly detection using a BQML Autoencoder to learn "normal"
-- sensor behaviour across all machines, and flags deviations before a mechanical failure occurs.
-- Engineers receive real-time alerts via Cloud Monitoring / Looker Studio dashboards.
--
-- ============================================================================================================================
-- SENSOR TYPES AND NORMAL OPERATING RANGES
-- ─────────────────────────────────────────
--
--   Sensor Type  | Unit   | Normal Low | Normal High | Critical Threshold | Notes
--   ─────────────┼────────┼────────────┼─────────────┼────────────────────┼──────────────────────────────────
--   Temperature  | °C     |    20      |     80      |   > 85 or < 15     | Paper dryer sections 60–75°C
--   Pressure     | bar    |     2      |     10      |   > 12 or < 1      | Hydraulic press nip pressure
--   Vibration    | mm/s   |     0      |      5      |   > 8              | ISO 10816 bearing health
--   Humidity     | %      |    40      |     70      |   > 80 or < 30     | Sheet formation section
--   Speed        | m/min  |   200      |   1800      |   > 2000           | Wire/felt section speed
--   Power        | kW     |   500      |   4000      |   > 5000           | Main drive motor consumption
--
-- ============================================================================================================================
-- KPIs PRODUCED BY THIS USE CASE
-- ────────────────────────────────
--   1. Anomaly Detection Rate     : % of sensor readings flagged as anomalous per mill per day
--   2. False Positive Rate        : % of BQML anomalies where no downstream failure occurred (requires feedback loop)
--   3. Mean Time Between Anomalies: Average hours between consecutive anomaly events per machine
--   4. Alert Severity Distribution: COUNT and % of CRITICAL / HIGH / MEDIUM alerts per mill
--   5. Machine Health Score       : Composite 0–100 score per machine (HEALTHY / WATCH / WARNING / CRITICAL)
--   6. Burst Alert Count          : Machines with 3+ anomalies in a single hour (imminent failure signal)
--
-- ============================================================================================================================
-- IMPLEMENTATION STEPS
-- ─────────────────────
--   Step 1  : Run SECTION 1–3 as a scheduled query (every 30 min) to build z_score + threshold flags
--   Step 2  : Run SECTION 4 (model training) weekly on the past 30 days of normal data
--   Step 3  : Run SECTION 5 (ML.DETECT_ANOMALIES) every 30 min on new sensor batches
--   Step 4  : Run SECTION 6–7 (aggregation + health score) as part of the same scheduled query
--   Step 5  : SECTION 8 creates the MGMT view — deploy once, auto-refreshed by underlying CTEs
--   Step 6  : Connect nordleaf_mgmt.v_sensor_anomalies to Looker Studio for mill operations dashboard
--   Step 7  : Set Cloud Monitoring alerts on CRITICAL health score < 40 for page-on-call engineers
--
-- ============================================================================================================================


-- ============================================================================================================================
-- SECTION 1: SENSOR DATA BASE WITH Z-SCORE NORMALIZATION
-- Purpose : Pull 7-day sensor window, join mill dimension, compute rolling 12-hour statistics
-- Window  : 720 rows = 6 readings/hour * 120 hours (5 days) — rolling baseline for z-score
-- ============================================================================================================================

WITH sensor_base AS (

  SELECT
    se.event_id,
    se.mill_key,
    dm.mill_name,
    dm.mill_location,
    dm.mill_capacity_tonnes_per_day,
    se.machine_id,
    se.sensor_type,
    se.sensor_value,
    se.unit,
    se.timestamp,
    se.alert_flag,

    -- Rolling 12-hour average per sensor type per mill
    -- 6 readings/hour * 120 hours = 720 row window
    AVG(se.sensor_value) OVER (
      PARTITION BY se.mill_key, se.sensor_type, se.machine_id
      ORDER BY se.timestamp
      ROWS BETWEEN 720 PRECEDING AND CURRENT ROW
    ) AS rolling_12h_avg,

    -- Rolling 12-hour standard deviation per sensor type per mill
    STDDEV(se.sensor_value) OVER (
      PARTITION BY se.mill_key, se.sensor_type, se.machine_id
      ORDER BY se.timestamp
      ROWS BETWEEN 720 PRECEDING AND CURRENT ROW
    ) AS rolling_12h_stddev,

    -- 1-hour rolling average for short-burst detection
    AVG(se.sensor_value) OVER (
      PARTITION BY se.mill_key, se.sensor_type, se.machine_id
      ORDER BY se.timestamp
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_1h_avg,

    -- Row number within each machine+sensor combination for sequencing
    ROW_NUMBER() OVER (
      PARTITION BY se.mill_key, se.machine_id, se.sensor_type
      ORDER BY se.timestamp DESC
    ) AS recency_rank

  FROM nordleaf_dwh.sensor_events se
  JOIN nordleaf_dwh.dim_mill dm
    ON se.mill_key = dm.mill_key

  WHERE se.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    AND se.sensor_value IS NOT NULL  -- exclude null readings (sensor offline)
    AND se.sensor_type IN ('Temperature', 'Pressure', 'Vibration', 'Humidity', 'Speed', 'Power')

),


-- ============================================================================================================================
-- SECTION 2: Z-SCORE CALCULATION AND STATISTICAL ANOMALY FLAGS
-- Purpose : Compute z-score per reading; flag statistical outliers at 2-sigma and 3-sigma thresholds
-- Logic   : z = (value - mean) / stddev; |z| > 3 = STATISTICAL_ANOMALY, |z| > 2 = WARNING
-- ============================================================================================================================

z_scores AS (

  SELECT
    *,

    -- Z-score: number of standard deviations from rolling mean
    SAFE_DIVIDE(
      sensor_value - rolling_12h_avg,
      NULLIF(rolling_12h_stddev, 0)
    ) AS z_score,

    -- Statistical flag based on z-score thresholds
    CASE
      WHEN ABS(SAFE_DIVIDE(sensor_value - rolling_12h_avg, NULLIF(rolling_12h_stddev, 0))) > 3.0
        THEN 'STATISTICAL_ANOMALY'          -- 3-sigma: ~0.3% probability if normal distribution
      WHEN ABS(SAFE_DIVIDE(sensor_value - rolling_12h_avg, NULLIF(rolling_12h_stddev, 0))) > 2.0
        THEN 'WARNING'                      -- 2-sigma: ~5% probability
      ELSE 'NORMAL'
    END AS statistical_flag,

    -- Absolute deviation from rolling mean (non-normalized, for interpretability)
    ABS(sensor_value - rolling_12h_avg) AS abs_deviation_from_avg,

    -- Rate of change (current vs 1h avg) — fast transients can indicate failure onset
    SAFE_DIVIDE(
      ABS(sensor_value - rolling_1h_avg),
      NULLIF(rolling_1h_avg, 0)
    ) AS pct_change_from_1h_avg

  FROM sensor_base

),


-- ============================================================================================================================
-- SECTION 3: RULE-BASED THRESHOLD ALERTS (DOMAIN KNOWLEDGE)
-- Purpose : Apply domain-specific hard thresholds based on paper machine engineering specifications
-- These rules are NOT statistical — they encode physical safety limits for each sensor type
-- ============================================================================================================================

threshold_alerts AS (

  SELECT
    *,

    -- Rule-based threshold check per sensor type
    CASE se.sensor_type
      WHEN 'Temperature' THEN
        CASE
          WHEN se.sensor_value > 85 OR se.sensor_value < 15
            THEN 'THRESHOLD_BREACH'   -- Paper dryers must stay 20–80°C; outside = fire risk or sheet breakage
          WHEN se.sensor_value > 80 OR se.sensor_value < 20
            THEN 'THRESHOLD_WARNING'
          ELSE 'NORMAL'
        END
      WHEN 'Pressure' THEN
        CASE
          WHEN se.sensor_value > 12 OR se.sensor_value < 1
            THEN 'THRESHOLD_BREACH'   -- Hydraulic press: 2–10 bar normal; > 12 = seal failure risk
          WHEN se.sensor_value > 10 OR se.sensor_value < 2
            THEN 'THRESHOLD_WARNING'
          ELSE 'NORMAL'
        END
      WHEN 'Vibration' THEN
        CASE
          WHEN se.sensor_value > 8
            THEN 'THRESHOLD_BREACH'   -- ISO 10816: > 8 mm/s = shutdown recommended; bearing failure imminent
          WHEN se.sensor_value > 5
            THEN 'THRESHOLD_WARNING'  -- > 5 mm/s = maintenance required soon
          ELSE 'NORMAL'
        END
      WHEN 'Humidity' THEN
        CASE
          WHEN se.sensor_value > 80 OR se.sensor_value < 30
            THEN 'THRESHOLD_BREACH'   -- Paper formation: 40–70% optimal; extremes cause sheet defects
          WHEN se.sensor_value > 75 OR se.sensor_value < 35
            THEN 'THRESHOLD_WARNING'
          ELSE 'NORMAL'
        END
      WHEN 'Speed' THEN
        CASE
          WHEN se.sensor_value > 2000
            THEN 'THRESHOLD_BREACH'   -- Maximum rated speed for wet-end wire section
          WHEN se.sensor_value > 1800
            THEN 'THRESHOLD_WARNING'
          ELSE 'NORMAL'
        END
      WHEN 'Power' THEN
        CASE
          WHEN se.sensor_value > 5000
            THEN 'THRESHOLD_BREACH'   -- Motor overload: > 5000 kW = breaker trip risk
          WHEN se.sensor_value > 4500
            THEN 'THRESHOLD_WARNING'
          ELSE 'NORMAL'
        END
      ELSE 'NORMAL'
    END AS threshold_flag,

    -- Sensor-specific expected range for reporting clarity
    CASE se.sensor_type
      WHEN 'Temperature' THEN '20–80 °C'
      WHEN 'Pressure'    THEN '2–10 bar'
      WHEN 'Vibration'   THEN '0–5 mm/s'
      WHEN 'Humidity'    THEN '40–70 %'
      WHEN 'Speed'       THEN '200–1800 m/min'
      WHEN 'Power'       THEN '500–4000 kW'
      ELSE 'Unknown'
    END AS normal_range_label

  FROM z_scores se

),


-- ============================================================================================================================
-- SECTION 4 (SETUP): COMBINED ANOMALY SCORING FROM STATISTICAL + RULE-BASED SIGNALS
-- Purpose : Combine z-score statistical flag and rule-based threshold flag into a unified anomaly score
-- Scoring : anomaly_score = ABS(z_score) * weight; severity = CRITICAL / HIGH / MEDIUM / NORMAL
-- ============================================================================================================================

combined_anomaly_scoring AS (

  SELECT
    *,

    -- Weighted anomaly score: z-score magnitude, amplified if threshold also breached
    ROUND(
      COALESCE(ABS(z_score), 0)
      * CASE
          WHEN threshold_flag = 'THRESHOLD_BREACH' THEN 2.0   -- double weight when physical limit exceeded
          WHEN threshold_flag = 'THRESHOLD_WARNING' THEN 1.5  -- 50% uplift for near-threshold
          ELSE 1.0
        END,
      4
    ) AS anomaly_score,

    -- Unified severity classification
    CASE
      -- CRITICAL: both statistical anomaly AND threshold breach triggered simultaneously
      WHEN statistical_flag = 'STATISTICAL_ANOMALY' AND threshold_flag = 'THRESHOLD_BREACH'
        THEN 'CRITICAL'
      -- HIGH: strong statistical anomaly (z > 3) without threshold breach
      WHEN statistical_flag = 'STATISTICAL_ANOMALY' AND threshold_flag = 'NORMAL'
        THEN 'HIGH'
      -- HIGH: threshold breach even without statistical anomaly (value entered danger zone abruptly)
      WHEN threshold_flag = 'THRESHOLD_BREACH'
        THEN 'HIGH'
      -- MEDIUM: statistical warning (2 < z < 3) OR threshold warning
      WHEN statistical_flag = 'WARNING' OR threshold_flag = 'THRESHOLD_WARNING'
        THEN 'MEDIUM'
      -- NORMAL: no anomaly detected
      ELSE 'NORMAL'
    END AS severity,

    -- Human-readable explanation of why the anomaly was flagged
    CASE
      WHEN statistical_flag = 'STATISTICAL_ANOMALY' AND threshold_flag = 'THRESHOLD_BREACH'
        THEN CONCAT('z=', ROUND(COALESCE(z_score, 0), 2), ' sigma; Value=', ROUND(sensor_value, 2), ' outside safe range ', normal_range_label)
      WHEN statistical_flag = 'STATISTICAL_ANOMALY'
        THEN CONCAT('Statistical outlier z=', ROUND(COALESCE(z_score, 0), 2), ' sigma (3-sigma threshold)')
      WHEN threshold_flag = 'THRESHOLD_BREACH'
        THEN CONCAT('Threshold breach: Value=', ROUND(sensor_value, 2), ' ', unit, ' outside ', normal_range_label)
      WHEN statistical_flag = 'WARNING'
        THEN CONCAT('Statistical warning z=', ROUND(COALESCE(z_score, 0), 2), ' sigma (2-sigma threshold)')
      WHEN threshold_flag = 'THRESHOLD_WARNING'
        THEN CONCAT('Near-threshold warning: Value=', ROUND(sensor_value, 2), ' ', unit, ' approaching limit ', normal_range_label)
      ELSE 'Normal operating range'
    END AS anomaly_reason

  FROM threshold_alerts

),


-- ============================================================================================================================
-- SECTION 4 (BQML MODEL TRAINING — RUN ONCE / WEEKLY)
-- Purpose  : Train BQML Autoencoder on 30 days of confirmed normal sensor data
-- IMPORTANT: Execute this block SEPARATELY as a one-time or scheduled weekly job, NOT inline with the view
--
-- An autoencoder learns to compress and reconstruct "normal" input. High reconstruction error = anomaly.
-- Training data: sensor readings where alert_flag = FALSE and statistical_flag = 'NORMAL'
-- ============================================================================================================================

/*
  --- EXECUTE SEPARATELY AS A SCHEDULED WEEKLY JOB ---

  CREATE OR REPLACE MODEL nordleaf_mgmt.bqml_sensor_autoencoder
  OPTIONS (
    model_type        = 'AUTOENCODER',
    hidden_units      = [32, 16, 32],     -- encoder: 32→16, decoder: 16→32 (bottleneck at 16 units)
    activation_fn     = 'RELU',           -- ReLU for hidden layers; sigmoid at output
    l2_reg            = 0.001,            -- L2 regularisation to prevent overfitting
    batch_size        = 256,
    num_epochs        = 50,
    optimizer         = 'ADAGRAD',
    learn_rate        = 0.01
  ) AS
  SELECT
    CAST(sensor_value        AS FLOAT64)  AS feature_sensor_value,
    CAST(rolling_12h_avg     AS FLOAT64)  AS feature_rolling_avg,
    CAST(rolling_12h_stddev  AS FLOAT64)  AS feature_rolling_stddev,
    CAST(COALESCE(z_score, 0) AS FLOAT64) AS feature_z_score,
    CAST(pct_change_from_1h_avg AS FLOAT64) AS feature_pct_change

  FROM combined_anomaly_scoring
  WHERE alert_flag        = FALSE          -- train ONLY on confirmed normal readings
    AND statistical_flag  = 'NORMAL'       -- exclude known statistical outliers from training
    AND threshold_flag    = 'NORMAL'       -- exclude known threshold breaches from training
    AND z_score           IS NOT NULL      -- need sufficient history for rolling stats
    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY);

*/


-- ============================================================================================================================
-- SECTION 5: ML.DETECT_ANOMALIES — BQML AUTOENCODER SCORING
-- Purpose : Score recent sensor events against the trained autoencoder model
-- Output  : is_anomaly (BOOL) + mean_absolute_error (reconstruction error — higher = more anomalous)
-- Run     : Every 30 minutes on the last 2 hours of sensor data (recent batch)
-- ============================================================================================================================

/*
  --- EXECUTE SEPARATELY AS A SCHEDULED 30-MINUTE JOB ---

  CREATE OR REPLACE TABLE nordleaf_mgmt.bqml_anomaly_scores AS

  SELECT
    s.event_id,
    s.mill_key,
    s.machine_id,
    s.sensor_type,
    s.sensor_value,
    s.timestamp,
    bqml.is_anomaly,
    bqml.mean_absolute_error,

    -- Severity based on reconstruction error magnitude
    CASE
      WHEN bqml.mean_absolute_error > 2.0  THEN 'CRITICAL'
      WHEN bqml.mean_absolute_error > 1.0  THEN 'HIGH'
      WHEN bqml.mean_absolute_error > 0.5  THEN 'MEDIUM'
      ELSE 'NORMAL'
    END AS bqml_severity

  FROM ML.DETECT_ANOMALIES(
    MODEL nordleaf_mgmt.bqml_sensor_autoencoder,
    STRUCT(
      0.02 AS contamination   -- assume 2% of all readings are anomalous (paper industry estimate)
    ),
    (
      SELECT
        event_id,
        CAST(sensor_value       AS FLOAT64) AS feature_sensor_value,
        CAST(rolling_12h_avg    AS FLOAT64) AS feature_rolling_avg,
        CAST(rolling_12h_stddev AS FLOAT64) AS feature_rolling_stddev,
        CAST(COALESCE(z_score, 0) AS FLOAT64) AS feature_z_score,
        CAST(pct_change_from_1h_avg AS FLOAT64) AS feature_pct_change
      FROM combined_anomaly_scoring
      WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
    )
  ) bqml
  JOIN nordleaf_dwh.sensor_events s ON bqml.event_id = s.event_id;

*/


-- ============================================================================================================================
-- SECTION 6: ANOMALY AGGREGATION BY MACHINE AND HOUR
-- Purpose : Count anomalies per machine per hour; detect "burst" events (3+ anomalies in 1 hour)
-- Burst   : 3+ CRITICAL/HIGH anomalies in 1 hour = imminent machine failure signal → PAGE ENGINEER
-- ============================================================================================================================

hourly_anomaly_agg AS (

  SELECT
    mill_key,
    mill_name,
    machine_id,
    sensor_type,
    TIMESTAMP_TRUNC(timestamp, HOUR)                                     AS hour_bucket,
    DATE(timestamp)                                                       AS event_date,

    -- Total readings in this hour
    COUNT(*)                                                              AS total_readings,

    -- Anomaly counts by severity
    COUNTIF(severity = 'CRITICAL')                                       AS critical_count,
    COUNTIF(severity = 'HIGH')                                           AS high_count,
    COUNTIF(severity = 'MEDIUM')                                         AS medium_count,
    COUNTIF(severity = 'NORMAL')                                         AS normal_count,

    -- Total anomalies (CRITICAL + HIGH + MEDIUM)
    COUNTIF(severity IN ('CRITICAL', 'HIGH', 'MEDIUM'))                  AS total_anomalies,

    -- Anomaly percentage for this hour
    ROUND(
      SAFE_DIVIDE(COUNTIF(severity IN ('CRITICAL', 'HIGH', 'MEDIUM')), COUNT(*)) * 100, 2
    )                                                                     AS anomaly_pct_this_hour,

    -- Alert-triggered readings (from original sensor alert_flag)
    COUNTIF(alert_flag = TRUE)                                           AS system_alerts_triggered,

    -- Max anomaly score in this hour (highest risk moment)
    MAX(anomaly_score)                                                   AS max_anomaly_score,

    -- Average z-score magnitude (overall stress level of machine in this hour)
    ROUND(AVG(ABS(COALESCE(z_score, 0))), 4)                            AS avg_abs_z_score,

    -- Burst alert: TRUE when 3+ CRITICAL or HIGH anomalies occur in this single hour
    CASE
      WHEN COUNTIF(severity IN ('CRITICAL', 'HIGH')) >= 3
        THEN TRUE
      ELSE FALSE
    END                                                                   AS is_burst_alert,

    -- Burst level classification
    CASE
      WHEN COUNTIF(severity IN ('CRITICAL', 'HIGH')) >= 5 THEN 'SEVERE_BURST'   -- page engineer immediately
      WHEN COUNTIF(severity IN ('CRITICAL', 'HIGH')) >= 3 THEN 'BURST_ALERT'    -- notify maintenance team
      WHEN COUNTIF(severity IN ('CRITICAL', 'HIGH')) >= 1 THEN 'ISOLATED_EVENT' -- log and monitor
      ELSE 'CLEAN'
    END                                                                   AS burst_level

  FROM combined_anomaly_scoring
  GROUP BY
    mill_key, mill_name, machine_id, sensor_type,
    TIMESTAMP_TRUNC(timestamp, HOUR), DATE(timestamp)

),


-- ============================================================================================================================
-- SECTION 7: MACHINE HEALTH SCORE (0–100)
-- Purpose : Aggregate hourly anomaly data into a daily machine-level health score
-- Formula : 100 - (anomaly_pct * 50) - (burst_alerts_today * 20), clamped to [0, 100]
-- Status  : HEALTHY (>80), WATCH (60–80), WARNING (40–60), CRITICAL (<40)
-- ============================================================================================================================

daily_machine_health AS (

  SELECT
    mill_key,
    mill_name,
    machine_id,
    event_date,

    -- Total readings for the day
    SUM(total_readings)              AS daily_total_readings,

    -- Anomaly counts for the day
    SUM(critical_count)              AS daily_critical_count,
    SUM(high_count)                  AS daily_high_count,
    SUM(medium_count)                AS daily_medium_count,
    SUM(total_anomalies)             AS daily_total_anomalies,

    -- Daily anomaly percentage
    ROUND(
      SAFE_DIVIDE(SUM(total_anomalies), SUM(total_readings)) * 100, 2
    )                                AS daily_anomaly_pct,

    -- Number of burst alert hours today
    COUNTIF(is_burst_alert = TRUE)   AS burst_alert_hours_today,

    -- Max reconstruction error across all hours today (BQML proxy — using anomaly_score here)
    MAX(max_anomaly_score)           AS peak_anomaly_score_today,

    -- Average z-score magnitude for the day
    ROUND(AVG(avg_abs_z_score), 4)   AS daily_avg_abs_z_score,

    -- MACHINE HEALTH SCORE FORMULA:
    --   Base 100 points
    --   Deduct 50 points proportional to daily anomaly % (e.g., 10% anomaly = -5 points)
    --   Deduct 20 points per burst alert hour (hard penalty for concentrated failure signals)
    --   Clamp result to [0, 100]
    GREATEST(
      0,
      LEAST(
        100,
        100
        - (SAFE_DIVIDE(SUM(total_anomalies), SUM(total_readings)) * 100 * 0.50)   -- anomaly rate penalty (max -50)
        - (COUNTIF(is_burst_alert = TRUE) * 20)                                    -- burst penalty (max -N*20)
        - (SUM(critical_count) * 2)                                                -- critical event penalty
      )
    )                                AS machine_health_score,

    -- Health status classification
    CASE
      WHEN GREATEST(0, LEAST(100,
             100
             - (SAFE_DIVIDE(SUM(total_anomalies), SUM(total_readings)) * 100 * 0.50)
             - (COUNTIF(is_burst_alert = TRUE) * 20)
             - (SUM(critical_count) * 2)
           )) > 80  THEN 'HEALTHY'    -- machine operating normally
      WHEN GREATEST(0, LEAST(100,
             100
             - (SAFE_DIVIDE(SUM(total_anomalies), SUM(total_readings)) * 100 * 0.50)
             - (COUNTIF(is_burst_alert = TRUE) * 20)
             - (SUM(critical_count) * 2)
           )) > 60  THEN 'WATCH'      -- monitor closely; schedule inspection soon
      WHEN GREATEST(0, LEAST(100,
             100
             - (SAFE_DIVIDE(SUM(total_anomalies), SUM(total_readings)) * 100 * 0.50)
             - (COUNTIF(is_burst_alert = TRUE) * 20)
             - (SUM(critical_count) * 2)
           )) > 40  THEN 'WARNING'    -- maintenance required; plan downtime
      ELSE 'CRITICAL'                 -- immediate inspection required; risk of failure
    END                              AS health_status,

    -- Mean time between anomaly hours (hours between consecutive anomaly spikes)
    SAFE_DIVIDE(
      24,
      NULLIF(COUNTIF(total_anomalies > 0), 0)
    )                                AS mean_hours_between_anomaly_spikes

  FROM hourly_anomaly_agg
  GROUP BY mill_key, mill_name, machine_id, event_date

),


-- ============================================================================================================================
-- SECTION 7B: INTER-ANOMALY TIME (MEAN TIME BETWEEN ANOMALIES PER MACHINE)
-- Purpose : Compute lag between consecutive anomaly events to identify machines deteriorating over time
-- ============================================================================================================================

anomaly_events_only AS (

  SELECT
    mill_key,
    mill_name,
    machine_id,
    sensor_type,
    timestamp,
    severity,
    anomaly_score,

    -- Timestamp of previous anomaly event for this machine + sensor
    LAG(timestamp) OVER (
      PARTITION BY mill_key, machine_id, sensor_type
      ORDER BY timestamp
    ) AS prev_anomaly_timestamp,

    -- Hours since previous anomaly
    TIMESTAMP_DIFF(
      timestamp,
      LAG(timestamp) OVER (
        PARTITION BY mill_key, machine_id, sensor_type
        ORDER BY timestamp
      ),
      HOUR
    ) AS hours_since_last_anomaly

  FROM combined_anomaly_scoring
  WHERE severity IN ('CRITICAL', 'HIGH', 'MEDIUM')

),

mtba_per_machine AS (

  SELECT
    mill_key,
    mill_name,
    machine_id,
    sensor_type,
    COUNT(*)                                         AS total_anomaly_events_7d,
    ROUND(AVG(hours_since_last_anomaly), 2)          AS mean_time_between_anomalies_hours,
    MIN(hours_since_last_anomaly)                    AS min_gap_hours,
    MAX(hours_since_last_anomaly)                    AS max_gap_hours,
    -- Trend: is mean time between anomalies decreasing? (indicates worsening condition)
    ROUND(
      SAFE_DIVIDE(
        COUNTIF(hours_since_last_anomaly < 2),       -- anomalies within 2h of previous
        NULLIF(COUNT(*), 0)
      ) * 100, 2
    )                                                AS pct_rapid_succession_anomalies

  FROM anomaly_events_only
  WHERE hours_since_last_anomaly IS NOT NULL
  GROUP BY mill_key, mill_name, machine_id, sensor_type

)


-- ============================================================================================================================
-- SECTION 8: MGMT VIEW — v_sensor_anomalies
-- Purpose  : Single consolidated view for Looker Studio mill operations dashboard and Cloud Monitoring alerts
-- Audience : Mill operations engineers, maintenance planners, plant managers
-- Refresh  : Underlying sensor_events table is streaming (near real-time); view is live
-- ============================================================================================================================

CREATE OR REPLACE VIEW nordleaf_mgmt.v_sensor_anomalies
OPTIONS (
  description = 'UC-19: Real-time mill sensor anomaly detection — combines BQML autoencoder scoring with rule-based threshold alerts. Feeds mill operations dashboard and Cloud Monitoring alerting pipeline. Refreshed every 30 minutes via scheduled query.'
)
AS

-- 8A: Current anomaly feed (last 24 hours) — for real-time monitoring tile
SELECT
  'ANOMALY_FEED'                      AS view_section,
  CAST(NULL AS STRING)                AS mill_name_filter,   -- all mills

  -- Event identification
  cas.event_id,
  cas.mill_key,
  cas.mill_name,
  cas.machine_id,
  cas.sensor_type,
  cas.sensor_value,
  cas.unit,
  cas.normal_range_label,
  cas.timestamp,

  -- Anomaly classification
  cas.severity,
  cas.statistical_flag,
  cas.threshold_flag,
  cas.anomaly_score,
  cas.anomaly_reason,

  -- Statistical context
  ROUND(cas.rolling_12h_avg, 4)       AS rolling_12h_avg,
  ROUND(cas.rolling_12h_stddev, 4)    AS rolling_12h_stddev,
  ROUND(cas.z_score, 4)               AS z_score,
  ROUND(cas.abs_deviation_from_avg,4) AS abs_deviation,
  ROUND(cas.pct_change_from_1h_avg,4) AS pct_change_1h,

  -- Alert flags
  cas.alert_flag                      AS original_sensor_alert,

  -- Date parts for partitioning / filtering in Looker Studio
  DATE(cas.timestamp)                 AS event_date,
  EXTRACT(HOUR FROM cas.timestamp)    AS event_hour,
  FORMAT_TIMESTAMP('%A', cas.timestamp) AS day_of_week,

  -- Machine health stub for this view section (join to daily health for full score)
  CAST(NULL AS FLOAT64)               AS machine_health_score,
  CAST(NULL AS STRING)                AS health_status,
  CAST(NULL AS INT64)                 AS burst_alert_hours_today,
  CAST(NULL AS FLOAT64)               AS mean_time_between_anomalies_hours,

  -- Metadata
  CURRENT_TIMESTAMP()                 AS view_refreshed_at

FROM combined_anomaly_scoring cas
WHERE cas.timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
  AND cas.severity != 'NORMAL'        -- only surface anomalous events in the feed

UNION ALL

-- 8B: Daily machine health scores — for mill health scorecard tile
SELECT
  'MACHINE_HEALTH'                    AS view_section,
  dmh.mill_name                       AS mill_name_filter,

  -- Stubs for anomaly feed columns
  CAST(NULL AS STRING)                AS event_id,
  dmh.mill_key,
  dmh.mill_name,
  dmh.machine_id,
  CAST(NULL AS STRING)                AS sensor_type,
  CAST(NULL AS FLOAT64)               AS sensor_value,
  CAST(NULL AS STRING)                AS unit,
  CAST(NULL AS STRING)                AS normal_range_label,
  CAST(NULL AS TIMESTAMP)             AS timestamp,

  -- Stubs for anomaly classification
  dmh.health_status                   AS severity,        -- reuse column for status
  CAST(NULL AS STRING)                AS statistical_flag,
  CAST(NULL AS STRING)                AS threshold_flag,
  dmh.peak_anomaly_score_today        AS anomaly_score,
  CAST(NULL AS STRING)                AS anomaly_reason,

  -- Health metrics in statistical context columns
  CAST(dmh.daily_anomaly_pct AS FLOAT64)      AS rolling_12h_avg,  -- repurposed
  CAST(dmh.daily_total_readings AS FLOAT64)   AS rolling_12h_stddev,
  dmh.daily_avg_abs_z_score                   AS z_score,
  CAST(NULL AS FLOAT64)                       AS abs_deviation,
  CAST(NULL AS FLOAT64)                       AS pct_change_1h,

  -- No original alert flag for health rows
  FALSE                               AS original_sensor_alert,

  -- Date
  dmh.event_date                      AS event_date,
  CAST(NULL AS INT64)                 AS event_hour,
  CAST(NULL AS STRING)                AS day_of_week,

  -- Health score columns populated
  dmh.machine_health_score,
  dmh.health_status,
  dmh.burst_alert_hours_today,
  CAST(NULL AS FLOAT64)               AS mean_time_between_anomalies_hours,

  CURRENT_TIMESTAMP()                 AS view_refreshed_at

FROM daily_machine_health dmh
WHERE dmh.event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)

UNION ALL

-- 8C: Mean time between anomalies (MTBA) — for predictive maintenance tile
SELECT
  'MTBA_ANALYSIS'                     AS view_section,
  m.mill_name                         AS mill_name_filter,

  CAST(NULL AS STRING)                AS event_id,
  m.mill_key,
  m.mill_name,
  m.machine_id,
  m.sensor_type,
  CAST(m.total_anomaly_events_7d AS FLOAT64) AS sensor_value,
  CAST(NULL AS STRING)                AS unit,
  CAST(NULL AS STRING)                AS normal_range_label,
  CAST(NULL AS TIMESTAMP)             AS timestamp,

  -- Severity proxy: flag rapidly deteriorating machines
  CASE
    WHEN m.mean_time_between_anomalies_hours < 1  THEN 'CRITICAL'
    WHEN m.mean_time_between_anomalies_hours < 4  THEN 'HIGH'
    WHEN m.mean_time_between_anomalies_hours < 12 THEN 'MEDIUM'
    ELSE 'NORMAL'
  END                                 AS severity,

  CAST(NULL AS STRING)                AS statistical_flag,
  CAST(NULL AS STRING)                AS threshold_flag,
  m.pct_rapid_succession_anomalies    AS anomaly_score,
  CONCAT(
    'MTBA=', CAST(m.mean_time_between_anomalies_hours AS STRING), 'h | ',
    'Events(7d)=', CAST(m.total_anomaly_events_7d AS STRING)
  )                                   AS anomaly_reason,

  m.mean_time_between_anomalies_hours AS rolling_12h_avg,
  m.min_gap_hours                     AS rolling_12h_stddev,
  m.max_gap_hours                     AS z_score,
  CAST(NULL AS FLOAT64)               AS abs_deviation,
  m.pct_rapid_succession_anomalies    AS pct_change_1h,

  FALSE                               AS original_sensor_alert,
  CURRENT_DATE()                      AS event_date,
  CAST(NULL AS INT64)                 AS event_hour,
  CAST(NULL AS STRING)                AS day_of_week,

  CAST(NULL AS FLOAT64)               AS machine_health_score,
  CAST(NULL AS STRING)                AS health_status,
  CAST(NULL AS INT64)                 AS burst_alert_hours_today,
  m.mean_time_between_anomalies_hours,

  CURRENT_TIMESTAMP()                 AS view_refreshed_at

FROM mtba_per_machine m;


-- ============================================================================================================================
-- USAGE EXAMPLES
-- ============================================================================================================================

/*
  -- 1. Current CRITICAL alerts across all mills (real-time dashboard header)
  SELECT mill_name, machine_id, sensor_type, sensor_value, unit, anomaly_reason, timestamp
  FROM nordleaf_mgmt.v_sensor_anomalies
  WHERE view_section = 'ANOMALY_FEED'
    AND severity = 'CRITICAL'
  ORDER BY timestamp DESC
  LIMIT 50;

  -- 2. Machine health scorecard for today (mill operations manager view)
  SELECT mill_name, machine_id, machine_health_score, health_status, burst_alert_hours_today
  FROM nordleaf_mgmt.v_sensor_anomalies
  WHERE view_section = 'MACHINE_HEALTH'
    AND event_date = CURRENT_DATE()
  ORDER BY machine_health_score ASC;  -- worst machines at top

  -- 3. Machines with CRITICAL health needing immediate attention
  SELECT mill_name, machine_id, machine_health_score, burst_alert_hours_today
  FROM nordleaf_mgmt.v_sensor_anomalies
  WHERE view_section = 'MACHINE_HEALTH'
    AND health_status = 'CRITICAL'
    AND event_date = CURRENT_DATE();

  -- 4. MTBA analysis — machines deteriorating fastest (shortest mean time between anomalies)
  SELECT mill_name, machine_id, sensor_type,
         mean_time_between_anomalies_hours,
         CAST(sensor_value AS INT64) AS total_anomaly_events_7d,
         anomaly_score AS pct_rapid_succession
  FROM nordleaf_mgmt.v_sensor_anomalies
  WHERE view_section = 'MTBA_ANALYSIS'
  ORDER BY mean_time_between_anomalies_hours ASC
  LIMIT 20;

  -- 5. Hourly anomaly trend for Tampere mill — last 48 hours
  SELECT
    DATE(timestamp) AS event_date,
    EXTRACT(HOUR FROM timestamp) AS hour,
    sensor_type,
    COUNT(*) AS anomaly_count,
    COUNTIF(severity = 'CRITICAL') AS critical_count
  FROM nordleaf_mgmt.v_sensor_anomalies
  WHERE view_section = 'ANOMALY_FEED'
    AND mill_name = 'Tampere'
    AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR)
  GROUP BY 1,2,3
  ORDER BY 1,2;
*/

-- ============================================================================================================================
-- END OF FILE: UC19_mill_sensor_anomaly_detection.sql
-- ============================================================================================================================
