-- ============================================================
-- NordLeaf GCP Monitoring Dashboard Queries
-- Use in Cloud Monitoring custom dashboards
-- ============================================================

-- 1. Pipeline Health: Last 7 days
SELECT
  DATE(logged_at)  AS log_date,
  task_name,
  status,
  COUNT(*)         AS runs,
  AVG(duration_sec) AS avg_duration_sec,
  MAX(duration_sec) AS max_duration_sec
FROM `nordleaf_mgmt.pipeline_log`
WHERE logged_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
GROUP BY 1,2,3
ORDER BY 1 DESC,3;

-- 2. BigQuery Job Cost Monitoring (last 30 days)
SELECT
  DATE(creation_time)                                      AS query_date,
  user_email,
  COUNT(*)                                                  AS query_count,
  SUM(total_bytes_processed)                                AS total_bytes,
  ROUND(SUM(total_bytes_processed) / POW(1024,4) * 6.25, 2) AS estimated_cost_usd,
  COUNTIF(cache_hit)                                        AS cached_queries,
  ROUND(COUNTIF(cache_hit) / COUNT(*) * 100, 1)            AS cache_hit_pct
FROM `region-europe-west1`.INFORMATION_SCHEMA.JOBS
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
GROUP BY 1,2
ORDER BY 1 DESC, 5 DESC;

-- 3. Data Freshness Check
SELECT
  'nordleaf_stg.orders_raw'              AS table_name,
  MAX(loaded_at)                          AS last_loaded,
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(loaded_at), HOUR) AS hours_since_load,
  IF(MAX(loaded_at) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR), 'FRESH','STALE') AS status
FROM `nordleaf_stg.orders_raw`
UNION ALL
SELECT
  'nordleaf_dwh.fact_orders',
  MAX(created_at),
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(created_at), HOUR),
  IF(MAX(created_at) > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 25 HOUR), 'FRESH','STALE')
FROM `nordleaf_dwh.fact_orders`
UNION ALL
SELECT
  'nordleaf_dwh.fact_production',
  MAX(TIMESTAMP(production_date)),
  TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(TIMESTAMP(production_date)), HOUR),
  IF(MAX(production_date) > DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY), 'FRESH','STALE')
FROM `nordleaf_dwh.fact_production`;

-- 4. DQ Score Summary (last 24h)
SELECT
  table_name,
  COUNT(*)                      AS checks_run,
  COUNTIF(status='PASS')        AS passed,
  COUNTIF(status='FAIL')        AS failed,
  ROUND(COUNTIF(status='PASS') / COUNT(*) * 100, 1) AS overall_dq_score
FROM `nordleaf_mgmt.dq_results`
WHERE run_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY 1
ORDER BY overall_dq_score;

-- 5. Sensor Alert Summary (last hour)
SELECT
  mill_id, sensor_type, COUNT(*) AS alert_count, MAX(event_ts) AS latest_alert
FROM `nordleaf_stg.sensor_events_stream`
WHERE alert_flag = TRUE
  AND event_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY 1,2 ORDER BY 3 DESC;
