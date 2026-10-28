-- ============================================================
-- UC-13: 30-Day Demand Forecasting with BigQuery ML
-- Domain: Sales / S&OP Planning
-- Business Question: What volume of each paper grade will
--   each mill need to produce in the next 30 days?
-- Priority: P1 | Users: Sales, Production Planning, Logistics
-- ============================================================
--
-- ARCHITECTURE DIAGRAM
--
--  fact_orders (historical weekly demand per product × mill)
--    + dim_date (Finnish holidays, seasonal factors)
--         |
--         | BigQuery ML ARIMA_PLUS model training
--         | (weekly grain, holiday_region='FI')
--         v
--  nordleaf_dwh.nordleaf_demand_forecast model
--         |
--         | ML.FORECAST (30-day horizon)
--         v
--  nordleaf_dwh.demand_forecast
--    (forecast_week, product_type, mill_name, forecast_tons,
--     lower_bound, upper_bound)
--         |
--         v
--  S&OP Planning Dashboard + Production Schedule input
-- ============================================================

-- Step 1: Train the ARIMA_PLUS demand forecast model
CREATE OR REPLACE MODEL `nordleaf_dwh.nordleaf_demand_forecast`
OPTIONS (
  model_type                   = 'ARIMA_PLUS',
  time_series_timestamp_col    = 'order_week',
  time_series_data_col         = 'total_tons',
  time_series_id_col           = ['product_type', 'mill_name'],
  horizon                      = 30,
  auto_arima                   = TRUE,
  data_frequency               = 'WEEKLY',
  decompose_time_series        = TRUE,
  holiday_region               = 'FI'
)
AS
SELECT
  DATE_TRUNC(dd.full_date, WEEK(MONDAY)) AS order_week,
  dp.product_type,
  dm.mill_name,
  SUM(fo.quantity_tons)                  AS total_tons
FROM `nordleaf_dwh.fact_orders` fo
JOIN `nordleaf_dwh.dim_date`    dd ON fo.order_date_id  = dd.date_id
JOIN `nordleaf_dwh.dim_product` dp ON fo.product_key    = dp.product_key
JOIN `nordleaf_dwh.dim_mill`    dm ON fo.mill_key        = dm.mill_key
WHERE fo.status != 'CANCELLED'
  AND dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
GROUP BY 1,2,3
HAVING COUNT(*) >= 4;

-- Step 2: Generate and persist the 30-day forecast
CREATE OR REPLACE TABLE `nordleaf_dwh.demand_forecast` AS
SELECT
  forecast_timestamp                     AS forecast_week,
  product_type,
  mill_name,
  ROUND(forecast_value, 1)               AS forecast_tons,
  ROUND(prediction_interval_lower_bound, 1) AS lower_bound_tons,
  ROUND(prediction_interval_upper_bound, 1) AS upper_bound_tons,
  -- Estimated revenue at standard price
  ROUND(forecast_value * CASE product_type
    WHEN 'Newsprint'       THEN 680
    WHEN 'Kraft Paper'     THEN 520
    WHEN 'Cardboard'       THEN 750
    WHEN 'Specialty Paper' THEN 1200
    WHEN 'Tissue'          THEN 920
    ELSE 700 END, 0)                     AS forecast_revenue_eur,
  CURRENT_TIMESTAMP()                    AS model_run_ts
FROM ML.FORECAST (
  MODEL `nordleaf_dwh.nordleaf_demand_forecast`,
  STRUCT (30 AS horizon, 0.90 AS confidence_level)
)
ORDER BY forecast_timestamp, product_type, mill_name;

-- Step 3: Forecast vs capacity check (production feasibility)
SELECT
  f.forecast_week,
  f.product_type,
  f.mill_name,
  f.forecast_tons,
  f.lower_bound_tons,
  f.upper_bound_tons,
  f.forecast_revenue_eur,
  dm.capacity_tons_day * 7              AS weekly_capacity_tons,
  ROUND(f.forecast_tons
    / NULLIF(dm.capacity_tons_day * 7, 0) * 100, 1) AS capacity_utilisation_pct,
  CASE
    WHEN f.forecast_tons > dm.capacity_tons_day * 7 * 0.90
      THEN 'CAPACITY WARNING — may need overtime or 3rd party'
    WHEN f.forecast_tons < dm.capacity_tons_day * 7 * 0.50
      THEN 'LOW UTILISATION — optimise schedule'
    ELSE 'NORMAL'
  END AS capacity_flag
FROM `nordleaf_dwh.demand_forecast` f
JOIN `nordleaf_dwh.dim_mill` dm ON f.mill_name = dm.mill_name
ORDER BY f.forecast_week, f.product_type;

-- Step 4: Evaluate model accuracy
SELECT
  product_type,
  mill_name,
  ROUND(mean_absolute_error, 1)              AS mae_tons,
  ROUND(mean_absolute_percentage_error * 100, 2) AS mape_pct,
  ROUND(symmetric_mean_absolute_percentage_error * 100, 2) AS smape_pct
FROM ML.EVALUATE(MODEL `nordleaf_dwh.nordleaf_demand_forecast`)
ORDER BY mean_absolute_percentage_error;
