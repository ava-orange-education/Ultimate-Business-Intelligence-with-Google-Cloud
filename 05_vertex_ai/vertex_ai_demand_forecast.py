"""
NordLeaf Vertex AI - Demand Forecasting with BigQuery ML
=========================================================
Reads project settings from  ../environment_config.toml
Reads credentials       from  ../secrets.toml  (optional)

Authentication priority:
  1. service_account_key_path in secrets.toml [GCP] section
  2. GOOGLE_APPLICATION_CREDENTIALS env var
  3. Application Default Credentials  (gcloud auth application-default login)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

# ── TOML parser ──────────────────────────────────────────────
try:
    import tomllib           # Python 3.11+
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # pip install tomli
    except ModuleNotFoundError:
        sys.exit("tomllib/tomli not found. Run: pip install tomli")

from google.cloud import bigquery
from google.oauth2 import service_account


# ============================================================
# CONFIG  –  read from environment_config.toml + secrets.toml
# ============================================================

def _load_toml(path: Path) -> dict:
    with open(path, "rb") as f:
        return tomllib.load(f)


def load_config() -> tuple[dict, dict]:
    """Return (config, secrets). Searches for config files at project root."""
    # Script lives at <root>/08_vertex_ai/vertex_ai_demand_forecast.py
    root = Path(__file__).parent.parent

    cfg_path = root / "environment_config.toml"
    if not cfg_path.exists():
        sys.exit(f"environment_config.toml not found at {cfg_path}")
    cfg = _load_toml(cfg_path)

    sec_path = root / "secrets.toml"
    secrets: dict = _load_toml(sec_path) if sec_path.exists() else {}
    if not sec_path.exists():
        print("[WARN] secrets.toml not found – using ADC / env var")

    return cfg, secrets


cfg, secrets = load_config()

PROJECT_ID = cfg["PROJECT_ID"]
REGION     = cfg.get("LOCATION", "europe-west1")
DATASET    = cfg.get("BIGQUERY_SETTINGS", {}).get("dataset_dwh", "nordleaf_dwh")
MODEL_ID   = cfg.get("VERTEX_AI_SETTINGS", {}).get("model_display_name", "nordleaf_demand_forecast")


# ============================================================
# AUTH  –  service account key → env var → ADC
# ============================================================

def make_bq_client(project_id: str, secrets: dict) -> bigquery.Client:
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    # 1. secrets.toml key path
    key_path_str = secrets.get("GCP", {}).get("service_account_key_path", "").strip()

    # 2. fall back to GOOGLE_APPLICATION_CREDENTIALS env var
    if not key_path_str:
        key_path_str = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

    if key_path_str:
        key_path = Path(key_path_str).expanduser().resolve()
        if key_path.exists():
            creds = service_account.Credentials.from_service_account_file(
                str(key_path), scopes=scopes
            )
            print(f"Auth: using service account key → {creds.service_account_email}")
            print(f"Key file: {key_path}")
            return bigquery.Client(project=project_id, credentials=creds)
        else:
            print(f"[WARN] Key file not found: {key_path} – falling back to ADC")

    # 3. Application Default Credentials
    print("Auth: using Application Default Credentials (ADC)")
    print("      Run `gcloud auth application-default login` if this fails.")
    return bigquery.Client(project=project_id)


client = make_bq_client(PROJECT_ID, secrets)


# ============================================================
# STEP 1: Create BQML ARIMA_PLUS Demand Forecast Model
# ============================================================
# FIX: dim_date column is `full_date`, not `date`
CREATE_MODEL_SQL = f"""
CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET}.{MODEL_ID}`
OPTIONS(
  model_type                = 'ARIMA_PLUS',
  time_series_timestamp_col = 'order_week',
  time_series_data_col      = 'total_tons',
  time_series_id_col        = ['product_type', 'mill_name'],
  horizon                   = 30,
  auto_arima                = TRUE,
  data_frequency            = 'WEEKLY',
  decompose_time_series     = TRUE,
  holiday_region            = 'FI'
)
AS
SELECT
  DATE_TRUNC(dd.full_date, WEEK)       AS order_week,
  dp.product_type,
  dm.mill_name,
  SUM(fo.quantity_tons)                AS total_tons
FROM `{PROJECT_ID}.{DATASET}.fact_orders` fo
JOIN `{PROJECT_ID}.{DATASET}.dim_product` dp ON fo.product_key = dp.product_key
JOIN `{PROJECT_ID}.{DATASET}.dim_mill`    dm ON fo.mill_key    = dm.mill_key
JOIN `{PROJECT_ID}.{DATASET}.dim_date`    dd ON fo.order_date_id = dd.date_id
WHERE fo.status = 'DELIVERED'
  AND dd.full_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR)
GROUP BY 1, 2, 3
HAVING COUNT(*) >= 4
ORDER BY 1
"""

# ============================================================
# STEP 2: Generate 30-day forecast and save
# ============================================================
FORECAST_SQL = f"""
SELECT
  forecast_timestamp                                        AS forecast_week,
  product_type,
  mill_name,
  ROUND(forecast_value, 2)                                  AS forecast_tons,
  ROUND(prediction_interval_lower_bound, 2)                 AS forecast_lower_bound,
  ROUND(prediction_interval_upper_bound, 2)                 AS forecast_upper_bound,
  ROUND(forecast_value * CASE product_type
    WHEN 'Newsprint'       THEN 680
    WHEN 'Kraft Paper'     THEN 520
    WHEN 'Cardboard'       THEN 750
    WHEN 'Specialty Paper' THEN 1200
    WHEN 'Tissue'          THEN 920
    ELSE 700 END, 2)                                        AS forecast_revenue_eur
FROM ML.FORECAST(
  MODEL `{PROJECT_ID}.{DATASET}.{MODEL_ID}`,
  STRUCT(30 AS horizon, 0.9 AS confidence_level)
)
ORDER BY forecast_timestamp, product_type, mill_name
"""

SAVE_FORECAST_SQL = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.demand_forecast` AS
{FORECAST_SQL}
"""

# ============================================================
# STEP 3: Model evaluation
# ============================================================
EVALUATE_SQL = f"""
SELECT
  product_type,
  mill_name,
  ROUND(mean_absolute_error, 2)                             AS mae,
  ROUND(mean_squared_error, 2)                              AS mse,
  ROUND(mean_absolute_percentage_error, 4)                  AS mape,
  ROUND(symmetric_mean_absolute_percentage_error, 4)        AS smape
FROM ML.EVALUATE(MODEL `{PROJECT_ID}.{DATASET}.{MODEL_ID}`)
ORDER BY mean_absolute_percentage_error
"""

# ============================================================
# STEP 4: Anomaly detection on sensor data (autoencoder)
# ============================================================
ANOMALY_DETECTION_SQL = f"""
CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET}.sensor_anomaly_model`
OPTIONS(
  model_type         = 'AUTOENCODER',
  activation_fn      = 'RELU',
  batch_size         = 32,
  dropout            = 0.2,
  hidden_units       = [32, 16, 8, 16, 32],
  learn_rate         = 0.001,
  l1_reg             = 0.001,
  max_iterations     = 100,
  input_label_cols   = ['reading_value', 'mill_numeric', 'sensor_type_numeric'],
  auto_class_weights = FALSE
)
AS
SELECT
  reading_value,
  CASE mill_id
    WHEN 'MILL-TMP' THEN 1 WHEN 'MILL-OUL' THEN 2 WHEN 'MILL-TKU' THEN 3
    WHEN 'MILL-ESP' THEN 4 WHEN 'MILL-JYV' THEN 5 ELSE 0
  END AS mill_numeric,
  CASE sensor_type
    WHEN 'Temperature' THEN 1 WHEN 'Pressure' THEN 2 WHEN 'Humidity' THEN 3 ELSE 0
  END AS sensor_type_numeric
FROM `{PROJECT_ID}.nordleaf_stg.sensor_events_stream`
WHERE event_ts > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
  AND alert_flag = FALSE
"""

# ============================================================
# STEP 5: Customer churn prediction (logistic regression)
# ============================================================
CHURN_MODEL_SQL = f"""
CREATE OR REPLACE MODEL `{PROJECT_ID}.{DATASET}.customer_churn_model`
OPTIONS(
  model_type         = 'LOGISTIC_REG',
  input_label_cols   = ['is_churned'],
  auto_class_weights = TRUE
)
AS
WITH customer_features AS (
  SELECT
    c.customer_name,
    c.country,
    c.customer_type,
    c.credit_rating,
    COUNT(fo.order_id)                                      AS order_count_12m,
    SUM(fo.total_value_eur)                                 AS total_revenue_12m,
    AVG(fo.quantity_tons)                                   AS avg_order_size,
    DATE_DIFF(
      CURRENT_DATE(),
      MAX(SAFE.PARSE_DATE('%Y%m%d', CAST(fo.order_date_id AS STRING))),
      DAY
    )                                                       AS days_since_last_order,
    AVG(COALESCE(fb.rating, 3))                             AS avg_feedback_rating,
    IF(
      DATE_DIFF(
        CURRENT_DATE(),
        MAX(SAFE.PARSE_DATE('%Y%m%d', CAST(fo.order_date_id AS STRING))),
        DAY
      ) > 90, 1, 0
    )                                                       AS is_churned
  FROM `{PROJECT_ID}.{DATASET}.dim_customer` c
  LEFT JOIN `{PROJECT_ID}.{DATASET}.fact_orders`           fo USING (customer_key)
  LEFT JOIN `{PROJECT_ID}.nordleaf_stg.customer_feedback`  fb
         ON c.customer_name = fb.customer_name
  WHERE c.is_current = TRUE
  GROUP BY 1, 2, 3, 4
)
SELECT * EXCEPT(customer_name) FROM customer_features
"""


# ============================================================
# RUNNER
# ============================================================

def run_query(sql: str, description: str) -> None:
    print(f"\nRunning: {description}...")
    job = client.query(sql)
    job.result()
    print(f"  Done. Bytes processed: {job.total_bytes_processed:,}")


if __name__ == "__main__":
    print(f"NordLeaf Vertex AI / BQML Pipeline - {datetime.now().isoformat()}")
    print(f"Project: {PROJECT_ID}  |  Dataset: {DATASET}  |  Region: {REGION}")

    run_query(CREATE_MODEL_SQL,      "Create ARIMA_PLUS demand forecast model")
    run_query(SAVE_FORECAST_SQL,     "Generate and save 30-day demand forecast")
    run_query(ANOMALY_DETECTION_SQL, "Create sensor anomaly detection model (autoencoder)")
    run_query(CHURN_MODEL_SQL,       "Create customer churn prediction model")

    print("\n=== Model Evaluation ===")
    eval_job = client.query(EVALUATE_SQL)
    for row in eval_job.result():
        print(f"  {row.product_type} / {row.mill_name}: "
              f"MAPE={row.mape:.2%}, MAE={row.mae:.1f} tons")

    print("\nAll BQML models trained successfully ✅")
