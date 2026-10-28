"""
NordLeaf Vertex AI Pipeline - Retraining Orchestration
Uses Vertex AI Pipelines (Kubeflow Pipelines v2) to automate weekly model retraining

Reads all settings from ../environment_config.toml.
GCP_PROJECT_ID env var can override the config PROJECT_ID if set.
"""
import os
import sys
from pathlib import Path

from kfp import dsl
from kfp.v2 import compiler
from google.cloud import aiplatform

# ── TOML parser (stdlib 3.11+, else pip install tomli) ───────────────────────
try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib          # type: ignore[no-redef]
    except ModuleNotFoundError:
        sys.exit("tomllib/tomli not found.  Run: pip install tomli")

# ── Load environment_config.toml ─────────────────────────────────────────────
_cfg_path = Path(__file__).parent.parent / "environment_config.toml"
if not _cfg_path.exists():
    sys.exit(f"environment_config.toml not found at {_cfg_path}")
with open(_cfg_path, "rb") as _f:
    _cfg = tomllib.load(_f)

_va  = _cfg.get("VERTEX_AI_SETTINGS", {})
_st  = _cfg.get("STORAGE", {})
_bq  = _cfg.get("BIGQUERY_SETTINGS", {})

# GCP_PROJECT_ID env var overrides config so CI/CD can inject the value without
# editing the toml file.
PROJECT_ID    = os.environ.get("GCP_PROJECT_ID", _cfg["PROJECT_ID"])
REGION        = _cfg.get("LOCATION", "europe-west1")
PIPELINE_ROOT = _va.get(
    "pipeline_root",
    f"gs://{_st.get('vertex_ai_bucket', 'nordleaf-vertex-ai')}/pipelines"
)
DATASET       = _bq.get("dataset_dwh", "nordleaf_dwh")


@dsl.component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-bigquery>=3.11"]
)
def train_demand_forecast_model(project_id: str, dataset: str) -> str:
    """Train BQML ARIMA_PLUS demand forecast model."""
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    sql = f"""
        CREATE OR REPLACE MODEL `{project_id}.{dataset}.nordleaf_demand_forecast`
        OPTIONS(model_type='ARIMA_PLUS', time_series_timestamp_col='order_week',
                time_series_data_col='total_tons', time_series_id_col=['product_type','mill_name'],
                horizon=30, auto_arima=TRUE, data_frequency='WEEKLY', holiday_region='FI')
        AS SELECT DATE_TRUNC(order_date, WEEK) AS order_week, product_type, mill_name,
                  SUM(quantity_tons) AS total_tons
           FROM `{project_id}.{dataset}.fact_orders` fo
           JOIN `{project_id}.{dataset}.dim_product` dp ON fo.product_key=dp.product_key
           JOIN `{project_id}.{dataset}.dim_mill`    dm ON fo.mill_key=dm.mill_key
           WHERE status='DELIVERED' GROUP BY 1,2,3 HAVING COUNT(*)>=4
    """
    client.query(sql).result()
    return f"{project_id}.{dataset}.nordleaf_demand_forecast"


@dsl.component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-bigquery>=3.11"]
)
def save_forecast(project_id: str, dataset: str, model_path: str) -> int:
    """Save 30-day forecast to BigQuery table."""
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    sql = f"""
        CREATE OR REPLACE TABLE `{project_id}.{dataset}.demand_forecast` AS
        SELECT forecast_timestamp AS forecast_week, product_type, mill_name,
               ROUND(forecast_value,2) AS forecast_tons,
               ROUND(prediction_interval_lower_bound,2) AS lower_bound,
               ROUND(prediction_interval_upper_bound,2) AS upper_bound
        FROM ML.FORECAST(MODEL `{model_path}`, STRUCT(30 AS horizon, 0.9 AS confidence_level))
    """
    job = client.query(sql)
    job.result()
    return job.total_rows or 0


@dsl.component(
    base_image="python:3.11",
    packages_to_install=["google-cloud-bigquery>=3.11"]
)
def evaluate_model(project_id: str, dataset: str, model_path: str) -> float:
    """Evaluate model and return MAPE score."""
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    sql = f"SELECT AVG(mean_absolute_percentage_error) AS avg_mape FROM ML.EVALUATE(MODEL `{model_path}`)"
    row = list(client.query(sql))[0]
    mape = float(row.avg_mape or 0.0)
    print(f"Average MAPE: {mape:.4f}")
    if mape > 0.20:
        raise ValueError(f"Model MAPE too high: {mape:.2%}. Investigate data quality.")
    return mape


@dsl.pipeline(
    name="nordleaf-demand-forecast-retraining",
    description="Weekly retraining of NordLeaf demand forecast model"
)
def nordleaf_retraining_pipeline(
    project_id: str = PROJECT_ID,
    dataset: str = DATASET
):
    train_task = train_demand_forecast_model(project_id=project_id, dataset=dataset)
    forecast_task = save_forecast(
        project_id=project_id, dataset=dataset, model_path=train_task.output
    ).after(train_task)
    evaluate_task = evaluate_model(
        project_id=project_id, dataset=dataset, model_path=train_task.output
    ).after(forecast_task)


if __name__ == "__main__":
    # Compile pipeline
    compiler.Compiler().compile(
        pipeline_func=nordleaf_retraining_pipeline,
        package_path="nordleaf_retraining_pipeline.json"
    )

    # Initialize Vertex AI and submit pipeline run
    aiplatform.init(project=PROJECT_ID, location=REGION)
    job = aiplatform.PipelineJob(
        display_name="nordleaf-demand-forecast-weekly",
        template_path="nordleaf_retraining_pipeline.json",
        pipeline_root=PIPELINE_ROOT,
        parameter_values={"project_id": PROJECT_ID, "dataset": DATASET},
        enable_caching=True,
    )
    job.submit(service_account=f"nordleaf-vertex-ai@{PROJECT_ID}.iam.gserviceaccount.com")
    print(f"Pipeline submitted: {job.resource_name}")
