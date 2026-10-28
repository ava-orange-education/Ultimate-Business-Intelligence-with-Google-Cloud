# NordLeaf Vertex AI & BigQuery ML

## Overview

```
NordLeaf historical data in BigQuery
    |
    | Weekly (Sundays 02:00 EET via Composer DAG)
    v
Vertex AI Pipeline: nordleaf-demand-forecast-retraining
    |
    |-- Step 1: train_demand_forecast_model
    |     BQML ARIMA_PLUS on nordleaf_dwh.fact_orders
    |     Groups by: product_type x mill_name x week
    |     Finnish holiday awareness (holiday_region='FI')
    |
    |-- Step 2: save_forecast
    |     ML.FORECAST -> nordleaf_dwh.demand_forecast
    |     30-day horizon, 90% confidence intervals
    |
    |-- Step 3: evaluate_model
          ML.EVALUATE -> check MAPE < 20%
          Fails pipeline if quality threshold not met
    |
    v
Vertex AI Model Registry: nordleaf-demand-forecast (versioned)
    |
    v
Looker Studio Dashboard: NordLeaf Demand Forecast
```

## Models Trained

| Model | Type | Purpose | Target |
|-------|------|---------|--------|
| nordleaf_demand_forecast | ARIMA_PLUS | 30-day demand forecast | quantity_tons per product+mill |
| sensor_anomaly_model | AUTOENCODER | Mill sensor anomaly detection | alert_flag |
| customer_churn_model | LOGISTIC_REG | Customer churn prediction | is_churned (90-day inactivity) |

## Steps to Run

### 1. Install dependencies
```bash
pip install google-cloud-bigquery kfp google-cloud-aiplatform
```

### 2. Train models directly via BQML
```bash
export GCP_PROJECT_ID=nordleaf-project
python vertex_ai_demand_forecast.py
```

### 3. Deploy Vertex AI Pipeline (weekly retraining)
```bash
python vertex_ai_pipeline.py
```

### 4. View Forecasts in BigQuery
```bash
bq query --use_legacy_sql=false < demand_forecast_dashboard.sql
```
