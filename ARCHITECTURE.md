# NordLeaf — Platform Architecture

## Overview

NordLeaf's data platform follows a modern cloud-native **medallion architecture** on GCP. Source data flows from operational systems through a structured lake-to-warehouse pipeline, feeds machine learning models, and surfaces in self-service dashboards.

---

## End-to-End Data Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                     SOURCE SYSTEMS                              │
│   ERP (orders, production)  ·  IoT sensors  ·  Supplier feeds   │
│   Customer feedback CSV     ·  Financial systems                │
└──────────────────────────┬──────────────────────────────────────┘
                           │  SFTP / file drop / streaming
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              02 — CLOUD STORAGE (Data Lake)                     │
│                                                                 │
│  gs://nordleaf-raw-data/                                        │
│    orders/   suppliers/   deliveries/   production/             │
│    sensors/  feedback/                                          │
│                                                                 │
│  gs://nordleaf-processed-data/   (Parquet, post-ETL)            │
│  gs://nordleaf-archive/          (Coldline, 90 days+)           │
│  gs://nordleaf-vertex-ai/        (ML artifacts)                 │
│  gs://nordleaf-composer-bucket/  (Airflow DAGs)                 │
└──────────────────────────┬──────────────────────────────────────┘
                           │  GCS Object Finalize → Pub/Sub
                           │  → Cloud Function → BQ Load Job
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│            04 — CLOUD DATA FUSION (ETL Pipelines)               │
│                                                                 │
│  nordleaf-suppliers-etl      daily 05:00 EET                    │
│    GCS CSV → Validate → Deduplicate → dim_supplier              │
│                                                                 │
│  nordleaf-orders-batch-etl   daily 06:00 EET                    │
│    GCS CSV → Wrangler → Joiner (dim lookup) → fact_orders       │
│                                                                 │
│  nordleaf-production-etl     daily 04:00 EET                    │
│    GCS JSON → Flatten → Quality Check → fact_production         │
│                                                                 │
│  Transforms: Wrangler · JavaScript · Deduplicate · Joiner       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼───────────────────┐
          ▼                ▼                   ▼
   ┌────────────┐  ┌──────────────┐  ┌───────────────┐
   │nordleaf_ext│  │ nordleaf_stg │  │ nordleaf_mgmt │
   │(GCS-backed │  │ (Staging —   │  │ (Pipeline ops │
   │ ext tables)│  │  raw ingest) │  │  DQ results)  │
   └────────────┘  └──────┬───────┘  └───────────────┘
                          │  Transformation + dimension lookup
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│           03 — BIGQUERY (Data Warehouse — nordleaf_dwh)         │
│                                                                 │
│  Dimensions                   Facts                             │
│  ─────────────────────        ──────────────────────────        │
│  dim_customer                 fact_orders                       │
│  dim_product                  fact_production                   │
│  dim_mill                     fact_deliveries                   │
│  dim_date                                                       │
│                                                                 │
│  All tables: partitioned by date, clustered by mill/customer    │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼───────────────────┐
          ▼                ▼                   ▼
┌──────────────┐  ┌─────────────────┐  ┌──────────────────────┐
│ 05 VERTEX AI │  │ 08 BI USE CASES │  │  07 BI DASHBOARDS    │
│              │  │                 │  │                      │
│ ARIMA_PLUS   │  │ 24 SQL queries  │  │ Looker Studio        │
│ demand fcast │  │ across 8 domain │  │ ├─ Executive Revenue │
│              │  │                 │  │ ├─ Mill Operations   │
│ Logistic Reg │  │ Forest & Fibre  │  │ ├─ Supplier Scorecard│
│ churn risk   │  │ Production      │  │ ├─ Customer Analytics│
│              │  │ Supply Chain    │  │ └─ Pipeline Status   │
│ Autoencoder  │  │ Sales           │  │                      │
│ sensor anom. │  │ Customer        │  │ Scheduled email:     │
│              │  │ IoT & Ops       │  │ daily 08:00 EET      │
│ KFP pipeline │  │ Finance         │  └──────────────────────┘
│ weekly retrain  │ Governance/ESG  │
└──────┬───────┘  └─────────────────┘
       │  Model output written back to nordleaf_dwh
       └─────────────────────────────────────────────────────────┐
                                                                 ▼
┌─────────────────────────────────────────────────────────────────┐
│              06 — CLOUD MONITORING & ALERTING                   │
│                                                                 │
│  Pipeline failures   → Email + Slack #data-alerts    │
│  Data freshness > 25h → Email + Slack                           │
│  DQ score < 95%      → Email + Slack                            │
│  Cost > EUR 200/day  → Email management                         │
│  Sensor spike > 10   → Slack #ops + PagerDuty                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## Security Architecture

```
Google Workspace (nordleaf.fi)
  │  SSO + MFA
  ▼
Cloud IAM
  ├── Human Groups
  │     nordleaf-data-engineers@     BigQuery dataEditor · GCS objectAdmin
  │     nordleaf-analysts@           BigQuery dataViewer (dwh, mgmt only)
  │     nordleaf-finance@            BigQuery dataViewer + column-level policy tags
  │     nordleaf-management@         BigQuery dataViewer
  │
  └── Service Accounts (machine identities)
        nordleaf-data-pipeline@      ETL read/write
        nordleaf-data-fusion@        Data Fusion orchestration
        nordleaf-composer@           Airflow DAG execution
        nordleaf-vertex-ai@          ML training and inference
        nordleaf-bi-reader@          Looker Studio read-only
        nordleaf-dataplex@           Data quality scanning

Column-level Policy Tags (Dataplex taxonomy)
  Confidential/Financial  →  unit_price_eur, total_value_eur, contract_value
  PII/Email               →  contact_email
  Visible to: finance group + management group only
```

---

## BigQuery Dataset Access Matrix

| Dataset | Engineers | Analysts | Finance | Pipeline SA |
|---------|-----------|----------|---------|-------------|
| nordleaf_ext | OWNER | — | — | WRITER |
| nordleaf_stg | OWNER | — | — | WRITER |
| nordleaf_dwh | OWNER | READER | READER | WRITER |
| nordleaf_mgmt | OWNER | READER | READER | WRITER |

---

## ETL Pipeline Architecture (Data Fusion)

```
GCS Source (CSV / JSON)
    │
    ├── GCSFile (batchsource)       Read raw files
    ├── Wrangler (transform)        Type casting, field derivations, null removal
    ├── JavaScript (transform)      Custom validation logic
    ├── Deduplicate (transform)     Remove duplicates by business key
    ├── Joiner (batchjoiner)        Dimension key lookups (mill_key, product_key)
    ├── BigQueryTable (stg sink)    Write to nordleaf_stg
    └── BigQueryTable (dwh sink)    Write to nordleaf_dwh
```

**Pipeline schedules (Cloud Composer / Data Fusion Scheduler):**

| Pipeline | Schedule | Source → Target |
|----------|----------|-----------------|
| nordleaf-production-etl | 04:00 EET daily | GCS JSON → fact_production |
| nordleaf-suppliers-etl | 05:00 EET daily | GCS CSV → dim_supplier |
| nordleaf-orders-batch-etl | 06:00 EET daily | GCS CSV → fact_orders |

---

## ML Architecture (Vertex AI)

```
nordleaf_dwh.fact_orders  (historical)
    │
    │  Vertex AI Pipeline — weekly, Sunday 02:00 EET
    │
    ├── Step 1: train_demand_forecast_model
    │     BQML ARIMA_PLUS, grouped by product_type × mill_name × week
    │     Finnish holiday awareness (holiday_region = 'FI')
    │
    ├── Step 2: save_forecast
    │     ML.FORECAST → nordleaf_dwh.demand_forecast
    │     30-day horizon, 90% confidence intervals
    │
    └── Step 3: evaluate_model
          ML.EVALUATE → MAPE check (threshold < 20%)
          Pipeline fails and alerts if threshold not met

Model Registry: nordleaf-demand-forecast (versioned)
```

**All three models:**

| Model | Type | Trigger | Output table |
|-------|------|---------|--------------|
| nordleaf_demand_forecast | ARIMA_PLUS | Weekly retrain | demand_forecast |
| customer_churn_model | Logistic Regression | Weekly retrain | churn_scores |
| sensor_anomaly_model | Autoencoder | Continuous | sensor_alerts |

---

## GCS Bucket Layout

| Bucket | Storage class | Retention | Purpose |
|--------|--------------|-----------|---------|
| nordleaf-raw-data | STANDARD | 90 days | Source file ingestion |
| nordleaf-processed-data | STANDARD | — | Parquet after ETL |
| nordleaf-archive | COLDLINE | 7 years | Historical archive |
| nordleaf-composer-bucket | STANDARD | — | Airflow DAGs and plugins |
| nordleaf-dataflow-temp | STANDARD | Short-lived | Dataflow staging |
| nordleaf-vertex-ai | STANDARD | — | ML artifacts and training data |

---

## BI Domain Coverage

```
 ┌───────────────────────────────────────────────────────────────┐
 │  🌲 FOREST & FIBRE    🏭 PRODUCTION       🚚 SUPPLY CHAIN     │
 │  UC-01 Timber balance  UC-03 Mill OEE      UC-07 Suppliers    │
 │  UC-02 FSC/PEFC        UC-04 Quality RCA   UC-08 Material cost│
 │                        UC-05 Shifts        UC-09 Delivery OTD │
 │                        UC-06 S&OP          UC-10 Supply risk  │
 │                                                               │
 │  💰 SALES & REVENUE   👥 CUSTOMER CRM     ⚙️  IoT & REAL-TIME │
 │  UC-11 Revenue margin  UC-12 CLV           UC-19 Sensor anom. │
 │  UC-13 Demand forecast UC-16 NPS/CSAT      UC-20 Energy KPIs  │
 │  UC-14 Order OTD       UC-17 Churn ML                         │
 │  UC-15 Markets         UC-18 Credit risk                      │
 │                                                               │
 │  📊 FINANCE            ✅ GOVERNANCE & ESG                     │
 │  UC-21 P&L             UC-23 Data quality                     │
 │  UC-22 Working capital UC-24 Carbon / ESG                     │
 └───────────────────────────────────────────────────────────────┘
```

---

## Configuration Reference

All infrastructure parameters are centralised in `environment_config.toml`:

| Section | Key settings |
|---------|-------------|
| Root | `PROJECT_ID`, `LOCATION`, `RAW_BUCKET_NAME` |
| `[STORAGE]` | All 6 GCS bucket names |
| `[BIGQUERY_SETTINGS]` | Dataset names: ext, stg, dwh, mgmt |
| `[DATAPLEX_SETTINGS]` | Lake, zones, DQ scan schedule, policy taxonomy |
| `[DATAFUSION_SETTINGS]` | Instance name, region, pipeline names |
| `[COMPOSER_SETTINGS]` | Airflow version, environment name, alert email |
| `[VERTEX_AI_SETTINGS]` | Pipeline root, model name, endpoint |
| `[IAM_SETTINGS]` | Domain, group names, service account names |
| `[DATA_FILES]` | Local CSV → GCS path mapping |


## Component Dependency Order

```
01_iam_and_security
      │
      └──► 02_gcs_storage
                │
                └──► 03_bigquery_datasets
                          │
                          └──► 04_data_fusion_pipelines
                                    │
                          ┌─────────┴──────────┐
                          ▼                    ▼
                   05_vertex_ai         08_BI_Use_Cases
                          │                    │
                          └─────────┬──────────┘
                                    ▼
                             07_bi_dashboards
                                    │
                                    ▼
                              06_monitoring
```
