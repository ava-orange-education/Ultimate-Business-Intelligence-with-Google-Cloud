# NordLeaf — GCP Data Platform

NordLeaf is a vertically integrated Finnish paper manufacturer. This repository contains the full GCP-based data engineering and analytics platform — from infrastructure setup to 24 real-world BI use cases.

**Company profile:**
- Owns forests and manages timber supply (Metsä Forest, UPM Silvesta, Pölkky)
- Operates 5 mills across Finland (Tampere, Oulu, Turku, Espoo, Jyväskylä)
- Sells to 18 EU B2B customers (publishers, packagers, retailers)
- Reports ESG metrics under EU Green Deal and EUDR requirements

---

## Repository Structure

```
Nordleaf/
├── 01_iam_and_security/          # IAM roles, service accounts, column-level security
├── 02_gcs_storage/               # GCS bucket layout and auto-ingest triggers
├── 03_bigquery_datasets/         # BigQuery schema — ext, stg, dwh, mgmt datasets
├── 04_data_fusion_pipelines/     # Cloud Data Fusion ETL pipeline definitions (JSON + Python)
├── 05_vertex_ai/                 # Vertex AI + BigQuery ML models (forecast, churn, anomaly)
├── 06_monitoring/                # Cloud Monitoring dashboards and alert policies
├── 07_bi_dashboards/             # Looker Studio dashboard queries and setup guide
├── 08_BI_Use_Cases/              # 24 SQL use cases across 8 business domains
│   ├── 01_forest_and_fibre/
│   ├── 02_production/
│   ├── 03_supply_chain/
│   ├── 04_sales/
│   ├── 05_customer/
│   ├── 06_iot_and_operations/
│   ├── 07_finance/
│   └── 08_governance_and_esg/
├── data/csv_data/                # Sample CSV/JSON datasets for local testing
├── environment_config.toml       # Central config — project ID, bucket names, dataset names
├── nordleaf_setup.py             # Python setup orchestrator
├── gcp_setup.py                  # GCP resource initialisation script
├── environment.yml               # Conda environment spec
└── requirements.txt              # Python package dependencies
```

---

## GCP Stack

| Layer | GCP Service | Purpose |
|-------|-------------|---------|
| Security | Cloud IAM + Dataplex | Role-based and column-level access control |
| Data Lake | Cloud Storage (GCS) | Raw file ingestion and archival |
| Data Warehouse | BigQuery | Star schema analytics warehouse |
| ETL | Cloud Data Fusion | Visual no-code/low-code batch pipelines |
| Orchestration | Cloud Composer (Airflow) | Pipeline scheduling and DAG management |
| ML / AI | Vertex AI + BigQuery ML | Demand forecasting, churn, anomaly detection |
| BI | Looker Studio | Dashboards, reports, scheduled email delivery |
| Observability | Cloud Monitoring + Logging | Alerting, pipeline health, cost tracking |
| Governance | Dataplex | Data cataloguing and quality scanning |

---

## Setup Order

Follow folders in sequence:

```
Step 1 — 01_iam_and_security     Create service accounts and IAM bindings
Step 2 — 02_gcs_storage          Create GCS buckets and event notification topics
Step 3 — 03_bigquery_datasets    Create datasets and DDL tables
Step 4 — 04_data_fusion_pipelines Deploy ETL pipelines and schedule runs
Step 5 — data/csv_data           Upload sample data to GCS for pipeline testing
Step 6 — 05_vertex_ai            Train and deploy ML models
Step 7 — 06_monitoring           Set up dashboards and alert policies
Step 8 — 07_bi_dashboards        Connect Looker Studio to BigQuery
Step 9 — 08_BI_Use_Cases         Run SQL use cases against the populated warehouse
```

---

## Quick Start

### 1. Configure your environment

Edit `environment_config.toml` and set your GCP project:

```toml
PROJECT_ID = "your-gcp-project-id"
LOCATION   = "europe-west1"
```

### 2. Set up infrastructure

```bash
# Install dependencies
pip install -r requirements.txt

# Run the setup orchestrator (reads environment_config.toml)
python nordleaf_setup.py
```

Or run each shell script individually in order:

```bash
./01_iam_and_security/setup_iam.sh        your-project
./02_gcs_storage/setup_gcs.sh             your-project
./03_bigquery_datasets/create_all_datasets.sh
./04_data_fusion_pipelines/setup_data_fusion.sh  your-project
./06_monitoring/setup_monitoring.sh       your-project
```

### 3. Run an analytics query

```bash
bq query --use_legacy_sql=false < 08_BI_Use_Cases/04_sales/UC11_revenue_margin_analysis.sql
```

---

## BigQuery Data Model

```
nordleaf_ext    External tables backed by GCS (zero storage cost)
nordleaf_stg    Staging layer — raw ingest with load metadata
nordleaf_dwh    Analytics layer — star schema, partitioned + clustered
nordleaf_mgmt   Operations layer — pipeline logs, DQ results, deployments
```

**Dimension tables:** `dim_customer`, `dim_product`, `dim_mill`, `dim_date`
**Fact tables:** `fact_orders`, `fact_production`, `fact_deliveries`

---

## BI Use Cases (24 total)

| Domain | Use Cases |
|--------|-----------|
| Forest & Fibre | Timber harvest balance, sustainable sourcing (FSC/PEFC) |
| Production | Mill OEE, quality root cause, shift performance, S&OP |
| Supply Chain | Supplier scorecard, material cost, delivery OTD, supply risk |
| Sales | Revenue & margin, demand forecast, order fulfilment, market penetration |
| Customer | Customer CLV, NPS tracking, churn risk (ML), credit segmentation |
| IoT & Operations | Sensor anomaly detection (ML), energy & ESG KPIs |
| Finance | Integrated P&L, working capital & payment terms |
| Governance & ESG | Data quality & pipeline health, carbon footprint reporting |

See `08_BI_Use_Cases/README.md` for the full index with priorities and dashboard assignments.

---

## ML Models

| Model | Algorithm | Schedule | Target metric |
|-------|-----------|----------|---------------|
| Demand forecast | ARIMA_PLUS (BQML) | Weekly — Sunday 02:00 EET | MAPE < 20% |
| Churn risk | Logistic Regression | Weekly | 90-day inactivity flag |
| Sensor anomaly | Autoencoder | Real-time | Alert flag per sensor |

---

## Monitoring & Alerts

| Alert | Threshold | Severity |
|-------|-----------|---------|
| Composer DAG failure | Any failure | P1 — Email + Slack + PagerDuty |
| Data freshness | > 25 hours late | P2 — Email + Slack |
| Data quality score | < 95% | P2 — Email + Slack |
| BigQuery cost | > EUR 200/day | P3 — Email |
| Sensor spike | > 10 alerts in 5 min | P1 — Slack + PagerDuty |

---

## Secrets & Credentials

`secrets.toml` is listed in `.gitignore` and must **never** be committed.
See `01_iam_and_security/service_account_keys_guide.md` for best practices on managing GCP credentials locally.

---

## For Book Readers

This repository is structured as a learning companion. Each folder maps to a chapter sequence

