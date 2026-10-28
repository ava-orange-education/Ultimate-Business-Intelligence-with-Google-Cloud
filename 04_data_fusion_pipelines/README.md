# NordLeaf Cloud Data Fusion Pipelines

## Overview

Cloud Data Fusion provides a **visual, no-code/low-code ETL pipeline builder** for NordLeaf data ingestion and transformation. Pipelines are defined as JSON and run on managed Apache Spark clusters.

## Architecture

```
NordLeaf Source Systems
    |
    | (GCS file drop / SFTP / Streaming)
    v
Data Fusion Studio (GUI Pipeline Builder)
    |
    |-- nordleaf-suppliers-etl    (daily 05:00 EET)
    |     GCS CSV -> Validate -> STG + DWH dim_supplier
    |
    |-- nordleaf-orders-batch-etl (daily 06:00 EET)
    |     GCS CSV -> Transform -> Dim Lookup -> STG + DWH fact_orders
    |
    |-- nordleaf-production-etl   (daily 04:00 EET)
          GCS JSON -> Flatten -> Quality Check -> DWH fact_production
    |
    v
BigQuery: nordleaf_stg -> nordleaf_dwh
```

## Pipeline Components Used

| Plugin | Purpose |
|--------|---------|
| GCSFile (batchsource) | Read CSV/JSON from Cloud Storage |
| BigQueryTable (batchsource) | Read dimension tables for lookup joins |
| Wrangler (transform) | Data cleansing, type casting, field derivations |
| JavaScript (transform) | Custom validation logic |
| Deduplicate (transform) | Remove duplicate records by key |
| Joiner (batchjoiner) | Lookup dimension keys (mill_key, product_key) |
| BigQueryTable (batchsink) | Write to staging and DWH tables |

## Steps to Deploy

### 1. Create Data Fusion Instance
```bash
chmod +x setup_data_fusion.sh
./setup_data_fusion.sh your-project
```

### 2. Deploy Pipelines via API - Adjust location as per your need
```bash
# Get instance URL
INSTANCE_URL=$(gcloud beta data-fusion instances describe nordleaf-datafusion \
  --location=europe-west1 --format="value(apiEndpoint)")

# Deploy supplier ETL pipeline
curl -X PUT "${INSTANCE_URL}/v3/namespaces/default/apps/nordleaf-suppliers-etl" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d @nordleaf_suppliers_etl_pipeline.json

# Deploy orders ETL pipeline
curl -X PUT "${INSTANCE_URL}/v3/namespaces/default/apps/nordleaf-orders-batch-etl" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)" \
  -H "Content-Type: application/json" \
  -d @nordleaf_orders_etl_pipeline.json
```

### 3. Trigger Pipeline Run
```bash
curl -X POST "${INSTANCE_URL}/v3/namespaces/default/apps/nordleaf-orders-batch-etl/workflows/DataPipelineWorkflow/start" \
  -H "Authorization: Bearer $(gcloud auth print-access-token)"
```

## Data Fusion

| Concept | Snowflake | Data Fusion |
|---------|-----------|-------------|
| ETL Tool | Data Fusion Studio pipelines |
| Scheduling | Data Fusion Scheduler + Composer |
| Data validation  | Wrangler + JavaScript transform |
| Dim key lookup | Joiner plugin |
| Error handling  | Error sink |
| Monitoring  | Data Fusion pipeline runs UI |
