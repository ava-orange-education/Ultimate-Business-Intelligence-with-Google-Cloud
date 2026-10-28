# NordLeaf GCS Storage Architecture

## Bucket Structure

```
gs://nordleaf-raw-data/            [STANDARD storage, EU region]
  |-- orders/                      CSV daily order files
  |-- suppliers/                   Supplier master data
  |-- deliveries/                  JSON delivery manifests
  |-- production/                  Production batch JSON
  |-- sensors/                     IoT sensor events
  |-- feedback/                    Customer feedback CSV

gs://nordleaf-processed-data/      [STANDARD storage, EU region]
  |-- dim_date/                    Processed dimension tables (Parquet)
  |-- dim_customer/                Customer dimension (Parquet)
  |-- fact_orders/                 Processed fact orders (Parquet)

gs://nordleaf-archive/             [COLDLINE after 90 days, auto-delete after 7 years]
  |-- orders/YYYY/MM/              Historical order archives
  |-- production/YYYY/MM/          Historical production data

gs://nordleaf-composer-bucket/     [Airflow DAG storage]
  |-- dags/                        Airflow DAG Python files
  |-- plugins/                     Airflow plugins

gs://nordleaf-dataflow-temp/       [Dataflow staging & temp]
gs://nordleaf-vertex-ai/           [ML model artifacts, training data]
```

## Event Notifications (Auto-ingest trigger)

```
New file uploaded to gs://nordleaf-raw-data/orders/
         |
         | GCS Object Finalize event
         v
Pub/Sub topic: nordleaf-gcs-orders-events
         |
         | Push subscription
         v
Cloud Function: trigger_bq_load
         |
         | BigQuery Load Job
         v
nordleaf_stg.orders_raw
```

## Setup - Substitute your-project with your actual GCP project. 
```bash
chmod +x setup_gcs.sh
./setup_gcs.sh your-project 
```
