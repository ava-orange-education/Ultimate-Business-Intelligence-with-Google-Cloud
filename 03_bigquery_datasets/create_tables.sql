-- ============================================================
-- NordLeaf BigQuery - All Core Table Definitions
-- Run after creating datasets
-- ============================================================

-- ============================================================
-- EXT LAYER - Pulls data from the Cloud Bucket
-- ============================================================
CREATE OR REPLACE EXTERNAL TABLE `nordleaf_ext.orders_ext` (
  order_id       STRING, customer_name STRING, order_date DATE,
  delivery_date  DATE, product_type STRING, quantity_tons NUMERIC,
  unit_price_eur NUMERIC, mill_origin STRING, status STRING, region STRING
)
OPTIONS (format='CSV', uris=['gs://nordleaf-raw-data/orders/*.csv'], skip_leading_rows=1);

CREATE OR REPLACE EXTERNAL TABLE `nordleaf_ext.suppliers_ext` (
  supplier_id STRING, supplier_name STRING, country STRING,
  supplier_type STRING, contact_email STRING, payment_terms STRING,
  annual_contract_eur NUMERIC, active BOOL, rating STRING
)
OPTIONS (format='CSV', uris=['gs://nordleaf-raw-data/suppliers/*.csv'], skip_leading_rows=1);

CREATE OR REPLACE EXTERNAL TABLE `nordleaf_ext.deliveries_ext` (
  delivery_id STRING, supplier_id STRING, supplier_name STRING, delivery_date DATE,
  material_type STRING, quantity_tons FLOAT64, quality_grade STRING,
  mill_destination STRING, truck_id STRING, invoice_eur FLOAT64
)
OPTIONS (format='CSV', uris=['gs://nordleaf-raw-data/deliveries/*.csv'], skip_leading_rows=1);

-- ============================================================
-- STG LAYER - Setup of Schemas 
-- ============================================================
CREATE TABLE IF NOT EXISTS `nordleaf_stg.orders_raw` (
  order_id STRING, customer_name STRING, order_date DATE, delivery_date DATE,
  product_type STRING, quantity_tons NUMERIC, unit_price_eur NUMERIC,
  mill_origin STRING, status STRING, region STRING, source_file STRING,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `nordleaf_stg.suppliers` (
  supplier_id STRING, supplier_name STRING, country STRING,
  supplier_type STRING, contact_email STRING, payment_terms STRING,
  annual_contract_eur NUMERIC, active BOOL, rating STRING,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `nordleaf_stg.deliveries` (
  delivery_id STRING, supplier_id STRING, supplier_name STRING, delivery_date DATE,
  material_type STRING, quantity_tons FLOAT64, quality_grade STRING,
  mill_destination STRING, truck_id STRING, invoice_eur FLOAT64,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `nordleaf_stg.production_batches` (
  batch_id STRING, mill_id STRING, mill_name STRING, production_date DATE,
  product_type STRING, planned_tons FLOAT64, actual_tons FLOAT64,
  efficiency_pct FLOAT64, quality_grade STRING, shift STRING, supervisor STRING,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `nordleaf_stg.customer_feedback` (
  feedback_id STRING, customer_name STRING, order_id STRING, feedback_date DATE,
  channel STRING, rating INT64, feedback_text STRING, language STRING, product_type STRING,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `nordleaf_stg.pipeline_watermarks` (
  table_name STRING NOT NULL, last_loaded TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- ============================================================
-- DWH LAYER - Dimensions - Schema definitions
-- ============================================================
CREATE TABLE IF NOT EXISTS `nordleaf_dwh.dim_date` (
  date_id INT64, full_date DATE, year INT64, quarter INT64, month INT64,
  month_name STRING, week_of_year INT64, day_of_month INT64, day_of_week INT64,
  day_name STRING, is_weekend BOOL, is_holiday BOOL, holiday_name STRING,
  is_working_day BOOL, fiscal_year INT64, fiscal_quarter INT64
);

CREATE TABLE IF NOT EXISTS `nordleaf_dwh.dim_customer` (
  customer_key INT64, customer_name STRING NOT NULL, country STRING,
  region STRING, customer_type STRING, credit_rating STRING,
  is_active BOOL DEFAULT TRUE, valid_from DATE, valid_to DATE, is_current BOOL DEFAULT TRUE
) CLUSTER BY customer_type, country;

CREATE TABLE IF NOT EXISTS `nordleaf_dwh.dim_product` (
  product_key INT64, product_type STRING NOT NULL, product_category STRING,
  grammage_gsm INT64, width_mm INT64, coating STRING, is_recycled BOOL,
  certifications STRING, is_active BOOL DEFAULT TRUE
) CLUSTER BY product_category;

CREATE TABLE IF NOT EXISTS `nordleaf_dwh.dim_mill` (
  mill_key INT64, mill_id STRING NOT NULL, mill_name STRING, city STRING,
  country STRING DEFAULT 'Finland', latitude FLOAT64, longitude FLOAT64,
  capacity_tons_day INT64, founded_year INT64, certifications STRING, is_active BOOL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS `nordleaf_dwh.dim_supplier` (
  supplier_key INT64, supplier_id STRING NOT NULL, supplier_name STRING,
  country STRING, supplier_type STRING, payment_terms STRING,
  annual_contract_eur NUMERIC, rating STRING, is_active BOOL DEFAULT TRUE
) CLUSTER BY supplier_type, country;

-- ============================================================
-- DWH LAYER - Facts - Schema definitions
-- ============================================================
CREATE TABLE IF NOT EXISTS `nordleaf_dwh.fact_orders` (
  order_id STRING NOT NULL, customer_key INT64, product_key INT64, mill_key INT64,
  order_date_id INT64, delivery_date_id INT64,
  quantity_tons NUMERIC, unit_price_eur NUMERIC, total_value_eur NUMERIC,
  lead_time_days INT64, status STRING, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY RANGE_BUCKET(order_date_id, GENERATE_ARRAY(20200101, 20310101, 10000))
CLUSTER BY product_key, mill_key;

CREATE TABLE IF NOT EXISTS `nordleaf_dwh.fact_production` (
  batch_id STRING NOT NULL, mill_id STRING, production_date DATE, product_type STRING,
  planned_tons FLOAT64, actual_tons FLOAT64, efficiency_pct FLOAT64, quality_grade STRING, shift STRING
)
PARTITION BY production_date
CLUSTER BY mill_id, product_type;

CREATE TABLE IF NOT EXISTS `nordleaf_dwh.fact_deliveries` (
  delivery_id STRING NOT NULL, supplier_id STRING, mill_destination STRING, delivery_date DATE,
  material_type STRING, quantity_tons FLOAT64, quality_grade STRING, invoice_eur FLOAT64,
  loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY delivery_date
CLUSTER BY supplier_id, mill_destination;

-- ============================================================
-- MGMT LAYER - Pipeline and DQ Logs
-- ============================================================
CREATE TABLE IF NOT EXISTS `nordleaf_mgmt.pipeline_log` (
  log_id STRING DEFAULT GENERATE_UUID(), dag_run_id STRING, task_name STRING,
  status STRING, rows_affected INT64, duration_sec FLOAT64, error_message STRING,
  logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS `nordleaf_mgmt.dq_results` (
  dq_run_id STRING DEFAULT GENERATE_UUID(), run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
  table_name STRING, check_name STRING, check_type STRING, column_name STRING,
  rows_checked INT64, rows_failed INT64, pass_threshold FLOAT64, pass_rate FLOAT64,
  status STRING, details STRING
);

CREATE TABLE IF NOT EXISTS `nordleaf_mgmt.deployments` (
  deployment_id STRING DEFAULT GENERATE_UUID(), build_id STRING, environment STRING,
  deployed_by STRING, git_commit_sha STRING, git_branch STRING,
  objects_deployed INT64, status STRING, deployed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
