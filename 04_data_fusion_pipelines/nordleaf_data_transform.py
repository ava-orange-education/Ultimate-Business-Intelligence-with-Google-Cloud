#!/usr/bin/env python3
"""
nordleaf_data_transform.py – STG → DWH transformation layer.

Runs on top of nordleaf_stg data already loaded by nordleaf_etl.py.
Replicates what previously ran inside Cloud Data Fusion + Cloud Composer:

  Data Fusion                          │  This script
  ─────────────────────────────────────┼──────────────────────────────────────
  DWH-Fact-Orders-Sink (Joiner output) │  merge_fact_orders()
  DWH-BigQuery-Sink   (dim_supplier)   │  already in nordleaf_etl.py (kept there)
  (no deliveries pipeline existed)     │  merge_fact_deliveries()   ← new
  (no production DWH write existed)    │  merge_fact_production()   ← new

  Airflow nordleaf_master_dag          │  This script
  ─────────────────────────────────────┼──────────────────────────────────────
  merge_fact_orders   (Phase 1a)       │  merge_fact_orders()
  merge_fact_production (Phase 1b)     │  merge_fact_production()
  run_dq_scan         (Phase 2)        │  run_dq_checks()
  log_pipeline_run    (Phase 3)        │  log_pipeline()
  (no dim population in DAG)           │  run_dims()                ← new
  (no deliveries in DAG)               │  merge_fact_deliveries()   ← new

  Airflow nordleaf_dq_dag              │  This script
  ─────────────────────────────────────┼──────────────────────────────────────
  run_dq_checks (every 6 h)           │  --pipeline dq

Pipelines
─────────
  dims        Merge dim_customer, dim_product, dim_mill from STG data
  orders      nordleaf_stg.orders_raw        → nordleaf_dwh.fact_orders
  production  nordleaf_stg.production_batches → nordleaf_dwh.fact_production
  deliveries  nordleaf_stg.deliveries        → nordleaf_dwh.fact_deliveries
  dq          DQ checks on fact_orders       → nordleaf_mgmt.dq_results
  all         dims → orders + production + deliveries → dq → pipeline_log

Usage
─────
  python nordleaf_data_transform.py all
  python nordleaf_data_transform.py orders --date 2026-03-03
  python nordleaf_data_transform.py dq --fail-on-dq-error
  python nordleaf_data_transform.py all --full-refresh
"""

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import date
from pathlib import Path

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# ── TOML parser (stdlib 3.11+, else pip install tomli) ───────────────────────
try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib          # type: ignore[no-redef]
    except ModuleNotFoundError:
        sys.exit("tomllib/tomli not found.  Run: pip install tomli")


# ─────────────────────────────────────────────────────────────────────────────
# Config  –  read from environment_config.toml + secrets.toml
# Script lives at <project_root>/06_data_fusion_pipelines/
# ─────────────────────────────────────────────────────────────────────────────

def _load_config() -> tuple[dict, dict]:
    root = Path(__file__).parent.parent          # project root

    cfg_path = root / "environment_config.toml"
    if not cfg_path.exists():
        sys.exit(f"environment_config.toml not found at {cfg_path}")
    with open(cfg_path, "rb") as f:
        cfg = tomllib.load(f)

    sec_path = root / "secrets.toml"
    secrets: dict = {}
    if sec_path.exists():
        with open(sec_path, "rb") as f:
            secrets = tomllib.load(f)

    return cfg, secrets


_cfg, _secrets = _load_config()

PROJECT_ID  = _cfg["PROJECT_ID"]
BQ_LOCATION = _cfg.get("BIGQUERY_SETTINGS", {}).get("location", "EU")
DS_STG      = _cfg.get("BIGQUERY_SETTINGS", {}).get("dataset_stg",  "nordleaf_stg")
DS_DWH      = _cfg.get("BIGQUERY_SETTINGS", {}).get("dataset_dwh",  "nordleaf_dwh")
DS_MGMT     = _cfg.get("BIGQUERY_SETTINGS", {}).get("dataset_mgmt", "nordleaf_mgmt")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("nordleaf_transform")


# ─────────────────────────────────────────────────────────────────────────────
# BigQuery client
# ─────────────────────────────────────────────────────────────────────────────

def _make_client(project_id: str) -> bigquery.Client:
    """
    Build BQ client.
    Auth priority:
      1. secrets.toml  [GCP] service_account_key_path
      2. GOOGLE_APPLICATION_CREDENTIALS env var
      3. Application Default Credentials (ADC)
    """
    scopes = ["https://www.googleapis.com/auth/cloud-platform"]
    key_path_str = (
        _secrets.get("GCP", {}).get("service_account_key_path", "").strip()
        or os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    )
    creds = None
    if key_path_str:
        key_path = Path(key_path_str).expanduser().resolve()
        if key_path.exists():
            creds = service_account.Credentials.from_service_account_file(
                str(key_path), scopes=scopes
            )
            log.info("Auth: service account → %s", creds.service_account_email)
        else:
            log.warning("Key file not found: %s – falling back to ADC", key_path)
    if creds is None:
        log.info("Auth: Application Default Credentials (ADC)")
    return bigquery.Client(project=project_id, credentials=creds,
                           location=BQ_LOCATION)


# ─────────────────────────────────────────────────────────────────────────────
# SQL templates
# All queries are parameterised with {project}, {stg}, {dwh} so that
# dataset names are always sourced from environment_config.toml.
#
# The fact_orders SQL is taken verbatim from nordleaf_master_dag._SRC_ORDERS_SQL
# (the USING clause of the original Data Fusion Joiner-Dim-Lookup).
# ─────────────────────────────────────────────────────────────────────────────

# ── fact_orders source (mirrors Airflow merge_fact_orders + Data Fusion Joiner)
_SRC_ORDERS_SQL = """
SELECT
  o.order_id,
  COALESCE(c.customer_key, -1)                          AS customer_key,
  COALESCE(p.product_key,  -1)                          AS product_key,
  COALESCE(m.mill_key,     -1)                          AS mill_key,
  CAST(FORMAT_DATE('%Y%m%d', o.order_date)    AS INT64) AS order_date_id,
  CAST(FORMAT_DATE('%Y%m%d', o.delivery_date) AS INT64) AS delivery_date_id,
  o.quantity_tons,
  o.unit_price_eur,
  ROUND(o.quantity_tons * o.unit_price_eur, 2)          AS total_value_eur,
  DATE_DIFF(o.delivery_date, o.order_date, DAY)         AS lead_time_days,
  o.status
FROM `{project}.{stg}.orders_raw` o
LEFT JOIN `{project}.{dwh}.dim_customer` c
       ON o.customer_name = c.customer_name AND c.is_current = TRUE
LEFT JOIN `{project}.{dwh}.dim_product`  p ON o.product_type = p.product_type
LEFT JOIN `{project}.{dwh}.dim_mill`     m ON o.mill_origin  = m.mill_name
{date_filter}
"""

# ── fact_production source (mirrors Airflow merge_fact_production)
_SRC_PRODUCTION_SQL = """
SELECT
  batch_id, mill_id, production_date, product_type,
  planned_tons, actual_tons, efficiency_pct, quality_grade, shift
FROM `{project}.{stg}.production_batches`
{date_filter}
"""

# ── fact_deliveries source (new pipeline; table exists in create_tables.sql)
_SRC_DELIVERIES_SQL = """
SELECT
  delivery_id, supplier_id, mill_destination, delivery_date,
  material_type, quantity_tons, quality_grade, invoice_eur, loaded_at
FROM `{project}.{stg}.deliveries`
{date_filter}
"""

# ── dim_customer: derived from orders_raw (SCD Type 1 – customer_name key)
_SRC_DIM_CUSTOMER_SQL = """
SELECT
  customer_name,
  MAX(region) AS region
FROM `{project}.{stg}.orders_raw`
WHERE customer_name IS NOT NULL
GROUP BY customer_name
"""

# ── dim_product: derived from orders_raw (SCD Type 1 – product_type key)
_SRC_DIM_PRODUCT_SQL = """
SELECT DISTINCT product_type
FROM `{project}.{stg}.orders_raw`
WHERE product_type IS NOT NULL
"""

# ── dim_mill: derived from production_batches (SCD Type 1 – mill_id key)
_SRC_DIM_MILL_SQL = """
SELECT
  mill_id,
  MAX(mill_name) AS mill_name
FROM `{project}.{stg}.production_batches`
WHERE mill_id IS NOT NULL
GROUP BY mill_id
"""

# ── DQ checks (mirrors nordleaf_master_dag._DQ_CHECKS_SQL and
#    nordleaf_dq_dag._DQ_CHECKS_SQL exactly)
_DQ_CHECKS_SQL = """
WITH checks AS (
  SELECT
    COUNT(*)                                                               AS total,
    COUNTIF(order_id IS NULL)                                             AS null_ids,
    COUNTIF(quantity_tons <= 0)                                           AS bad_qty,
    COUNTIF(unit_price_eur <= 0)                                          AS bad_price,
    COUNTIF(status NOT IN ('DELIVERED','IN_TRANSIT','PENDING','CANCELLED')) AS bad_status,
    COUNT(*) - COUNT(DISTINCT order_id)                                   AS duplicates
  FROM `{project}.{dwh}.fact_orders`
)
SELECT
  '{dwh}.fact_orders'                                                     AS table_name,
  c.check_name,
  c.check_type,
  c.col_name                                                              AS column_name,
  checks.total                                                            AS rows_checked,
  c.failed                                                                AS rows_failed,
  99.0                                                                    AS pass_threshold,
  ROUND((checks.total - c.failed) / NULLIF(checks.total, 0) * 100, 2)   AS pass_rate,
  IF((checks.total - c.failed) / NULLIF(checks.total, 0) >= 0.99,
     'PASS', 'FAIL')                                                      AS status,
  CONCAT('Failed: ', CAST(c.failed AS STRING))                           AS details
FROM checks
CROSS JOIN UNNEST([
  STRUCT('null_order_id'   AS check_name, 'NULL_CHECK'  AS check_type,
         'order_id'        AS col_name,   null_ids       AS failed),
  STRUCT('qty_positive'    AS check_name, 'RANGE'       AS check_type,
         'quantity_tons'   AS col_name,   bad_qty        AS failed),
  STRUCT('price_positive'  AS check_name, 'RANGE'       AS check_type,
         'unit_price_eur'  AS col_name,   bad_price      AS failed),
  STRUCT('status_valid'    AS check_name, 'ENUM'        AS check_type,
         'status'          AS col_name,   bad_status     AS failed),
  STRUCT('order_id_unique' AS check_name, 'UNIQUENESS'  AS check_type,
         'order_id'        AS col_name,   duplicates     AS failed)
]) AS c
"""


# ─────────────────────────────────────────────────────────────────────────────
# I/O helpers
# ─────────────────────────────────────────────────────────────────────────────

def _read_table(bq: bigquery.Client, fq_table: str) -> pd.DataFrame:
    """Read a full BigQuery table into a DataFrame."""
    return bq.query(f"SELECT * FROM `{fq_table}`").to_dataframe()


def _write_table(bq: bigquery.Client, df: pd.DataFrame,
                 fq_table: str, disposition: str = "WRITE_TRUNCATE") -> None:
    """Write a DataFrame to BigQuery (CREATE_IF_NEEDED + chosen disposition)."""
    job = bq.load_table_from_dataframe(
        df, fq_table,
        job_config=bigquery.LoadJobConfig(
            write_disposition=disposition,
            create_disposition="CREATE_IF_NEEDED",
        ),
    )
    job.result()
    log.info("Wrote %d rows → %s (%s)", len(df), fq_table, disposition)


def _merge(tgt_df: pd.DataFrame, src_df: pd.DataFrame,
           key: str, update_cols: list[str]) -> pd.DataFrame:
    """
    In-memory MERGE equivalent (mirrors the pandas pattern in Airflow
    merge_fact_orders / merge_fact_production):
      WHEN MATCHED     → UPDATE update_cols from src
      WHEN NOT MATCHED → INSERT from src
    Returns the full merged DataFrame ready for WRITE_TRUNCATE.
    """
    if tgt_df.empty:
        return src_df

    matched_mask = src_df[key].isin(tgt_df[key])
    updates      = src_df.loc[matched_mask, [key] + update_cols].set_index(key)
    tgt          = tgt_df.set_index(key)
    tgt.update(updates)
    tgt          = tgt.reset_index()
    result       = pd.concat([tgt, src_df[~matched_mask]], ignore_index=True)

    log.info("_merge on '%s': %d updated, %d inserted",
             key, matched_mask.sum(), (~matched_mask).sum())
    return result


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline management  →  nordleaf_mgmt.pipeline_log
# Mirrors Airflow nordleaf_master_dag.log_pipeline_run()
# ─────────────────────────────────────────────────────────────────────────────

def log_pipeline(bq: bigquery.Client, project_id: str, run_id: str,
                 task_name: str, status: str,
                 rows: int, duration_sec: float,
                 error_msg: str = "") -> None:
    """Append one row to nordleaf_mgmt.pipeline_log."""
    df = pd.DataFrame([{
        "log_id":        str(uuid.uuid4()),
        "dag_run_id":    run_id,
        "task_name":     task_name,
        "status":        status,
        "rows_affected": rows,
        "duration_sec":  round(duration_sec, 2),
        "error_message": error_msg,
        "logged_at":     pd.Timestamp.utcnow(),
    }])
    _write_table(bq, df, f"{project_id}.{DS_MGMT}.pipeline_log",
                 disposition="WRITE_APPEND")
    log.info("pipeline_log: %s – %s (%.1fs, %d rows)",
             task_name, status, duration_sec, rows)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 0 – Dimension merges  (STG → DWH dim_*)
#
# Must run BEFORE fact pipelines so that key lookups in _SRC_ORDERS_SQL
# resolve correctly.  Data Fusion previously sourced dim_mill and dim_product
# from existing BQ tables via Dim-Mill-Source / Dim-Product-Source plugins;
# this script derives them from STG data instead (no separate dim pipeline
# existed in Data Fusion for customer, product, or mill).
# ─────────────────────────────────────────────────────────────────────────────

def _assign_surrogate_keys(src_df: pd.DataFrame,
                           tgt_df: pd.DataFrame,
                           natural_key: str,
                           surrogate_key: str) -> pd.DataFrame:
    """
    Preserve surrogate keys for existing rows; assign new incremental keys
    to rows that do not exist in the current dim table.
    """
    src_df = src_df.copy()
    if not tgt_df.empty and natural_key in tgt_df.columns and surrogate_key in tgt_df.columns:
        key_map = tgt_df.set_index(natural_key)[surrogate_key].to_dict()
        src_df[surrogate_key] = src_df[natural_key].map(key_map)
        max_key = int(tgt_df[surrogate_key].max())
    else:
        src_df[surrogate_key] = pd.NA
        max_key = 0

    needs_key = src_df[surrogate_key].isna()
    new_keys  = range(max_key + 1, max_key + 1 + int(needs_key.sum()))
    src_df.loc[needs_key, surrogate_key] = list(new_keys)
    src_df[surrogate_key] = src_df[surrogate_key].astype("Int64")
    return src_df


def merge_dim_customer(bq: bigquery.Client, project_id: str) -> int:
    """
    Derives dim_customer from nordleaf_stg.orders_raw.
    SCD Type 1 – natural key: customer_name.
    Full schema (matches create_tables.sql + CLUSTER BY customer_type, country):
      customer_key, customer_name, country, region, customer_type,
      credit_rating, is_active, valid_from, valid_to, is_current
    Columns not available from STG are initialised to None so BQ accepts the write.
    """
    log.info("── merge_dim_customer START ──")
    src_df = bq.query(
        _SRC_DIM_CUSTOMER_SQL.format(project=project_id, stg=DS_STG)
    ).to_dataframe()
    if src_df.empty:
        log.warning("No customer data in STG – skipping dim_customer")
        return 0

    tgt_df = _read_table(bq, f"{project_id}.{DS_DWH}.dim_customer")
    src_df = _assign_surrogate_keys(src_df, tgt_df, "customer_name", "customer_key")

    # Populate all schema columns (cluster cols country/customer_type must be present)
    src_df["country"]       = None
    src_df["customer_type"] = None
    src_df["credit_rating"] = None
    src_df["is_active"]     = True
    src_df["valid_from"]    = pd.Timestamp(date.today())
    src_df["valid_to"]      = None
    src_df["is_current"]    = True

    update_cols = [c for c in src_df.columns if c != "customer_name"]
    result = _merge(tgt_df, src_df, "customer_name", update_cols)
    _write_table(bq, result, f"{project_id}.{DS_DWH}.dim_customer")
    log.info("── merge_dim_customer COMPLETE: %d rows ──", len(result))
    return len(result)


def merge_dim_product(bq: bigquery.Client, project_id: str) -> int:
    """
    Derives dim_product from nordleaf_stg.orders_raw.
    SCD Type 1 – natural key: product_type.
    Full schema (matches create_tables.sql + CLUSTER BY product_category):
      product_key, product_type, product_category, grammage_gsm, width_mm,
      coating, is_recycled, certifications, is_active
    Columns not available from STG are initialised to None/pd.NA.
    """
    log.info("── merge_dim_product START ──")
    src_df = bq.query(
        _SRC_DIM_PRODUCT_SQL.format(project=project_id, stg=DS_STG)
    ).to_dataframe()
    if src_df.empty:
        log.warning("No product data in STG – skipping dim_product")
        return 0

    tgt_df = _read_table(bq, f"{project_id}.{DS_DWH}.dim_product")
    src_df = _assign_surrogate_keys(src_df, tgt_df, "product_type", "product_key")

    # Populate all schema columns (cluster col product_category must be present)
    src_df["product_category"] = None
    src_df["grammage_gsm"]     = pd.NA
    src_df["width_mm"]         = pd.NA
    src_df["coating"]          = None
    src_df["is_recycled"]      = None
    src_df["certifications"]   = None
    src_df["is_active"]        = True

    update_cols = [c for c in src_df.columns if c != "product_type"]
    result = _merge(tgt_df, src_df, "product_type", update_cols)
    _write_table(bq, result, f"{project_id}.{DS_DWH}.dim_product")
    log.info("── merge_dim_product COMPLETE: %d rows ──", len(result))
    return len(result)


def merge_dim_mill(bq: bigquery.Client, project_id: str) -> int:
    """
    Derives dim_mill from nordleaf_stg.production_batches.
    SCD Type 1 – natural key: mill_id.
    Full schema (matches create_tables.sql):
      mill_key, mill_id, mill_name, city, country, latitude, longitude,
      capacity_tons_day, founded_year, certifications, is_active
    Columns not available from STG are initialised to None/pd.NA.
    """
    log.info("── merge_dim_mill START ──")
    src_df = bq.query(
        _SRC_DIM_MILL_SQL.format(project=project_id, stg=DS_STG)
    ).to_dataframe()
    if src_df.empty:
        log.warning("No mill data in STG – skipping dim_mill")
        return 0

    tgt_df = _read_table(bq, f"{project_id}.{DS_DWH}.dim_mill")
    src_df = _assign_surrogate_keys(src_df, tgt_df, "mill_id", "mill_key")

    # Populate all schema columns
    src_df["city"]              = None
    src_df["country"]           = "Finland"
    src_df["latitude"]          = pd.NA
    src_df["longitude"]         = pd.NA
    src_df["capacity_tons_day"] = pd.NA
    src_df["founded_year"]      = pd.NA
    src_df["certifications"]    = None
    src_df["is_active"]         = True

    update_cols = [c for c in src_df.columns if c != "mill_id"]
    result = _merge(tgt_df, src_df, "mill_id", update_cols)
    _write_table(bq, result, f"{project_id}.{DS_DWH}.dim_mill")
    log.info("── merge_dim_mill COMPLETE: %d rows ──", len(result))
    return len(result)


def run_dims(bq: bigquery.Client, project_id: str, run_id: str) -> dict[str, int]:
    """Run all three dimension merges and log each to pipeline_log."""
    results: dict[str, int] = {}
    for fn, label in [
        (merge_dim_customer, "merge_dim_customer"),
        (merge_dim_product,  "merge_dim_product"),
        (merge_dim_mill,     "merge_dim_mill"),
    ]:
        t0 = time.monotonic()
        try:
            rows = fn(bq, project_id)
            log_pipeline(bq, project_id, run_id, label, "SUCCESS",
                         rows, time.monotonic() - t0)
            results[label] = rows
        except Exception as exc:
            log_pipeline(bq, project_id, run_id, label, "FAILED",
                         0, time.monotonic() - t0, str(exc))
            raise
    return results


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1a – fact_orders  (STG orders_raw → DWH fact_orders)
#
# Mirrors Airflow nordleaf_master_dag.merge_fact_orders() and the
# Data Fusion DWH-Fact-Orders-Sink (Joiner output).
# WHEN MATCHED     → UPDATE status only (same as Airflow DAG)
# WHEN NOT MATCHED → INSERT
# ─────────────────────────────────────────────────────────────────────────────

def merge_fact_orders(bq: bigquery.Client, project_id: str,
                      run_date: str, full_refresh: bool = False) -> int:
    """
    Reads nordleaf_stg.orders_raw, resolves dim surrogate keys via BQ SQL,
    then merges into nordleaf_dwh.fact_orders.
    """
    log.info("══════ merge_fact_orders START (date=%s, full_refresh=%s) ══════",
             run_date, full_refresh)

    date_filter = ("" if full_refresh
                   else f"WHERE DATE(o.loaded_at) = '{run_date}'")
    sql = _SRC_ORDERS_SQL.format(
        project=project_id, stg=DS_STG, dwh=DS_DWH, date_filter=date_filter
    )
    src_df = bq.query(sql).to_dataframe()
    if src_df.empty:
        log.info("No orders to process for %s – skipping", run_date)
        return 0

    tgt_df = _read_table(bq, f"{project_id}.{DS_DWH}.fact_orders")
    # Mirror Airflow: MATCHED → update status only
    result = _merge(tgt_df, src_df, "order_id", ["status"])
    _write_table(bq, result, f"{project_id}.{DS_DWH}.fact_orders")

    log.info("══════ merge_fact_orders COMPLETE: %d rows ══════", len(result))
    return len(result)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1b – fact_production  (STG production_batches → DWH fact_production)
#
# Mirrors Airflow nordleaf_master_dag.merge_fact_production().
# WHEN MATCHED     → UPDATE actual_tons, efficiency_pct (same as Airflow DAG)
# WHEN NOT MATCHED → INSERT
# ─────────────────────────────────────────────────────────────────────────────

def merge_fact_production(bq: bigquery.Client, project_id: str,
                          run_date: str, full_refresh: bool = False) -> int:
    """
    Reads nordleaf_stg.production_batches and merges into
    nordleaf_dwh.fact_production.
    """
    log.info("══════ merge_fact_production START ══════")

    date_filter = ("" if full_refresh
                   else f"WHERE DATE(loaded_at) = '{run_date}'")
    sql = _SRC_PRODUCTION_SQL.format(
        project=project_id, stg=DS_STG, date_filter=date_filter
    )
    src_df = bq.query(sql).to_dataframe()
    if src_df.empty:
        log.info("No production batches to process – skipping")
        return 0

    tgt_df = _read_table(bq, f"{project_id}.{DS_DWH}.fact_production")
    # Mirror Airflow: MATCHED → update actual_tons, efficiency_pct
    result = _merge(tgt_df, src_df, "batch_id", ["actual_tons", "efficiency_pct"])
    _write_table(bq, result, f"{project_id}.{DS_DWH}.fact_production")

    log.info("══════ merge_fact_production COMPLETE: %d rows ══════", len(result))
    return len(result)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 1c – fact_deliveries  (STG deliveries → DWH fact_deliveries)
#
# New pipeline – fact_deliveries table exists in create_tables.sql but
# had no corresponding Data Fusion pipeline or Airflow task.
# WHEN MATCHED     → UPDATE quantity_tons, invoice_eur
# WHEN NOT MATCHED → INSERT
# ─────────────────────────────────────────────────────────────────────────────

def merge_fact_deliveries(bq: bigquery.Client, project_id: str,
                          run_date: str, full_refresh: bool = False) -> int:
    """
    Reads nordleaf_stg.deliveries and merges into nordleaf_dwh.fact_deliveries.
    """
    log.info("══════ merge_fact_deliveries START ══════")

    date_filter = ("" if full_refresh
                   else f"WHERE DATE(loaded_at) = '{run_date}'")
    sql = _SRC_DELIVERIES_SQL.format(
        project=project_id, stg=DS_STG, date_filter=date_filter
    )
    src_df = bq.query(sql).to_dataframe()
    if src_df.empty:
        log.info("No deliveries to process – skipping")
        return 0

    tgt_df = _read_table(bq, f"{project_id}.{DS_DWH}.fact_deliveries")
    result = _merge(tgt_df, src_df, "delivery_id", ["quantity_tons", "invoice_eur"])
    _write_table(bq, result, f"{project_id}.{DS_DWH}.fact_deliveries")

    log.info("══════ merge_fact_deliveries COMPLETE: %d rows ══════", len(result))
    return len(result)


# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 – DQ checks  →  nordleaf_mgmt.dq_results
#
# Mirrors both:
#   • Airflow nordleaf_master_dag.run_dq_scan()
#   • Airflow nordleaf_dq_dag.run_dq_checks()
# The same SQL is used in both DAGs; this script runs it once per invocation.
# ─────────────────────────────────────────────────────────────────────────────

def run_dq_checks(bq: bigquery.Client, project_id: str,
                  fail_on_error: bool = False) -> int:
    """
    Execute DQ checks via BQ SQL → DataFrame.
    Writes results to nordleaf_mgmt.dq_results (WRITE_APPEND).
    Returns count of FAIL checks (0 = all passed).
    """
    log.info("══════ run_dq_checks START ══════")

    # Mirrors Airflow check_orders_loaded: pre-check that fact_orders has data
    count_df = bq.query(
        f"SELECT COUNT(*) AS cnt FROM `{project_id}.{DS_DWH}.fact_orders`"
    ).to_dataframe()
    if int(count_df["cnt"].iloc[0]) == 0:
        log.warning("DQ PRE-CHECK: fact_orders is empty – DQ scan skipped")
        return 0

    sql   = _DQ_CHECKS_SQL.format(project=project_id, dwh=DS_DWH)
    dq_df = bq.query(sql).to_dataframe()

    # Prepend metadata columns that match the nordleaf_mgmt.dq_results schema
    dq_df.insert(0, "dq_run_id",     str(uuid.uuid4()))
    dq_df.insert(1, "run_timestamp", pd.Timestamp.utcnow())

    _write_table(bq, dq_df, f"{project_id}.{DS_MGMT}.dq_results",
                 disposition="WRITE_APPEND")
    log.info("Wrote %d DQ result rows → %s.dq_results", len(dq_df), DS_MGMT)

    failures = dq_df[dq_df["status"] == "FAIL"]
    if not failures.empty:
        msg = (f"DQ FAIL: {len(failures)} check(s) did not pass: "
               + ", ".join(failures["check_name"].tolist()))
        if fail_on_error:
            raise ValueError(msg)
        log.warning(msg)
    else:
        log.info("All %d DQ checks PASSED", len(dq_df))

    log.info("══════ run_dq_checks COMPLETE: %d/%d passed ══════",
             len(dq_df) - len(failures), len(dq_df))
    return len(failures)


# ─────────────────────────────────────────────────────────────────────────────
# CLI entrypoint
# ─────────────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NordLeaf Transform – STG → DWH (replicates Data Fusion + Airflow)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Pipelines
─────────
  dims        Merge dim_customer, dim_product, dim_mill from STG
  orders      nordleaf_stg.orders_raw        → nordleaf_dwh.fact_orders
  production  nordleaf_stg.production_batches → nordleaf_dwh.fact_production
  deliveries  nordleaf_stg.deliveries        → nordleaf_dwh.fact_deliveries
  dq          DQ checks on fact_orders       → nordleaf_mgmt.dq_results
  all         dims → orders+production+deliveries → dq → pipeline_log

Execution order in 'all'
  1. dims       (must precede facts so surrogate key lookups resolve)
  2. orders, production, deliveries
  3. dq
  4. pipeline_log entry written for the full run

Examples
  python nordleaf_data_transform.py all
  python nordleaf_data_transform.py all --full-refresh
  python nordleaf_data_transform.py orders --date 2026-03-01
  python nordleaf_data_transform.py dq --fail-on-dq-error
        """,
    )
    parser.add_argument(
        "pipeline",
        choices=["dims", "orders", "production", "deliveries", "dq", "all"],
        help="Pipeline to run",
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Run date YYYY-MM-DD used for incremental WHERE DATE(loaded_at) filter"
             " (default: today; ignored when --full-refresh is set)",
    )
    parser.add_argument(
        "--project",
        default=PROJECT_ID,
        help=f"GCP project ID (default: {PROJECT_ID} from environment_config.toml)",
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Disable the date filter – reprocess all rows currently in STG tables",
    )
    parser.add_argument(
        "--fail-on-dq-error",
        action="store_true",
        help="Exit with code 1 if any DQ check fails (default: log warning and continue)",
    )
    args = parser.parse_args()

    bq     = _make_client(args.project)
    run_id = f"transform-{args.date}-{uuid.uuid4().hex[:8]}"
    t0     = time.monotonic()
    total_rows = 0

    try:
        if args.pipeline in ("dims", "all"):
            dim_rows = run_dims(bq, args.project, run_id)
            total_rows += sum(dim_rows.values())
            log.info("Dimensions merged: %s", dim_rows)

        if args.pipeline in ("orders", "all"):
            t = time.monotonic()
            n = merge_fact_orders(bq, args.project, args.date, args.full_refresh)
            log_pipeline(bq, args.project, run_id, "merge_fact_orders",
                         "SUCCESS", n, time.monotonic() - t)
            total_rows += n

        if args.pipeline in ("production", "all"):
            t = time.monotonic()
            n = merge_fact_production(bq, args.project, args.date, args.full_refresh)
            log_pipeline(bq, args.project, run_id, "merge_fact_production",
                         "SUCCESS", n, time.monotonic() - t)
            total_rows += n

        if args.pipeline in ("deliveries", "all"):
            t = time.monotonic()
            n = merge_fact_deliveries(bq, args.project, args.date, args.full_refresh)
            log_pipeline(bq, args.project, run_id, "merge_fact_deliveries",
                         "SUCCESS", n, time.monotonic() - t)
            total_rows += n

        if args.pipeline in ("dq", "all"):
            t = time.monotonic()
            fails = run_dq_checks(bq, args.project, args.fail_on_dq_error)
            status = "FAIL" if fails else "SUCCESS"
            log_pipeline(bq, args.project, run_id, "run_dq_checks",
                         status, 0, time.monotonic() - t)

        # Master run log (mirrors Airflow log_pipeline_run task)
        if args.pipeline == "all":
            log_pipeline(bq, args.project, run_id,
                         task_name="nordleaf_data_transform",
                         status="SUCCESS",
                         rows=total_rows,
                         duration_sec=time.monotonic() - t0)

    except Exception as exc:
        log.exception("Transform pipeline failed: %s", exc)
        log_pipeline(bq, args.project, run_id,
                     task_name=f"nordleaf_data_transform.{args.pipeline}",
                     status="FAILED",
                     rows=0,
                     duration_sec=time.monotonic() - t0,
                     error_msg=str(exc))
        sys.exit(1)

    log.info("Done. run_id=%s  total_rows=%d  elapsed=%.1fs",
             run_id, total_rows, time.monotonic() - t0)


if __name__ == "__main__":
    main()
