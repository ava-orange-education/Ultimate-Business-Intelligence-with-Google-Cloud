#!/usr/bin/env python3
"""
nordleaf_etl.py – Python replacement for NordLeaf Cloud Data Fusion pipelines.

Replaces the three Data Fusion pipelines with pandas + BigQuery:
  • nordleaf-suppliers-etl      (daily 05:00 EET)
      GCS CSV → Wrangler → Validate → STG suppliers + DWH dim_supplier
  • nordleaf-orders-batch-etl   (daily 06:00 EET)
      GCS CSV → Wrangler → Deduplicate → STG orders_raw + Joiner → DWH fact_orders
  • nordleaf-production-etl     (daily 04:00 EET)
      GCS JSON → Flatten → Quality Check → STG production_batches

Data Fusion concept mapping
───────────────────────────────────────────────────────────────
  Data Fusion plugin              │  Python equivalent
  ────────────────────────────────┼──────────────────────────────
  GCSFile (batchsource)           │  read_gcs_csv / read_gcs_json
  Wrangler (transform)            │  wrangle_suppliers / wrangle_orders / flatten_production
  JavaScript (transform)          │  validate_suppliers / validate_production
  Deduplicate (transform)         │  deduplicate()
  Joiner (batchjoiner)            │  lookup_dims() via pd.merge
  BigQueryTable (batchsink)       │  write_bq()
  Error sink                      │  write_errors() → nordleaf_mgmt.pipeline_errors
───────────────────────────────────────────────────────────────

Usage
─────
  python nordleaf_etl.py suppliers
  python nordleaf_etl.py orders --date 2026-03-01
  python nordleaf_etl.py production
  python nordleaf_etl.py all --date 2026-03-01
"""

import argparse
import io
import logging
import os
import sys
from datetime import date
from pathlib import Path
from typing import Tuple

import pandas as pd
from google.cloud import bigquery, storage
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
# Script lives at <project_root>/06_data_fusion_pipelines/nordleaf_etl.py
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
GCS_BUCKET  = _cfg.get("STORAGE", {}).get("raw_bucket",
              _cfg.get("RAW_BUCKET_NAME", "nordleaf-raw-data"))
BQ_LOCATION = _cfg.get("BIGQUERY_SETTINGS", {}).get("location", "EU")


def _make_clients(project_id: str) -> tuple[bigquery.Client, storage.Client]:
    """
    Build BQ + GCS clients.
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

    bq_client  = bigquery.Client(project=project_id, credentials=creds,
                                 location=BQ_LOCATION)
    gcs_client = storage.Client(project=project_id, credentials=creds)
    return bq_client, gcs_client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
log = logging.getLogger("nordleaf_etl")


# ═════════════════════════════════════════════════════════════════════════════
# I/O helpers
# ═════════════════════════════════════════════════════════════════════════════

def read_gcs_csv(gcs: storage.Client, bucket: str, prefix: str) -> pd.DataFrame:
    """GCSFile (batchsource) equivalent – reads all CSVs under a GCS prefix."""
    blobs = [b for b in gcs.bucket(bucket).list_blobs(prefix=prefix)
             if b.name.endswith(".csv")]
    if not blobs:
        raise FileNotFoundError(f"No CSV files found at gs://{bucket}/{prefix}")
    dfs = [pd.read_csv(io.BytesIO(b.download_as_bytes())) for b in blobs]
    df  = pd.concat(dfs, ignore_index=True)
    log.info("GCSFile read: %d rows from %d file(s) at gs://%s/%s",
             len(df), len(blobs), bucket, prefix)
    return df


def read_gcs_json(gcs: storage.Client, bucket: str, prefix: str) -> pd.DataFrame:
    """GCSFile (batchsource) equivalent – reads all JSON / NDJSON under a GCS prefix."""
    blobs = [b for b in gcs.bucket(bucket).list_blobs(prefix=prefix)
             if b.name.endswith((".json", ".ndjson"))]
    if not blobs:
        raise FileNotFoundError(f"No JSON files found at gs://{bucket}/{prefix}")
    dfs = []
    for blob in blobs:
        content = blob.download_as_text()
        try:
            dfs.append(pd.read_json(io.StringIO(content)))
        except ValueError:
            dfs.append(pd.read_json(io.StringIO(content), lines=True))
    df = pd.concat(dfs, ignore_index=True)
    log.info("GCSFile read: %d rows from %d file(s) at gs://%s/%s",
             len(df), len(blobs), bucket, prefix)
    return df


def read_bq_table(bq: bigquery.Client, project_id: str, table: str) -> pd.DataFrame:
    """BigQueryTable (batchsource) equivalent – reads a full BQ table as a DataFrame."""
    return bq.query(f"SELECT * FROM `{project_id}.{table}`").to_dataframe()


def write_bq(bq: bigquery.Client, df: pd.DataFrame,
             project_id: str, table: str,
             disposition: str = "WRITE_APPEND") -> None:
    """BigQueryTable (batchsink) equivalent – writes a DataFrame to BigQuery."""
    job = bq.load_table_from_dataframe(
        df,
        f"{project_id}.{table}",
        job_config=bigquery.LoadJobConfig(
            write_disposition=disposition,
            create_disposition="CREATE_IF_NEEDED",
        ),
    )
    job.result()
    log.info("BigQueryTable sink: %d rows → %s.%s (%s)",
             len(df), project_id, table, disposition)


def write_errors(bq: bigquery.Client, df: pd.DataFrame,
                 project_id: str, pipeline: str) -> None:
    """Error sink – writes rejected rows to nordleaf_mgmt.pipeline_errors."""
    if df.empty:
        return
    df = df.copy()
    df["_pipeline"]  = pipeline
    df["_logged_at"] = pd.Timestamp.utcnow()
    df = df.astype(str)   # coerce all cols to str to avoid schema conflicts
    write_bq(bq, df, project_id, "nordleaf_mgmt.pipeline_errors",
             disposition="WRITE_APPEND")
    log.warning("Error sink: %d rejected row(s) → nordleaf_mgmt.pipeline_errors",
                len(df))


# ═════════════════════════════════════════════════════════════════════════════
# Shared transforms
# ═════════════════════════════════════════════════════════════════════════════

def deduplicate(df: pd.DataFrame, key: str,
                order_col: str = "loaded_at") -> pd.DataFrame:
    """
    Deduplicate plugin equivalent.
    Keeps the last record per key ordered by order_col (filterOperation=last).
    """
    before = len(df)
    if order_col in df.columns:
        df = df.sort_values(order_col, ascending=True)
    df      = df.drop_duplicates(subset=[key], keep="last").reset_index(drop=True)
    removed = before - len(df)
    if removed:
        log.info("Deduplicate: removed %d duplicate(s) on '%s'", removed, key)
    return df


def upsert_df(tgt_df: pd.DataFrame, src_df: pd.DataFrame,
              key: str, update_cols: list) -> pd.DataFrame:
    """
    DataFrame equivalent of BigQueryTable UPSERT (operation=UPSERT):
      MATCHED     → update update_cols in target
      NOT MATCHED → append to target
    """
    if tgt_df.empty:
        return src_df
    matched_mask = src_df[key].isin(tgt_df[key])
    updates      = src_df.loc[matched_mask, [key] + update_cols].set_index(key)
    tgt_df       = tgt_df.set_index(key)
    tgt_df.update(updates)
    tgt_df       = tgt_df.reset_index()
    result_df    = pd.concat([tgt_df, src_df[~matched_mask]], ignore_index=True)
    log.info("UPSERT: %d updated, %d inserted (key=%s)",
             matched_mask.sum(), (~matched_mask).sum(), key)
    return result_df


# ═════════════════════════════════════════════════════════════════════════════
# Pipeline 1: nordleaf-suppliers-etl
#
# JSON plugin chain (nordleaf_suppliers_etl_pipeline.json):
#   GCS-Source-Suppliers
#     → CSV-Parser        (Wrangler: parse-as-csv, set-type, filter-rows-on)
#     → Data-Validator    (JavaScript: null checks, negative contract value)
#     → STG-BigQuery-Sink (nordleaf_stg.suppliers  – UPSERT on supplier_id)
#     → DWH-BigQuery-Sink (nordleaf_dwh.dim_supplier – UPSERT on supplier_id)
#     → Error-Table       (nordleaf_mgmt.pipeline_errors – INSERT)
# ═════════════════════════════════════════════════════════════════════════════

def wrangle_suppliers(df: pd.DataFrame) -> pd.DataFrame:
    """
    CSV-Parser (Wrangler) equivalent.
    Mirrors directives:
      parse-as-csv :body ',' true
      set-type :annual_contract_eur double
      set-type :active boolean
      filter-rows-on empty-or-null-columns supplier_id
    """
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # set-type :annual_contract_eur double
    if "annual_contract_eur" in df.columns:
        df["annual_contract_eur"] = pd.to_numeric(
            df["annual_contract_eur"], errors="coerce"
        )

    # set-type :active boolean
    if "active" in df.columns:
        df["active"] = df["active"].map(
            {True: True, False: False,
             "true": True, "false": False,
             "True": True, "False": False,
             "1": True, "0": False, 1: True, 0: False}
        ).fillna(False).astype(bool)

    # filter-rows-on empty-or-null-columns supplier_id
    df = df[df["supplier_id"].notna() &
            (df["supplier_id"].astype(str).str.strip() != "")]
    df["loaded_at"] = pd.Timestamp.utcnow()
    return df.reset_index(drop=True)


def validate_suppliers(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Data-Validator (JavaScript transform) equivalent.
    Mirrors the script:
      if (!input.supplier_id)            → 'NULL supplier_id'
      if (!input.supplier_name)          → 'NULL supplier_name'
      if (input.annual_contract_eur < 0) → 'Negative contract value'
    Returns (valid_df, errors_df).
    """
    null_id   = df["supplier_id"].isna() | (df["supplier_id"].astype(str).str.strip() == "")
    null_name = (df["supplier_name"].isna() | (df["supplier_name"].astype(str).str.strip() == "")) \
                if "supplier_name" in df.columns else pd.Series([False] * len(df))
    neg_val   = df["annual_contract_eur"] < 0 \
                if "annual_contract_eur" in df.columns else pd.Series([False] * len(df))

    invalid_mask = null_id | null_name | neg_val
    valid_df     = df[~invalid_mask].reset_index(drop=True)
    errors_df    = df[invalid_mask].copy()

    if not errors_df.empty:
        reasons = (
            null_id[invalid_mask].map({True: "NULL supplier_id"}).fillna("") + "; " +
            null_name[invalid_mask].map({True: "NULL supplier_name"}).fillna("") + "; " +
            neg_val[invalid_mask].map({True: "Negative contract value"}).fillna("")
        ).str.strip("; ")
        errors_df["_error_reason"] = reasons.values

    log.info("Data-Validator: %d valid, %d rejected", len(valid_df), len(errors_df))
    return valid_df, errors_df


def run_suppliers_etl(bq: bigquery.Client, gcs: storage.Client,
                      project_id: str, bucket: str) -> None:
    """nordleaf-suppliers-etl pipeline."""
    log.info("══════ nordleaf-suppliers-etl START ══════")

    # 1. GCS-Source-Suppliers (GCSFile batchsource)
    raw_df    = read_gcs_csv(gcs, bucket, "suppliers/")

    # 2. CSV-Parser (Wrangler)
    parsed_df = wrangle_suppliers(raw_df)

    # 3. Data-Validator (JavaScript transform) + Error-Table sink
    valid_df, errors_df = validate_suppliers(parsed_df)
    write_errors(bq, errors_df, project_id, "nordleaf-suppliers-etl")

    # 4. STG-BigQuery-Sink → nordleaf_stg.suppliers (UPSERT on supplier_id)
    stg_tgt    = read_bq_table(bq, project_id, "nordleaf_stg.suppliers")
    stg_result = upsert_df(
        stg_tgt, valid_df, key="supplier_id",
        update_cols=[c for c in valid_df.columns if c != "supplier_id"],
    )
    write_bq(bq, stg_result, project_id, "nordleaf_stg.suppliers",
             disposition="WRITE_TRUNCATE")

    # 5. DWH-BigQuery-Sink → nordleaf_dwh.dim_supplier (UPSERT on supplier_id)
    dim_cols = [
        "supplier_id", "supplier_name", "country", "supplier_type",
        "contact_email", "payment_terms", "annual_contract_eur", "active", "rating",
    ]
    dim_src    = valid_df[[c for c in dim_cols if c in valid_df.columns]].copy()
    dim_tgt    = read_bq_table(bq, project_id, "nordleaf_dwh.dim_supplier")
    dim_result = upsert_df(
        dim_tgt, dim_src, key="supplier_id",
        update_cols=[c for c in dim_cols if c != "supplier_id"],
    )
    write_bq(bq, dim_result, project_id, "nordleaf_dwh.dim_supplier",
             disposition="WRITE_TRUNCATE")

    log.info("══════ nordleaf-suppliers-etl COMPLETE: %d rows ══════", len(valid_df))


# ═════════════════════════════════════════════════════════════════════════════
# Pipeline 2: nordleaf-orders-batch-etl
#
# JSON plugin chain (nordleaf_orders_etl_pipeline.json):
#   GCS-Orders-Source
#     → Field-Transformer   (Wrangler: set-type, derived total_value_eur, format-date)
#     → Deduplicator        (Deduplicate: uniqueFields=order_id, last(loaded_at))
#     ├→ STG-Orders-Sink    (nordleaf_stg.orders_raw – UPSERT on order_id)
#     └→ Joiner-Dim-Lookup  ← Dim-Mill-Source (dim_mill)
#                           ← Dim-Product-Source (dim_product)
#          → DWH-Fact-Orders-Sink (nordleaf_dwh.fact_orders – UPSERT on order_id)
# ═════════════════════════════════════════════════════════════════════════════

def wrangle_orders(df: pd.DataFrame) -> pd.DataFrame:
    """
    Field-Transformer (Wrangler) equivalent.
    Mirrors directives:
      set-type :quantity_tons double
      set-type :unit_price_eur double
      set-column :total_value_eur multiply(:quantity_tons, :unit_price_eur)
      format-date :order_date yyyy-MM-dd
      format-date :delivery_date yyyy-MM-dd
    """
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "quantity_tons" in df.columns:
        df["quantity_tons"]  = pd.to_numeric(df["quantity_tons"],  errors="coerce")
    if "unit_price_eur" in df.columns:
        df["unit_price_eur"] = pd.to_numeric(df["unit_price_eur"], errors="coerce")
    if {"quantity_tons", "unit_price_eur"}.issubset(df.columns):
        df["total_value_eur"] = (df["quantity_tons"] * df["unit_price_eur"]).round(2)
    if "order_date" in df.columns:
        df["order_date"]    = pd.to_datetime(df["order_date"],    errors="coerce").dt.date
    if "delivery_date" in df.columns:
        df["delivery_date"] = pd.to_datetime(df["delivery_date"], errors="coerce").dt.date

    df["loaded_at"] = pd.Timestamp.utcnow()
    return df


def lookup_dims(bq: bigquery.Client, df: pd.DataFrame, project_id: str) -> pd.DataFrame:
    """
    Joiner-Dim-Lookup (Joiner batchjoiner) equivalent.
    Joins dim_mill, dim_product, dim_customer and derives all columns
    required by the fact_orders schema:
      order_id, customer_key, product_key, mill_key,
      order_date_id (INT64 YYYYMMDD), delivery_date_id (INT64 YYYYMMDD),
      quantity_tons, unit_price_eur, total_value_eur,
      lead_time_days, status
    """
    df = df.copy()

    # ── Dim lookups ───────────────────────────────────────────
    dim_mill = bq.query(
        f"SELECT mill_name, mill_key FROM `{project_id}.nordleaf_dwh.dim_mill`"
    ).to_dataframe()

    dim_product = bq.query(
        f"SELECT product_type, product_key FROM `{project_id}.nordleaf_dwh.dim_product`"
    ).to_dataframe()

    dim_customer = bq.query(
        f"SELECT customer_name, customer_key "
        f"FROM `{project_id}.nordleaf_dwh.dim_customer` WHERE is_current = TRUE"
    ).to_dataframe()

    # ── Joins ─────────────────────────────────────────────────
    df = df.merge(dim_mill,     left_on="mill_origin",   right_on="mill_name", how="left") \
           .drop(columns=["mill_name"], errors="ignore")
    df = df.merge(dim_product,  on="product_type",       how="left")
    df = df.merge(dim_customer, on="customer_name",      how="left")

    # ── COALESCE unresolved surrogate keys to -1 ──────────────
    for key_col in ["mill_key", "product_key", "customer_key"]:
        if key_col in df.columns:
            df[key_col] = df[key_col].fillna(-1).astype("Int64")

    # ── Derive partition + measure columns ────────────────────
    # order_date_id / delivery_date_id: INT64 in YYYYMMDD format
    # (fact_orders is RANGE_BUCKET-partitioned on order_date_id)
    for src_col, id_col in [("order_date", "order_date_id"),
                             ("delivery_date", "delivery_date_id")]:
        if src_col in df.columns:
            df[id_col] = (
                pd.to_datetime(df[src_col], errors="coerce")
                  .dt.strftime("%Y%m%d")
                  .astype("Int64")
            )
        else:
            df[id_col] = pd.NA

    # lead_time_days: integer days between order and delivery
    if {"order_date", "delivery_date"}.issubset(df.columns):
        df["lead_time_days"] = (
            (pd.to_datetime(df["delivery_date"], errors="coerce") -
             pd.to_datetime(df["order_date"],    errors="coerce"))
            .dt.days.astype("Int64")
        )
    else:
        df["lead_time_days"] = pd.NA

    # ── Select only fact_orders columns ───────────────────────
    keep = [
        "order_id", "customer_key", "product_key", "mill_key",
        "order_date_id", "delivery_date_id",
        "quantity_tons", "unit_price_eur", "total_value_eur",
        "lead_time_days", "status",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    log.info("Joiner: resolved dim keys + derived IDs for %d orders", len(df))
    return df


def run_orders_etl(bq: bigquery.Client, gcs: storage.Client,
                   project_id: str, bucket: str,
                   run_date: str | None = None) -> None:
    """nordleaf-orders-batch-etl pipeline."""
    ds = run_date or date.today().isoformat()
    log.info("══════ nordleaf-orders-batch-etl START (date=%s) ══════", ds)

    # 1. GCS-Orders-Source (GCSFile batchsource – date-partitioned prefix )
    #raw_df = read_gcs_csv(gcs, bucket, f"orders/Orders_{ds}")

    raw_df = read_gcs_csv(gcs, bucket, f"orders/Orders_2023.csv")

    # 2. Field-Transformer (Wrangler)
    transformed_df = wrangle_orders(raw_df)

    # 3. Deduplicator (Deduplicate plugin – uniqueFields=order_id, last(loaded_at))
    deduped_df = deduplicate(transformed_df, key="order_id", order_col="loaded_at")

    # 4a. STG-Orders-Sink → nordleaf_stg.orders_raw (UPSERT on order_id)
    stg_tgt    = read_bq_table(bq, project_id, "nordleaf_stg.orders_raw")
    stg_result = upsert_df(
        stg_tgt, deduped_df, key="order_id",
        update_cols=[c for c in deduped_df.columns if c != "order_id"],
    )
    write_bq(bq, stg_result, project_id, "nordleaf_stg.orders_raw",
             disposition="WRITE_TRUNCATE")

    # 4b. Joiner-Dim-Lookup → DWH-Fact-Orders-Sink
    #     (nordleaf_dwh.fact_orders – UPSERT on order_id)
    enriched_df = lookup_dims(bq, deduped_df, project_id)
    dwh_tgt     = read_bq_table(bq, project_id, "nordleaf_dwh.fact_orders")
    dwh_result  = upsert_df(
        dwh_tgt, enriched_df, key="order_id",
        update_cols=[c for c in enriched_df.columns if c != "order_id"],
    )
    write_bq(bq, dwh_result, project_id, "nordleaf_dwh.fact_orders",
             disposition="WRITE_TRUNCATE")

    log.info("══════ nordleaf-orders-batch-etl COMPLETE: %d rows ══════", len(deduped_df))


# ═════════════════════════════════════════════════════════════════════════════
# Pipeline 3: nordleaf-production-etl
#
# Architecture (from README – no JSON definition exists):
#   GCS JSON → Flatten → Quality Check → STG production_batches
# ═════════════════════════════════════════════════════════════════════════════

def flatten_production(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wrangler flatten equivalent.
    Normalises column names, flattens nested JSON dict columns,
    casts numerics and dates.
    """
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Flatten any nested dict columns (JSON sub-objects)
    for col in list(df.columns):
        if df[col].dtype == object:
            try:
                sample = df[col].dropna().iloc[0]
                if isinstance(sample, dict):
                    nested = pd.json_normalize(df[col].dropna().tolist())
                    nested.columns = [f"{col}_{nc}" for nc in nested.columns]
                    df = df.drop(columns=[col])
                    df = pd.concat(
                        [df.reset_index(drop=True), nested.reset_index(drop=True)],
                        axis=1,
                    )
            except (IndexError, TypeError):
                pass

    for col in ["actual_tons", "efficiency_pct", "planned_tons"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "production_date" in df.columns:
        df["production_date"] = pd.to_datetime(
            df["production_date"], errors="coerce"
        ).dt.date

    df["loaded_at"] = pd.Timestamp.utcnow()
    return df


def validate_production(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    JavaScript quality check equivalent.
    Rules:
      batch_id must not be null
      actual_tons must be > 0
      efficiency_pct must be between 0 and 100
    Returns (valid_df, errors_df).
    """
    null_id  = df["batch_id"].isna() if "batch_id" in df.columns \
               else pd.Series([False] * len(df))
    bad_tons = df["actual_tons"] <= 0 if "actual_tons" in df.columns \
               else pd.Series([False] * len(df))
    bad_eff  = ((df["efficiency_pct"] < 0) | (df["efficiency_pct"] > 100)) \
               if "efficiency_pct" in df.columns else pd.Series([False] * len(df))

    invalid_mask = null_id | bad_tons | bad_eff
    valid_df     = df[~invalid_mask].reset_index(drop=True)
    errors_df    = df[invalid_mask].copy()

    if not errors_df.empty:
        reasons = (
            null_id[invalid_mask].map({True: "NULL batch_id"}).fillna("") + "; " +
            bad_tons[invalid_mask].map({True: "actual_tons non-positive"}).fillna("") + "; " +
            bad_eff[invalid_mask].map({True: "efficiency_pct out of range [0,100]"}).fillna("")
        ).str.strip("; ")
        errors_df["_error_reason"] = reasons.values

    log.info("Quality check: %d valid, %d rejected", len(valid_df), len(errors_df))
    return valid_df, errors_df


def run_production_etl(bq: bigquery.Client, gcs: storage.Client,
                       project_id: str, bucket: str) -> None:
    """nordleaf-production-etl pipeline."""
    log.info("══════ nordleaf-production-etl START ══════")

    # 1. GCSFile (batchsource) – JSON
    raw_df = read_gcs_json(gcs, bucket, "production/")

    # 2. Wrangler flatten
    flat_df = flatten_production(raw_df)

    # 3. JavaScript quality check + error sink
    valid_df, errors_df = validate_production(flat_df)
    write_errors(bq, errors_df, project_id, "nordleaf-production-etl")

    # 4. Deduplicate plugin (by batch_id)
    deduped_df = deduplicate(valid_df, key="batch_id", order_col="loaded_at")

    # 5. BigQueryTable sink → nordleaf_stg.production_batches (UPSERT on batch_id)
    stg_tgt    = read_bq_table(bq, project_id, "nordleaf_stg.production_batches")
    stg_result = upsert_df(
        stg_tgt, deduped_df, key="batch_id",
        update_cols=[c for c in deduped_df.columns if c != "batch_id"],
    )
    write_bq(bq, stg_result, project_id, "nordleaf_stg.production_batches",
             disposition="WRITE_TRUNCATE")

    log.info("══════ nordleaf-production-etl COMPLETE: %d rows ══════", len(deduped_df))


# ═════════════════════════════════════════════════════════════════════════════
# Pipeline 4: nordleaf-deliveries-etl  (new – no Data Fusion equivalent)
#
# GCS CSV deliveries/ → Wrangler → Validate → STG deliveries
#
# STG schema (create_tables.sql):
#   delivery_id, supplier_id, supplier_name, delivery_date DATE,
#   material_type, quantity_tons FLOAT64, quality_grade,
#   mill_destination, truck_id, invoice_eur FLOAT64, loaded_at TIMESTAMP
# ═════════════════════════════════════════════════════════════════════════════

def wrangle_deliveries(df: pd.DataFrame) -> pd.DataFrame:
    """
    Wrangler equivalent for deliveries.
    Directives (mirroring orders pattern):
      set-type :quantity_tons double
      set-type :invoice_eur double
      format-date :delivery_date yyyy-MM-dd
      filter-rows-on empty-or-null-columns delivery_id
    """
    df = df.copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    if "quantity_tons" in df.columns:
        df["quantity_tons"] = pd.to_numeric(df["quantity_tons"], errors="coerce")
    if "invoice_eur" in df.columns:
        df["invoice_eur"] = pd.to_numeric(df["invoice_eur"], errors="coerce")
    if "delivery_date" in df.columns:
        df["delivery_date"] = pd.to_datetime(
            df["delivery_date"], errors="coerce"
        ).dt.date

    # filter-rows-on empty-or-null-columns delivery_id
    df = df[df["delivery_id"].notna() &
            (df["delivery_id"].astype(str).str.strip() != "")]

    df["loaded_at"] = pd.Timestamp.utcnow()
    return df.reset_index(drop=True)


def validate_deliveries(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Data-Validator equivalent for deliveries.
    Rules:
      delivery_id must not be null
      quantity_tons must be > 0
      invoice_eur must be >= 0
    Returns (valid_df, errors_df).
    """
    null_id   = df["delivery_id"].isna() | \
                (df["delivery_id"].astype(str).str.strip() == "")
    bad_qty   = df["quantity_tons"] <= 0 \
                if "quantity_tons" in df.columns else pd.Series([False] * len(df))
    bad_inv   = df["invoice_eur"] < 0 \
                if "invoice_eur" in df.columns else pd.Series([False] * len(df))

    invalid_mask = null_id | bad_qty | bad_inv
    valid_df     = df[~invalid_mask].reset_index(drop=True)
    errors_df    = df[invalid_mask].copy()

    if not errors_df.empty:
        reasons = (
            null_id[invalid_mask].map({True: "NULL delivery_id"}).fillna("") + "; " +
            bad_qty[invalid_mask].map({True: "quantity_tons non-positive"}).fillna("") + "; " +
            bad_inv[invalid_mask].map({True: "invoice_eur negative"}).fillna("")
        ).str.strip("; ")
        errors_df["_error_reason"] = reasons.values

    log.info("Data-Validator: %d valid, %d rejected", len(valid_df), len(errors_df))
    return valid_df, errors_df


def run_deliveries_etl(bq: bigquery.Client, gcs: storage.Client,
                       project_id: str, bucket: str) -> None:
    """nordleaf-deliveries-etl pipeline: GCS CSV → STG deliveries."""
    log.info("══════ nordleaf-deliveries-etl START ══════")

    # 1. GCSFile (batchsource)
    raw_df = read_gcs_csv(gcs, bucket, "deliveries/")

    # 2. Wrangler
    wrangled_df = wrangle_deliveries(raw_df)

    # 3. Data-Validator + Error sink
    valid_df, errors_df = validate_deliveries(wrangled_df)
    write_errors(bq, errors_df, project_id, "nordleaf-deliveries-etl")

    # 4. Deduplicate on delivery_id (keep last loaded_at)
    deduped_df = deduplicate(valid_df, key="delivery_id", order_col="loaded_at")

    # 5. STG sink → nordleaf_stg.deliveries (UPSERT on delivery_id)
    stg_tgt    = read_bq_table(bq, project_id, "nordleaf_stg.deliveries")
    stg_result = upsert_df(
        stg_tgt, deduped_df, key="delivery_id",
        update_cols=[c for c in deduped_df.columns if c != "delivery_id"],
    )
    write_bq(bq, stg_result, project_id, "nordleaf_stg.deliveries",
             disposition="WRITE_TRUNCATE")

    log.info("══════ nordleaf-deliveries-etl COMPLETE: %d rows ══════", len(deduped_df))


# ═════════════════════════════════════════════════════════════════════════════
# CLI entrypoint
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    parser = argparse.ArgumentParser(
        description="NordLeaf ETL – Python replacement for Cloud Data Fusion",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Pipelines
─────────
  suppliers    nordleaf-suppliers-etl      → nordleaf_stg.suppliers
                                           → nordleaf_dwh.dim_supplier
  orders       nordleaf-orders-batch-etl  → nordleaf_stg.orders_raw
                                           → nordleaf_dwh.fact_orders
  production   nordleaf-production-etl    → nordleaf_stg.production_batches
  deliveries   nordleaf-deliveries-etl    → nordleaf_stg.deliveries
  all          Run all four in sequence

Failed rows always go to nordleaf_mgmt.pipeline_errors.
        """,
    )
    parser.add_argument(
        "pipeline",
        choices=["suppliers", "orders", "production", "deliveries", "all"],
        help="Name of the pipeline to run",
    )
    parser.add_argument(
        "--date",
        default=date.today().isoformat(),
        help="Run date YYYY-MM-DD for date-partitioned sources (default: today)",
    )
    parser.add_argument(
        "--project",
        default=PROJECT_ID,
        help=f"GCP project ID (default: {PROJECT_ID})",
    )
    parser.add_argument(
        "--bucket",
        default=GCS_BUCKET,
        help=f"GCS bucket name (default: {GCS_BUCKET})",
    )
    args = parser.parse_args()

    # Create clients once; pass them into every pipeline function
    bq, gcs = _make_clients(args.project)

    try:
        if args.pipeline in ("suppliers", "all"):
            run_suppliers_etl(bq, gcs, args.project, args.bucket)
        if args.pipeline in ("orders", "all"):
            run_orders_etl(bq, gcs, args.project, args.bucket, run_date=args.date)
        if args.pipeline in ("production", "all"):
            run_production_etl(bq, gcs, args.project, args.bucket)
        if args.pipeline in ("deliveries", "all"):
            run_deliveries_etl(bq, gcs, args.project, args.bucket)
    except FileNotFoundError as exc:
        log.error("Source file missing: %s", exc)
        sys.exit(1)
    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)
        sys.exit(1)

    log.info("Done.")


if __name__ == "__main__":
    main()
