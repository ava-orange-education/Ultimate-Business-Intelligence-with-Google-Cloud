#!/usr/bin/env python3
"""
NordLeaf BI Platform - Unified Infrastructure Setup
====================================================
Reads environment_config.toml and secrets.toml then wires up every
GCP service needed for the NordLeaf data-engineering stack.

Steps (run individually or all at once):
  buckets      – Create / verify all GCS buckets
  upload       – Copy csv_data/ files to the raw GCS bucket
  bigquery     – Create BQ datasets and all empty tables
  load         – Load CSV data from GCS into STG + DWH
  datafusion   – Print setup instructions for Cloud Data Fusion
  airflow      – Print setup instructions for Cloud Composer / local Airflow
  dataplex     – Apply lake/zone/DQ scan + metadata tags (PII, Financial)
  security     – Enable row-access policies + column-level policy tags
  instructions – Print a full setup checklist (no GCP calls)
  all          – Run every step in order

Usage:
  source .venv/bin/activate
  python nordleaf_setup.py --step all
  python nordleaf_setup.py --step buckets
  python nordleaf_setup.py --step upload
  python nordleaf_setup.py --step bigquery --dry-run
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import textwrap
from pathlib import Path

# ── TOML parser ─────────────────────────────────────────────
try:
    import tomllib           # Python 3.11+
except ModuleNotFoundError:
    try:
        import tomli as tomllib  # pip install tomli
    except ModuleNotFoundError:
        sys.exit(
            "tomllib/tomli not found. "
            "Run:  pip install tomli   (or upgrade to Python 3.11+)"
        )

# ── Google Cloud clients (lazy imports per step) ─────────────
try:
    from google.cloud import bigquery, storage
    from google.oauth2 import service_account
except ImportError:
    sys.exit(
        "google-cloud packages missing. "
        "Run:  pip install google-cloud-bigquery[pandas] google-cloud-storage"
    )

# ── Colour helpers ───────────────────────────────────────────
GREEN  = "\033[0;32m"
YELLOW = "\033[1;33m"
BLUE   = "\033[0;34m"
RED    = "\033[0;31m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


def ok(msg: str)   -> None: print(f"{GREEN}[OK]{RESET}    {msg}")
def info(msg: str) -> None: print(f"{BLUE}[INFO]{RESET}  {msg}")
def warn(msg: str) -> None: print(f"{YELLOW}[WARN]{RESET}  {msg}")
def err(msg: str)  -> None: print(f"{RED}[ERROR]{RESET} {msg}", file=sys.stderr)
def head(msg: str) -> None:
    bar = "─" * 60
    print(f"\n{BOLD}{bar}{RESET}")
    print(f"{BOLD}  {msg}{RESET}")
    print(f"{BOLD}{bar}{RESET}")


# ============================================================
# CONFIG LOADING
# ============================================================

def load_config() -> tuple[dict, dict]:
    """Return (config, secrets) parsed from TOML files."""
    root = Path(__file__).parent

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
    else:
        warn("secrets.toml not found – using Application Default Credentials.")

    return cfg, secrets


def make_bq_client(cfg: dict, secrets: dict) -> bigquery.Client:
    project = cfg["PROJECT_ID"]
    key_path = secrets.get("GCP", {}).get("service_account_key_path", "")
    if key_path:
        creds = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return bigquery.Client(project=project, credentials=creds)
    return bigquery.Client(project=project)


def make_gcs_client(cfg: dict, secrets: dict) -> storage.Client:
    project = cfg["PROJECT_ID"]
    key_path = secrets.get("GCP", {}).get("service_account_key_path", "")
    if key_path:
        creds = service_account.Credentials.from_service_account_file(
            key_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        return storage.Client(project=project, credentials=creds)
    return storage.Client(project=project)


# ============================================================
# STEP 1 – GCS BUCKETS
# ============================================================

def step_buckets(cfg: dict, secrets: dict, dry_run: bool = False) -> None:
    head("Step 1 · GCS Buckets")
    gcs      = make_gcs_client(cfg, secrets)
    project  = cfg["PROJECT_ID"]
    location = cfg["LOCATION"]
    storage_cfg = cfg.get("STORAGE", {})

    bucket_names = [
        storage_cfg.get("raw_bucket",       "nordleaf-raw-data"),
        storage_cfg.get("processed_bucket", "nordleaf-processed-data"),
        storage_cfg.get("archive_bucket",   "nordleaf-archive"),
        storage_cfg.get("composer_bucket",  "nordleaf-composer-bucket"),
        storage_cfg.get("dataflow_bucket",  "nordleaf-dataflow-temp"),
        storage_cfg.get("vertex_ai_bucket", "nordleaf-vertex-ai"),
    ]

    for name in bucket_names:
        info(f"Bucket: gs://{name}")
        if dry_run:
            info("  [dry-run] skipped")
            continue
        try:
            bucket = gcs.bucket(name)
            if not bucket.exists():
                new_bucket = gcs.create_bucket(name, location=location)
                new_bucket.iam_configuration.uniform_bucket_level_access_enabled = True
                new_bucket.patch()
                # Public access prevention
                new_bucket.iam_configuration.public_access_prevention = "enforced"
                new_bucket.patch()
                ok(f"  Created gs://{name} in {location}")
            else:
                ok(f"  Already exists gs://{name}")
        except Exception as exc:
            err(f"  Failed to create gs://{name}: {exc}")

    # Sub-folder "placeholders" in raw bucket (GCS is flat but folders help)
    raw_bucket_name = storage_cfg.get("raw_bucket", "nordleaf-raw-data")
    folders = ["orders/", "suppliers/", "deliveries/", "production/",
               "sensors/", "feedback/", "dims/"]
    if not dry_run:
        raw_bucket = gcs.bucket(raw_bucket_name)
        for folder in folders:
            blob = raw_bucket.blob(folder + ".keep")
            if not blob.exists():
                blob.upload_from_string("")
                info(f"  Created folder placeholder: {folder}")


# ============================================================
# STEP 2 – UPLOAD LOCAL DATA TO GCS
# ============================================================

def step_upload(cfg: dict, secrets: dict, dry_run: bool = False) -> None:
    head("Step 2 · Upload Local Data Files (CSV & JSON) → GCS")
    gcs      = make_gcs_client(cfg, secrets)
    raw_name = cfg.get("STORAGE", {}).get("raw_bucket", cfg.get("RAW_BUCKET_NAME", ""))
    root     = Path(__file__).parent

    # Build ordered list of local search directories from config
    # DATA_FOLDERS keys + LOCAL_DATA_FOLDER as fallback
    data_folders_cfg: dict[str, str] = cfg.get("DATA_FOLDERS", {})
    primary_folder = root / cfg.get("LOCAL_DATA_FOLDER", "./csv_data")
    search_dirs: list[Path] = []
    if data_folders_cfg:
        for folder_str in data_folders_cfg:
            p = (root / folder_str).resolve()
            if p not in search_dirs:
                search_dirs.append(p)
    if primary_folder.resolve() not in search_dirs:
        search_dirs.insert(0, primary_folder.resolve())

    raw_bucket = gcs.bucket(raw_name)
    uploaded   = 0
    seen_blobs: set[str] = set()   # avoid double-uploading the same GCS path

    # ── Pass 1: explicit DATA_FILES mapping ───────────────────
    data_files: dict[str, str] = cfg.get("DATA_FILES", {})
    for filename, gcs_prefix in data_files.items():
        local_path = None
        for search_dir in search_dirs:
            candidate = search_dir / filename
            if candidate.exists():
                local_path = candidate
                break
        if local_path is None:
            warn(f"  {filename} not found in any data folder – skipping")
            continue
        blob_name = gcs_prefix + filename
        seen_blobs.add(blob_name)
        blob = raw_bucket.blob(blob_name)
        if dry_run:
            info(f"  [dry-run] {filename} → gs://{raw_name}/{blob_name}")
            continue
        if blob.exists():
            info(f"  Already uploaded: gs://{raw_name}/{blob_name}")
        else:
            blob.upload_from_filename(str(local_path))
            ok(f"  Uploaded {filename} → gs://{raw_name}/{blob_name}")
            uploaded += 1

    # ── Pass 2: auto-discover remaining files in DATA_FOLDERS ─
    # Picks up any .csv / .json / .ndjson not listed in DATA_FILES
    for folder_str, gcs_prefix in data_folders_cfg.items():
        folder_path = (root / folder_str).resolve()
        if not folder_path.exists():
            warn(f"  DATA_FOLDERS path not found: {folder_path} – skipping")
            continue
        glob_patterns = ["*.csv", "*.json", "*.ndjson"]
        for pattern in glob_patterns:
            for local_path in sorted(folder_path.glob(pattern)):
                blob_name = gcs_prefix + local_path.name
                if blob_name in seen_blobs:
                    continue   # already handled in pass 1
                seen_blobs.add(blob_name)
                blob = raw_bucket.blob(blob_name)
                if dry_run:
                    info(f"  [dry-run] {local_path.name} → gs://{raw_name}/{blob_name}")
                    continue
                if blob.exists():
                    info(f"  Already uploaded: gs://{raw_name}/{blob_name}")
                else:
                    blob.upload_from_filename(str(local_path))
                    ok(f"  Uploaded {local_path.name} → gs://{raw_name}/{blob_name}")
                    uploaded += 1

    if not dry_run:
        ok(f"Upload complete. {uploaded} new file(s) pushed to gs://{raw_name}/")


# ============================================================
# STEP 3 – BIGQUERY DATASETS + TABLES
# ============================================================

def step_bigquery(cfg: dict, secrets: dict, dry_run: bool = False) -> None:
    head("Step 3 · BigQuery Datasets & Tables")
    bq_cfg   = cfg.get("BIGQUERY_SETTINGS", {})
    project  = cfg["PROJECT_ID"]
    location = bq_cfg.get("location")

    datasets = {
        bq_cfg.get("dataset_ext",  "nordleaf_ext"):  "External tables (GCS-backed)",
        bq_cfg.get("dataset_stg",  "nordleaf_stg"):  "Staging – raw ingest with load metadata",
        bq_cfg.get("dataset_dwh",  "nordleaf_dwh"):  "Data Warehouse – star schema",
        bq_cfg.get("dataset_mgmt", "nordleaf_mgmt"): "Management – pipeline ops & DQ logs",
    }

    if dry_run:
        for ds, desc in datasets.items():
            info(f"  [dry-run] Would create dataset {project}.{ds}  ({desc})")
        info("  [dry-run] Would execute create_tables.sql")
        return

    bq = make_bq_client(cfg, secrets)

    # Create datasets
    for dataset_id, description in datasets.items():
        ref = f"{project}.{dataset_id}"
        try:
            bq.get_dataset(ref)
            ok(f"  Dataset already exists: {ref}")
        except Exception:
            ds_obj = bigquery.Dataset(ref)
            ds_obj.location = location
            ds_obj.description = description
            bq.create_dataset(ds_obj, exists_ok=True)
            ok(f"  Created dataset: {ref}")

    # Execute create_tables.sql
    sql_file = Path(__file__).parent / "03_bigquery_datasets" / "create_tables.sql"
    if not sql_file.exists():
        err(f"create_tables.sql not found at {sql_file}")
        return

    raw_sql = sql_file.read_text()

    # Split on semicolons, drop blank statements
    statements = [s.strip() for s in raw_sql.split(";") if s.strip()]
    info(f"Executing {len(statements)} SQL statements from create_tables.sql …")

    for i, stmt in enumerate(statements, 1):
        # Rewrite unqualified table refs to include project_id
        for ds in datasets:
            stmt = stmt.replace(f"`{ds}.", f"`{project}.{ds}.")
        try:
            job = bq.query(stmt)
            job.result()
            ok(f"  [{i}/{len(statements)}] {stmt[:60].replace(chr(10),' ')} …")
        except Exception as exc:
            err(f"  [{i}/{len(statements)}] FAILED: {exc}")
            err(f"  Statement was: {stmt[:120]}")

    ok("BigQuery schema setup complete.")


# ============================================================
# STEP 4 – LOAD DATA INTO BIGQUERY
# ============================================================

def step_load_data(cfg: dict, secrets: dict, dry_run: bool = False) -> None:
    head("Step 4 · Load Data into BigQuery")
    print(textwrap.dedent("""
    Four loading methods are available.  Choose the one that fits your workflow:

    ┌─────────────────────────────────────────────────────────────┐
    │  4a. Python ETL script  (recommended for initial load)      │
    │  4b. Cloud Data Fusion pipelines  (visual, incremental)     │
    │  4c. Cloud Composer / Airflow  (scheduled, orchestrated)    │
    │  4d. BigQuery Scheduled Query  (SQL-only, serverless)       │
    └─────────────────────────────────────────────────────────────┘
    """))

    _load_via_python(cfg, secrets, dry_run)
    _print_datafusion_load_instructions(cfg)
    _print_airflow_load_instructions(cfg)
    _print_scheduled_query_instructions(cfg)


def _load_via_python(cfg: dict, secrets: dict, dry_run: bool) -> None:
    """4a – Run nordleaf_etl.py for the initial full load."""
    info("4a · Python ETL (nordleaf_etl.py)")

    etl_script = Path(__file__).parent / "04_data_fusion_pipelines" / "nordleaf_etl.py"
    if not etl_script.exists():
        warn(f"  ETL script not found at {etl_script} – skipping")
        return

    project    = cfg["PROJECT_ID"]
    location   = cfg["LOCATION"]
    raw_bucket = cfg.get("STORAGE", {}).get("raw_bucket", cfg.get("RAW_BUCKET_NAME", ""))
    key_path   = secrets.get("GCP", {}).get("service_account_key_path", "")
    bq_location = cfg.get("BIGQUERY_SETTINGS", {}).get("location", "EU")

    env = os.environ.copy()
    env["PROJECT_ID"]  = project
    env["GCS_BUCKET"]  = raw_bucket
    env["BQ_LOCATION"] = bq_location
    env["LOCATION"]    = location
    if key_path:
        env["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

    # nordleaf_etl.py CLI: <pipeline> [--project P] [--bucket B] [--date D]
    # pipeline choices: suppliers | orders | production | all
    cmd = [
        sys.executable, str(etl_script),
        "all",                      # run all pipelines
        "--project", project,
        "--bucket",  raw_bucket,
    ]
    info(f"  Running: {' '.join(cmd)}")
    if dry_run:
        info("  [dry-run] skipped")
        return
    try:
        subprocess.run(cmd, env=env, check=True, text=True)
        ok("  ETL completed successfully.")
    except subprocess.CalledProcessError as exc:
        err(f"  ETL failed with exit code {exc.returncode}")


def _print_datafusion_load_instructions(cfg: dict) -> None:
    df_cfg   = cfg.get("DATAFUSION_SETTINGS", {})
    project  = cfg["PROJECT_ID"]
    location = cfg["LOCATION"]
    name     = df_cfg.get("instance_name", "nordleaf-datafusion")
    region   = df_cfg.get("region", location)
    pipe_dir = df_cfg.get("pipeline_dir", "./06_data_fusion_pipelines")

    print(textwrap.dedent(f"""
    4b · Cloud Data Fusion  (incremental / GUI-driven loading)
    ──────────────────────────────────────────────────────────
    If you prefer the visual ETL canvas, import the JSON pipeline definitions:

      1. Open Cloud Data Fusion:
         https://console.cloud.google.com/datafusion/instances?project={project}

      2. Create instance (first time only):
         gcloud datafusion instances create {name} \\
           --project={project} --location={region} \\
           --type=BASIC --version=6.9

      3. Import pipeline definitions from:
         {pipe_dir}/nordleaf_orders_etl_pipeline.json
         {pipe_dir}/nordleaf_suppliers_etl_pipeline.json

         In Data Fusion Studio → Menu → Import Pipeline → select JSON file.

      4. In each pipeline, update the Source "Path" to:
         gs://{cfg.get("STORAGE", {}).get("raw_bucket", "nordleaf-raw-data")}/orders/
         ... etc., then click "Run".
    """))


def _print_airflow_load_instructions(cfg: dict) -> None:
    comp_cfg = cfg.get("COMPOSER_SETTINGS", {})
    dag_dir  = comp_cfg.get("dag_dir", "./07_composer_orchestration")
    local_dir = comp_cfg.get("local_airflow_dir", "./07_composer_orchestration/airflow-local")
    project  = cfg["PROJECT_ID"]
    env_name = comp_cfg.get("environment_name", "nordleaf-composer")
    region   = comp_cfg.get("region", cfg["LOCATION"])

    print(textwrap.dedent(f"""
    4c · Airflow / Cloud Composer  (scheduled orchestration)
    ─────────────────────────────────────────────────────────
    Local development (Docker Compose):
      cd {local_dir}
      cp ../../environment_config.toml .
      docker compose up -d
      # UI → http://localhost:8080  (admin / admin)
      # Trigger: nordleaf_master_pipeline → Run

    Cloud Composer (managed Airflow in GCP):
      bash {dag_dir}/../setup_composer.sh (or):

      gcloud composer environments create {env_name} \\
        --project={project} --location={region} \\
        --image-version=composer-2.5.0-airflow-2.9.3 \\
        --service-account=nordleaf-composer@{project}.iam.gserviceaccount.com

      # Upload DAGs:
      gcloud composer environments storage dags import \\
        --environment={env_name} --location={region} \\
        --source={dag_dir}/nordleaf_master_dag.py
      gcloud composer environments storage dags import \\
        --environment={env_name} --location={region} \\
        --source={dag_dir}/nordleaf_dq_dag.py
    """))


def _print_scheduled_query_instructions(cfg: dict) -> None:
    bq_cfg  = cfg.get("BIGQUERY_SETTINGS", {})
    project = cfg["PROJECT_ID"]
    dataset_stg = bq_cfg.get("dataset_stg", "nordleaf_stg")
    dataset_dwh = bq_cfg.get("dataset_dwh", "nordleaf_dwh")
    location = bq_cfg.get("location", "EU")

    print(textwrap.dedent(f"""
    4d · BigQuery Scheduled Query  (SQL-only, serverless refresh)
    ──────────────────────────────────────────────────────────────
    Use this for lightweight incremental merges without Airflow.

      1. Open BigQuery Studio:
         https://console.cloud.google.com/bigquery?project={project}

      2. Create a new scheduled query:
         • Click "Scheduled queries" → "Create scheduled query"
         • Paste the SQL below
         • Schedule: every day 06:00 Europe/Helsinki
         • Destination dataset: {project}.{dataset_dwh}

    -- Incremental merge: STG orders → DWH fact_orders
    MERGE `{project}.{dataset_dwh}.fact_orders` T
    USING (
      SELECT
        o.order_id,
        c.customer_key,
        p.product_key,
        m.mill_key,
        CAST(FORMAT_DATE('%Y%m%d', o.order_date) AS INT64)    AS order_date_id,
        CAST(FORMAT_DATE('%Y%m%d', o.delivery_date) AS INT64) AS delivery_date_id,
        o.quantity_tons,
        o.unit_price_eur,
        ROUND(o.quantity_tons * o.unit_price_eur, 2)          AS total_value_eur,
        DATE_DIFF(o.delivery_date, o.order_date, DAY)         AS lead_time_days,
        o.status
      FROM `{project}.{dataset_stg}.orders_raw` o
      LEFT JOIN `{project}.{dataset_dwh}.dim_customer` c
        ON LOWER(TRIM(o.customer_name)) = LOWER(TRIM(c.customer_name))
       AND c.is_current = TRUE
      LEFT JOIN `{project}.{dataset_dwh}.dim_product`  p
        ON o.product_type = p.product_type
      LEFT JOIN `{project}.{dataset_dwh}.dim_mill`     m
        ON o.mill_origin  = m.mill_name
      WHERE DATE(o.loaded_at) = CURRENT_DATE()
    ) S ON T.order_id = S.order_id
    WHEN MATCHED     THEN UPDATE SET
      T.status = S.status, T.created_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT VALUES (
      S.order_id, S.customer_key, S.product_key, S.mill_key,
      S.order_date_id, S.delivery_date_id, S.quantity_tons,
      S.unit_price_eur, S.total_value_eur, S.lead_time_days,
      S.status, CURRENT_TIMESTAMP()
    );
    """))


# ============================================================
# STEP 5 – DATAPLEX  (metadata tags, DQ scan, PII/Financial)
# ============================================================

def step_dataplex(cfg: dict, secrets: dict, dry_run: bool = False) -> None:
    head("Step 5 · Dataplex – Governance & Metadata Tags")

    project  = cfg["PROJECT_ID"]
    location = cfg["LOCATION"]
    dp_cfg   = cfg.get("DATAPLEX_SETTINGS", {})
    bq_cfg   = cfg.get("BIGQUERY_SETTINGS", {})

    lake_name    = dp_cfg.get("lake_name",    "nordleaf-lake")
    raw_zone     = dp_cfg.get("raw_zone",     "nordleaf-raw-zone")
    curated_zone = dp_cfg.get("curated_zone", "nordleaf-curated-zone")
    dq_scan      = dp_cfg.get("dq_scan_name", "nordleaf-orders-dq")
    schedule     = dp_cfg.get("dq_scan_schedule", "0 7 * * *")
    dataset_dwh  = bq_cfg.get("dataset_dwh",  "nordleaf_dwh")
    dataset_stg  = bq_cfg.get("dataset_stg",  "nordleaf_stg")

    raw_bucket   = cfg.get("STORAGE", {}).get("raw_bucket", "nordleaf-raw-data")

    if dry_run:
        info(f"[dry-run] Would create lake {lake_name}, zones, DQ scan {dq_scan}")
        info("[dry-run] Would apply policy tags for PII / Financial columns")
        return

    _run_gcloud("Dataplex lake", [
        "gcloud", "dataplex", "lakes", "create", lake_name,
        f"--project={project}", f"--location={location}",
        "--display-name=NordLeaf Data Lake",
        "--description=NordLeaf managed data lake"
    ])

    _run_gcloud("Raw zone", [
        "gcloud", "dataplex", "zones", "create", raw_zone,
        f"--lake={lake_name}", f"--project={project}", f"--location={location}",
        "--type=RAW", "--resource-spec-type=STORAGE_BUCKET",
        "--display-name=Raw Zone"
    ])

    _run_gcloud("Curated zone", [
        "gcloud", "dataplex", "zones", "create", curated_zone,
        f"--lake={lake_name}", f"--project={project}", f"--location={location}",
        "--type=CURATED", "--resource-spec-type=BIGQUERY_DATASET",
        "--display-name=Curated Zone"
    ])

    # Attach assets to zones
    _run_gcloud("Attach raw bucket asset", [
        "gcloud", "dataplex", "assets", "create", "nordleaf-raw-bucket",
        f"--lake={lake_name}", f"--zone={raw_zone}",
        f"--project={project}", f"--location={location}",
        f"--resource-spec-name=projects/{project}/buckets/{raw_bucket}",
        "--resource-spec-type=STORAGE_BUCKET",
        "--display-name=Raw Data Bucket"
    ])

    _run_gcloud("Attach DWH dataset asset", [
        "gcloud", "dataplex", "assets", "create", "nordleaf-dwh-dataset",
        f"--lake={lake_name}", f"--zone={curated_zone}",
        f"--project={project}", f"--location={location}",
        f"--resource-spec-name=projects/{project}/datasets/{dataset_dwh}",
        "--resource-spec-type=BIGQUERY_DATASET",
        "--display-name=Data Warehouse (DWH)"
    ])

    # ── Data Quality Scan ────────────────────────────────────
    dq_spec_yaml = textwrap.dedent(f"""
    rules:
      - column: order_id
        non_null_expectation: {{}}
        dimension: COMPLETENESS
        threshold: 1.0
      - column: quantity_tons
        range_expectation:
          min_value: "0"
          strict_min_value: true
        dimension: VALIDITY
        threshold: 0.99
      - column: unit_price_eur
        range_expectation:
          min_value: "0"
          strict_min_value: true
        dimension: VALIDITY
        threshold: 0.99
      - column: status
        set_expectation:
          values: ["DELIVERED","IN_TRANSIT","PENDING","CANCELLED"]
        dimension: VALIDITY
        threshold: 1.0
      - column: order_id
        uniqueness_expectation: {{}}
        dimension: UNIQUENESS
        threshold: 1.0
    """).strip()

    dq_yaml_path = Path(__file__).parent / ".nordleaf_dq_spec.yaml"
    dq_yaml_path.write_text(dq_spec_yaml)

    _run_gcloud("DQ scan create", [
        "gcloud", "dataplex", "datascans", "create", "data-quality", dq_scan,
        f"--project={project}", f"--location={location}",
        "--display-name=NordLeaf Orders DQ Scan",
        f"--data-source-resource=//bigquery.googleapis.com/projects/{project}/datasets/{dataset_stg}/tables/orders_raw",
        "--execution-spec-trigger-type=ON_SCHEDULE",
        f"--execution-spec-trigger-schedule={schedule}",
        f"--data-quality-spec-file={dq_yaml_path}",
    ])

    dq_yaml_path.unlink(missing_ok=True)

    # ── Policy Tags (PII / Financial) ───────────────────────
    info("Applying column-level policy tags via Data Catalog …")
    _apply_policy_tags(cfg, secrets)

    ok("Dataplex setup complete.")


def _apply_policy_tags(cfg: dict, secrets: dict) -> None:
    """Create a policy taxonomy and apply PII/Financial tags to columns."""
    project  = cfg["PROJECT_ID"]
    location = cfg.get("LOCATION", "europe-west1")
    dp_cfg   = cfg.get("DATAPLEX_SETTINGS", {})
    taxonomy  = dp_cfg.get("policy_taxonomy",  "NordLeaf Data Classification")
    pii_tag   = dp_cfg.get("pii_tag",          "PII")
    fin_tag   = dp_cfg.get("financial_tag",     "Confidential/Financial")

    # gcloud data-catalog is the CLI surface for policy tags
    # Step A: create taxonomy
    _run_gcloud("Create taxonomy", [
        "gcloud", "data-catalog", "taxonomies", "create",
        f"--project={project}", f"--location={location}",
        f"--display-name={taxonomy}",
        "--description=NordLeaf column classification taxonomy"
    ])

    # Steps B & C are shown as instructions (taxonomy IDs are dynamic;
    # users need the GCP console or REST API to look them up after creation)
    print(textwrap.dedent(f"""
    ── Column-Level Policy Tags ───────────────────────────────────
    After the taxonomy is created, complete these steps in the
    GCP Console → Data Catalog → Policy Tags:

    1. Navigate to:
       https://console.cloud.google.com/datacatalog/taxonomies?project={project}

    2. Inside "{taxonomy}" create two policy tags:
       • {pii_tag}                (for email addresses, personal identifiers)
       • {fin_tag}   (for pricing and contract values)

    3. Apply tags to these columns in BigQuery:
       Table                         Column               Tag
       ─────────────────────────────────────────────────────────
       nordleaf_dwh.dim_supplier     contact_email        {pii_tag}
       nordleaf_dwh.dim_supplier     annual_contract_eur  {fin_tag}
       nordleaf_dwh.fact_orders      unit_price_eur       {fin_tag}
       nordleaf_dwh.fact_orders      total_value_eur      {fin_tag}
       nordleaf_dwh.fact_deliveries  invoice_eur          {fin_tag}

    4. Grant fine-grained reader access:
       gcloud data-catalog taxonomies set-iam-policy TAXONOMY_ID \\
         --location={location} --project={project} \\
         --member="group:nordleaf-finance@{cfg.get('IAM_SETTINGS', {}).get('domain','googlegroups.com')}" \\
         --role="roles/datacatalog.categoryFineGrainedReader"
    """))


# ============================================================
# STEP 6 – ROW-LEVEL & COLUMN-LEVEL SECURITY
# ============================================================

def step_security(cfg: dict, secrets: dict, dry_run: bool = False) -> None:
    head("Step 6 · Row-Level & Column-Level Security")
    project  = cfg["PROJECT_ID"]
    bq_cfg   = cfg.get("BIGQUERY_SETTINGS", {})
    iam_cfg  = cfg.get("IAM_SETTINGS", {})
    domain   = iam_cfg.get("domain", "googlegroups.com")
    dataset_dwh  = bq_cfg.get("dataset_dwh",  "nordleaf_dwh")
    dataset_stg  = bq_cfg.get("dataset_stg",  "nordleaf_stg")
    dataset_mgmt = bq_cfg.get("dataset_mgmt", "nordleaf_mgmt")

    bq = make_bq_client(cfg, secrets)

    # ── 6a: Row Access Policies ──────────────────────────────
    info("6a · Row-Level Security (Row Access Policies)")
    row_policies = [
        # Filter fact_orders so finance group sees only delivered orders
        {
            "name":       "finance_delivered_orders",
            "table":      f"{project}.{dataset_dwh}.fact_orders",
            "filter_pred": "status = 'DELIVERED'",
            "grantees":   [f"group:nordleaf-finance@{domain}"],
            "comment":    "Finance group sees only DELIVERED orders",
        },
        # Management sees all orders (no filter) – bypassed by admins
        {
            "name":       "management_all_orders",
            "table":      f"{project}.{dataset_dwh}.fact_orders",
            "filter_pred": "TRUE",
            "grantees":   [f"group:nordleaf-management@{domain}"],
            "comment":    "Management sees all order rows",
        },
        # Analysts see only DWH tables (enforced via dataset IAM, but
        # row policy adds extra guard for multi-region queries)
        {
            "name":       "analysts_current_suppliers",
            "table":      f"{project}.{dataset_dwh}.dim_supplier",
            "filter_pred": "active = TRUE",
            "grantees":   [f"group:nordleaf-analysts@{domain}"],
            "comment":    "Analysts see only active suppliers",
        },
    ]

    for policy in row_policies:
        info(f"  Row policy: {policy['name']} on {policy['table']}")
        info(f"  Comment: {policy['comment']}")
        if dry_run:
            info("  [dry-run] skipped")
            continue

        grantee_list = ",".join(f'"{g}"' for g in policy["grantees"])
        sql = textwrap.dedent(f"""
            CREATE OR REPLACE ROW ACCESS POLICY `{policy['name']}`
            ON `{policy['table']}`
            GRANT TO ({grantee_list})
            FILTER USING ({policy['filter_pred']})
        """).strip()
        try:
            job = bq.query(sql)
            job.result()
            ok(f"  Applied row access policy: {policy['name']}")
        except Exception as exc:
            err(f"  Failed to apply {policy['name']}: {exc}")

    # ── 6b: Dataset-Level IAM (Column security via policy tags) ─
    info("\n6b · Dataset-Level IAM Bindings")
    print(textwrap.dedent(f"""
    These dataset-level IAM bindings restrict group access to specific layers.
    Run the following bq commands (or apply via IAM setup script):

    # Analysts: read-only on DWH + MGMT
    bq add-iam-policy-binding --member="group:nordleaf-analysts@{domain}" \\
      --role="roles/bigquery.dataViewer" {project}:{dataset_dwh}

    bq add-iam-policy-binding --member="group:nordleaf-analysts@{domain}" \\
      --role="roles/bigquery.dataViewer" {project}:{dataset_mgmt}

    # Finance: read-only on DWH (financial tags control column visibility)
    bq add-iam-policy-binding --member="group:nordleaf-finance@{domain}" \\
      --role="roles/bigquery.dataViewer" {project}:{dataset_dwh}

    # Management: read-only on DWH + MGMT
    bq add-iam-policy-binding --member="group:nordleaf-management@{domain}" \\
      --role="roles/bigquery.dataViewer" {project}:{dataset_dwh}

    bq add-iam-policy-binding --member="group:nordleaf-management@{domain}" \\
      --role="roles/bigquery.dataViewer" {project}:{dataset_mgmt}

    Note: Column masking for {cfg.get("DATAPLEX_SETTINGS", {}).get("pii_tag", "PII")} /
    Financial columns is enforced by Data Catalog policy tags applied in step_dataplex().
    Users WITHOUT the fine-grained reader role will see NULL for those columns.
    """) if not dry_run else info("[dry-run] dataset IAM bindings skipped"))

    ok("Security configuration complete.")


# ============================================================
# DATAFUSION INSTRUCTIONS  (informational)
# ============================================================

def step_datafusion_instructions(cfg: dict) -> None:
    head("Cloud Data Fusion – Setup Instructions")
    project  = cfg["PROJECT_ID"]
    location = cfg["LOCATION"]
    df_cfg   = cfg.get("DATAFUSION_SETTINGS", {})
    name     = df_cfg.get("instance_name", "nordleaf-datafusion")
    region   = df_cfg.get("region", location)
    pipe_dir = df_cfg.get("pipeline_dir", "./06_data_fusion_pipelines")

    print(textwrap.dedent(f"""
    1. Enable the Data Fusion API (already done by setup_iam.sh):
       gcloud services enable datafusion.googleapis.com --project={project}

    2. Create a BASIC instance (once per project):
       gcloud datafusion instances create {name} \\
         --project={project} --location={region} \\
         --type=BASIC --version=6.9 \\
         --service-account=nordleaf-data-fusion@{project}.iam.gserviceaccount.com

    3. Open the Data Fusion UI:
       gcloud datafusion instances describe {name} \\
         --project={project} --location={region} \\
         --format="value(serviceEndpoint)"
       # Open the returned URL in your browser.

    4. Import pipeline JSON files from {pipe_dir}/:
       • nordleaf_orders_etl_pipeline.json   → Orders batch ETL
       • nordleaf_suppliers_etl_pipeline.json → Supplier master data ETL

       In Data Fusion Studio: Menu → Import Pipeline → select JSON file.

    5. Update source "Path" properties to match your bucket:
       gs://{cfg.get("STORAGE", {}).get("raw_bucket", "nordleaf-raw-data")}/orders/

    6. Alternatively, use the Python ETL script as a drop-in replacement:
       python 06_data_fusion_pipelines/nordleaf_etl.py

    7. For incremental scheduled runs, deploy via Cloud Composer (step_airflow).
    """))


# ============================================================
# AIRFLOW INSTRUCTIONS  (informational)
# ============================================================

def step_airflow_instructions(cfg: dict) -> None:
    head("Cloud Composer / Airflow – Setup Instructions")
    project   = cfg["PROJECT_ID"]
    comp_cfg  = cfg.get("COMPOSER_SETTINGS", {})
    env_name  = comp_cfg.get("environment_name", "nordleaf-composer")
    region    = comp_cfg.get("region", cfg["LOCATION"])
    dag_dir   = comp_cfg.get("dag_dir", "./07_composer_orchestration")
    local_dir = comp_cfg.get("local_airflow_dir", "./07_composer_orchestration/airflow-local")

    print(textwrap.dedent(f"""
    ── Local Airflow (Docker Compose – development) ─────────────
      cd {local_dir}
      docker compose up -d
      # UI: http://localhost:8080  (admin / admin)
      # Trigger "nordleaf_master_pipeline" from the UI or:
      docker exec -it airflow-airflow-scheduler-1 \\
        airflow dags trigger nordleaf_master_pipeline

    ── Cloud Composer 2 (production / managed) ──────────────────
    Create environment (once):
      bash {dag_dir}/../setup_composer.sh
      # or manually:
      gcloud composer environments create {env_name} \\
        --project={project} --location={region} \\
        --image-version=composer-2.5.0-airflow-2.9.3 \\
        --service-account=nordleaf-composer@{project}.iam.gserviceaccount.com \\
        --env-variables=PROJECT_ID={project}

    Upload DAGs:
      for dag in nordleaf_master_dag.py nordleaf_dq_dag.py; do
        gcloud composer environments storage dags import \\
          --environment={env_name} --location={region} \\
          --source={dag_dir}/$dag
      done

    Pipeline schedule:
      • nordleaf_master_pipeline : Weekdays 05:00 Europe/Helsinki
      • nordleaf_dq_pipeline      : Every 6 hours (00/06/12/18 UTC)

    Monitor:
      gcloud composer environments run {env_name} \\
        --project={project} --location={region} \\
        dags list
    """))


# ============================================================
# FULL INSTRUCTIONS PRINTOUT
# ============================================================

def step_instructions(cfg: dict) -> None:
    head("NordLeaf BI – Full Setup Checklist")
    project = cfg["PROJECT_ID"]
    domain  = cfg.get("IAM_SETTINGS", {}).get("domain", "googlegroups.com")

    print(textwrap.dedent(f"""
    ┌──────────────────────────────────────────────────────────────────┐
    │              NordLeaf BI – Recommended Setup Order               │
    └──────────────────────────────────────────────────────────────────┘

    PRE-REQUISITES (manual / one-time)
    ────────────────────────────────────────────────────────────────────
    □ 1.  Create GCP project and enable billing.
    □ 2.  Edit environment_config.toml  → set PROJECT_ID, LOCATION, etc.
    □ 3.  Edit secrets.toml             → set service_account_key_path.
    □ 4.  Run local machine setup:
            bash setup_local.sh

    GCP INFRASTRUCTURE
    ────────────────────────────────────────────────────────────────────
    □ 5.  IAM – Run as project Owner in Cloud Shell:
            bash 01_iam_and_security/setup_iam.sh {project}
          Then in GCP Console → IAM → Groups, create:
            nordleaf-data-engineers@{domain}
            nordleaf-analysts@{domain}
            nordleaf-finance@{domain}
            nordleaf-management@{domain}
          Add users to groups as appropriate.

    □ 6.  GCS Buckets:
            python nordleaf_setup.py --step buckets

    □ 7.  Upload raw CSV data:
            python nordleaf_setup.py --step upload

    □ 8.  BigQuery datasets + schema:
            python nordleaf_setup.py --step bigquery

    □ 9.  Load data into BigQuery:
            python nordleaf_setup.py --step load
          (Runs Python ETL + prints Datafusion / Airflow / SQL instructions)

    □ 10. Dataplex lake, zones, DQ scan, metadata tags:
            python nordleaf_setup.py --step dataplex
          Then complete policy-tag column assignment in GCP Console
          (instructions printed during step).

    □ 11. Row-level & column-level security:
            python nordleaf_setup.py --step security
          Run the printed `bq add-iam-policy-binding` commands.

    ORCHESTRATION (choose one or both)
    ────────────────────────────────────────────────────────────────────
    □ 12. Cloud Data Fusion (GUI pipelines):
            python nordleaf_setup.py --step instructions  (see Datafusion section)

    □ 13. Cloud Composer / local Airflow:
            python nordleaf_setup.py --step instructions  (see Airflow section)

    BUSINESS INTELLIGENCE
    ────────────────────────────────────────────────────────────────────
    □ 14. Review BI use cases:   BI_Use_Cases/NordLeaf_BI_Use_Cases_Full.md
    □ 15. Run dashboard queries: 09_bi_dashboards/looker_studio_queries.sql
    □ 16. Transform data:        SQL in 05_bigquery_datasets/create_tables.sql
    □ 17. Connect Looker Studio to BigQuery with nordleaf-bi-reader SA.

    VERTEX AI / ML (optional)
    ────────────────────────────────────────────────────────────────────
    □ 18. Train demand-forecast model:
            python 08_vertex_ai/vertex_ai_demand_forecast.py
    □ 19. Automate weekly retraining via Cloud Composer
            08_vertex_ai/vertex_ai_pipeline.py

    MONITORING
    ────────────────────────────────────────────────────────────────────
    □ 20. Enable Cloud Monitoring alerts:
            bash 10_monitoring/setup_monitoring.sh {project}

    ────────────────────────────────────────────────────────────────────
    All steps can be re-run safely (idempotent).
    For questions see README.md or BI_Use_Cases/README.md.
    """))


# ============================================================
# HELPER
# ============================================================

def _run_gcloud(label: str, cmd: list[str]) -> None:
    """Run a gcloud CLI command, ignoring 'already exists' errors."""
    info(f"  {label} …")
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, check=False
        )
        if result.returncode == 0:
            ok(f"  {label} done.")
        elif "already exists" in result.stderr.lower():
            info(f"  {label} already exists – skipping.")
        else:
            warn(f"  {label} exited {result.returncode}: {result.stderr.strip()[:200]}")
    except FileNotFoundError:
        warn(f"  gcloud not found in PATH. Install Google Cloud SDK and retry.")


# ============================================================
# MAIN
# ============================================================

STEP_MAP = {
    "buckets":      lambda cfg, sec, dr: step_buckets(cfg, sec, dr),
    "upload":       lambda cfg, sec, dr: step_upload(cfg, sec, dr),
    "bigquery":     lambda cfg, sec, dr: step_bigquery(cfg, sec, dr),
    "load":         lambda cfg, sec, dr: step_load_data(cfg, sec, dr),
    "datafusion":   lambda cfg, sec, _:  step_datafusion_instructions(cfg),
    "airflow":      lambda cfg, sec, _:  step_airflow_instructions(cfg),
    "dataplex":     lambda cfg, sec, dr: step_dataplex(cfg, sec, dr),
    "security":     lambda cfg, sec, dr: step_security(cfg, sec, dr),
    "instructions": lambda cfg, sec, _:  step_instructions(cfg),
}

ALL_STEPS = [
    "buckets", "upload", "bigquery", "load",
    "datafusion", "airflow", "dataplex", "security",
]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NordLeaf BI Platform – Unified Infrastructure Setup",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent("""
        Examples:
          python nordleaf_setup.py --step all
          python nordleaf_setup.py --step bigquery --dry-run
          python nordleaf_setup.py --step instructions
        """),
    )
    parser.add_argument(
        "--step",
        choices=list(STEP_MAP.keys()) + ["all"],
        default="instructions",
        help="Which setup step to run (default: instructions)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be done without making GCP API calls",
    )
    args = parser.parse_args()

    cfg, secrets = load_config()

    head("NordLeaf BI Platform – Infrastructure Setup")
    info(f"Project : {cfg['PROJECT_ID']}")
    info(f"Region  : {cfg['LOCATION']}")
    if args.dry_run:
        warn("DRY-RUN mode enabled – no GCP changes will be made.")

    steps_to_run = ALL_STEPS if args.step == "all" else [args.step]

    for step in steps_to_run:
        fn = STEP_MAP[step]
        fn(cfg, secrets, args.dry_run)

    head("Done")
    ok("All requested steps completed.")
    if args.step in ("all", "instructions"):
        print(f"\n  Run  {GREEN}python nordleaf_setup.py --step instructions{RESET}  for a full checklist.\n")


if __name__ == "__main__":
    main()
