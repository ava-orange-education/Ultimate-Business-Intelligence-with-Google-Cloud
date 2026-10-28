#!/bin/bash
# ============================================================
# NordLeaf GCP IAM & Security Setup
# ============================================================
# Run this script as a GCP Project Owner in the Cloud Shell
# Usage: ./setup_iam.sh your-project-id
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../environment_config.toml"

# ── Read a scalar value from environment_config.toml via Python ──────────────
_read_toml() {
  local key="$1" section="${2:-}"
  python3 - "$CONFIG_FILE" "$key" "$section" <<'PYEOF'
import sys
try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        print(""); sys.exit(0)
cfg_path, key, section = sys.argv[1], sys.argv[2], sys.argv[3]
with open(cfg_path, "rb") as f:
    cfg = tomllib.load(f)
val = cfg.get(section, {}).get(key, "") if section else cfg.get(key, "")
print(str(val) if val != "" else "")
PYEOF
}

if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "[WARN] environment_config.toml not found at ${CONFIG_FILE} – using defaults / CLI args"
fi

# ── Read config values (CLI arg $1 overrides PROJECT_ID) ─────────────────────
CFG_PROJECT=$(_read_toml "PROJECT_ID")
CFG_REGION=$(_read_toml  "LOCATION")
CFG_DOMAIN=$(_read_toml  "domain" "IAM_SETTINGS")

PROJECT_ID="${1:-${CFG_PROJECT:-your-project-id}}"
REGION="${CFG_REGION:-europe-west1}"
DOMAIN="${CFG_DOMAIN:-googlegroups.com}"

echo "Setting up IAM for project: $PROJECT_ID"

# ============================================================
# STEP 1: Enable Required APIs
# ============================================================
gcloud services enable --project="$PROJECT_ID" \
  bigquery.googleapis.com \
  storage.googleapis.com \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  dataplex.googleapis.com \
  datacatalog.googleapis.com \
  datafusion.googleapis.com \
  composer.googleapis.com \
  aiplatform.googleapis.com \
  monitoring.googleapis.com \
  logging.googleapis.com \
  cloudresourcemanager.googleapis.com \
  iam.googleapis.com \
  secretmanager.googleapis.com \
  datastream.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com

echo "APIs enabled."

# ============================================================
# STEP 2: Create Service Accounts
# ============================================================

declare -A SERVICE_ACCOUNTS=(
  ["nordleaf-data-pipeline"]="NordLeaf Data Pipeline - ETL and batch jobs"
  ["nordleaf-dataflow"]="NordLeaf Dataflow - streaming ingestion jobs"
  ["nordleaf-composer"]="NordLeaf Cloud Composer - Airflow orchestration"
  ["nordleaf-vertex-ai"]="NordLeaf Vertex AI - ML training and inference"
  ["nordleaf-data-fusion"]="NordLeaf Data Fusion - pipeline execution"
  ["nordleaf-bi-reader"]="NordLeaf BI Reader - Looker/BI tool access"
  ["nordleaf-dataplex"]="NordLeaf Dataplex - data governance scans"
)

for SA_NAME in "${!SERVICE_ACCOUNTS[@]}"; do
  DESCRIPTION="${SERVICE_ACCOUNTS[$SA_NAME]}"
  gcloud iam service-accounts create "$SA_NAME" \
    --project="$PROJECT_ID" \
    --display-name="$DESCRIPTION" \
    --description="$DESCRIPTION" \
    2>/dev/null || echo "SA $SA_NAME already exists"
done

echo "Service accounts created."

# ============================================================
# STEP 3: Grant IAM Roles to Service Accounts
# ============================================================

# Data Pipeline SA - BigQuery Editor + GCS access
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-data-pipeline@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-data-pipeline@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-data-pipeline@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Dataflow SA
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-dataflow@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-dataflow@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-dataflow@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

# Composer SA
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-composer@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/composer.worker"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-composer@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-composer@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/dataflow.developer"

# Vertex AI SA
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-vertex-ai@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/aiplatform.user"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-vertex-ai@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-vertex-ai@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# BI Reader SA (Looker / dashboard tools)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-bi-reader@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataViewer"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:nordleaf-bi-reader@${PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

echo "IAM roles assigned to service accounts."

# ============================================================
# STEP 4: Grant Access to Google Groups (Human Users) - 
#Update the Groups to suit your use cases. Adjust the permissions in the GCP console. 
# ============================================================

# Data Engineers group
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="group:nordleaf-data-engineers@${DOMAIN}" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="group:nordleaf-data-engineers@${DOMAIN}" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="group:nordleaf-data-engineers@${DOMAIN}" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="group:nordleaf-data-engineers@${DOMAIN}" \
  --role="roles/composer.environmentAndStorageObjectAdmin"

# Data Analysts group (read-only DWH + MGMT datasets)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="group:nordleaf-analysts@${DOMAIN}" \
  --role="roles/bigquery.jobUser"

# Finance group (can see pricing columns)
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="group:nordleaf-finance@${DOMAIN}" \
  --role="roles/bigquery.jobUser"

echo "Group IAM bindings applied."

# ============================================================
# STEP 5: Grant dataset-level access for analysts
# ============================================================
# Analysts get READER on nordleaf_dwh and nordleaf_mgmt only
bq update \
  --dataset \
  "${PROJECT_ID}:nordleaf_dwh" 2>/dev/null || true

echo "IAM setup complete for project: $PROJECT_ID"
echo ""
echo "Summary of Service Accounts created:"
gcloud iam service-accounts list --project="$PROJECT_ID"
