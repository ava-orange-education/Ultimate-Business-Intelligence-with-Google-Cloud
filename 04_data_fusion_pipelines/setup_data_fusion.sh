#!/bin/bash
# ============================================================
# NordLeaf Cloud Data Fusion Setup
# ============================================================
# Cloud Data Fusion provides a GUI-based ETL pipeline builder
# (no-code / low-code) backed by Apache Spark/MapReduce
#
# Architecture:
#   Data Fusion Instance: nordleaf-datafusion
#     Pipeline: nordleaf-suppliers-etl
#     Pipeline: nordleaf-orders-batch-etl
#     Pipeline: nordleaf-production-etl
# ============================================================

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

CFG_PROJECT=$(_read_toml "PROJECT_ID")
CFG_REGION=$(_read_toml  "region"         "DATAFUSION_SETTINGS")
CFG_INSTANCE=$(_read_toml "instance_name" "DATAFUSION_SETTINGS")

PROJECT_ID="${1:-${CFG_PROJECT:-nordleaf-project}}"
REGION="${CFG_REGION:-europe-west1}"
INSTANCE_NAME="${CFG_INSTANCE:-nordleaf-datafusion}"

# Create Data Fusion instance (Developer edition here)
# Please note that version is omitted here to have it as a default 
gcloud beta data-fusion instances create "$INSTANCE_NAME" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --edition='developer' \
  # --version=6.11.1 \
  --enable_stackdriver_logging \
  --enable_stackdriver_monitoring

echo "Data Fusion instance created: $INSTANCE_NAME"
echo "Access the GUI at: https://datafusion.cloud.google.com/projects/$PROJECT_ID/locations/$REGION/instances/$INSTANCE_NAME"

# Grant Data Fusion Service Account access to BigQuery and GCS
DATAFUSION_SA=$(gcloud beta data-fusion instances describe "$INSTANCE_NAME" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --format="value(serviceAccount)")

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$DATAFUSION_SA" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$DATAFUSION_SA" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:$DATAFUSION_SA" \
  --role="roles/storage.objectAdmin"

echo "IAM granted to Data Fusion SA: $DATAFUSION_SA"
