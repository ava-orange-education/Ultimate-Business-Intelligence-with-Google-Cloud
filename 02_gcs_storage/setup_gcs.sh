#!/bin/bash
# ============================================================
# NordLeaf GCS Bucket Setup
# ============================================================
# Architecture:
#   gs://nordleaf-raw-data/          <- Raw ingestion zone
#     orders/, suppliers/, deliveries/, production/, sensors/
#   gs://nordleaf-processed-data/    <- Processed / curated zone
#   gs://nordleaf-archive/           <- Long-term cold archive
#   gs://nordleaf-composer-bucket/   <- Airflow DAGs
#   gs://nordleaf-dataflow-temp/     <- Dataflow temp files
#   gs://nordleaf-vertex-ai/         <- ML training artifacts

# Usage $ ./setup_gcs.sh your-project-name
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

# ── Read config values ────────────────────────────────────────────────────────
CFG_PROJECT=$(_read_toml "PROJECT_ID")
CFG_REGION=$(_read_toml  "LOCATION")

PROJECT_ID="${1:-${CFG_PROJECT:-nordleaf-project}}"
REGION="${CFG_REGION:-europe-west1}"

# Bucket names from [STORAGE] section
BUCKET_RAW=$(_read_toml       "raw_bucket"       "STORAGE"); BUCKET_RAW="${BUCKET_RAW:-nordleaf-raw-data}"
BUCKET_PROCESSED=$(_read_toml "processed_bucket" "STORAGE"); BUCKET_PROCESSED="${BUCKET_PROCESSED:-nordleaf-processed-data}"
BUCKET_ARCHIVE=$(_read_toml   "archive_bucket"   "STORAGE"); BUCKET_ARCHIVE="${BUCKET_ARCHIVE:-nordleaf-archive}"
BUCKET_COMPOSER=$(_read_toml  "composer_bucket"  "STORAGE"); BUCKET_COMPOSER="${BUCKET_COMPOSER:-nordleaf-composer-bucket}"
BUCKET_DATAFLOW=$(_read_toml  "dataflow_bucket"  "STORAGE"); BUCKET_DATAFLOW="${BUCKET_DATAFLOW:-nordleaf-dataflow-temp}"
BUCKET_VERTEX=$(_read_toml    "vertex_ai_bucket" "STORAGE"); BUCKET_VERTEX="${BUCKET_VERTEX:-nordleaf-vertex-ai}"

# Raw data landing zone
gcloud storage buckets create "gs://${BUCKET_RAW}" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  --public-access-prevention

# Processed / curated zone
gcloud storage buckets create "gs://${BUCKET_PROCESSED}" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  --public-access-prevention

# Archive (cold storage, lifecycle rules)
gcloud storage buckets create "gs://${BUCKET_ARCHIVE}" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  --public-access-prevention

# Cloud Composer DAGs bucket
gcloud storage buckets create "gs://${BUCKET_COMPOSER}" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  --public-access-prevention

# Dataflow temp
gcloud storage buckets create "gs://${BUCKET_DATAFLOW}" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  --public-access-prevention

# Vertex AI artifacts
gcloud storage buckets create "gs://${BUCKET_VERTEX}" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --uniform-bucket-level-access \
  --public-access-prevention

# Apply lifecycle rules to archive bucket (move to coldline after 90 days)
cat > /tmp/lifecycle.json << 'EOF'
{
  "rule": [
    {
      "action": {"type": "SetStorageClass", "storageClass": "COLDLINE"},
      "condition": {"age": 90}
    },
    {
      "action": {"type": "Delete"},
      "condition": {"age": 2555}
    }
  ]
}
EOF
gcloud storage buckets update "gs://${BUCKET_ARCHIVE}" --lifecycle-file=/tmp/lifecycle.json

# Upload NordLeaf sample data
DATA_DIR="${SCRIPT_DIR}/../data/csv_data"
gcloud storage cp "${DATA_DIR}/orders_2023.csv"             "gs://${BUCKET_RAW}/orders/"
gcloud storage cp "${DATA_DIR}/suppliers.csv"               "gs://${BUCKET_RAW}/suppliers/"
gcloud storage cp "${DATA_DIR}/deliveries_2023.csv"         "gs://${BUCKET_RAW}/deliveries/"
gcloud storage cp "${DATA_DIR}/production_batches_2023.csv" "gs://${BUCKET_RAW}/production/"
gcloud storage cp "${DATA_DIR}/customer_feedback.csv"       "gs://${BUCKET_RAW}/feedback/"
gcloud storage cp "${DATA_DIR}/sensor_events_sample.csv"    "gs://${BUCKET_RAW}/sensors/"

# Set up GCS event notifications for auto-ingest
gcloud pubsub topics create nordleaf-gcs-orders-events --project="$PROJECT_ID"

gcloud storage buckets notifications create "gs://${BUCKET_RAW}" \
  --topic=nordleaf-gcs-orders-events \
  --event-types=OBJECT_FINALIZE \
  --object-prefix=orders/

echo "GCS setup complete!"
gcloud storage buckets list --project="$PROJECT_ID" | grep nordleaf
