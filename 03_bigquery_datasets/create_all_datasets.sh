#!/bin/bash
# ============================================================
# NordLeaf BigQuery Datasets and Core Tables Setup
# ============================================================
# Reads project settings from ../environment_config.toml.
# A PROJECT_ID passed as $1 overrides the config file value.
#
# Usage:
#   bash create_all_datasets.sh              # use environment_config.toml
#   bash create_all_datasets.sh my-project   # override project ID only
# ============================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${SCRIPT_DIR}/../environment_config.toml"

# ── Read values from environment_config.toml via Python ──────
if [[ ! -f "$CONFIG_FILE" ]]; then
  echo "[ERROR] environment_config.toml not found at ${CONFIG_FILE}"
  echo "        Create it or pass PROJECT_ID as \$1."
  exit 1
fi

_read_toml() {
  # Usage: _read_toml <KEY> [<SECTION>]
  # Reads a scalar from a flat key or from a [SECTION] table.
  local key="$1"
  local section="${2:-}"
  python3 - "$CONFIG_FILE" "$key" "$section" <<'PYEOF'
import sys, pathlib

try:
    import tomllib
except ModuleNotFoundError:
    try:
        import tomli as tomllib
    except ModuleNotFoundError:
        print("")
        sys.exit(0)          # silently return empty; caller falls back

cfg_path, key, section = sys.argv[1], sys.argv[2], sys.argv[3]
with open(cfg_path, "rb") as f:
    cfg = tomllib.load(f)

if section:
    val = cfg.get(section, {}).get(key, "")
else:
    val = cfg.get(key, "")

print(str(val) if val != "" else "")
PYEOF
}

# ── Project / location ────────────────────────────────────────
CFG_PROJECT=$(_read_toml "PROJECT_ID")
CFG_BQ_LOC=$(_read_toml  "location"  "BIGQUERY_SETTINGS")

# CLI arg $1 overrides config PROJECT_ID
PROJECT_ID="${1:-${CFG_PROJECT:-nordleaf-project}}"
BQ_LOCATION="${CFG_BQ_LOC:-EU}"

# ── Dataset names ─────────────────────────────────────────────
DS_EXT=$(_read_toml  "dataset_ext"  "BIGQUERY_SETTINGS");  DS_EXT="${DS_EXT:-nordleaf_ext}"
DS_STG=$(_read_toml  "dataset_stg"  "BIGQUERY_SETTINGS");  DS_STG="${DS_STG:-nordleaf_stg}"
DS_DWH=$(_read_toml  "dataset_dwh"  "BIGQUERY_SETTINGS");  DS_DWH="${DS_DWH:-nordleaf_dwh}"
DS_MGMT=$(_read_toml "dataset_mgmt" "BIGQUERY_SETTINGS");  DS_MGMT="${DS_MGMT:-nordleaf_mgmt}"

echo "============================================================"
echo "  NordLeaf – BigQuery Dataset Setup"
echo "============================================================"
echo "  Project  : ${PROJECT_ID}"
echo "  Location : ${BQ_LOCATION}"
echo "  Datasets : ${DS_EXT}, ${DS_STG}, ${DS_DWH}, ${DS_MGMT}"
echo "  Config   : ${CONFIG_FILE}"
echo ""

# ── Create datasets ───────────────────────────────────────────
for ds in "$DS_EXT" "$DS_STG" "$DS_DWH" "$DS_MGMT"; do
  echo "Creating dataset: ${PROJECT_ID}:${ds} ..."
  bq mk \
    --dataset \
    --project_id="$PROJECT_ID" \
    --location="$BQ_LOCATION" \
    --description="NordLeaf ${ds} dataset" \
    "${PROJECT_ID}:${ds}" 2>/dev/null \
    || echo "  Dataset ${ds} already exists – skipping."
done

echo ""
echo "All BigQuery datasets ready."
echo ""

# ── Run SQL setup scripts in this directory ───────────────────
shopt -s nullglob
SQL_FILES=( "${SCRIPT_DIR}"/*.sql )
shopt -u nullglob

if [[ ${#SQL_FILES[@]} -eq 0 ]]; then
  echo "[WARN] No .sql files found in ${SCRIPT_DIR} – skipping table creation."
else
  for sql_file in "${SQL_FILES[@]}"; do
    echo "Running $(basename "$sql_file") ..."
    bq query \
      --use_legacy_sql=false \
      --project_id="$PROJECT_ID" \
      --location="$BQ_LOCATION" \
      < "$sql_file"
  done
fi

echo ""
echo "BigQuery setup complete!"
