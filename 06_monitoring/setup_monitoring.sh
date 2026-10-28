#!/bin/bash
# ============================================================
# NordLeaf GCP Monitoring & Alerting Setup
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

CFG_PROJECT=$(_read_toml "PROJECT_ID")
CFG_EMAIL=$(_read_toml   "alert_email" "COMPOSER_SETTINGS")

PROJECT_ID="${1:-${CFG_PROJECT:-nordleaf-project}}"
ALERT_EMAIL="${CFG_EMAIL:-data-alerts@nordleaf.fi}"

# Create notification channel (email)
gcloud alpha monitoring channels create \
  --project="$PROJECT_ID" \
  --display-name="NordLeaf Data Alerts" \
  --type=email \
  --channel-labels="email_address=${ALERT_EMAIL}"

# Budget alert (Cloud Billing)
gcloud billing budgets create \
  --billing-account="$(gcloud billing projects describe $PROJECT_ID --format='value(billingAccountName)')" \
  --display-name="NordLeaf Monthly Budget" \
  --budget-amount=2000EUR \
  --threshold-rule=percent=75 \
  --threshold-rule=percent=90 \
  --threshold-rule=percent=100

echo "Monitoring setup complete for project: $PROJECT_ID"
