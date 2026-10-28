# NordLeaf GCP IAM Roles & Permissions

## Overview

```
Google Workspace Identity (nordleaf.fi)
         |
         | Authentication (SSO / MFA enforced)
         v
Cloud IAM Policy
    |
    |-- Groups (Human users)
    |       nordleaf-data-engineers@nordleaf.fi
    |       nordleaf-analysts@nordleaf.fi
    |       nordleaf-finance@nordleaf.fi
    |       nordleaf-management@nordleaf.fi
    |
    |-- Service Accounts (Machine identities)
            nordleaf-data-pipeline@
            nordleaf-dataflow@
            nordleaf-composer@
            nordleaf-vertex-ai@
            nordleaf-data-fusion@
            nordleaf-bi-reader@
            nordleaf-dataplex@
```

## Role Matrix

| Identity | BigQuery | GCS | Dataflow | Composer | Vertex AI | Dataplex |
|----------|---------|-----|----------|----------|-----------|----------|
| nordleaf-data-engineers@ | dataEditor | objectAdmin | developer | envAdmin | user | viewer |
| nordleaf-analysts@ | dataViewer (dwh,mgmt only) | - | - | - | - | viewer |
| nordleaf-finance@ | dataViewer + finegrained | - | - | - | - | - |
| nordleaf-management@ | dataViewer | - | - | - | - | viewer |
| nordleaf-data-pipeline SA | dataEditor | objectAdmin | - | - | - | - |
| nordleaf-dataflow SA | dataEditor | objectViewer | worker | - | - | - |
| nordleaf-composer SA | dataEditor | objectAdmin | developer | worker | - | - |
| nordleaf-vertex-ai SA | dataEditor | objectAdmin | - | - | user | - |
| nordleaf-bi-reader SA | dataViewer | - | - | - | - | - |

## BigQuery Dataset Access

| Dataset | Engineers | Analysts | Finance | Pipeline SA |
|---------|-----------|---------|---------|-------------|
| nordleaf_ext | OWNER | - | - | WRITER |
| nordleaf_stg | OWNER | - | - | WRITER |
| nordleaf_dwh | OWNER | READER | READER | WRITER |
| nordleaf_mgmt | OWNER | READER | READER | WRITER |

## Column-Level Security (Policy Tags)

| Column | Tag | Who can see unmasked |
|--------|-----|---------------------|
| unit_price_eur | Confidential/Financial | finance group, management group |
| total_value_eur | Confidential/Financial | finance group, management group |
| contact_email | PII/Email | data-engineers group, pipeline SA |
| contract_value | Confidential/Financial | finance group |

## Setup
```bash
chmod +x setup_iam.sh
./setup_iam.sh nordleaf-project
```
