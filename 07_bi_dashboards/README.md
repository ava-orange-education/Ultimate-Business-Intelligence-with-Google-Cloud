# NordLeaf BI Dashboards (Looker Studio)

## Overview

```
BigQuery nordleaf_dwh + nordleaf_mgmt
         |
         | BigQuery connector
         v
Looker Studio (formerly Data Studio)
    |
    |-- Dashboard 1: Executive Revenue Overview
    |     - Revenue KPIs (current month, YoY)
    |     - Monthly revenue trend by product type
    |     - Mill revenue comparison
    |     - Top 10 customers by revenue
    |
    |-- Dashboard 2: Mill Operations & Production KPIs
    |     - Efficiency heatmap (mill x month)
    |     - Quality grade distribution
    |     - Shift performance comparison
    |     - Sensor alerts timeline
    |
    |-- Dashboard 3: Supplier Scorecard
    |     - Supplier quality scores
    |     - Delivery volume by supplier
    |     - Spend analysis by supplier type
    |
    |-- Dashboard 4: Customer Analytics
    |     - Customer segmentation (type, region)
    |     - 360-degree customer view
    |     - Churn risk indicators (from Vertex AI)
    |     - Cancellation rate analysis
    |
    |-- Dashboard 5: Data Quality & Pipeline Status
          - DQ pass/fail rates by table
          - Pipeline run history
          - Last load timestamps
          - Data freshness indicators
```

## Setting Up Looker Studio

### 1. Connect to BigQuery
- Go to [Looker Studio](https://lookerstudio.google.com)
- Create New Report -> Add Data -> BigQuery
- Select project: `nordleaf-project`
- Use Custom Query -> paste queries from `looker_studio_queries.sql`

### 2. Recommended Chart Types

| Dashboard | Chart | Dimensions | Metrics |
|-----------|-------|-----------|---------|
| Revenue Overview | Scorecard | - | total_revenue_eur, order_count |
| Revenue Overview | Time series | month | revenue_eur |
| Revenue Overview | Bar chart | product_type | revenue_eur |
| Mill Operations | Heatmap | mill_name x month | avg_efficiency_pct |
| Mill Operations | Pie chart | quality_grade | batch_count |
| Supplier Scorecard | Table | supplier_name | quality_score_pct |
| Customer Analytics | Geo map | country | total_revenue_eur |
| DQ Status | Scorecard | - | pass_rate_pct |

### 3. Scheduled Email Reports
In Looker Studio: File -> Schedule email delivery -> daily 08:00 EET
Recipients: management@nordleaf.fi, operations@nordleaf.fi
