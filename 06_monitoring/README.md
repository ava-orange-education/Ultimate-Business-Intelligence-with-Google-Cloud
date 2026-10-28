# NordLeaf Monitoring & Alerting

## Overview

```
GCP Resources (BigQuery, Composer, Dataflow, Vertex AI)
         |
         | Telemetry (metrics, logs, traces)
         v
Cloud Monitoring + Cloud Logging
    |
    |-- Dashboards
    |     NordLeaf Operations Dashboard
    |     Pipeline Health Dashboard
    |     Cost Dashboard
    |
    |-- Alert Policies
    |     Pipeline failure     -> Email + Slack #data-alerts
    |     DQ score < 95%      -> Email data-team
    |     Cost > budget 90%   -> Email management
    |     Sensor alert spike  -> PagerDuty + Slack #ops
    |     Data freshness > 25h -> Email + Slack
    |
    |-- Log-based Metrics
    |     Composer DAG failures  -> custom metric
    |     BigQuery job errors    -> custom metric
    |
    v
Notification Channels:
    Email: data-alerts@nordleaf.fi
    Slack: #data-ops, #data-alerts
    PagerDuty (P1 incidents)
```

## Alert Policies

| Alert | Threshold | Severity | Channel |
|-------|-----------|---------|---------|
| Composer DAG failure | Any failure | P1 | Email + Slack + PagerDuty |
| Data freshness | > 25 hours | P2 | Email + Slack |
| DQ score | < 95% | P2 | Email + Slack |
| BigQuery cost | > EUR 200/day | P3 | Email |
| Budget threshold | 75%, 90%, 100% | P3 | Email |
| Sensor alerts | > 10 in 5 min | P1 | Slack + PagerDuty |

## Setup
```bash
chmod +x setup_monitoring.sh
./setup_monitoring.sh nordleaf-project
```
