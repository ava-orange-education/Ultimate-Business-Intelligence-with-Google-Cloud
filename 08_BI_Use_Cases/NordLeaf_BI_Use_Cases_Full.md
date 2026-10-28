# NordLeaf BI Use Cases
## Comprehensive Business Intelligence for a Finnish Paper Manufacturer
### Inspired by Stora Enso — Implemented on Google Cloud Platform + BigQuery

---

## Company Context

NordLeaf Oy is a vertically integrated Finnish paper manufacturer modelled on **Stora Enso** — one of the world's largest producers of renewable packaging, biomaterials, wooden constructions, and paper. Like Stora Enso, NordLeaf:

- **Owns and manages forests** (through timber supply agreements with Metsä Forest, UPM Silvesta, Pölkky)
- **Operates multiple production mills** across Finland (5 mills: Tampere, Oulu, Turku, Espoo, Jyväskylä)
- **Controls the full supply chain** — from fibre procurement through chemicals, energy, and logistics
- **Sells to major European B2B customers** — publishers, packaging companies, retailers
- **Reports on ESG and sustainability** in line with EU Green Deal and EUDR requirements

---

## The 8 BI Domains

```
┌─────────────────────────────────────────────────────────────────────┐
│              NORDLEAF BI USE CASE DOMAINS                           │
│                                                                     │
│  🌲 FOREST &        🏭 PRODUCTION       🚚 SUPPLY CHAIN            │
│     FIBRE              OPERATIONS           PROCUREMENT             │
│   UC-01 Timber      UC-03 Mill OEE      UC-07 Supplier Scorecard   │
│   UC-02 FSC/PEFC    UC-04 Quality RCA   UC-08 Material Cost        │
│                     UC-05 Shifts        UC-09 Delivery OTD         │
│                     UC-06 S&OP          UC-10 Supply Risk          │
│                                                                     │
│  💰 SALES &         👥 CUSTOMER         ⚙️  IoT & OPERATIONS       │
│     REVENUE             CRM                 REAL-TIME              │
│   UC-11 Revenue     UC-12 Customer CLV  UC-19 Sensor Anomaly      │
│   UC-13 Forecast    UC-16 NPS/CSAT      UC-20 Energy KPIs         │
│   UC-14 OTD         UC-17 Churn ML                                │
│   UC-15 Markets     UC-18 Credit Risk                             │
│                                                                     │
│  📊 FINANCE &       ✅ GOVERNANCE &                                 │
│     P&L                 ESG                                        │
│   UC-21 P&L         UC-23 Data Quality                            │
│   UC-22 Working     UC-24 Carbon / ESG                            │
│          Capital        Reporting                                  │
└─────────────────────────────────────────────────────────────────────┘
```

---

## UC-01: Timber Harvest vs Mill Consumption Balance
**Domain:** Forest & Fibre | **Priority:** P1

### Business Problem
NordLeaf sources timber from 4 suppliers (Metsä Forest, UPM Silvesta, Pölkky, Baltic Timber). Each of the 5 mills consumes different fibre quantities depending on the paper grade being produced. Without visibility into the **supply-demand balance**, mills risk production stoppages from fibre shortfall or high carrying costs from surplus.

### Data Used
- `nordleaf_stg.deliveries` — material deliveries to each mill
- `nordleaf_dwh.fact_production` — actual production output per mill
- Industry fibre yield ratios: Newsprint 2.3×, Kraft 2.5×, Cardboard 1.8×, Specialty 2.8×, Tissue 1.5×

### Key Metrics
| Metric | Target |
|--------|--------|
| Fibre Balance per Mill (tons) | ≥ 0 tons (no shortfall) |
| Grade-A input material % | ≥ 70% |
| Cost per ton of fibre | Track vs market benchmark |

### Output & Action
- `nordleaf_mgmt.v_timber_balance_monthly` view
- Status: `CRITICAL SHORTFALL` → procurement alert to Metsä Forest
- Triggers Cloud Monitoring alert when balance < 500 tons

---

## UC-02: Sustainable Sourcing Scorecard (FSC/PEFC)
**Domain:** Forest & Fibre — ESG | **Priority:** P2

### Business Problem
The **EU Deforestation Regulation (EUDR)** requires 100% certified timber sourcing by 2025. NordLeaf must track what proportion of fibre comes from FSC/PEFC certified sources to ensure regulatory compliance and satisfy Scope 3 ESG reporting requirements.

### Data Used
- `nordleaf_stg.suppliers` — supplier certification status
- `nordleaf_stg.deliveries` — material type and volume

### Key Metrics
| Metric | Target |
|--------|--------|
| % FSC/PEFC certified fibre | 100% by 2025 |
| EUDR Compliance Status | COMPLIANT |
| Uncertified supplier volume | 0 tons |

---

## UC-03: Mill Efficiency & OEE Dashboard
**Domain:** Production Operations | **Priority:** P1

### Business Problem
NordLeaf's 5 mills collectively represent €500M+ in capital investment. Each mill runs 3 shifts, producing 5 paper grades. **Overall Equipment Effectiveness (OEE)** is the single most important operational KPI — world-class paper mills achieve 85%+ OEE. Without daily monitoring, inefficiencies compound invisibly.

### Data Used
- `nordleaf_dwh.fact_production` — planned_tons, actual_tons, efficiency_pct, quality_grade, shift

### OEE Formula
```
OEE = Performance × Quality
Performance = actual_tons / planned_tons
Quality     = Grade-A batches / total batches
```

### Key Metrics
| Metric | Target |
|--------|--------|
| OEE % | ≥ 85% (World Class) |
| Production efficiency | ≥ 95% |
| Grade-A batch rate | ≥ 90% |
| Capacity utilisation | 75-90% |

### Output
- `nordleaf_mgmt.v_mill_oee_monthly` — feeds Executive and Operations dashboards
- Cloud Monitoring alert: OEE < 75% → PagerDuty to Mill Manager + COO

---

## UC-04: Quality Grade Root Cause Analysis
**Domain:** Production Quality | **Priority:** P1

### Business Problem
When Grade-C paper is produced, NordLeaf loses approximately 15-20% margin vs Grade-A (Grade C sold at discount or recycled). The business needs to understand **why** Grade C occurs — is it:
- Low-quality input pulp (supplier issue)?
- Night shift performance degradation?
- Specific product type complexity?
- Mill equipment state?

### Data Used
- `nordleaf_dwh.fact_production` (output quality)
- `nordleaf_stg.deliveries` (input material quality by mill)
- Correlation: input Grade A% → output Grade A%

### Key Metrics
| Metric | Target |
|--------|--------|
| Grade-C batch rate | < 5% |
| Input Grade-A material | ≥ 70% |
| Night shift OEE vs Day | < 10% gap |

---

## UC-06: Sales & Operations Planning (S&OP)
**Domain:** Production Planning | **Priority:** P1

### Business Problem
S&OP is the heartbeat of paper manufacturing. NordLeaf must align:
- **Demand signal** (customer orders by product)
- **Supply capability** (mill production output by product)
- **Forecast** (ML 30-day demand by product × mill)

Misalignment creates either costly overproduction (storage, waste) or lost sales (stockouts, customer churn).

### Data Used
- `nordleaf_dwh.fact_orders` — demand by product per month
- `nordleaf_dwh.fact_production` — supply by product per month
- `nordleaf_dwh.demand_forecast` — ML forecast (ARIMA_PLUS)

### Key Metrics
| Metric | Target |
|--------|--------|
| Supply-Demand Gap | ± 200 tons/month |
| Forecast accuracy (MAPE) | < 15% |
| Cancellation rate | < 5% |

---

## UC-07: Supplier Performance Scorecard
**Domain:** Supply Chain / Procurement | **Priority:** P1

### Business Problem
NordLeaf spends approximately €18M/year on raw materials across 12 suppliers. Without a structured scorecard, procurement decisions are relationship-driven rather than data-driven. The scorecard combines:
- Quality (40% weight) — % Grade-A deliveries
- Reliability (30% weight) — delivery frequency consistency
- Contract compliance (30% weight) — volume and rating adherence

### Data Used
- `nordleaf_stg.deliveries` — delivery history
- `nordleaf_stg.suppliers` — contract data, rating, payment terms

### Supplier Tiers
| Score | Tier | Action |
|-------|------|--------|
| ≥ 85 | PREFERRED | Renew & expand volume |
| 70-84 | APPROVED | Maintain current terms |
| 50-69 | MONITOR | Issue improvement plan |
| < 50 | AT RISK | Renegotiate or replace |

---

## UC-08: Raw Material Cost & Spend Analytics
**Domain:** Supply Chain / Finance | **Priority:** P1

### Business Problem
Raw materials (timber, chemicals, energy) represent 60-70% of NordLeaf's COGS. Energy alone (Fortum Oyj, €6.2M contract) is the single largest spend. Understanding cost volatility, month-on-month trends, and cost-per-ton by mill enables smarter procurement and better margin management.

### Key Metrics
| Material | Typical Cost/Ton | Volatility Risk |
|----------|-----------------|----------------|
| Softwood Pulp | €150-250/ton | HIGH (market-linked) |
| Energy (electricity) | Variable/MWh | HIGH (Nordic power market) |
| Bleaching Chemicals | €200-400/ton | MEDIUM |
| Logistics | per shipment | LOW |

---

## UC-10: Supply Risk Dashboard
**Domain:** Supply Chain Risk | **Priority:** P1

### Business Problem
Single-supplier dependency is an existential risk in paper manufacturing. If Metsä Forest (NordLeaf's largest timber supplier) has a disruption, Tampere and Jyväskylä mills could face production stoppages within days. The risk dashboard monitors:
- Concentration risk (>80% from single supplier = CRITICAL)
- Quality streak risk (2+ Grade-C deliveries = ALERT)

---

## UC-11: Revenue & Margin Analysis
**Domain:** Sales / Finance | **Priority:** P1

### Business Problem
NordLeaf sells 5 paper grades at prices ranging from €520/ton (Kraft Paper) to €1,200/ton (Specialty Paper). But high price doesn't always mean high margin — Specialty Paper requires expensive inputs and high-precision production. The integrated revenue-margin analysis reveals the true profitability of each product × mill × customer region combination.

### Key Metrics
| Metric | Target |
|--------|--------|
| Gross Margin % | ≥ 25% |
| Revenue per ton | Benchmark vs index |
| Cancellation rate | < 5% |

---

## UC-12: Customer Profitability & CLV (RFM)
**Domain:** Sales / CRM | **Priority:** P1

### Business Problem
NordLeaf has 18 major EU customers — but not all are equal. Using **RFM analysis** (Recency, Frequency, Monetary), the system segments customers into:

| Segment | Definition | Action |
|---------|-----------|--------|
| CHAMPION | Recent, frequent, high value | Protect, cross-sell |
| LOYAL | Consistent, high value | Reward, upsell |
| PROMISING | Recent, lower value | Develop relationship |
| AT RISK | Previously high-value, now inactive | Win-back campaign |
| LOST | Inactive + low spend | Reactivate or exit |

---

## UC-13: 30-Day Demand Forecasting with BigQuery ML
**Domain:** Sales / S&OP | **Priority:** P1

### Business Problem
Without a demand forecast, NordLeaf plans production based on historical averages and sales rep estimates. The **ARIMA_PLUS model** in BigQuery ML:
- Learns seasonal patterns (summer slowdown, year-end publishing peak)
- Accounts for Finnish public holidays (reduced demand weeks)
- Provides 90% confidence intervals for production planning
- Identifies capacity bottlenecks 30 days ahead

### Model Performance Targets
| Metric | Target |
|--------|--------|
| MAPE | < 15% |
| Forecast horizon | 30 days |
| Granularity | Product × Mill × Week |
| Retrain frequency | Weekly (Sunday 02:00 EET) |

---

## UC-14: Order Fulfilment & On-Time Delivery (OTD)
**Domain:** Sales / Logistics | **Priority:** P1

### Business Problem
Newspaper publishers (Helsinki Publishers, Oslo Newspapers, Copenhagen Print House) have **zero tolerance** for late delivery — if Newsprint doesn't arrive, the press can't run. NordLeaf's SLA: Newsprint ≤10 days lead time, all others ≤14 days.

### Key Metrics
| Metric | Target |
|--------|--------|
| OTD rate (Newsprint) | ≥ 98% |
| OTD rate (other grades) | ≥ 95% |
| Avg lead time | ≤ SLA |
| Cancellation rate | < 5% |

---

## UC-16: Customer Satisfaction & NPS
**Domain:** Customer Experience | **Priority:** P2

### Business Problem
NordLeaf collects feedback in **Finnish and English** across 4 channels (Email, Web, Phone, Portal). German and Southern European customers (Berlin Media, Madrid Embalajes) give lower ratings. Understanding satisfaction by language, channel, and product helps prioritise service improvement.

### NPS Calculation (on 1-5 scale)
- Rating 5 → **Promoter**
- Rating 3 → **Passive**
- Rating 1-2 → **Detractor**
- NPS = (% Promoters - % Detractors) × 100

---

## UC-21: Integrated P&L (Revenue vs COGS)
**Domain:** Finance / Executive | **Priority:** P1

### Business Problem
The CFO needs a monthly P&L view combining:
- **Revenue** (from fact_orders)
- **Raw Material COGS** (from fact_deliveries)
- **Estimated EBITDA** (assuming 30% fixed cost ratio)

This is the primary financial reporting use case — feeds board packs, investor relations, and strategic pricing decisions.

### Expected Output
| Line Item | Typical Range |
|-----------|--------------|
| Revenue per ton | €520-1,200 |
| Material cost per ton | €150-400 |
| Gross Margin % | 25-50% |
| Est. EBITDA Margin % | -5% to +20% |

---

## UC-24: Carbon Footprint & ESG Reporting
**Domain:** Sustainability | **Priority:** P1

### Business Problem
Stora Enso publishes an annual **sustainability report** with granular carbon data. NordLeaf must do the same. EU Green Deal requires 55% emissions reduction by 2030 vs 2020 baseline. The BI system tracks:

| Scope | Source | Emission Factor |
|-------|--------|----------------|
| Scope 1 | Process heat (biomass boilers) | 0.40 t CO2e/t paper |
| Scope 2 | Finnish grid electricity | 0.079 kg CO2e/kWh |
| Scope 3 | Supplier logistics (trucks) | 0.168 kg CO2e/t-km |

### Carbon Intensity KPI
- **Target**: < 0.35 t CO2e per tonne of paper = ON TRACK for Green Deal
- **Red flag**: > 0.55 t CO2e/tonne = investment required

---

## Technology Stack Summary

| Layer | GCP Service | Use Cases |
|-------|------------|-----------|
| Data Ingestion | Cloud Data Fusion, Dataflow, Pub/Sub | All UCs |
| Storage | Cloud Storage (GCS) | Raw files |
| Warehouse | BigQuery (EXT→STG→DWH→MGMT) | All UCs |
| ML / AI | BigQuery ML, Vertex AI Gemini | UC-13, UC-17, UC-19, UC-16 |
| Orchestration | Cloud Composer (Airflow) | All UCs |
| Governance | Dataplex, Data Catalog | UC-23, UC-24, UC-02 |
| Visualisation | Looker Studio | All UCs |
| Monitoring | Cloud Monitoring, Alerting | UC-03, UC-10, UC-14 |
| Security | IAM, Policy Tags | All UCs |

---

## Looker Studio Dashboard Map

| Dashboard | Use Cases | Primary Users |
|-----------|-----------|--------------|
| **Executive Overview** | UC-11, UC-21, UC-06 | CEO, CFO, Board |
| **Mill Operations** | UC-03, UC-04, UC-05 | COO, Mill Managers |
| **Supply Chain** | UC-07, UC-08, UC-09, UC-10 | Supply Chain Director |
| **Sales & Forecast** | UC-12, UC-13, UC-14, UC-15 | Sales Director, Account Mgrs |
| **Customer 360** | UC-12, UC-16, UC-17 | CRM, Account Managers |
| **Forest & Fibre** | UC-01, UC-02 | Forest Director, Procurement |
| **ESG & Sustainability** | UC-02, UC-20, UC-24 | Sustainability Officer, Board |
| **Data Quality** | UC-23 | Data Engineering, IT |
