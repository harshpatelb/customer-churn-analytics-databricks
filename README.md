<div align="center">

# 🏦 Customer Churn Analytics
**Databricks Lakehouse · PySpark · SQL · Power BI**

*An end-to-end analytics pipeline that turns raw banking data into retention intelligence.*

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](#)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](#)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat-square&logo=delta&logoColor=white)](#)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white)](#)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)](#)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](#)

</div>

---

## The Problem

Banks lose customers quietly. By the time a relationship manager notices a drop in activity, the customer has already half-left. This project tackles that blind spot — building a full lakehouse pipeline that ingests raw customer data, engineers churn signals, models a star schema, and delivers a Power BI dashboard where retention decisions actually happen.

It's scoped around a real Kaggle banking dataset, but built the way a production data team would build it: layered architecture, governed storage, modular notebooks, and business-facing KPIs.

---

## Architecture

The pipeline follows the **Medallion Architecture** — a pattern used by data teams at banks, fintechs, and enterprise analytics shops worldwide.

```
                      ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
                      │  🥉 Bronze  │ ──▶ │  🥈 Silver  │ ──▶ │  🥇 Gold    │ ──▶ │  📊 Power BI│
                      │             │     │             │     │             │     │             │
                      │  Raw CSV    │     │  Cleaned &  │     │  Star schema│     │  Executive  │
                      │  → Delta    │     │  engineered │     │  + KPI tabs │     │  dashboard  │
                      └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
                        Raw data            Trusted data        Business data         Decisions
```

| Layer | What happens |
|-------|-------------|
| 🥉 **Bronze** | Kaggle CSV lands in Databricks. No transformations — raw Delta table, full lineage. |
| 🥈 **Silver** | Nulls handled, types cast, categories standardized. Features engineered for churn analysis. |
| 🥇 **Gold** | Aggregated into a star schema. SQL-based KPI tables pre-built for fast BI queries. |
| 📊 **Power BI** | Gold tables connected via Databricks SQL. Interactive dashboard for business stakeholders. |

---

## Data Pipeline

**Bronze — Ingestion**
The raw Kaggle CSV is loaded into Databricks and written to Delta Lake as-is. Schema is inferred and stored. Nothing is changed at this layer — that's the point. Every downstream issue can always be traced back to source.

**Silver — Cleaning & Feature Engineering**
This is where the dataset becomes usable:
- Nulls removed, data types enforced, duplicate rows dropped
- `country` and `gender` standardized for consistent joins
- New features derived: age buckets, balance tiers, tenure groups, engagement score (activity + product count)

**Gold — Star Schema Modeling**
Data is restructured into analytical tables ready for BI consumption:
- One central fact table (`fact_churn_events`) joined to four dimension tables
- Pre-aggregated KPI tables for churn rate, segment breakdowns, and financial profiles

**Power BI — Visualization**
Gold tables connect to Power BI via DirectQuery. The dashboard exposes churn KPIs, segment filters, and trend lines — designed for a retention analyst, not a data engineer.

---

## Data Model

A classic star schema. Simple, fast, and exactly what a BI layer needs.

```
                    ┌──────────────────┐
                    │   dim_customer   │
                    │  age · gender    │
                    │  tenure          │
                    └────────┬─────────┘
                             │
   ┌──────────────────┐      │      ┌──────────────────────┐
   │  dim_geography   │──────┼──────│  dim_financial       │
   │  country         │      │      │  balance · score     │
   │  region          │      │      │  num_products        │
   └──────────────────┘      │      └──────────────────────┘
                             │
                    ┌────────▼─────────┐
                    │  fact_churn_     │
                    │    events        │
                    │                  │
                    │  churn flag      │
                    │  customer_id FK  │
                    │  all dim FKs     │
                    └────────┬─────────┘
                             │
                    ┌────────▼─────────┐
                    │  dim_engagement  │
                    │  is_active       │
                    │  has_credit_card │
                    │  engagement_score│
                    └──────────────────┘
```

| Table | Role |
|-------|------|
| `fact_churn_events` | One row per customer. Churn flag + metrics + foreign keys to all dims. |
| `dim_customer` | Age, gender, tenure. The who. |
| `dim_geography` | Country, region. The where. |
| `dim_financial` | Balance, credit score, number of products. The what they have. |
| `dim_engagement` | Activity status, credit card ownership, computed engagement score. The how engaged. |

---

## KPIs Generated

| KPI | What it answers |
|-----|----------------|
| Overall churn rate | What percentage of customers are leaving? |
| Churn by age group | Which generation is most at risk? |
| Churn by country | Are there geographic patterns worth acting on? |
| Active vs. inactive churn | Does engagement actually predict retention? |
| Avg. balance of churned customers | Are we losing high-value customers? |
| Churn by products held | Does cross-selling improve loyalty? |
| Credit card ownership vs. churn | Does product depth reduce churn risk? |

---

## What the Data Shows

A few patterns that came through clearly in this dataset:

- Customers holding just one product churn at a significantly higher rate than those with two or more. Cross-sell isn't just a revenue play — it's a retention play.
- Inactive members churn at roughly 2x the rate of active ones. Engagement signals are predictive, not just descriptive.
- Certain countries show disproportionately high churn, suggesting regional service gaps or competitive pressure worth investigating.
- High-balance customers are not immune — the data challenges the assumption that wealthier customers are stickier.

These aren't conclusions — they're starting points for a retention team. The dashboard lets them drill in.

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Platform | Databricks Lakehouse |
| Processing | PySpark, Spark SQL |
| Storage | Delta Lake |
| Modeling | SQL (star schema) |
| Visualization | Power BI |
| Language | Python |
| Source data | Kaggle — Bank Customer Churn Dataset |
| Version control | Git + GitHub |

---

## Project Structure

```
customer-churn-analytics/
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformation.ipynb
│   ├── 03_gold_modeling.ipynb
│   └── 04_kpi_generation.ipynb
├── sql/
│   ├── create_star_schema.sql
│   ├── kpi_queries.sql
│   └── data_quality_checks.sql
├── data/
│   └── bank_customer_churn.csv
├── docs/
│   ├── architecture.md
│   └── data_dictionary.md
├── images/
│   └── (screenshots go here)
└── README.md
```

---

## How to Run

**1. Clone the repo**
```bash
git clone https://github.com/your-username/customer-churn-analytics.git
```

**2. Upload the dataset**
Add `bank_customer_churn.csv` to DBFS or a Databricks Volume. Update the path in `01_bronze_ingestion.ipynb`.

**3. Run notebooks in order**
```
01_bronze_ingestion       → raw Delta tables
02_silver_transformation  → cleaned + feature-engineered tables
03_gold_modeling          → star schema built
04_kpi_generation         → KPI tables ready for BI
```

**4. Connect Power BI**
Open Power BI Desktop → Get Data → Databricks → enter your SQL Warehouse hostname and HTTP path → import Gold tables.

---

## Screenshots

| | |
|--|--|
| Architecture diagram | `images/architecture_diagram.png` |
| Bronze layer | `images/bronze_layer.png` |
| Silver transformation | `images/silver_transform.png` |
| Power BI dashboard | `images/powerbi_dashboard.png` |

*(Replace placeholders with exported screenshots once notebooks are run)*

---

## Why This Project Exists

Built as a portfolio project to demonstrate end-to-end data analytics skills relevant to **Data Analyst** and **Data Engineer** roles — specifically in banking, fintech, and enterprise analytics.

The skills it covers: Lakehouse architecture design, PySpark ETL, dimensional modeling, SQL-based KPI generation, and translating raw data into business dashboards. The kind of work that shows up in real job descriptions, not just tutorials.

---

<div align="center">
<sub>Kaggle dataset · Databricks Community Edition · Power BI Desktop</sub>
</div>
