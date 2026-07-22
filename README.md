<div align="center">

# 🏦 NovaBC Bank — Customer Churn Analytics
**Databricks Lakehouse · PySpark · Spark SQL · Delta Lake · Power BI**

*An end-to-end analytics pipeline that turns 80,000 raw banking records into retention intelligence — identifying not just who churned, but who's about to.*

[![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)](#)
[![PySpark](https://img.shields.io/badge/PySpark-E25A1C?style=flat-square&logo=apachespark&logoColor=white)](#)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-00ADD8?style=flat-square&logo=delta&logoColor=white)](#)
[![SQL](https://img.shields.io/badge/SQL-4479A1?style=flat-square&logo=postgresql&logoColor=white)](#)
[![Power BI](https://img.shields.io/badge/Power_BI-F2C811?style=flat-square&logo=powerbi&logoColor=black)](#)
[![Python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)](#)

</div>

---

## The Problem

Banks lose customers quietly. By the time a relationship manager notices a drop in activity, the customer has already half-left — and most risk models only measure *financial* health, not behavioural disengagement. This project tackles that blind spot.

It builds a full lakehouse pipeline that ingests raw customer data, engineers behavioural churn signals, models a star schema, and delivers a two-page Power BI dashboard where the headline finding isn't just "18% of customers churned" — it's that **34,400 currently active customers show the exact same warning signs**, and are still retainable.

Built the way a production data team would build it: layered architecture, governed storage (Unity Catalog), modular notebooks, and business-facing KPIs — not a single notebook running top to bottom on a flat CSV.

---

## Architecture

The pipeline follows the **Medallion Architecture** — the pattern used by data teams at banks, fintechs, and enterprise analytics shops running on Databricks.

```
+-------------+       +-------------+       +-------------+       +-------------+
|    BRONZE   |       |    SILVER   |       |     GOLD    |       |   POWER BI  |
|             |  -->  |             |  -->  |             |  -->  |             |
| Raw -> Delta|       | Star schema |       | 8 KPI tables|       |  Dashboard  |
+-------------+       +-------------+       +-------------+       +-------------+
 Raw data           Trusted data        Business data       Decisions
```

| Layer | What happens |
|-------|--------------|
| 🥉 **Bronze** | 80,000-row CSV lands in a Unity Catalog Volume, read into Spark and written to Delta with zero transformations. Metadata columns (`_ingestion_timestamp`, `_source_file`, `_pipeline_layer`) added for full data lineage. |
| 🥈 **Silver** | PII dropped, columns renamed and type-cast, duplicates checked. 6 features engineered from actual data distributions — not arbitrary cutoffs. Split into a 4-table star schema. |
| 🥇 **Gold** | 8 KPI tables built with Spark SQL — CTEs, `UNION ALL`, and a `RANK()` window function — aggregating churn across every dimension the business cares about. |
| 📊 **Power BI** | Gold tables + a unified view imported into Power BI (Import mode). Two-page interactive dashboard for executive and root-cause analysis. |

---

## Data Pipeline

**🥉 Bronze — Ingestion**
The raw CSV is loaded from a Unity Catalog Volume and written to Delta Lake exactly as it came in. No cleaning, no fixing — Bronze is a safety net, not a workspace. Data quality checks (row count, nulls, churn split) confirm the write landed correctly: **80,000 rows, zero nulls, 18% churn rate.**

**🥈 Silver — Cleaning & Feature Engineering**
This is where the dataset becomes analysis-ready:
- PII (name, address) and Bronze-only metadata columns dropped
- Truncated column names fixed (`credit_sco` → `credit_score`, etc.), `balance` cast to double for decimal precision
- Occupation values translated from Vietnamese to English for dashboard readability
- 6 features engineered — several using **quartile-based thresholds derived from the actual data distribution** rather than round numbers, so segments are evenly populated
- Data modeled into a star schema: 1 fact table + 3 dimension tables

**🥇 Gold — KPI Modeling**
Silver tables are joined into a master view and aggregated into 8 business-ready tables — covering overall KPIs, multi-dimensional segmentation (via CTE), churn drivers, risk validation, occupation, activity status, tenure, and a province ranking built with a `RANK()` window function.

**📊 Power BI — Visualization**
Gold tables and the master view are imported into Power BI. Page 1 answers *what* is happening with churn; Page 2 answers *why*, and flags which currently active customers are next.

---

## Data Model

A lean star schema — 1 fact table, 3 dimensions. No unnecessary tables: geography lives inside `dim_customer` since the dataset has a single geographic column, and financial metrics live in the fact table since they're core churn-event measures, not descriptive attributes.

```
                         +------------------+
                         |   dim_customer   |
                         +------------------+
                         | gender, age      |
                         | occupation       |
                         | origin_province  |
                         | customer_segment |
                         +------------------+
                                   |
                        +--------------------+
                        | fact_churn_events  |
                        +--------------------+
                        | churn flag (exit)  |
                        | balance, tenure    |
                        | num_cards/services |
                        | active_member      |
                        +--------------------+
 
                    (fact_churn_events joins to)
 
       +------------------+        +-------------------------+
       |    dim_scores    |        |  dim_customer_features  |
       +------------------+        +-------------------------+
       | engagement_score |        | age_group, balance_tier |
       | risk_score       |        | tenure_group            |
       +------------------+        | engagement_tier         |
                                   | churn_risk_label *      |
                                   | churn_reason_inferred * |
                                   +-------------------------+
 
  * = custom-engineered feature, independently validated against churn outcomes
```

| Table | Role |
|-------|------|
| `fact_churn_events` | One row per customer. Churn flag + core numeric metrics + FK. |
| `dim_customer` | Demographics and geography. The *who* and *where*. |
| `dim_scores` | Pre-existing engagement and risk scores from the source data. |
| `dim_customer_features` | 6 engineered features, including two custom, independently-validated churn signals (⭐). |

---

## Engineered Features

| Feature | How it was built | Why |
|---|---|---|
| `age_group` | Young / Middle Aged / Senior | Standard demographic segmentation |
| `balance_tier` | Quartile cutoffs (25th/50th/75th percentile) | Round-number tiers left "Zero Balance" empty since the minimum balance was 400K+ — quartiles ensure every tier holds ~25% of customers |
| `tenure_group` | New / Regular / Loyal, cutoffs chosen after checking actual tenure range (max = 4 yrs) | Avoids meaningless categories like "8+ years" when no customer has that tenure |
| `engagement_tier` | Low / Medium / High, based on the engagement_score distribution | Turns a raw 0–100 score into an actionable segment |
| ⭐ `churn_risk_label` | Rule-based: inactive + low engagement → **High Risk** (independent of the dataset's built-in `risk_score`) | The source `risk_segment` only ever labelled customers Low/Medium — it measures *financial* risk, not churn propensity. This label was engineered from behavioural signals and validated: High Risk customers churn at **4x** the rate of Low Risk customers |
| ⭐ `churn_reason_inferred` | Rule-based, applied to churned *and* still-active customers (e.g. `Churned - Disengagement`, `At Risk - Disengagement`) | Since the dataset has no explicit churn-reason field, this infers probable drivers from behaviour — and flags at-risk customers *before* they leave |

---

## Gold Layer — KPI Tables

| Table | SQL Technique | Answers |
|-------|---------------|---------|
| `gold_churn_summary` | Aggregation | Overall churn rate and executive KPI cards |
| `gold_churn_by_segment` | **CTE + UNION ALL** | Churn rate by age, balance, and engagement in one unified table |
| `gold_churn_by_reason` | Aggregation | What's driving churn — and who's at risk next |
| `gold_churn_risk_analysis` | Aggregation | Validates the custom `churn_risk_label` against real churn outcomes |
| `gold_churn_by_occupation` | Aggregation | Which professions churn most |
| `gold_churn_active_vs_inactive` | Aggregation | Does activity status predict churn? |
| `gold_churn_by_tenure` | Aggregation | Does relationship length protect against churn? |
| `gold_churn_top_provinces` | **Window Function `RANK()`** | Ranks provinces by churn rate |

Additional CTEs, window functions, and subqueries are showcased in [`sql/kpi_queries.sql`](sql/kpi_queries.sql).

---

## What the Data Shows

- **Disengagement is the #1 churn driver — and it's still spreading.** 12,266 customers already churned while inactive and disengaged. A further **34,400 active customers show the identical pattern** and haven't left yet. That's 43% of the entire base — the single most actionable number in this project.
- **Inactive members churn at 3x the rate of active members** (21.1% vs 6.5%), with a stark gap in average engagement score (20.4 vs 73.3 out of 100).
- **The bank's existing risk score doesn't predict churn.** It classifies most customers as Low/Medium financial risk regardless of churn outcome. The custom `churn_risk_label` built from behavioural signals instead shows High Risk customers churning at **4x** the rate of Low Risk.
- **Low-balance customers churn at 38% vs 2% for Premium-balance customers** — a ~20x gap, the strongest single financial predictor in the dataset.
- **Life-stage matters.** Housewives/students churn at 42.5%; small business owners and managers churn under 1%. The common thread isn't income — it's engagement score, which tracks the same pattern almost exactly.
- **Tenure is a weak lever.** Loyal customers (4 yrs) churn at 13.3% vs 19% for new customers — real, but far smaller than the 3–4x swings driven by engagement.

Full write-up with business recommendations: [`docs/case_study.md`](docs/case_study.md)

---

## Dashboard

**Page 1 — Executive Overview** (*what's happening*)
KPI cards (Total / Churned / Churn Rate / Retention Rate / Avg Balance) · Churn by Age Group · Province Performance · Activity Impact · Churn Risk Scatter · Churn by Balance Tier

**Page 2 — Churn Risk & Root Cause Analysis** (*why, and who's next*)
KPI cards (High Risk / At Risk / Avg Engagement / High Risk Churn Rate) · Churn by Inferred Reason · Churn by Risk Label · Occupation Risk Treemap · Engagement Score Distribution · Risk vs Engagement Heatmap Matrix

| | |
|--|--|
| Page 1 | `images/dashboard_page1.png` |
| Page 2 | `images/dashboard_page2.png` |
| Architecture diagram | `images/architecture_diagram.png` |

---

## Tech Stack

| Layer | Tool |
|-------|------|
| Platform | Databricks Lakehouse, Unity Catalog |
| Processing | PySpark, Spark SQL |
| Storage | Delta Lake |
| Modeling | Star schema (SQL) |
| Visualization | Power BI (Import mode) |
| Language | Python |
| Version control | Git + GitHub |

---

## Project Structure

```
meridian-bank-churn-analytics/
├── notebooks/
│   ├── bronze/
│   │   └── 01_bronze_ingestion.py
│   ├── silver/
│   │   └── 02_silver_transformation.py
│   └── gold/
│       └── 03_gold_kpi_tables.py
├── sql/
│   └── kpi_queries.sql
├── docs/
│   └── case_study.md
├── images/
│   ├── architecture_diagram.png
│   ├── dashboard_page1.png
│   └── dashboard_page2.png
├── powerbi/
│   └── churn_dashboard.pbix
└── README.md
```

---

## How to Run

**1. Clone the repo**
```bash
git clone https://github.com/your-username/meridian-bank-churn-analytics.git
```

**2. Upload the dataset**
Add the source CSV to a Databricks Unity Catalog Volume. Update `FILE_PATH` in `01_bronze_ingestion.py`.

**3. Run notebooks in order**
```
01_bronze_ingestion       → raw Delta table, lineage tracked
02_silver_transformation  → cleaned, feature-engineered star schema
03_gold_kpi_tables        → 8 aggregated KPI tables
```

**4. Connect Power BI**
Power BI Desktop → Get Data → Azure Databricks → enter SQL Warehouse hostname + HTTP path → authenticate with a Personal Access Token → import Gold tables + `vw_churn_master` (Import mode).

---

## Why This Project Exists

Built as a portfolio project to demonstrate end-to-end data analyst skills relevant to **Data Analyst** and **Analytics Engineer** roles in banking and fintech: lakehouse architecture, PySpark transformations, dimensional modeling, feature engineering grounded in real data distributions rather than guesswork, advanced SQL (CTEs, window functions), and translating raw signals into a retention strategy a business can actually act on.

---

<div align="center">
<sub>Databricks Community Edition · Power BI Desktop · Author: Harshkumar Patel</sub>
</div>
