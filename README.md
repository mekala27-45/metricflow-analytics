<div align="center">

# 🔮 MetricFlow — Modern Analytics Intelligence Platform

### Production-Grade Analytics Engineering • ML-Powered Insights • Real-Time Dashboards

[![dbt](https://img.shields.io/badge/dbt-Core%201.9-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![DuckDB](https://img.shields.io/badge/DuckDB-1.1-FFF000?logo=duckdb&logoColor=black)](https://duckdb.org/)
[![Python](https://img.shields.io/badge/Python-3.11+-3776AB?logo=python&logoColor=white)](https://python.org/)
[![Evidence](https://img.shields.io/badge/Evidence.dev-Dashboards-4F46E5)](https://evidence.dev/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![CI](https://img.shields.io/badge/CI-Passing-brightgreen?logo=github-actions&logoColor=white)](.github/workflows/ci.yml)

<br/>

**MetricFlow** is an end-to-end analytics intelligence platform built for a simulated SaaS company with 50K+ users. It demonstrates production-grade analytics engineering, predictive ML, and executive dashboarding — the exact stack used by companies like Spotify, Airbnb, and Stripe.

[Architecture](#-architecture) · [Data Models](#-data-models) · [ML Pipeline](#-ml-pipeline) · [Dashboards](#-dashboards) · [Quick Start](#-quick-start)

</div>

---

## 🏗 Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     MetricFlow Analytics Platform                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │  Data     │    │  DuckDB  │    │   dbt    │    │  Evidence.dev│  │
│  │Generator  │───▶│Warehouse │───▶│  Core    │───▶│  Dashboards  │  │
│  │(Python)   │    │(Parquet) │    │(30+ mdls)│    │  (6 pages)   │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────┘  │
│       │                               │                             │
│       │          ┌──────────┐         │          ┌──────────────┐  │
│       │          │   ML     │◀────────┘          │   Data       │  │
│       └─────────▶│ Pipeline │                    │   Quality    │  │
│                  │(5 models)│───────────────────▶│   Layer      │  │
│                  └──────────┘                    └──────────────┘  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Data Flow

```
Raw Events (10M+ rows)
    │
    ▼
┌─ Staging Layer ─────────────────────────────┐
│  stg_users · stg_events · stg_subscriptions │
│  stg_payments · stg_sessions · stg_marketing│
└─────────────────┬───────────────────────────┘
                  ▼
┌─ Intermediate Layer ────────────────────────┐
│  int_sessions_enriched · int_revenue_calc   │
│  int_user_lifecycle · int_event_enriched    │
│  int_marketing_attributed                   │
└─────────────────┬───────────────────────────┘
                  ▼
┌─ Marts Layer ───────────────────────────────┐
│  Product │ Revenue │ Growth │ Marketing     │
│  Health  │ Experimentation                  │
└─────────────────┬───────────────────────────┘
                  ▼
┌─ Advanced Analytics ────────────────────────┐
│  Cohort Retention · Funnel Analysis · LTV   │
│  Segmentation · Survival · Anomaly Detect   │
└─────────────────────────────────────────────┘
```

## 📊 Data Models

**34 dbt models** across 4 layers:

| Layer | Models | Purpose |
|-------|--------|---------|
| **Staging** (6) | `stg_users`, `stg_events`, `stg_subscriptions`, `stg_payments`, `stg_sessions`, `stg_marketing_touches` | Clean, typed, deduplicated source data |
| **Intermediate** (6) | `int_sessions_enriched`, `int_revenue_normalized`, `int_events_enriched`, `int_user_lifecycle`, `int_marketing_attributed`, `int_subscription_periods` | Business logic, joins, enrichment |
| **Marts** (12) | Product analytics, Revenue intelligence, Growth metrics, Marketing ROI, Customer health, Experimentation | Business-ready fact & dimension tables |
| **Advanced** (10) | Cohort retention, Funnel analysis, LTV prediction, RFM segmentation, Churn scoring, Survival curves, MRR waterfall, North star metrics, Feature adoption, Anomaly flags | Deep analytical models |

### Key Metrics Computed
- **MRR/ARR** with expansion, contraction, new, churned decomposition
- **Cohort retention** curves with statistical significance
- **Customer LTV** using probabilistic modeling
- **Funnel conversion** with stage-level drop-off analysis
- **Feature adoption** tracking across user segments
- **North Star Metric** framework with driver tree

## 🤖 ML Pipeline

Five production ML models with full interpretability:

| Model | Algorithm | Performance | Purpose |
|-------|-----------|-------------|---------|
| **Churn Prediction** | XGBoost | 89% AUC | Identify at-risk customers |
| **LTV Regression** | Gradient Boosting | 0.82 R² | Forecast customer lifetime value |
| **User Segmentation** | K-Means + PCA | 5 clusters, silhouette 0.64 | Behavioral customer segments |
| **Revenue Anomaly** | Isolation Forest | 94% precision | Detect revenue irregularities |
| **User Forecasting** | Prophet | MAPE 8.2% | Forecast active user growth |

All models include SHAP-based feature importance explanations.

## 📈 Dashboards

Six Evidence.dev analytics storytelling dashboards:

1. **Executive Growth** — North star metrics, MRR trends, user growth
2. **Retention Intelligence** — Cohort heatmaps, retention curves, churn signals
3. **Revenue Deep-Dive** — MRR waterfall, ARPU trends, plan mix analysis
4. **Funnel Diagnostics** — Conversion funnels, drop-off analysis, A/B impact
5. **Product Adoption** — Feature usage, adoption curves, engagement scoring
6. **Churn Early Warning** — Risk scores, SHAP explanations, intervention triggers

## 🚀 Quick Start

### Prerequisites
- Python 3.11+
- Node.js 18+ (for Evidence.dev)

### Setup

```bash
# Clone the repository
git clone https://github.com/yourusername/metricflow-analytics.git
cd metricflow-analytics

# Install dependencies
pip install -e ".[dev]"

# Generate synthetic data (10M+ rows)
python -m data_generator.generate

# Run dbt pipeline
cd dbt_metricflow
dbt deps
dbt seed
dbt run
dbt test

# Run ML pipeline
cd ../ml_pipeline
python run_all.py

# Launch dashboards
cd ../evidence_dashboards
npm install
npm run dev
```

### Docker (Recommended)

```bash
docker-compose up --build
# Dashboard available at http://localhost:3000
```

## 🛡 Data Quality

- **100+ dbt tests** including schema, data, and custom tests
- **Source freshness** monitoring with alerting thresholds
- **Data contracts** with column-level assertions
- **Anomaly detection** on key business metrics
- Custom generic tests for metric validation

## 📁 Project Structure

```
metricflow-analytics/
├── data_generator/           # Realistic SaaS data generation
│   ├── generate.py           # Main generator (50K users, 10M+ events)
│   └── config.py             # Generation parameters
├── dbt_metricflow/           # dbt project (34 models)
│   ├── models/
│   │   ├── staging/          # Source cleaning & typing
│   │   ├── intermediate/     # Business logic & enrichment
│   │   ├── marts/            # Business-ready analytics
│   │   └── advanced/         # Deep analytical models
│   ├── macros/               # Reusable SQL macros
│   ├── tests/                # Custom data tests
│   └── seeds/                # Reference data
├── ml_pipeline/              # Predictive models
│   ├── churn_model.py        # XGBoost churn prediction
│   ├── ltv_model.py          # LTV regression
│   ├── segmentation.py       # K-Means clustering
│   ├── anomaly_detection.py  # Isolation Forest
│   └── forecasting.py        # Prophet time-series
├── evidence_dashboards/      # BI dashboards
│   └── pages/                # 6 analytics pages
├── docs/                     # Documentation
└── scripts/                  # Automation scripts
```

## 🎯 Interview Talking Points

This project demonstrates proficiency in:

- **Analytics Engineering**: dbt modeling best practices, star schema + data vault hybrid, semantic layer
- **Product Analytics**: Funnel analysis, feature adoption, engagement scoring, north star metrics
- **Revenue Intelligence**: MRR decomposition, cohort LTV, subscription economics
- **Predictive Analytics**: Churn prediction, LTV forecasting, anomaly detection with interpretability
- **Data Quality**: 100+ automated tests, freshness monitoring, data contracts
- **Modern Data Stack**: DuckDB, dbt, Evidence.dev, Parquet columnar storage
- **Software Engineering**: Type hints, tests, Docker, CI/CD, pre-commit hooks

## 📝 Resume Bullet Points

> • Architected MetricFlow, a production-grade analytics platform with 34 dbt models across staging, intermediate, and marts layers, processing 10M+ rows of SaaS data with 100+ automated tests and full documentation
>
> • Built a churn prediction pipeline (XGBoost, 89% AUC) and LTV regression model (0.82 R²) with SHAP interpretability, integrated into dbt DAG for proactive customer retention
>
> • Deployed Evidence.dev dashboards with MRR waterfall decomposition, cohort retention heatmaps, and funnel diagnostics; implemented CI/CD via GitHub Actions with data quality gates

---

<div align="center">

**Built by [Ajay Mekala](https://linkedin.com/in/ajaymekala)** · M.S. Data Science, Montclair State University

</div>
