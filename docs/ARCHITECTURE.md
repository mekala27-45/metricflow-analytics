# Architecture

## System Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                     MetricFlow Analytics Platform                    │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────────┐  │
│  │  Data     │    │  DuckDB  │    │   dbt    │    │  Evidence.dev│  │
│  │Generator  │───▶│Warehouse │───▶│  Core    │───▶│  Dashboards  │  │
│  │(Python)   │    │(Parquet) │    │(34 mdls) │    │  (6 pages)   │  │
│  └──────────┘    └──────────┘    └──────────┘    └──────────────┘  │
│       │                               │                             │
│       │          ┌──────────┐         │          ┌──────────────┐  │
│       │          │   ML     │◀────────┘          │   Data       │  │
│       └─────────▶│ Pipeline │                    │   Quality    │  │
│                  │(5 models)│───────────────────▶│   (100+ tst) │  │
│                  └──────────┘                    └──────────────┘  │
└─────────────────────────────────────────────────────────────────────┘
```

## Data Flow

### Layer 1: Data Generation
- **50,000 users** with realistic demographics, seasonal signup patterns
- **10M+ events** with correlated user behavior
- **Subscriptions** with upgrade/downgrade/churn lifecycle
- **Payments** with failure and refund rates
- **Marketing touches** with multi-touch attribution

### Layer 2: DuckDB Warehouse
- Parquet files loaded into DuckDB
- Columnar storage optimized for analytics
- Zero-cost local development (no cloud bills)

### Layer 3: dbt Transformation
- **Staging (6 models)**: Clean, type, deduplicate
- **Intermediate (6 models)**: Join, enrich, classify
- **Marts (12 models)**: Business-ready analytics
- **Advanced (10 models)**: Deep analytical models

### Layer 4: ML Pipeline
- Churn Prediction (XGBoost, 89% AUC)
- LTV Regression (GBR, 0.82 R²)
- User Segmentation (K-Means, silhouette 0.64)
- Revenue Anomaly Detection (Isolation Forest)
- Active User Forecasting (Prophet)

### Layer 5: Evidence.dev Dashboards
- Executive growth overview
- Retention intelligence with cohort heatmaps
- Revenue deep-dive with MRR waterfall
- Funnel diagnostics
- Product adoption tracking
- Churn early warning system

## Design Decisions

### Why DuckDB?
- Free, zero-config local analytics
- Parquet-native columnar engine
- Compatible with BigQuery SQL patterns
- Perfect for portfolio demonstration

### Why dbt?
- Industry standard for analytics engineering
- Built-in testing and documentation
- DAG-based dependency management
- Showcases production best practices

### Why Evidence.dev?
- Code-as-configuration dashboards
- Markdown-based (version-controllable)
- Beautiful default visualizations
- SQL-first approach
