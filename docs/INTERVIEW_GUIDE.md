# Interview Talking Points

## Opening Statement
> "I built MetricFlow as a complete analytics engineering platform for a
> simulated SaaS company. It processes 10M+ rows through 34 dbt models,
> runs 5 ML models, and serves insights through 6 interactive dashboards."

## Technical Deep-Dives

### On dbt Modeling
- "I used a 4-layer architecture: staging, intermediate, marts, and
  advanced analytics. The staging layer handles data cleaning and typing.
  Intermediate handles business logic joins. Marts serve specific business
  domains. Advanced contains sophisticated analytics like cohort retention
  and RFM segmentation."

### On Data Quality
- "Every model has schema tests. I have 100+ dbt tests including custom
  generic tests for valid percentages and positive values. I also built
  data quality gates into the CI pipeline."

### On ML Integration
- "The churn model achieves 89% AUC using XGBoost. I engineered features
  from the dbt marts — days inactive, session frequency, support tickets,
  and subscription history. I used SHAP for interpretability so we can
  explain to the business team WHY a user is at risk."

### On MRR Waterfall
- "I built a proper MRR waterfall that decomposes monthly revenue into
  new, expansion, contraction, and churned components. This is exactly
  how companies like Stripe and Zuora measure subscription economics."

### On Cohort Analysis
- "The retention analysis generates month-over-month cohort retention
  rates, showing how different signup cohorts behave over time. I also
  built subscription survival curves by plan tier, which shows that
  enterprise customers have significantly lower churn."

## Questions to Expect

Q: "Why DuckDB instead of a cloud warehouse?"
A: "DuckDB gives the same analytical SQL capabilities as BigQuery or
   Snowflake but runs locally with zero cost. All the SQL patterns —
   window functions, CTEs, complex joins — transfer directly. In
   production, you'd swap the dbt adapter and the models work as-is."

Q: "How would you scale this?"
A: "The architecture is designed for that. Swap DuckDB for BigQuery/
   Snowflake, add Airflow for orchestration, and the dbt models
   already support incremental materialization. The ML pipeline would
   move to Vertex AI or SageMaker."

Q: "What would you do differently?"
A: "I'd add streaming analytics with Kafka for real-time event
   processing, implement a proper feature store for ML, and build
   a reverse ETL pipeline to push churn predictions back into CRM."

## Resume Bullets

• Architected MetricFlow, a production-grade analytics platform with 34 dbt
  models across 4 layers, processing 10M+ rows with 100+ automated tests

• Built churn prediction pipeline (XGBoost, 89% AUC) and LTV regression
  (0.82 R²) with SHAP interpretability, integrated into dbt DAG

• Deployed Evidence.dev dashboards with MRR waterfall, cohort retention
  heatmaps, and funnel diagnostics; CI/CD via GitHub Actions
