"""MetricFlow — Revenue Anomaly Detection using Isolation Forest."""

from __future__ import annotations
import json
from pathlib import Path
import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

DB_PATH = "data/metricflow.duckdb"
OUTPUT_DIR = Path("ml_pipeline/outputs")


def run() -> dict:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("🚨 Revenue Anomaly Detection Pipeline")

    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT payment_date, sum(recognized_mrr) as daily_revenue,
               count(distinct user_id) as unique_payers, count(*) as transaction_count
        FROM intermediate.int_revenue_normalized
        WHERE payment_status = 'succeeded'
        GROUP BY 1 ORDER BY 1
    """).fetchdf()
    con.close()

    features = ["daily_revenue", "unique_payers", "transaction_count"]
    # Add rolling features
    for col in features:
        df[f"{col}_rolling_7d"] = df[col].rolling(7, min_periods=1).mean()
        df[f"{col}_pct_change"] = df[col].pct_change().fillna(0)

    feature_cols = features + [f"{c}_rolling_7d" for c in features] + [f"{c}_pct_change" for c in features]
    X = df[feature_cols].fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = IsolationForest(contamination=0.03, random_state=42, n_estimators=200)
    df["anomaly_score"] = model.decision_function(X_scaled)
    df["is_anomaly"] = model.predict(X_scaled) == -1

    n_anomalies = df["is_anomaly"].sum()
    metrics = {
        "total_days": len(df),
        "anomalies_detected": int(n_anomalies),
        "anomaly_rate": round(n_anomalies / len(df), 4),
    }

    # Save
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS ml_outputs")
    con.execute("DROP TABLE IF EXISTS ml_outputs.revenue_anomalies")
    con.execute("CREATE TABLE ml_outputs.revenue_anomalies AS SELECT payment_date, daily_revenue, anomaly_score, is_anomaly FROM df")
    con.close()

    print(f"  ✓ {n_anomalies} anomalies detected out of {len(df)} days")
    with open(OUTPUT_DIR / "anomaly_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)
    print("  ✅ Anomalies saved")
    return metrics


if __name__ == "__main__":
    run()
