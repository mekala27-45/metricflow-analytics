"""
MetricFlow — Customer Lifetime Value Prediction
Gradient Boosted regression with feature importance.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.model_selection import KFold, cross_val_score
from sklearn.preprocessing import LabelEncoder

DB_PATH = "data/metricflow.duckdb"
OUTPUT_DIR = Path("ml_pipeline/outputs")


def load_features() -> pd.DataFrame:
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("SELECT * FROM advanced.ltv_features").fetchdf()
    con.close()
    return df


def prepare_data(df: pd.DataFrame) -> tuple:
    feature_cols = [
        "engagement_score", "total_payments", "active_days_first_30",
        "events_first_30", "unique_features_first_30",
        "avg_session_seconds", "total_sessions",
    ]

    # Encode categoricals
    le_channel = LabelEncoder()
    df["channel_encoded"] = le_channel.fit_transform(df["acquisition_channel"].fillna("unknown"))
    le_size = LabelEncoder()
    df["size_encoded"] = le_size.fit_transform(df["company_size"].fillna("unknown"))
    feature_cols += ["channel_encoded", "size_encoded"]

    X = df[feature_cols].fillna(0)
    y = df["lifetime_revenue"].clip(lower=0)  # no negative LTV

    return X, y, feature_cols


def train_model(X: pd.DataFrame, y: pd.Series) -> tuple:
    model = GradientBoostingRegressor(
        n_estimators=200, max_depth=5, learning_rate=0.05,
        subsample=0.8, random_state=42,
    )

    cv = KFold(n_splits=5, shuffle=True, random_state=42)
    cv_r2 = cross_val_score(model, X, y, cv=cv, scoring="r2")

    model.fit(X, y)
    y_pred = model.predict(X)

    metrics = {
        "cv_r2_mean": round(float(np.mean(cv_r2)), 4),
        "cv_r2_std": round(float(np.std(cv_r2)), 4),
        "train_r2": round(float(r2_score(y, y_pred)), 4),
        "train_mae": round(float(mean_absolute_error(y, y_pred)), 2),
    }

    return model, metrics, y_pred


def run() -> dict:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("💰 LTV Prediction Pipeline")
    df = load_features()
    print(f"  Dataset: {len(df):,} users | Median LTV: ${df['lifetime_revenue'].median():,.0f}")

    X, y, feature_cols = prepare_data(df)
    model, metrics, y_pred = train_model(X, y)

    print(f"  ✓ CV R²: {metrics['cv_r2_mean']:.4f} (±{metrics['cv_r2_std']:.4f})")
    print(f"  ✓ MAE: ${metrics['train_mae']:,.2f}")

    importance = pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    importance.to_csv(OUTPUT_DIR / "ltv_feature_importance.csv", index=False)

    # Save predictions
    predictions = df[["user_id"]].copy()
    predictions["predicted_ltv"] = np.round(y_pred, 2)
    predictions["ltv_tier"] = pd.cut(
        y_pred, bins=[0, 100, 500, 2000, float("inf")],
        labels=["low", "medium", "high", "whale"],
    )

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS ml_outputs")
    con.execute("DROP TABLE IF EXISTS ml_outputs.ltv_predictions")
    con.execute("CREATE TABLE ml_outputs.ltv_predictions AS SELECT * FROM predictions")
    con.close()

    with open(OUTPUT_DIR / "ltv_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print("  ✅ LTV predictions saved")
    return metrics


if __name__ == "__main__":
    run()
