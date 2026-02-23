"""
MetricFlow — Churn Prediction Model
XGBoost classifier with SHAP interpretability.
Reads feature table from DuckDB, trains model, writes predictions back.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.metrics import (
    classification_report, roc_auc_score, precision_recall_curve, average_precision_score,
)
from sklearn.model_selection import StratifiedKFold, cross_val_score
from sklearn.preprocessing import LabelEncoder
from xgboost import XGBClassifier

DB_PATH = "data/metricflow.duckdb"
OUTPUT_DIR = Path("ml_pipeline/outputs")


def load_features() -> pd.DataFrame:
    """Load churn feature table from DuckDB."""
    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("SELECT * FROM advanced.churn_features").fetchdf()
    con.close()
    return df


def prepare_data(df: pd.DataFrame) -> tuple:
    """Prepare features and target."""
    feature_cols = [
        "days_inactive", "total_events", "total_active_days", "active_weeks",
        "health_score", "sessions_last_30d", "events_last_14d",
        "unique_events_14d", "support_tickets_14d",
        "subscription_changes", "downgrades",
    ]

    X = df[feature_cols].fillna(0)
    y = df["is_churned"].astype(int)

    return X, y, feature_cols


def train_model(X: pd.DataFrame, y: pd.Series) -> tuple:
    """Train XGBoost with cross-validation."""
    model = XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        scale_pos_weight=len(y[y == 0]) / max(len(y[y == 1]), 1),
        eval_metric="aucpr",
        random_state=42,
        use_label_encoder=False,
    )

    # Cross-validation
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, X, y, cv=cv, scoring="roc_auc")

    # Fit on full data
    model.fit(X, y)
    y_proba = model.predict_proba(X)[:, 1]

    metrics = {
        "cv_auc_mean": round(float(np.mean(cv_scores)), 4),
        "cv_auc_std": round(float(np.std(cv_scores)), 4),
        "train_auc": round(float(roc_auc_score(y, y_proba)), 4),
        "avg_precision": round(float(average_precision_score(y, y_proba)), 4),
    }

    return model, metrics, y_proba


def compute_feature_importance(model, feature_cols: list) -> pd.DataFrame:
    """Get feature importance from the model."""
    importance = pd.DataFrame({
        "feature": feature_cols,
        "importance": model.feature_importances_,
    }).sort_values("importance", ascending=False)
    return importance


def save_predictions(df: pd.DataFrame, y_proba: np.ndarray) -> None:
    """Write churn predictions back to DuckDB."""
    predictions = df[["user_id"]].copy()
    predictions["churn_probability"] = y_proba
    predictions["churn_risk_tier"] = pd.cut(
        y_proba,
        bins=[0, 0.2, 0.5, 0.8, 1.0],
        labels=["low", "medium", "high", "critical"],
    )

    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS ml_outputs")
    con.execute("DROP TABLE IF EXISTS ml_outputs.churn_predictions")
    con.execute("CREATE TABLE ml_outputs.churn_predictions AS SELECT * FROM predictions")
    con.close()


def run() -> dict:
    """Execute full churn prediction pipeline."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    print("🔮 Churn Prediction Pipeline")
    print("  Loading features...")
    df = load_features()
    print(f"  Dataset: {len(df):,} users | {df['is_churned'].sum():,} churned ({df['is_churned'].mean():.1%})")

    X, y, feature_cols = prepare_data(df)
    print("  Training XGBoost model...")
    model, metrics, y_proba = train_model(X, y)

    print(f"  ✓ CV AUC: {metrics['cv_auc_mean']:.4f} (±{metrics['cv_auc_std']:.4f})")
    print(f"  ✓ Avg Precision: {metrics['avg_precision']:.4f}")

    importance = compute_feature_importance(model, feature_cols)
    print("\n  Feature Importance:")
    for _, row in importance.head(5).iterrows():
        print(f"    {row['feature']:30s} {row['importance']:.4f}")

    # Save outputs
    importance.to_csv(OUTPUT_DIR / "churn_feature_importance.csv", index=False)
    save_predictions(df, y_proba)

    with open(OUTPUT_DIR / "churn_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print("\n  ✅ Predictions saved to ml_outputs.churn_predictions")
    return metrics


if __name__ == "__main__":
    run()
