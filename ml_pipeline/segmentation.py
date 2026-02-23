"""
MetricFlow — User Segmentation via K-Means + PCA
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import numpy as np
import pandas as pd
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score
from sklearn.preprocessing import StandardScaler

DB_PATH = "data/metricflow.duckdb"
OUTPUT_DIR = Path("ml_pipeline/outputs")


def run() -> dict:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("🎯 User Segmentation Pipeline")

    con = duckdb.connect(DB_PATH, read_only=True)
    df = con.execute("""
        SELECT user_id, engagement_score, lifetime_revenue, total_payments,
               total_events, total_active_days
        FROM marts.dim_users WHERE total_events > 0
    """).fetchdf()
    con.close()

    features = ["engagement_score", "lifetime_revenue", "total_payments",
                "total_events", "total_active_days"]
    X = df[features].fillna(0)

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Find optimal k using silhouette
    best_k, best_score = 5, -1
    for k in range(3, 8):
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        labels = km.fit_predict(X_scaled)
        score = silhouette_score(X_scaled, labels, sample_size=min(5000, len(X_scaled)))
        if score > best_score:
            best_k, best_score = k, score

    # Final model
    model = KMeans(n_clusters=best_k, random_state=42, n_init=10)
    df["segment"] = model.fit_predict(X_scaled)

    # PCA for viz
    pca = PCA(n_components=2)
    coords = pca.fit_transform(X_scaled)
    df["pca_x"], df["pca_y"] = coords[:, 0], coords[:, 1]

    # Segment profiles
    profiles = df.groupby("segment")[features].mean().round(2)

    # Name segments
    segment_names = {}
    for seg in profiles.index:
        row = profiles.loc[seg]
        if row["lifetime_revenue"] > profiles["lifetime_revenue"].quantile(0.75):
            segment_names[seg] = "High-Value Power Users"
        elif row["engagement_score"] > profiles["engagement_score"].quantile(0.75):
            segment_names[seg] = "Engaged Free Users"
        elif row["total_active_days"] < profiles["total_active_days"].quantile(0.25):
            segment_names[seg] = "Dormant Users"
        elif row["total_payments"] > 0 and row["engagement_score"] < 40:
            segment_names[seg] = "At-Risk Paying Users"
        else:
            segment_names[seg] = f"Moderate Segment {seg}"

    df["segment_name"] = df["segment"].map(segment_names)

    # Save
    con = duckdb.connect(DB_PATH)
    con.execute("CREATE SCHEMA IF NOT EXISTS ml_outputs")
    con.execute("DROP TABLE IF EXISTS ml_outputs.user_segments")
    con.execute("CREATE TABLE ml_outputs.user_segments AS SELECT user_id, segment, segment_name, pca_x, pca_y FROM df")
    con.close()

    metrics = {"n_clusters": best_k, "silhouette_score": round(best_score, 4)}
    print(f"  ✓ {best_k} clusters | Silhouette: {best_score:.4f}")
    profiles.to_csv(OUTPUT_DIR / "segment_profiles.csv")

    with open(OUTPUT_DIR / "segmentation_metrics.json", "w") as f:
        json.dump(metrics, f, indent=2)

    print("  ✅ Segments saved")
    return metrics


if __name__ == "__main__":
    run()
