"""Run all ML pipelines sequentially."""

from ml_pipeline.churn_model import run as run_churn
from ml_pipeline.ltv_model import run as run_ltv
from ml_pipeline.segmentation import run as run_segmentation
from ml_pipeline.anomaly_detection import run as run_anomaly
from ml_pipeline.forecasting import run as run_forecast


def main():
    print("=" * 60)
    print("  MetricFlow — ML Pipeline Suite")
    print("=" * 60)
    print()

    results = {}
    for name, runner in [
        ("Churn Prediction", run_churn),
        ("LTV Prediction", run_ltv),
        ("User Segmentation", run_segmentation),
        ("Anomaly Detection", run_anomaly),
        ("User Forecasting", run_forecast),
    ]:
        print(f"\n{'─' * 50}")
        try:
            results[name] = runner()
        except Exception as e:
            print(f"  ❌ {name} failed: {e}")
            results[name] = {"error": str(e)}

    print(f"\n{'=' * 60}")
    print("  Pipeline Complete — Summary")
    print("=" * 60)
    for name, metrics in results.items():
        status = "✅" if "error" not in metrics else "❌"
        print(f"  {status} {name}: {metrics}")


if __name__ == "__main__":
    main()
