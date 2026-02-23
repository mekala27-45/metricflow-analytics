"""Load Parquet files into DuckDB for dbt."""
import duckdb
from pathlib import Path

DB_PATH = "data/metricflow.duckdb"
RAW_DIR = Path("data/raw")


def main():
    con = duckdb.connect(DB_PATH)

    tables = {
        "users": "users.parquet",
        "events": "events.parquet",
        "subscriptions": "subscriptions.parquet",
        "payments": "payments.parquet",
        "sessions": "sessions.parquet",
        "marketing_touches": "marketing_touches.parquet",
    }

    for table_name, filename in tables.items():
        path = RAW_DIR / filename
        if path.exists():
            con.execute(f"DROP TABLE IF EXISTS {table_name}")
            con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM read_parquet('{path}')")
            count = con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0]
            print(f"  ✓ {table_name}: {count:,} rows")
        else:
            print(f"  ✗ {filename} not found")

    con.close()
    print("\n✅ All tables loaded into DuckDB")


if __name__ == "__main__":
    main()
