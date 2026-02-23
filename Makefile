# MetricFlow — Command Reference
.PHONY: setup generate dbt ml dashboards all clean test lint

# Full setup from scratch
setup:
	pip install -e ".[dev]"
	@echo "✅ Dependencies installed"

# Generate synthetic data
generate:
	python -m data_generator.generate
	python scripts/load_to_duckdb.py
	@echo "✅ Data generated and loaded"

# Run dbt pipeline
dbt:
	cd dbt_metricflow && dbt deps && dbt seed && dbt run && dbt test
	@echo "✅ dbt pipeline complete"

# Run dbt docs
dbt-docs:
	cd dbt_metricflow && dbt docs generate && dbt docs serve --port 8081

# Run ML pipeline
ml:
	python -m ml_pipeline.run_all
	@echo "✅ ML pipeline complete"

# Launch dashboards
dashboards:
	cd evidence_dashboards && npm install && npm run dev

# Run everything end-to-end
all: setup generate dbt ml
	@echo "\n🎉 MetricFlow fully built!"
	@echo "Run 'make dashboards' to launch Evidence.dev"

# Run tests
test:
	cd dbt_metricflow && dbt test
	python -m pytest tests/ -v

# Lint
lint:
	ruff check .
	sqlfluff lint dbt_metricflow/models/ --dialect duckdb

# Clean all generated artifacts
clean:
	rm -rf data/ ml_pipeline/outputs/
	rm -rf dbt_metricflow/target/ dbt_metricflow/dbt_packages/ dbt_metricflow/logs/
	@echo "✅ Cleaned"
