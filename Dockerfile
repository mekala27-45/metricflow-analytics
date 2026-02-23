FROM python:3.11-slim

WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    git curl nodejs npm && rm -rf /var/lib/apt/lists/*

# Python deps
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]"

# Copy project
COPY . .

# Generate data + run dbt
RUN python -m data_generator.generate && \
    cd dbt_metricflow && dbt deps && dbt seed && dbt run && dbt test && cd ..

# Run ML pipeline
RUN python -m ml_pipeline.run_all || true

EXPOSE 3000

CMD ["bash", "-c", "cd evidence_dashboards && npm install && npm run dev -- --host 0.0.0.0"]
