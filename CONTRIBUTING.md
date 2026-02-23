# Contributing to MetricFlow

## Setup

```bash
git clone https://github.com/yourusername/metricflow-analytics.git
cd metricflow-analytics
pip install -e ".[dev]"
pre-commit install
```

## Development Workflow

1. Create a feature branch: `git checkout -b feat/my-feature`
2. Make changes
3. Run tests: `make test`
4. Lint: `make lint`
5. Commit with conventional commits: `feat: add retention dashboard`
6. Push and create PR

## Commit Convention

- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation
- `test:` Tests
- `refactor:` Code refactoring
- `ci:` CI/CD changes

## Adding a New dbt Model

1. Create SQL file in appropriate directory
2. Add schema YAML with tests
3. Run `dbt run --select my_model`
4. Run `dbt test --select my_model`
5. Update documentation
