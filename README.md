# Codrona Lens

Ingestion, warehouse, and the public observatory.

## What lives here

- Judge adapters (CodeNet, Codeforces, AtCoder) behind one interface
- PySpark normalisation into the canonical schema
- dbt project: staging, intermediate, marts, with SCD-2 dimensions
- Airflow DAGs
- Observatory dashboards

Design: `docs/architecture/ingestion.md` and `docs/architecture/warehouse.md`.

## Before running any adapter

Every source needs a verified row in `docs/LEGAL.md`. Rows marked VERIFY are
blocking: the adapter that reads them must not merge. Free to fetch is not free to
use.

Rate limits are enforced in code, not by convention. A 403 or a challenge response
aborts the run. The pipeline never routes around a block.

## Local development

DuckDB over Parquet, no cloud account required:

```
dbt deps
dbt build --target dev
```

## Licence

AGPL-3.0-or-later.
