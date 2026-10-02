# Data

This directory holds **generated, local-only** data and is git-ignored (except
this README). Nothing here is committed.

| Path | Contents | Produced by |
|------|----------|-------------|
| `landing/` | TM Forum Open API resource files per `tenant=<id>/<resource>.json` | `idp generate` (synthetic generator) |
| `lake/bronze/` | Raw immutable Parquet, partitioned by `tenant_id` | ingestion |
| `lake/silver/` | Canonical Iceberg-style tables (`dim_subscriber`, `fact_usage`, `fact_recharge`) | curation |
| `lake/gold/` | `credit_scores.parquet`, `fraud_scores.parquet` | ML scoring |

Reference **schema samples** (committed, not generated) live in the top-level
`data_samples/` directory: one TM Forum Open API v5 resource per landing file type,
as the standard profile (`config/tmf/profiles/standard.yaml`) writes it.

## Generating data
```bash
make generate      # writes synthetic landing files
make run           # full pipeline -> populates lake/
```

The synthetic generator (`src/idp/synthetic/generator.py`) is deterministic
given the seed in `config/platform.yaml`.
