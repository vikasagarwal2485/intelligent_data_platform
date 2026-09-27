#!/usr/bin/env bash
# The lake's Iceberg catalog database (REQ-PLT-09), idempotent: the `iceberg`
# database on the platform TimescaleDB/Postgres. The stages use it as an Iceberg
# JDBC catalog (pyiceberg SqlCatalog + Spark JdbcCatalog), which creates its own
# tables (iceberg_tables, iceberg_namespace_properties) on first use and keeps
# every table's full snapshot history.
#
# Usage:  bash deploy/platform/timescaledb/lake-catalog.sh [namespace]   (make lake-catalog)
set -euo pipefail

NS="${1:-idp-platform}"

kubectl -n "${NS}" rollout status deploy/timescaledb --timeout=300s >/dev/null
until kubectl -n "${NS}" exec deploy/timescaledb -- pg_isready -U postgres >/dev/null 2>&1; do
  echo "waiting for timescaledb..."; sleep 3
done
if [ "$(kubectl -n "${NS}" exec deploy/timescaledb -- psql -U postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname='iceberg'")" != "1" ]; then
  kubectl -n "${NS}" exec deploy/timescaledb -- psql -U postgres -c "CREATE DATABASE iceberg"
fi
echo "lake catalog database 'iceberg' ready"
