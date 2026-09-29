#!/usr/bin/env bash
# The lake's Iceberg catalog database (REQ-PLT-09), idempotent: the `iceberg`
# database on the platform TimescaleDB/Postgres. The stages use it as an Iceberg
# JDBC catalog (pyiceberg SqlCatalog + Spark JdbcCatalog), which creates its own
# tables (iceberg_tables, iceberg_namespace_properties) on first use and keeps
# every table's full snapshot history.
#
# --reset forgets every lake table: it deletes all catalog rows (tables and
# namespace properties) in one transaction and keeps the database and its schema.
# The catalog and the lake buckets must be reset TOGETHER. Emptying the buckets
# alone leaves rows pointing at deleted metadata files, and every stage then fails
# with "FileNotFoundError: ... metadata.json" (`make minio-reset` does both).
#
# --rename-to-lake renames every catalog row to the stable catalog name `lake`
# (one transaction, idempotent). Catalogs written before the name was pinned carry
# `lake_<hash of the URI>`; Spark, pyiceberg and Trino must all address `lake`.
# Run it with the DAGs paused and no Spark stage running, then deploy the image
# that pins the name (mcp-over-trino, design 1a).
#
# Usage:  bash deploy/platform/timescaledb/lake-catalog.sh [namespace] [--reset|--rename-to-lake]
#         (make lake-catalog; make minio-reset runs it with --reset;
#          make lake-catalog-rename runs it with --rename-to-lake)
set -euo pipefail

NS="idp-platform"
RESET=0
RENAME=0
for arg in "$@"; do
  case "$arg" in
    --reset) RESET=1 ;;
    --rename-to-lake) RENAME=1 ;;
    *) NS="$arg" ;;
  esac
done

kubectl -n "${NS}" rollout status deploy/timescaledb --timeout=300s >/dev/null
until kubectl -n "${NS}" exec deploy/timescaledb -- pg_isready -U postgres >/dev/null 2>&1; do
  echo "waiting for timescaledb..."; sleep 3
done
if [ "$(kubectl -n "${NS}" exec deploy/timescaledb -- psql -U postgres -tAc \
      "SELECT 1 FROM pg_database WHERE datname='iceberg'")" != "1" ]; then
  kubectl -n "${NS}" exec deploy/timescaledb -- psql -U postgres -c "CREATE DATABASE iceberg"
fi
echo "lake catalog database 'iceberg' ready"

if [ "$RESET" = 1 ]; then
  # The catalog tables exist only after a stage's first use, so each DELETE is guarded.
  kubectl -n "${NS}" exec -i deploy/timescaledb -- psql -U postgres -d iceberg -v ON_ERROR_STOP=1 -q <<'SQL'
DO $reset$
DECLARE n bigint := 0;
BEGIN
  IF to_regclass('public.iceberg_tables') IS NOT NULL THEN
    DELETE FROM iceberg_tables;
    GET DIAGNOSTICS n = ROW_COUNT;
  END IF;
  IF to_regclass('public.iceberg_namespace_properties') IS NOT NULL THEN
    DELETE FROM iceberg_namespace_properties;
  END IF;
  RAISE NOTICE 'lake catalog reset: % table entries removed', n;
END
$reset$;
SQL
fi

if [ "$RENAME" = 1 ]; then
  kubectl -n "${NS}" exec -i deploy/timescaledb -- psql -U postgres -d iceberg -v ON_ERROR_STOP=1 -q <<'SQL'
DO $rename$
DECLARE t bigint := 0; n bigint := 0;
BEGIN
  IF to_regclass('public.iceberg_tables') IS NOT NULL THEN
    UPDATE iceberg_tables SET catalog_name = 'lake' WHERE catalog_name <> 'lake';
    GET DIAGNOSTICS t = ROW_COUNT;
  END IF;
  IF to_regclass('public.iceberg_namespace_properties') IS NOT NULL THEN
    UPDATE iceberg_namespace_properties SET catalog_name = 'lake' WHERE catalog_name <> 'lake';
    GET DIAGNOSTICS n = ROW_COUNT;
  END IF;
  RAISE NOTICE 'lake catalog renamed to "lake": % table rows, % namespace-property rows', t, n;
END
$rename$;
SQL
fi
