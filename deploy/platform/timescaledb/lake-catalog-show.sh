#!/usr/bin/env bash
# Read-only views of the lake's Iceberg catalog (the `iceberg` database on the
# platform TimescaleDB) and of the table metadata it points to in MinIO.
#
# The catalog database stores only two things:
#   iceberg_tables                one row per table: its namespace (bronze/silver/gold),
#                                 name, and the location of its CURRENT metadata.json
#                                 (plus the previous one)
#   iceberg_namespace_properties  namespace properties, e.g. each layer's `location`
# Everything else (schema, partition spec, snapshots) is in that metadata.json on
# MinIO; `table` mode fetches and summarizes it.
#
# Usage:
#   bash lake-catalog-show.sh [namespace]                     catalog rows   (make lake-catalog-show)
#   bash lake-catalog-show.sh [namespace] table <ns.table>    one table      (make lake-table-show TABLE=bronze.accounts)
#   bash lake-catalog-show.sh [namespace] psql                interactive    (make lake-catalog-psql)
set -euo pipefail

NS="${1:-idp-platform}"
MODE="${2:-catalog}"
TABLE="${3:-}"
PSQL=(kubectl -n "${NS}" exec deploy/timescaledb -- psql -U postgres -d iceberg -P pager=off)

case "${MODE}" in
  catalog)
    "${PSQL[@]}" \
      -c "SELECT table_namespace AS namespace, table_name,
                 regexp_replace(metadata_location, '^.*/metadata/', '') AS current_metadata,
                 regexp_replace(coalesce(previous_metadata_location, '-'), '^.*/metadata/', '') AS previous_metadata,
                 catalog_name
            FROM iceberg_tables ORDER BY 1, 2" \
      -c "SELECT namespace, property_key, property_value, catalog_name
            FROM iceberg_namespace_properties ORDER BY 1, 2"
    echo "Full paths: make lake-table-show TABLE=<namespace>.<table>"
    ;;
  table)
    # strict identifier check (it is inlined into the query below)
    [[ "${TABLE}" =~ ^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$ ]] || {
      echo "usage: make lake-table-show TABLE=<namespace>.<table>  (e.g. bronze.accounts)" >&2; exit 2; }
    loc=$("${PSQL[@]}" -tA -c "SELECT metadata_location FROM iceberg_tables
            WHERE table_namespace = '${TABLE%%.*}' AND table_name = '${TABLE#*.}'")
    [ -n "${loc}" ] || { echo "no table ${TABLE} in the catalog" >&2; exit 1; }
    echo "catalog -> ${loc}"
    obj="m/${loc#s3://}"
    tmp=$(mktemp); trap 'rm -f "${tmp}"' EXIT
    kubectl -n "${NS}" exec deploy/minio -- sh -c \
      "export HOME=/tmp MC_HOST_m=http://minioadmin:minioadmin123@127.0.0.1:9000; mc cat '${obj}'" > "${tmp}"
    python3 - "${tmp}" <<'PY'
import datetime, json, sys

m = json.load(open(sys.argv[1]))


def ts(ms):
    return datetime.datetime.fromtimestamp(ms / 1000, datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


print(f"format-version {m['format-version']}  table-uuid {m['table-uuid']}")
print(f"location       {m['location']}")
print(f"last-updated   {ts(m['last-updated-ms'])}")
schema = next(s for s in m["schemas"] if s["schema-id"] == m["current-schema-id"])
print(f"\nschema (id {schema['schema-id']}):")
for f in schema["fields"]:
    req = "required" if f["required"] else "optional"
    print(f"  {f['id']:>3}  {f['name']:<28} {str(f['type']):<22} {req}")
spec = next(s for s in m["partition-specs"] if s["spec-id"] == m["default-spec-id"])
names = {f["id"]: f["name"] for f in schema["fields"]}
parts = ", ".join(f"{p['transform']}({names.get(p['source-id'], p['source-id'])})" for p in spec["fields"])
print(f"\npartition spec (id {spec['spec-id']}): {parts or 'unpartitioned'}")
if m.get("properties"):
    print("\nproperties:")
    for k, v in sorted(m["properties"].items()):
        print(f"  {k} = {v}")
cur = m.get("current-snapshot-id")
snaps = m.get("snapshots", [])
print(f"\nsnapshots ({len(snaps)}, * = current {cur}):")
for s in snaps:
    sm = s.get("summary", {})
    mark = "*" if s["snapshot-id"] == cur else " "
    print(f" {mark} {s['snapshot-id']:<20} {ts(s['timestamp-ms'])}  {sm.get('operation', '?'):<9}"
          f" rows +{sm.get('added-records', 0)} / total {sm.get('total-records', '?')}"
          f"   files +{sm.get('added-data-files', 0)} / total {sm.get('total-data-files', '?')}")
print(f"\nmetadata log: {len(m.get('metadata-log', []))} earlier metadata.json version(s)")
PY
    ;;
  psql)
    kubectl -n "${NS}" exec -it deploy/timescaledb -- psql -U postgres -d iceberg
    ;;
  *)
    echo "unknown mode ${MODE}: use catalog | table <ns.table> | psql" >&2; exit 2 ;;
esac
