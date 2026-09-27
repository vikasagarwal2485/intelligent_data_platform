# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-SLV-04 — Standalone stage reading Bronze
The Silver stage SHALL always read Bronze from the lake's Iceberg tables, through
the configured catalog (object storage in the cluster, a local warehouse
otherwise). It SHALL NOT accept in-memory input. It SHALL write Silver as Iceberg
tables partitioned by `tenant_id`, and SHALL fail clearly if required entity tables
are missing.

#### Scenario: SCN-SLV-04.1 — Missing Bronze entities
- **WHEN** the Silver stage runs but `parties`/`accounts`/`agreements` are absent
  or empty in Bronze
- **THEN** it raises a clear error instructing to populate Bronze first

#### Scenario: SCN-SLV-04.2 — Silver reads the Bronze the landing wrote
- **WHEN** Bronze has been landed (by `idp consume` or the local `run`) and the
  Silver stage then runs as a separate process
- **THEN** it builds Silver from exactly that Bronze, read through the catalog

### Requirement: REQ-SLV-06 — Idempotent Silver writes
Re-running the Silver stage SHALL NOT duplicate or lose rows.
- `dim_subscriber` is rebuilt and replaces the partitions of the tenants present.
- The event facts are merged on their natural key `(tenant_id, id)`, so a record
  already in Silver is never inserted twice. This holds even when the same Bronze
  records are processed again, for example after a failed run or a full refresh.
- Partitions of tenants absent from the new output SHALL be left untouched.
- Each table's changes SHALL be committed atomically, so a concurrent reader sees
  either the previous or the new state of the table, never a partial write.

#### Scenario: SCN-SLV-06.1 — Re-run does not duplicate
- **WHEN** the Silver stage runs twice on the same Bronze and persists both times
- **THEN** each Silver table has the same row count after the second run as after
  the first, and `dim_subscriber` still has one row per `(tenant_id, service_id)`

#### Scenario: SCN-SLV-06.2 — Other tenants untouched
- **WHEN** Silver is persisted with output for `tenant_a` only, while `tenant_b`
  partitions already exist
- **THEN** the `tenant_a` partitions are replaced and the `tenant_b` partitions are
  unchanged

#### Scenario: SCN-SLV-06.3 — Readers never see a partial write
- **WHEN** a reader (e.g. the Gold stage) reads a Silver table while the Silver stage
  is committing to it
- **THEN** the reader gets a complete snapshot, the one before or the one after the
  commit, and never fails because files disappeared

## ADDED Requirements

### Requirement: REQ-SLV-07 — Incremental event facts
The Silver stage SHALL build `fact_usage`, `fact_recharge` and `fact_fee_charge`
incrementally: each run processes only the Bronze records appended since the
previous successful run, tracked by a durable per-table checkpoint, and merges them
into the existing Silver fact. The cost of a run SHALL therefore scale with the new
Bronze data, not with the total history. A run with no new Bronze data SHALL leave
the facts unchanged. The resulting facts SHALL equal what a full rebuild from all of
Bronze would produce.

#### Scenario: SCN-SLV-07.1 — Only new Bronze is processed
- **WHEN** Silver has processed Bronze up to batch N, and batch N+1 is then landed
  and Silver runs again
- **THEN** only batch N+1's records are read from Bronze, and the facts gain exactly
  batch N+1's new records

#### Scenario: SCN-SLV-07.2 — Nothing new, nothing changes
- **WHEN** Silver runs with no Bronze appended since its previous run
- **THEN** no fact table gains a new snapshot and the row counts are unchanged

#### Scenario: SCN-SLV-07.3 — Incremental equals full rebuild
- **WHEN** Bronze is landed in several batches with Silver running after each, and a
  second lake is built from the same Bronze in one full run
- **THEN** both lakes' Silver fact tables contain the same rows

#### Scenario: SCN-SLV-07.4 — Failure resumes without loss
- **WHEN** a Silver run fails after reading new Bronze but before committing, and
  Silver runs again
- **THEN** the second run processes those records, and none is lost or duplicated

### Requirement: REQ-SLV-08 — Full refresh from Bronze
The Silver stage SHALL offer a full refresh (`idp silver --full-refresh`) that
discards the fact checkpoints and rebuilds every Silver table from all of Bronze.
This is the path for reprocessing history after a curation fix, and the reason Bronze
is kept immutable.

#### Scenario: SCN-SLV-08.1 — Rebuild after a logic change
- **WHEN** `idp silver --full-refresh` runs on a lake whose Silver was built
  incrementally
- **THEN** every Silver table is rebuilt from all Bronze records, and later
  incremental runs continue from the end of Bronze
