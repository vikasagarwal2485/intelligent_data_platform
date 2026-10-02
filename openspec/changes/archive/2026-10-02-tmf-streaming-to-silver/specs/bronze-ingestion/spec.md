# Bronze ingestion delta

## MODIFIED Requirements

### Requirement: REQ-BRZ-01 — Immutable, tenant-partitioned Bronze
The system SHALL land per-entity canonical records into Bronze as Iceberg tables
(`bronze.<entity>`), partitioned by `tenant_id` and `ingest_date`. The records SHALL
be the canonical records adapted from the source format by the TMF adapter
(REQ-BRZ-05), with no curation: no cleaning, deduplication or enrichment, which are
Silver's work. Each landed record SHALL carry the time it was landed
(`_ingested_at`, UTC) and the derived `ingest_date`. Bronze SHALL be append-only:
landing never rewrites or deletes earlier records, so Bronze stays a complete raw
history that can be reprocessed.

#### Scenario: SCN-BRZ-01.1 — Write and reload Bronze
- **WHEN** `to_bronze(cfg, frames)` runs with tenant-stamped frames
- **THEN** each non-empty entity is appended to the Iceberg table `bronze.<entity>`,
  and reading it back returns the landed records with `tenant_id`, `_ingested_at`
  and `ingest_date`

#### Scenario: SCN-BRZ-01.2 — Physical tenant partitioning
- **WHEN** a multi-tenant entity is written to Bronze
- **THEN** the table's partition spec includes `tenant_id` and `ingest_date`, so a
  read filtered to one tenant and day touches only that partition's files

#### Scenario: SCN-BRZ-01.3 — Append-only history
- **WHEN** the same entity is landed in two batches, including records that repeat
  earlier ones
- **THEN** Bronze holds both batches as separate snapshots, and no earlier record is
  modified or removed

### Requirement: REQ-BRZ-05 — TM Forum Open API adapter
The ingestion layer SHALL provide an adapter that turns a tenant's TMF resources
into the canonical records (`parties`, `accounts`, `agreements`, `products`,
`voice_events`, `data_events`, `recharge_events`, `fee_charges`) consumed by Bronze,
as defined by the active mapping profile (REQ-BRZ-06). It SHALL:

- read standard fields, characteristics and extension attributes as the profile
  maps them, and translate TMF values back to canonical values (e.g. status values)
- resolve references within the tenant's resources: an account's or agreement's
  `Customer` role to its engaged party, and so to the party and account ids the
  canonical records carry
- split `ProductUsage` into voice and data events by `usageType`
- yield organizations' person-only canonical fields (`first_name`, `last_name`,
  `date_of_birth`) as null

The adapter SHALL work on a tenant's whole set of landing files, and on one resource
at a time against an index of the tenant's customers (as the consumer uses it,
REQ-STR-02). Both ways SHALL give the same canonical records.

For the `standard` profile, adapting the resources the landing writer produced
from a canonical dataset SHALL return that dataset exactly. Money is compared to
the generator's precision.

#### Scenario: SCN-BRZ-05.1 — Round trip
- **WHEN** a generated two-tenant dataset is written as TMF resources with the
  `standard` profile and adapted back
- **THEN** every canonical entity equals the generated one, record for record

#### Scenario: SCN-BRZ-05.2 — References resolved through the Customer role
- **WHEN** a `BillingAccount` references a `Customer` whose `engagedParty` is an
  `Organization`
- **THEN** the canonical account carries that organization's id as `party_id` and
  `is_company = true`

#### Scenario: SCN-BRZ-05.3 — Usage split by type
- **WHEN** `productUsage.json` holds voice and data `ProductUsage` resources
- **THEN** the adapter yields them as `voice_events` and `data_events` records with
  `event_type` voice and data, and net, tax and gross amounts from the usage price

#### Scenario: SCN-BRZ-05.4 — Unresolvable reference
- **WHEN** an agreement references a customer that isn't in the tenant's files
- **THEN** the adapter reports the agreement file as invalid, naming the missing
  reference, and yields none of its records

#### Scenario: SCN-BRZ-05.5 — One resource at a time
- **WHEN** a generated dataset's TMF resources are adapted one at a time against the
  tenant's customer index
- **THEN** the canonical records equal those from adapting the whole landing files
