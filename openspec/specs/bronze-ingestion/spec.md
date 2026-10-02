# Bronze Ingestion Specification

## Purpose

Capture raw records immutably and tenant-partitioned as the Bronze layer.

## Requirements

### Requirement: REQ-BRZ-01 — Immutable, tenant-partitioned Bronze
The system SHALL land raw per-entity records into Bronze as Iceberg tables
(`bronze.<entity>`), without transformation, partitioned by `tenant_id` and
`ingest_date`. Each landed record SHALL carry the time it was landed
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

### Requirement: REQ-BRZ-02 — Bronze is a standalone stage
The Bronze stage SHALL NOT perform Silver or Gold work; it only lands raw data.

#### Scenario: SCN-BRZ-02.1 — No chaining
- **WHEN** the Bronze stage completes
- **THEN** no Silver or Gold tables have been produced by it

### Requirement: REQ-BRZ-03 — File landing zone
The system SHALL provide a file landing zone (`idp generate`) that writes the
generated data as TM Forum Open API resources, simulating a source system's file
drop. Each tenant's resources SHALL be written as one JSON array per resource type
to `<landing>/tenant=<tenant_id>/<resource>.json`:

- `individual.json` and `organization.json`: TMF632 `Individual` / `Organization`
- `customer.json`: TMF629 `Customer`, the party role through which accounts and
  agreements reference their party
- `billingAccount.json`: TMF666 `BillingAccount`
- `agreement.json`: TMF651 `Agreement`
- `product.json`: TMF637 `Product`
- `appliedCustomerBillingRate.json`: TMF678 `AppliedCustomerBillingRate` (fee charges)
- `topupBalance.json`: TMF654 `TopupBalance` (recharges)
- `productUsage.json`: TMF635 `ProductUsage` (voice and data usage, by `usageType`)

Every resource SHALL follow the TMF Open API v5 conventions: `id`, `href`, `@type`,
`@baseType` and `@schemaLocation`; references as reference objects; money as
`{unit, value}`; characteristics and contact media as polymorphic typed entries.
Where TM Forum publishes no stable v5 schema for a resource, the resource SHALL use
its latest stable shape with these conventions, and the mapping profile (REQ-BRZ-06)
SHALL record which version it follows. Every resource SHALL carry its tenant in the
`tenantId` extension attribute. The resources SHALL be produced from the canonical
records by the active mapping profile.

The landing location SHALL be the local landing directory, or the bucket
`<prefix>-landing` when S3 is enabled. `idp generate` SHALL generate and write one
tenant at a time, to bound memory. The landing zone SHALL be the source of the
Kafka path when the producer publishes from it (REQ-STR-01). Writing the landing
zone SHALL NOT write Bronze.

#### Scenario: SCN-BRZ-03.1 — Per-tenant landing files
- **WHEN** `idp generate` runs for tenants `tenant_a` and `tenant_b`
- **THEN** `tenant=tenant_a/` and `tenant=tenant_b/` each contain one
  `<resource>.json` per non-empty resource type, and every resource carries
  `tenantId` equal to its folder's tenant

#### Scenario: SCN-BRZ-03.2 — Landing does not populate Bronze
- **WHEN** `idp generate` completes
- **THEN** no Bronze tables have been written by it

#### Scenario: SCN-BRZ-03.3 — Landing on object storage
- **WHEN** S3 is enabled with prefix `idp` and `idp generate` runs
- **THEN** the landing files are written under `s3://idp-landing/tenant=<tenant_id>/`,
  with the same names and contents as a local run

#### Scenario: SCN-BRZ-03.4 — Landing feeds the Kafka path
- **WHEN** `idp generate` runs, then the producer publishes from the landing zone,
  then `idp consume` runs
- **THEN** Bronze holds exactly the canonical records the generator produced, per
  tenant and entity

#### Scenario: SCN-BRZ-03.5 — Resources conform to TM Forum
- **WHEN** the landing files of a generated dataset are validated
- **THEN** every resource validates against its vendored TMF schema and the
  extension schemas its `@type` declares, and every reference in it (customer,
  party, account, agreement, product) points at a resource in the same tenant's
  files

### Requirement: REQ-BRZ-05 — TM Forum Open API adapter
The ingestion layer SHALL provide an adapter that turns a tenant's TMF landing
resources into the canonical records (`parties`, `accounts`, `agreements`,
`products`, `voice_events`, `data_events`, `recharge_events`, `fee_charges`)
consumed by Bronze, as defined by the active mapping profile (REQ-BRZ-06). It SHALL:

- read standard fields, characteristics and extension attributes as the profile
  maps them, and translate TMF values back to canonical values (e.g. status values)
- resolve references within the tenant's resources: an account's or agreement's
  `Customer` role to its engaged party, and so to the party and account ids the
  canonical records carry
- split `ProductUsage` into voice and data events by `usageType`
- yield organizations' person-only canonical fields (`first_name`, `last_name`,
  `date_of_birth`) as null

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

### Requirement: REQ-BRZ-06 — TMF mapping profiles and extensions
The mapping between canonical records and TMF resources SHALL be configuration: a
mapping profile per deployment, selected by configuration, that defines for each
canonical entity its TMF resource, the `@type` to emit, and where each canonical
field lives (a standard field, a characteristic, or an extension attribute). The
writer (canonical → TMF) and the adapter (TMF → canonical) SHALL both use the same
profile.

- The `standard` profile SHALL use standard fields and characteristics only, plus
  the platform extension: `tenantId` on every resource, and the canonical fields
  that have no standard home on their resource.
- A customer profile MAY add `@type` subtypes, each with a `@baseType` and an
  `@schemaLocation` pointing at its JSON Schema. It SHALL NOT change the meaning of
  a standard field.
- Every emitted or adapted resource SHALL be validated against the TMF schema of its
  base type plus the schemas of its declared extensions. A resource that fails
  validation SHALL be rejected, never silently adapted.

#### Scenario: SCN-BRZ-06.1 — Standard profile is plain TMF plus tenantId
- **WHEN** the landing files are written with the `standard` profile
- **THEN** every resource's fields are standard v5 fields of its type, typed
  characteristics, or attributes of the platform extension, which includes
  `tenantId`

#### Scenario: SCN-BRZ-06.2 — Customer extension
- **WHEN** a customer profile maps a canonical field to an attribute of its own
  `@type` subtype with a published extension schema
- **THEN** the writer emits that subtype with `@baseType` and `@schemaLocation`, the
  resource validates against the base and extension schemas, and the adapter
  restores the field

#### Scenario: SCN-BRZ-06.3 — Invalid resource rejected
- **WHEN** a landing resource violates its schema (e.g. a money value that isn't a number)
- **THEN** its file is rejected with the validation error, and none of its records
  are adapted
