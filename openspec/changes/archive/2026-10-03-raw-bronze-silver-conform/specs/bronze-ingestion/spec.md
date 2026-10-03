# Bronze ingestion delta

## MODIFIED Requirements

### Requirement: REQ-BRZ-01 — Immutable, tenant-partitioned Bronze
The system SHALL land the source events into Bronze as received: one Iceberg table
per TMF event topic, `bronze.tmf_<resource>` (`tmf_customer`, `tmf_party`,
`tmf_billing_account`, `tmf_agreement`, `tmf_product`,
`tmf_applied_customer_billing_rate`, `tmf_topup_balance`, `tmf_product_usage`),
partitioned by `tenant_id` and `ingest_date`. Each row SHALL hold one event: the
message value as received (`payload`, the TMF create event as JSON text) and, as
columns, its tenant (from the topic), `event_id`, `event_type`, `event_time`, the
resource's `@type` and id, and the Kafka topic, partition, offset and key.

Bronze SHALL NOT parse, validate, adapt or de-duplicate the events: conforming them
to the canonical model is Silver's work (REQ-SLV-09). An event whose resource is
invalid, belongs to another tenant or holds a dangling reference SHALL still be
landed as received. Each landed record SHALL carry the time it was landed
(`_ingested_at`, UTC) and the derived `ingest_date`. Bronze SHALL be append-only:
landing never rewrites or deletes earlier records, so Bronze stays a complete raw
history that can be reprocessed.

#### Scenario: SCN-BRZ-01.1 — Write and reload Bronze
- **WHEN** `to_bronze(cfg, events)` runs with a batch of TMF events
- **THEN** each topic's events are appended to the Iceberg table
  `bronze.tmf_<resource>`, and reading it back returns each event's `payload`
  exactly as received, with `tenant_id`, its envelope and Kafka columns,
  `_ingested_at` and `ingest_date`

#### Scenario: SCN-BRZ-01.2 — Physical tenant partitioning
- **WHEN** a multi-tenant entity is written to Bronze
- **THEN** the table's partition spec includes `tenant_id` and `ingest_date`, so a
  read filtered to one tenant and day touches only that partition's files

#### Scenario: SCN-BRZ-01.3 — Append-only history
- **WHEN** the same entity is landed in two batches, including records that repeat
  earlier ones
- **THEN** Bronze holds both batches as separate snapshots, and no earlier record is
  modified or removed

#### Scenario: SCN-BRZ-01.4 — Bronze keeps what the profile doesn't map
- **WHEN** an event's resource carries an attribute the mapping profile doesn't map
  (e.g. an extra characteristic or `href`)
- **THEN** its Bronze `payload` still holds it, unchanged

#### Scenario: SCN-BRZ-01.5 — Invalid events are landed, not rejected
- **WHEN** an event's resource fails its schema, or references a customer that
  doesn't exist
- **THEN** the event is landed in Bronze as received, and Silver quarantines it
  (REQ-SLV-09)

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
  then `idp consume` runs, then Silver conforms Bronze
- **THEN** Bronze holds one raw event per landed resource, and the conformed records
  equal exactly the canonical records the generator produced, per tenant and entity

#### Scenario: SCN-BRZ-03.5 — Resources conform to TM Forum
- **WHEN** the landing files of a generated dataset are validated
- **THEN** every resource validates against its vendored TMF schema and the
  extension schemas its `@type` declares, and every reference in it (customer,
  party, account, agreement, product) points at a resource in the same tenant's
  files

### Requirement: REQ-BRZ-05 — TM Forum Open API adapter
The ingestion layer SHALL provide an adapter that turns a tenant's TMF resources
into the canonical records (`parties`, `customers`, `billing_accounts`,
`agreements`, `products`, `voice_events`, `data_events`, `recharge_events`,
`fee_charges`), as defined by the active mapping profile (REQ-BRZ-06). It SHALL:

- read standard fields, characteristics and extension attributes as the profile
  maps them, and translate TMF values back to canonical values (e.g. status values)
- resolve the references between the tenant's resources:
  - a billing account's payer (`relatedParty` with role `payer`) to its party: the
    canonical `payer_party_id`, and `is_company` from whether the party is an
    Organization
  - a customer's engaged party to its party (`party_id`)
  - an agreement's customer role (`engagedParty` with role `customer`) to the
    customer (`customer_id`), and through the customer to its billing account
    (`account_id`)
- split `ProductUsage` into voice and data events by `usageType`
- yield organizations' person-only canonical fields (`first_name`, `last_name`,
  `date_of_birth`) as null

The adapter SHALL work on a tenant's whole set of landing files (the producer's
reference check, REQ-STR-01), and on one resource at a time against an index of the
tenant's latest customers, as Silver's conform step uses it (REQ-SLV-09). Both ways
SHALL give the same canonical records.

For the `standard` profile, adapting the resources the landing writer produced
from a canonical dataset SHALL return that dataset exactly. Money is compared to
the generator's precision.

#### Scenario: SCN-BRZ-05.1 — Round trip
- **WHEN** a generated two-tenant dataset is written as TMF resources with the
  `standard` profile and adapted back
- **THEN** every canonical entity equals the generated one, record for record

#### Scenario: SCN-BRZ-05.2 — References resolved through the Customer role
- **WHEN** an agreement references a `Customer` whose `engagedParty` is an
  `Organization`
- **THEN** the canonical customer carries that organization's id as `party_id`, and
  the canonical agreement carries the customer's id as `customer_id`

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

#### Scenario: SCN-BRZ-05.6 — Agreement linked through its customer
- **WHEN** an agreement's customer role references a `Customer` whose account is a
  billing account of the tenant
- **THEN** the canonical agreement carries that customer's id as `customer_id` and
  the customer's billing account as `account_id`

#### Scenario: SCN-BRZ-05.7 — Billing account payer resolved
- **WHEN** a `BillingAccount`'s `relatedParty` with role `payer` references an
  `Organization`
- **THEN** the canonical billing account carries that organization's id as
  `payer_party_id` and `is_company = true`
