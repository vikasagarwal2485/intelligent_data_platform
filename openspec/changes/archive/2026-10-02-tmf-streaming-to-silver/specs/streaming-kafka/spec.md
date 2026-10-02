# Streaming (Kafka) delta

## MODIFIED Requirements

### Requirement: REQ-STR-01 — Per-tenant topic production
The producer SHALL publish each TM Forum resource (REQ-BRZ-03) as a TMF event
message to a per-tenant topic named after its resource, `t.<tenant>.<resource>`:

- `party`: `Individual` and `Organization`
- `customer`: `Customer`
- `billingAccount`, `agreement`, `product`, `appliedCustomerBillingRate`,
  `topupBalance`: the resource of the same name
- `productUsage`: `ProductUsage`, voice and data alike

Each message value SHALL be a TMF create event: an envelope with `eventId`,
`eventTime`, `eventType` (`<Resource>CreateEvent`, e.g. `ProductUsageCreateEvent`)
and `event` holding the resource under its resource name. The `eventId` SHALL be
derived from the tenant, the resource type and the resource id, so publishing the
same resource again gives the same `eventId`. The `eventTime` SHALL be the time the
event is published.

Each message SHALL be keyed by the subscriber's natural key (the MSISDN) where the
resource carries one, and by the resource id otherwise. Customer messages SHALL be
keyed by the customer id. The `customer` topic SHALL be log-compacted, keeping the
latest event of each customer. The producer SHALL publish a tenant's customers
before the resources that reference them. It SHALL work one tenant at a time to
bound memory.

The producer SHALL take its resources from one of two sources:
- **by default**, data it generates itself, written as TMF resources with the active
  mapping profile (REQ-BRZ-06)
- **with `--from-landing`**, the TMF resource files of the landing zone (REQ-BRZ-03),
  published as they are

Publishing from the landing zone SHALL produce the same messages (topic, key and
value, apart from `eventTime`) as publishing the same generated records from
memory.

Before publishing, the producer SHALL check each landing file. A file SHALL NOT be
published at all when:
- its folder names a tenant that isn't configured
- it isn't named after a known TMF resource file
- it isn't a JSON array of resources that validate against their schemas
  (REQ-BRZ-06)
- any of its resources has a `tenantId` that differs from its folder's tenant
- any of its resources holds a reference that doesn't resolve in the tenant's files

The producer SHALL report each rejected file with its reason, and still publish the
resources of the other files. The producer SHALL exit with a failure status when
any file was rejected.

#### Scenario: SCN-STR-01.1 — Topic mapping
- **WHEN** the producer publishes voice usage for `tenant_a`
- **THEN** the messages land on topic `t.tenant_a.productUsage`, each a
  `ProductUsageCreateEvent` whose `event.productUsage` has `usageType` `voice`

#### Scenario: SCN-STR-01.2 — Fee charges have their own topic
- **WHEN** the producer publishes fee charges for `tenant_b`
- **THEN** the messages land on topic `t.tenant_b.appliedCustomerBillingRate`

#### Scenario: SCN-STR-01.3 — Landing and in-memory production are equivalent
- **WHEN** the same generated data is published once from memory and once from its
  TMF landing files
- **THEN** both runs send the same messages to the same topics with the same keys,
  identical apart from `eventTime`

#### Scenario: SCN-STR-01.4 — Mismatched tenant file rejected
- **WHEN** `tenant=tenant_a/topupBalance.json` contains a resource with `tenantId`
  `tenant_b`, and the producer publishes from the landing zone
- **THEN** no resource of that file is published, the file is reported as rejected,
  the other files are published, and the command exits with a failure status

#### Scenario: SCN-STR-01.5 — Unknown tenant folder rejected
- **WHEN** the landing zone contains `tenant=tenant_x/`, and `tenant_x` isn't a
  configured tenant
- **THEN** none of its files are published, and they are reported as rejected

#### Scenario: SCN-STR-01.6 — Invalid or dangling resource file rejected
- **WHEN** `tenant=tenant_a/agreement.json` holds an agreement whose customer
  reference doesn't resolve, or a resource that fails its schema
- **THEN** that file is reported as rejected with the reason, none of its resources
  are published, and the command exits with a failure status

#### Scenario: SCN-STR-01.7 — Individuals and organizations share the party topic
- **WHEN** the producer publishes the parties of `tenant_a`, some of them
  organizations
- **THEN** all of them land on `t.tenant_a.party`, individuals as
  `IndividualCreateEvent` and organizations as `OrganizationCreateEvent`

#### Scenario: SCN-STR-01.8 — Stable event ids
- **WHEN** the same resource is published twice
- **THEN** both messages carry the same `eventId`

### Requirement: REQ-STR-02 — Consumer produces Bronze only
The consumer SHALL drain the `t.<tenant>.<resource>` topics, adapt the TMF resources
to canonical records with the TMF adapter (REQ-BRZ-05) and the active mapping
profile, group the records into per-entity frames (`productUsage` split into voice
and data events), and append them to the Iceberg Bronze tables (REQ-BRZ-01).

To resolve references through the Customer role, the consumer SHALL first read each
tenant's `customer` topic in full, so a customer published in an earlier run still
resolves. Customers SHALL NOT be landed in Bronze.

The consumer SHALL check each message. A message SHALL NOT be landed when:
- it isn't a TMF create event of its topic's resource type
- its resource fails its schema (REQ-BRZ-06)
- its resource's `tenantId` differs from its topic's tenant
- its resource holds a customer reference that doesn't resolve
- an adapted record doesn't conform to its entity's canonical contract (REQ-STR-03)

Such a message SHALL be sent to the tenant's dead-letter topic `t.<tenant>.dlq`
with its reason and its source topic, partition and offset. The consumer SHALL still
land the valid records, SHALL commit the offsets only after Bronze and the
dead-letter topic are written, and SHALL exit with a failure status when any message
was dead-lettered.

The consumer SHALL NOT perform Silver or Gold work, and SHALL exit cleanly when
topics are empty. Each consumed batch SHALL become one Bronze append (one Iceberg
snapshot) per entity.

#### Scenario: SCN-STR-02.1 — Read-only consume then Bronze
- **WHEN** `idp consume` runs after the producer completes
- **THEN** it appends the consumed records to the `bronze.<entity>` Iceberg tables,
  stored under `s3://idp-bronze`, and produces no Silver or Gold

#### Scenario: SCN-STR-02.2 — Empty topics
- **WHEN** `idp consume` runs with no messages available
- **THEN** it reports nothing consumed and exits successfully (no error), and no
  Bronze snapshot is created

#### Scenario: SCN-STR-02.3 — TMF in, canonical Bronze out
- **WHEN** a generated dataset is published and `idp consume` runs
- **THEN** Bronze holds exactly the dataset's canonical records, per tenant and
  entity, and no `customer` table

#### Scenario: SCN-STR-02.4 — Customer from an earlier run
- **WHEN** a tenant's customers were consumed in an earlier run, and a later run
  consumes only accounts, agreements and parties that reference them
- **THEN** the references resolve and the records are landed

#### Scenario: SCN-STR-02.5 — Bad message dead-lettered
- **WHEN** a batch holds valid messages and one agreement event whose customer
  reference doesn't resolve
- **THEN** the valid records are landed, the agreement event is on
  `t.<tenant>.dlq` with the reason and its source position, the offsets are
  committed, and the consume exits with a failure status

#### Scenario: SCN-STR-02.6 — Contract violation dead-lettered
- **WHEN** a message adapts to a canonical record that doesn't conform to its
  entity's Avro contract (e.g. a recharge status outside the contract's symbols)
- **THEN** the message is dead-lettered with the contract error, and no record of it
  is landed

### Requirement: REQ-STR-03 — Avro schema contracts
The platform SHALL keep two contracts:

- **Wire contract.** Each topic's value SHALL conform to a JSON Schema for its TMF
  create event and resource: the TMF schemas of the event and resource, plus the
  extension schemas their `@type`s declare (REQ-BRZ-06). The `party` topic's schema
  SHALL accept both the Individual and the Organization event. These schemas SHALL be
  registerable to a Confluent-compatible registry as `<topic>-value` subjects with
  schema type JSON. Message payloads on the wire SHALL be UTF-8 JSON.
- **Canonical contract.** Avro schemas in `config/avro/` SHALL describe the canonical
  record of every Bronze entity (`parties`, `accounts`, `agreements`, `products`,
  voice and data usage, recharges, fee charges), field for field as Bronze stores
  them. The consumer SHALL enforce them (REQ-STR-02). The Silver `dim_subscriber`
  contract SHALL be documented in `config/avro/silver/`. The canonical schemas SHALL
  NOT be registered as Kafka subjects.

#### Scenario: SCN-STR-03.1 — Schema registration
- **WHEN** the schema registration runs against the registry
- **THEN** subjects like `t.tenant_a.topupBalance-value` are created from the TMF
  JSON Schemas, with schema type JSON

#### Scenario: SCN-STR-03.2 — JSON payloads
- **WHEN** the consumer reads a message from any `t.<tenant>.<resource>` topic
- **THEN** the value decodes as a UTF-8 JSON TMF create event whose resource carries
  `tenantId`

#### Scenario: SCN-STR-03.3 — Canonical contract matches the records
- **WHEN** a generated dataset's canonical records are checked against
  `config/avro/`
- **THEN** every entity has a schema, every record conforms to it, and each schema's
  fields are exactly the entity's canonical columns

#### Scenario: SCN-STR-03.4 — Silver contract matches dim_subscriber
- **WHEN** Silver builds `dim_subscriber`
- **THEN** its columns are exactly the fields of
  `config/avro/silver/dim_subscriber.avsc`
