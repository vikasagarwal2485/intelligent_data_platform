# Streaming (Kafka) Specification

## Purpose

Bridge synthetic/source data into the lakehouse through Kafka: a producer that
publishes to per-tenant topics and a consumer that lands Bronze — with strict
separation of concerns (the consumer only produces Bronze — ADR-0009).

## Requirements

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
keyed by the customer id. Every topic, `customer` included, SHALL use the delete
cleanup policy with the platform's retention: Bronze keeps every event, and Silver
resolves customers from it (REQ-SLV-09), so no topic is log-compacted. The producer SHALL publish a tenant's customers
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

#### Scenario: SCN-STR-01.9 — Customer topic like the others
- **WHEN** the topics are created or re-applied (`make topics`)
- **THEN** `t.<tenant>.customer` has the delete cleanup policy and the same retention
  as the other resource topics, and its message count is reported like theirs

### Requirement: REQ-STR-03 — Avro schema contracts
The platform SHALL keep two contracts:

- **Wire contract.** Each topic's value SHALL conform to a JSON Schema for its TMF
  create event and resource: the TMF schemas of the event and resource, plus the
  extension schemas their `@type`s declare (REQ-BRZ-06). The `party` topic's schema
  SHALL accept both the Individual and the Organization event. These schemas SHALL be
  registerable to a Confluent-compatible registry as `<topic>-value` subjects with
  schema type JSON. Message payloads on the wire SHALL be UTF-8 JSON.
- **Canonical contract.** Avro schemas in `config/avro/canonical/` SHALL describe the
  canonical record of every conformed entity (`parties`, `customers`,
  `billing_accounts`, `agreements`, `products`, voice and data usage, recharges, fee
  charges), field for field as Silver's conform step produces them. The conform step
  SHALL enforce them (REQ-SLV-09). The canonical schemas SHALL NOT be registered as
  Kafka subjects.
- **Silver contracts.** Every Silver table (`dim_subscriber`, `dim_customer`,
  `dim_billing_account`, `dim_product`, `fact_usage`, `fact_recharge`,
  `fact_fee_charge` and `quarantine`) SHALL have an Avro schema in
  `config/avro/silver/` documenting its columns, in order, and their types as Silver
  stores them (tokenized identifiers, UTC timestamps). They are documentation,
  checked by tests, not enforced at run time.

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
  `config/avro/canonical/`
- **THEN** every entity has a schema, every record conforms to it, and each schema's
  fields are exactly the entity's canonical columns

#### Scenario: SCN-STR-03.4 — Silver contract matches dim_subscriber
- **WHEN** Silver builds `dim_subscriber`
- **THEN** its columns are exactly the fields of
  `config/avro/silver/dim_subscriber.avsc`

#### Scenario: SCN-STR-03.5 — Silver contracts match every Silver table
- **WHEN** Silver builds its dimensions and facts, and quarantines an event
- **THEN** each of `dim_customer`, `dim_billing_account`, `dim_product`,
  `fact_usage`, `fact_recharge`, `fact_fee_charge` and `quarantine` has exactly the
  fields of its contract in `config/avro/silver/`, in order, and the contract's
  timestamp fields are timestamps in the table

### Requirement: REQ-STR-04 — Consumer lands raw events
The consumer SHALL drain every `t.<tenant>.<resource>` topic, including `customer`,
and land each message as one raw event in the Bronze table of its topic
(REQ-BRZ-01), unchanged. It SHALL NOT validate, resolve or adapt the resources:
that is Silver's conform step (REQ-SLV-09).

A message SHALL be sent to the tenant's dead-letter topic `t.<tenant>.dlq`, with its
reason and its source topic, partition and offset, only when it can't be stored as
an event: its value isn't a JSON object, or it has no `eventType`, `eventId` or
`event`. The consumer SHALL still land the other messages, SHALL commit the offsets
only after Bronze and the dead-letter topic are written, and SHALL exit with a
failure status when any message was dead-lettered.

The consumer SHALL NOT perform Silver or Gold work, and SHALL exit cleanly when
topics are empty. Each consumed batch SHALL become one Bronze append (one Iceberg
snapshot) per topic table.

#### Scenario: SCN-STR-04.1 — Read-only consume then Bronze
- **WHEN** `idp consume` runs after the producer completes
- **THEN** it appends the consumed events to the `bronze.tmf_<resource>` Iceberg
  tables, stored under `s3://idp-bronze`, and produces no Silver or Gold

#### Scenario: SCN-STR-04.2 — Empty topics
- **WHEN** `idp consume` runs with no messages available
- **THEN** it reports nothing consumed and exits successfully (no error), and no
  Bronze snapshot is created

#### Scenario: SCN-STR-04.3 — TMF in, raw Bronze out
- **WHEN** a generated dataset is published and `idp consume` runs
- **THEN** Bronze holds each published event once, customers included, with its
  `payload` equal to the message value

#### Scenario: SCN-STR-04.4 — Customer from an earlier run
- **WHEN** a tenant's customers were consumed in an earlier run, and a later run
  consumes only billing accounts, agreements and parties that reference them
- **THEN** the later run lands them, and Silver resolves their references against
  the customers already in Bronze

#### Scenario: SCN-STR-04.5 — Bad message dead-lettered
- **WHEN** a batch holds valid messages and one whose value isn't JSON
- **THEN** the valid events are landed, the bad message is on `t.<tenant>.dlq` with
  the reason and its source position, the offsets are committed, and the consume
  exits with a failure status

#### Scenario: SCN-STR-04.6 — Contract violations reach Bronze
- **WHEN** a message's resource breaks the canonical contract once adapted, or holds
  a customer reference that doesn't resolve
- **THEN** the consumer lands it in Bronze unchanged and dead-letters nothing, and
  Silver quarantines it (REQ-SLV-09)
