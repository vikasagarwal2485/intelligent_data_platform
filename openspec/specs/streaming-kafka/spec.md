# Streaming (Kafka) Specification

## Purpose

Bridge synthetic/source data into the lakehouse through Kafka: a producer that
publishes to per-tenant topics and a consumer that lands Bronze — with strict
separation of concerns (the consumer only produces Bronze — ADR-0009).

## Requirements

### Requirement: REQ-STR-01 — Per-tenant topic production
The producer SHALL publish each entity/event record as a message to
`t.<tenant>.<entity>`. The entity suffixes SHALL be `parties`, `accounts`,
`agreements`, `products`, `usage.voice`, `usage.data`, `recharge` and `fees`. Each
message SHALL be keyed by the subscriber's natural key where the record has one, and
by the record id otherwise. The producer SHALL work one tenant at a time to bound
memory.

The producer SHALL take its records from one of two sources:
- **by default**, data it generates itself
- **with `--from-landing`**, the files of the landing zone (REQ-BRZ-03)

Publishing a landing file SHALL produce the same messages (topic, key and value) as
publishing the same records generated in memory.

Before publishing a landing file, the producer SHALL check it. A file SHALL NOT be
published at all when:
- its folder names a tenant that isn't configured
- it isn't named after a known entity
- it contains any record whose `tenant_id` differs from its folder's tenant The
producer SHALL report it as rejected and still publish the other files. The
producer SHALL exit with a failure status when any file was rejected.

#### Scenario: SCN-STR-01.1 — Topic mapping
- **WHEN** the producer publishes voice usage for `tenant_a`
- **THEN** the messages land on topic `t.tenant_a.usage.voice`

#### Scenario: SCN-STR-01.2 — Fee charges have their own topic
- **WHEN** the producer publishes fee charges for `tenant_b`
- **THEN** the messages land on topic `t.tenant_b.fees`

#### Scenario: SCN-STR-01.3 — Landing and in-memory production are equivalent
- **WHEN** the same generated data is published once from memory and once from its
  landing files
- **THEN** both runs send the same messages to the same topics with the same keys

#### Scenario: SCN-STR-01.4 — Mismatched tenant file rejected
- **WHEN** `tenant=tenant_a/recharge_events.json` contains a record with `tenant_id`
  `tenant_b`, and the producer publishes from the landing zone
- **THEN** no record of that file is published, the file is reported as rejected,
  the other files are published, and the command exits with a failure status

#### Scenario: SCN-STR-01.5 — Unknown tenant folder rejected
- **WHEN** the landing zone contains `tenant=tenant_x/`, and `tenant_x` isn't a
  configured tenant
- **THEN** none of its files are published, and they are reported as rejected

### Requirement: REQ-STR-02 — Consumer produces Bronze only
The consumer SHALL drain the `t.<tenant>.<entity>` topics, group messages back
into per-entity frames, and append them to the Iceberg Bronze tables
(REQ-BRZ-01). It SHALL NOT perform Silver or Gold work, and SHALL exit cleanly when
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

### Requirement: REQ-STR-03 — Avro schema contracts
Avro schemas for the streaming events (subscriber, usage, recharge, fee charge)
SHALL exist in `config/avro/` and be registerable to a Confluent-compatible registry
as `<topic>-value` subjects. The schemas are the documented contract. Message
payloads on the wire SHALL be JSON-encoded records whose fields conform to those
schemas.

#### Scenario: SCN-STR-03.1 — Schema registration
- **WHEN** the schema registration runs against the registry
- **THEN** subjects like `t.tenant_a.recharge-value` are created from the `.avsc` files

#### Scenario: SCN-STR-03.2 — JSON payloads
- **WHEN** the consumer reads a message from any `t.<tenant>.<entity>` topic
- **THEN** the value decodes as a UTF-8 JSON object carrying `tenant_id`
