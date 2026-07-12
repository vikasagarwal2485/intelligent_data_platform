# Streaming (Kafka) Specification

## Purpose

Bridge synthetic/source data into the lakehouse through Kafka: a producer that
publishes to per-tenant topics and a consumer that lands Bronze — with strict
separation of concerns (the consumer only produces Bronze — ADR-0009).

## Requirements

### Requirement: REQ-STR-01 — Per-tenant topic production
The producer SHALL publish each entity/event record as a message to
`t.<tenant>.<entity>` (e.g. `t.tenant_a.usage.voice`), keyed by the subscriber's
natural key, and SHALL generate/publish per tenant to bound memory.

#### Scenario: SCN-STR-01.1 — Topic mapping
- **WHEN** the producer publishes voice usage for `tenant_a`
- **THEN** the messages land on topic `t.tenant_a.usage.voice`

### Requirement: REQ-STR-02 — Consumer produces Bronze only
The consumer SHALL drain the `t.<tenant>.<entity>` topics, group messages back
into per-entity frames, and hand them to the Bronze stage; it SHALL NOT perform
Silver or Gold work, and SHALL exit cleanly when topics are empty.

#### Scenario: SCN-STR-02.1 — Read-only consume then Bronze
- **WHEN** `idp consume` runs after the producer completes
- **THEN** it writes Bronze tables to `s3://idp-bronze` and produces no Silver/Gold

#### Scenario: SCN-STR-02.2 — Empty topics
- **WHEN** `idp consume` runs with no messages available
- **THEN** it reports nothing consumed and exits successfully (no error)

### Requirement: REQ-STR-03 — Avro schema contracts
Avro schemas for the streaming events SHALL exist in `config/avro/` and be
registerable to a Confluent-compatible registry as `<topic>-value` subjects.

#### Scenario: SCN-STR-03.1 — Schema registration
- **WHEN** the schema registration runs against the registry
- **THEN** subjects like `t.tenant_a.recharge-value` are created from the `.avsc` files
