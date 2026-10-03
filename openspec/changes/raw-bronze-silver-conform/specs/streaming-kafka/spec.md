# Streaming (Kafka) delta

## MODIFIED Requirements

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

## REMOVED Requirements

### Requirement: REQ-STR-02 — Consumer produces Bronze only
**Reason**: The consumer no longer adapts TMF events to canonical Bronze records, resolves customers or enforces the canonical contract: Bronze holds the raw events (REQ-BRZ-01) and Silver conforms them (REQ-SLV-09). The consumer's remaining duties are restated as REQ-STR-04.
**Migration**: `idp consume` keeps its command, offsets and exit status, and still produces Bronze only. Its Bronze output is the raw `bronze.tmf_<resource>` tables instead of `bronze.<entity>`; only messages that can't be stored are dead-lettered. Run `make minio-reset` and stream again.

## ADDED Requirements

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
