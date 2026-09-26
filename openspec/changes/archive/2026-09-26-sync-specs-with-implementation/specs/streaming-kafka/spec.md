# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-STR-01 — Per-tenant topic production
The producer SHALL publish each entity/event record as a message to
`t.<tenant>.<entity>`. The entity suffixes SHALL be `parties`, `accounts`,
`agreements`, `products`, `usage.voice`, `usage.data`, `recharge` and `fees`. Each
message SHALL be keyed by the subscriber's natural key where the record has one, and
by the record id otherwise. The producer SHALL generate and publish one tenant at a
time to bound memory.

#### Scenario: SCN-STR-01.1 — Topic mapping
- **WHEN** the producer publishes voice usage for `tenant_a`
- **THEN** the messages land on topic `t.tenant_a.usage.voice`

#### Scenario: SCN-STR-01.2 — Fee charges have their own topic
- **WHEN** the producer publishes fee charges for `tenant_b`
- **THEN** the messages land on topic `t.tenant_b.fees`

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
