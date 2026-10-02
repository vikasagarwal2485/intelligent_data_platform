# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-STR-01 — Per-tenant topic production
The producer SHALL publish each entity/event record as a message to
`t.<tenant>.<entity>`. The entity suffixes SHALL be `parties`, `accounts`,
`agreements`, `products`, `usage.voice`, `usage.data`, `recharge` and `fees`. Each
message SHALL be keyed by the subscriber's natural key where the record has one, and
by the record id otherwise. The producer SHALL work one tenant at a time to bound
memory.

The producer SHALL take its records from one of two sources:
- **by default**, data it generates itself
- **with `--from-landing`**, the TMF resource files of the landing zone (REQ-BRZ-03),
  adapted to canonical records by the TMF adapter (REQ-BRZ-05) with the active
  mapping profile

Publishing from the landing zone SHALL produce the same messages (topic, key and
value) as publishing the same generated records from memory.

Before publishing, the producer SHALL check each landing file. A file SHALL NOT be
published at all when:
- its folder names a tenant that isn't configured
- it isn't named after a known TMF resource file
- it isn't a JSON array of resources that validate against their schemas
  (REQ-BRZ-06)
- any of its resources has a `tenantId` that differs from its folder's tenant
- any of its resources holds a reference that doesn't resolve in the tenant's files

The producer SHALL report each rejected file with its reason, and still publish the
canonical records of the other files. The producer SHALL exit with a failure status
when any file was rejected.

#### Scenario: SCN-STR-01.1 — Topic mapping
- **WHEN** the producer publishes voice usage for `tenant_a`
- **THEN** the messages land on topic `t.tenant_a.usage.voice`

#### Scenario: SCN-STR-01.2 — Fee charges have their own topic
- **WHEN** the producer publishes fee charges for `tenant_b`
- **THEN** the messages land on topic `t.tenant_b.fees`

#### Scenario: SCN-STR-01.3 — Landing and in-memory production are equivalent
- **WHEN** the same generated data is published once from memory and once from its
  TMF landing files
- **THEN** both runs send the same messages to the same topics with the same keys

#### Scenario: SCN-STR-01.4 — Mismatched tenant file rejected
- **WHEN** `tenant=tenant_a/topupBalance.json` contains a resource with `tenantId`
  `tenant_b`, and the producer publishes from the landing zone
- **THEN** no record of that file is published, the file is reported as rejected,
  the other files are published, and the command exits with a failure status

#### Scenario: SCN-STR-01.5 — Unknown tenant folder rejected
- **WHEN** the landing zone contains `tenant=tenant_x/`, and `tenant_x` isn't a
  configured tenant
- **THEN** none of its files are published, and they are reported as rejected

#### Scenario: SCN-STR-01.6 — Invalid or dangling resource file rejected
- **WHEN** `tenant=tenant_a/agreement.json` holds an agreement whose customer
  reference doesn't resolve, or a resource that fails its schema
- **THEN** that file is reported as rejected with the reason, none of its records are
  published, and the command exits with a failure status
