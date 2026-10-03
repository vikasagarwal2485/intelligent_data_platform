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

