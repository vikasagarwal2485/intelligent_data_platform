# Design: TMF through Kafka to Bronze and Silver

## Context

See proposal.md for why. The current state this design builds on:

- **Producer** (`streaming/producer.py`): `publish_records` sends canonical records,
  one topic per canonical entity (`TABLE_TO_SUFFIX`), keyed by
  `service_id`/`msisdn`/`id`, as JSON. With `--from-landing`, `read_landing`
  validates the TMF files and adapts them to canonical records first (change 1).
- **Consumer** (`streaming/consumer.py`):
  - drains every topic in one batch with the group `idp-bronze-loader`;
  - groups the messages into frames, calls `land` (`to_bronze`), then commits the
    offsets;
  - takes topics back to tables with `SUFFIX_TO_TABLE`.
- **TMF adapter** (`ingestion/tmf/profile.py`): `from_tmf(profile, files)` adapts
  a whole tenant folder. Customer references resolve through `_Customers.index`:
  - accounts and agreements by customer id;
  - parties by the Customer whose `engagedParty` they are.
- **Validation** (`ingestion/tmf/schemas.py`): one compiled `fastjsonschema`
  validator per `@type`, over the merged vendored and extension schemas.
- **Kafka:**
  - topics are Strimzi `KafkaTopic` CRs from `create-topics.sh`, including an
    unused `t.<tenant>.dlq`;
  - `register-schemas.sh` registers the four Avro event schemas in Apicurio.
- **Bronze:** columns are typed from the pandas frames. Timestamps are ISO-8601
  strings. `_ingested_at` and `ingest_date` are added at landing and dropped
  before Silver (`LANDING_COLUMNS`).
- **The Avro schemas** have drifted from the records (proposal, Why), and no code
  reads them.

## Goals / Non-Goals

**Goals:**
- Bronze receives exactly the canonical records it receives today, so Silver, Gold
  and the golden files don't change (SCN-STR-02.3).
- Consumer memory stays near today's level, despite the larger TMF messages.

**Non-Goals:**
- Replaying the DLQ. A dead-lettered source is fixed and re-produced.
- Event-id deduplication in Bronze. Silver already merges on natural keys.
- Streaming (always-on) consumption: consume stays a batch drain.
- Schema-registry serializers on the wire. Payloads stay plain JSON, and the
  registered schemas are the contract.

## Decisions

### 1. Event envelope: TMF `<Resource>CreateEvent`
Each message value is:

```json
{"@type": "ProductUsageCreateEvent", "eventId": "…", "eventTime": "…Z",
 "eventType": "ProductUsageCreateEvent",
 "event": {"productUsage": {"@type": "IdpProductUsage", "tenantId": "tenant_a", …}}}
```

- **`eventId`** is `uuid5(IDP_NS, "<tenant>/<@baseType>/<id>")`. It's stable, so
  the two production sources give equal messages (SCN-STR-01.3/01.8). A random id
  would break that equality.
- **`eventTime`** is the publish time. It's the only value allowed to differ
  between the two sources. A timestamp derived from the resource was considered,
  but reference resources have no natural event time.
- **Schemas.** The envelope's schema is the TMF `<Resource>CreateEvent` vendored
  from the same pinned spec file as its resource, added as an extra root in
  `vendor_schemas.py`.
  - Task 1.1 confirms which pinned specs define one: likely all v5 specs, and
    TMF651/654 v4.0 as `AgreementCreateEvent`/`TopupBalanceCreateEvent`.
  - Where a pinned spec has none, the platform defines `<Resource>CreateEvent` in
    `config/tmf/events/` on that spec's own `Event`. The messages are the same
    either way. Outcome of task 1.1: seven are vendored (TMF629, 632 ×2, 637, 666,
    and the v4 TMF651/654 events). `ProductUsageCreateEvent` (TMF635 has only
    ExportJob events) and `AppliedCustomerBillingRateCreateEvent` (TMF678 has only
    CustomerBill events) are platform-defined.
- **Validation is split.** The envelope is checked against the event schema with
  the payload left open. The resource is checked with the existing
  `@type`-dispatched validator (REQ-BRZ-06), because the TMF event schemas
  reference the base resource and so don't know `Idp<Resource>`.

### 2. Topics and keys
- **Topic names.** The suffix is the TMF resource name: `Individual` and
  `Organization` map to `party`, every other resource to its own name, as one fixed
  map in `streaming/__init__.py` (`RESOURCE_TOPIC`). Base types are fixed by TMF,
  so a customer profile can't change the topic of a resource.
- **Keys.** The key of a resource is the canonical natural key, read through the
  profile. The producer locates the canonical `service_id` or `msisdn` field of the
  resource's mapping and reads it with that `FieldMap`; without one, the key is the
  resource id. This keeps today's keys (usage, recharge and agreement by MSISDN),
  so partitioning per subscriber doesn't change. Customers are keyed by id, which
  log compaction needs.
- **Compaction.** `create-topics.sh` gives `customer` `cleanup.policy:
  compact,delete` with `retention.ms: -1`, and every other topic the 7-day delete
  policy. With unlimited retention the customer topic is compacted and never
  expires. Adding `delete` lets `purge-topics.sh` truncate it, because
  DeleteRecords needs it. Switching the policy around each purge was rejected:
  the Strimzi topic operator would race the change. `productUsage` keeps 6 partitions,
  the others 3.

### 3. Producer
Both sources yield, per tenant, `{file: [resources]}` validated in the same way, then
one publishing path turns them into events:

- **In memory:** generate a tenant, run `to_tmf`, then validate. This is the same
  function `write_landing` uses, so the two produce identical resources.
- **From landing:** a new `read_landing_resources` reuses `_check_file` (tenant
  folder, known file, schema, `tenantId`). For the reference rule it runs the
  adapter as a check and discards the canonical output. That keeps exactly
  change 1's rejection rules and messages, at the cost of one extra adaptation per
  tenant.

Publish order per tenant: customer, then parties, billing accounts, agreements,
products, fee charges, recharges and usage. The producer flushes after the
customers, so they are on the broker before anything that references them.

### 4. Consumer
1. **Customer index.** For each tenant, the consumer reads `t.<tenant>.customer`
   without the consumer group:
   - assign all partitions, seek to the beginning, and read up to the end offsets
     taken at start;
   - keep the last event per id, because compaction runs lazily;
   - drop invalid customers from the index, and report each one in every run.
     A record referencing one is then dead-lettered.
2. **Drain.** It drains the resource topics with the group, as today. Values are
   kept as raw bytes, so the DLQ gets the original payload.
3. **Per message:** decode the JSON, check the envelope and that the event type
   matches the topic, validate the resource, check `tenantId` against the topic's
   tenant, then adapt.
4. **Adapt per resource.** `from_tmf` is refactored into `adapt_resource(profile,
   resource, customers) -> (entity, record)` plus a loop. Whole-file adaptation
   is that loop over a folder, so SCN-BRZ-05.5 holds by construction and the
   round-trip tests keep covering it. `ProductUsage` routes to voice or data
   through the profile's `when`, as now.
5. **Contract check.** Each record is checked against its entity's parsed Avro
   schema with `fastavro.validation.validate`.
6. **Accumulate.** The consumer keeps only canonical dicts and dead-letter entries,
   never the TMF payloads. So memory is about today's level, even though the
   messages are about 5 times larger.
7. **Finish.** `land(frames)`, then produce the DLQ entries and flush, then commit
   the offsets.
   - DLQ entries keep the original key and value, with headers `idp.reason`,
     `idp.source.topic`, `idp.source.partition` and `idp.source.offset`.
   - If landing or the DLQ write fails, nothing is committed, and the batch is
     re-read next time, as today.
   - The CLI exits 1 when the DLQ count is above zero, so `make stream-from-landing`
     stops before Silver.

### 5. Canonical Avro contract
- **Files.** `config/avro/` gets one schema per Bronze entity: `party`, `account`,
  `agreement`, `product`, `usage_event` (voice and data), `recharge_event` and
  `fee_charge`.
- **Fields** are the canonical columns in canonical order:
  - timestamps are `string`, documented as ISO-8601 UTC, as Bronze stores them;
  - columns the generator can leave null are `["null", T]`;
  - the money fields are `net_amount` and `tax_amount`; gross is derived in
    Silver.
- **Enums only for closed domains:** `event_type`, recharge `status`,
  `payment_method` and `recharge_type`, `charge_type`, `account_segment`,
  `agreement_type`, `business_line`. Open values such as `network_tech` (a customer
  profile adds `4G+`) stay strings.
- **Silver contract.** `subscriber.avsc` moves to
  `config/avro/silver/dim_subscriber.avsc`, corrected to `DIM_COLUMNS`
  (`account_tenure_days`, not `account_valid_from`). It is checked by a test, not
  enforced at run time.
- **Library.** The check uses `fastavro`, a new dependency (C-accelerated). Avro
  checking with `fastjsonschema` was considered, but Avro is the documented contract
  format (ADR-0013) and the files already exist.

### 6. Wire contract in the registry
`schemas.registry_schema(topic)` builds one self-contained draft-07 JSON Schema per
topic:
- the TMF create event, with `event.<resource>` pointing at the `Idp<Resource>`
  extension (`anyOf` the two events for `party`);
- only the definitions reachable from those roots. The full merged set is about
  300 KB.

`idp registry-schemas --out DIR` writes them. `register-schemas.sh` runs it with the
repo's virtualenv Python and posts each file as `t.<tenant>.<resource>-value` with
`schemaType: JSON`. The Avro subjects are no longer registered. Old Avro subjects
already in Apicurio are left in place; they're harmless and listed in the migration.

### 7. Decision record
ADR-0031 records this design: TMF events on the wire, the adapter in the consumer,
and the two contracts. It supersedes ADR-0013's "Avro on the wire" part. The topic
naming `t.<tenant>.<x>` without a `tmf` segment stays.

## Risks / Trade-offs

- **[A consume runs before the producer's customers arrive]** Its references fail
  and are dead-lettered. → The make chains run consume after produce. The DLQ
  reason names the missing customer, and re-producing fixes it.
- **[Strict enums dead-letter a new value]** → That's the purpose: changing the
  domain means changing the contract. Open domains are strings.
- **[Validation cost in the consumer]** The consumer runs the schema check
  (about 10k resources a second, from change 1), the adaptation and `fastavro`. For
  about 75k messages that's around 10–15 s. → Measured in task 6.2: the consume
  Job took 63 s end to end for 75,270 events, including pod start and the 15 s idle
  timeout. Produce from landing took 37 s. Acceptable for a batch Job.
- **[The registry rejects or truncates large JSON Schemas]** → Each topic's schema
  is pruned to what it reaches, which is tens of KB.
- **[The old topics keep messages after migration]** → The old KafkaTopic CRs are
  deleted at migration (with confirmation). Their retention would expire them
  anyway.
- **[Bash 3.2 on macOS]** Lower-casing topic names for CR metadata uses `tr`, not
  `${s,,}`.

## Migration Plan

1. `make image`.
2. `make topics`: creates the nine new topics per tenant, with `customer` compacted.
3. Retire the old topics, after confirmation, since it drops their messages:
   - delete the 14 old KafkaTopic CRs (`t-<tenant>-{parties,accounts,agreements,
     products,usage-voice,usage-data,recharge}`); deleting a CR deletes its topic;
   - delete `t.<tenant>.fees` with `kafka-topics.sh --delete`. Kafka auto-created
     those topics on the first publish, so they have no CR. For the same reason,
     `make topics` must run before the new producer: otherwise `customer` would be
     auto-created as an ordinary delete topic.
4. `make schemas`: registers the TMF JSON subjects. The old Avro subjects stay
   unused.
5. `make stream-from-landing`, then `make silver`. Bronze and Silver equal what they
   were before for the same data.

**Rollback:** revert the code and run `make topics`, which recreates the old topics.
The new topics can stay unused.
