# Proposal: TMF through Kafka to Bronze and Silver

## Why

Change 1 (`tmf-landing-files`) made the landing zone TM Forum Open API v5, but the
producer still adapts the resources back to canonical records before publishing.
So Kafka carries the platform's internal model, not what a real operator's TMF
source would send. This change puts TMF events on Kafka and moves the adapter to
the consumer, the point where source data enters the lake. Bronze and Silver keep
the canonical model.

Two contracts follow from this, and both are out of date today:
- **The wire contract.** The Avro schemas in `config/avro/` are registered as the
  Kafka subjects. Once TMF is on the wire, they no longer describe it.
- **The canonical contract.** Nothing enforces the Avro schemas against the
  records, and they have drifted:
  - `usage_event.avsc` has `gross_amount`, but the records carry `net_amount`;
  - its timestamps are `long`, but the records and Bronze carry ISO-8601 strings;
  - `subscriber.avsc` describes Silver's `dim_subscriber`, not a Bronze entity;
  - parties, accounts, agreements and products have no schema at all.

## What Changes

- **Kafka carries TMF events.** Each message is a TMF `<Resource>CreateEvent`
  envelope (`eventId`, `eventTime`, `eventType`, `event: {<resource>: {…}}`),
  holding the same resource the landing files hold. **BREAKING** for anything
  reading the old topics.
- **Topics are per TMF resource:**
  - `t.<tenant>.party`, which carries both Individual and Organization;
  - `t.<tenant>.customer`, which is log-compacted;
  - `t.<tenant>.billingAccount`, `.agreement`, `.product`,
    `.appliedCustomerBillingRate`, `.topupBalance` and `.productUsage` (voice and
    data together);
  - the existing `t.<tenant>.dlq`.

  They replace `parties`, `accounts`, `agreements`, `products`, `usage.voice`,
  `usage.data`, `recharge` and `fees`. **BREAKING**
- **The producer publishes TMF from both sources.**
  - In memory, it generates canonical data and writes it as TMF with the active
    mapping profile.
  - From landing, it publishes the landing resources as they are. The landing
    checks and rejection rules don't change.
  - Both sources send the same messages.
  - Customers are published before the resources that reference them.
- **The consumer runs the TMF adapter (REQ-BRZ-05).** It:
  - reads the compacted customer topic in full to build each tenant's Customer
    index, so a Customer published in an earlier run still resolves;
  - validates each event and its resource;
  - adapts the resource to canonical records, splitting usage into voice and data;
  - checks every record against the canonical Avro contract;
  - appends to Bronze as today.

  A message that fails a check goes to `t.<tenant>.dlq` with its reason. The
  valid ones are still landed, offsets are committed, and the consume exits with
  a failure status.
- **Two contracts:**
  - **Wire:** each topic's TMF JSON Schema (its `Idp<Resource>` event and
    resource, with their TMF bases) is registered in Apicurio as `<topic>-value`,
    with schema type JSON.
  - **Canonical:** `config/avro/` describes the canonical record of every Bronze
    entity, and the consumer enforces it. The schemas are corrected to match the
    real records, the four reference entities get schemas, and `subscriber.avsc`
    moves to `config/avro/silver/dim_subscriber.avsc` as Silver's documented
    contract. The Avro schemas are no longer registered as Kafka subjects.
- **Bronze and Silver are unchanged.** Bronze receives the same canonical records,
  so Silver, Gold and the golden data don't change.

## Capabilities

### New Capabilities

(none)

### Modified Capabilities

- `streaming-kafka`:
  - REQ-STR-01: TMF event envelopes on per-resource topics, a compacted customer
    topic, the same messages from both sources;
  - REQ-STR-02: the consumer adapts TMF to canonical records, resolves Customers
    from the compacted topic, enforces the canonical contract, and sends failures
    to the DLQ;
  - REQ-STR-03: TMF JSON Schemas as the registered wire contract; canonical Avro
    contracts for Bronze (enforced) and `dim_subscriber` (documented).
- `bronze-ingestion`:
  - REQ-BRZ-01: Bronze holds canonical records adapted from the source format,
    with no curation (it no longer says "without transformation");
  - REQ-BRZ-05: the adapter also runs per message in the consumer, against a
    Customer index, with the same results as adapting whole landing files.

## Impact

- **Code:**
  - `streaming/producer.py`: builds and publishes TMF events, and computes keys
    through the profile;
  - `streaming/consumer.py`: customer index, validation, adaptation, the
    contract check and the DLQ;
  - `streaming/__init__.py`: the topic map;
  - `ingestion/tmf/`: event envelopes, adapting one resource at a time, and the
    registry schema export;
  - `ingestion/landing.py`: a reader that yields validated resources per file;
  - `cli.py` (`produce`, `consume`).
- **Config:**
  - `config/avro/`: corrected and new canonical schemas, plus `silver/`;
  - `config/tmf/schemas/`: the vendored `CreateEvent` schemas.
- **Deploy:**
  - `deploy/platform/kafka/create-topics.sh`: the new topics, with
    `customer` compacted;
  - `register-schemas.sh`: TMF JSON Schemas instead of Avro;
  - the old KafkaTopic CRs are deleted at migration;
  - the producer and consumer Jobs are unchanged.
- **Dependency:** `fastavro`, for the canonical contract check.
- **Docs:** README (streaming section, topics, contracts), `deploy/README.md`,
  `deploy/platform/kafka/README.md`, and ADR-0031 in the decision log, which
  supersedes ADR-0013's wire-contract part.
- **Data:** Bronze, Silver and Gold are unchanged. Messages on the old topics are
  abandoned at migration.
- **Out of scope:**
  - replaying DLQ messages;
  - storing event ids in Bronze;
  - Silver choosing the latest version of a party (the follow-up from change 1).
