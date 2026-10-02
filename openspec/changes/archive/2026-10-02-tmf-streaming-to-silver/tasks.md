# Tasks

## 1. Contracts

- [x] 1.1 Vendor the TMF create-event schemas. Add each resource's `<Resource>CreateEvent` (and its payload) as an extra root in `config/tmf/vendor_schemas.py`, from the same pinned spec files, and re-vendor. Where a pinned spec defines none, add a platform `<Resource>CreateEvent` on TMF's generic `Event` shape in `config/tmf/extensions/` (design §1), and record which resources needed one. Verify with a test that every topic's event schema resolves locally and that a sample envelope validates.
- [x] 1.2 Rewrite the canonical Avro contract (design §5):
  - add `party`, `account`, `agreement` and `product` schemas;
  - fix `usage_event` (`net_amount`, string timestamps), `recharge_event` and `fee_charge`;
  - move `subscriber.avsc` to `config/avro/silver/dim_subscriber.avsc`, matching `DIM_COLUMNS`;
  - add `fastavro` to `requirements.txt` and `pyproject.toml`.

  Verify SCN-STR-03.3 with a test: every entity has a schema whose fields equal the generator's columns in order, and every record of a generated two-tenant dataset validates. Verify SCN-STR-03.4 by comparing a Silver run's `dim_subscriber` columns with the Silver schema.
- [x] 1.3 Add `schemas.registry_schema(topic)` and `idp registry-schemas --out DIR`. Each schema is self-contained draft-07, pruned to the definitions it reaches, and `party` accepts both events. Verify with a test that each exported schema accepts a generated event of its topic, rejects one of another topic, and is under 100 KB.

## 2. TMF adapter per resource

- [x] 2.1 Refactor `from_tmf` into `adapt_resource(profile, resource, customers)` plus a whole-folder loop, keeping its sort and error behaviour. Add `customer_index(profile, customers)` for the consumer. Verify SCN-BRZ-05.5: adapting a generated dataset one resource at a time equals `from_tmf` on its files. Also verify that the existing round-trip and reference tests (SCN-BRZ-05.1–05.4) still pass.
- [x] 2.2 Add the event envelope helpers in `ingestion/tmf/`:
  - `to_event(resource, tenant)`, with the stable `uuid5` `eventId` and the publish-time `eventTime`;
  - `from_event(value, topic)`, which checks the envelope against the event schema and the event type against the topic, and returns the resource.

  Verify SCN-STR-01.8 (same resource, same `eventId`), and that a mismatched event type or a broken envelope is rejected with a reason.

## 3. Producer

- [x] 3.1 Replace `TABLE_TO_SUFFIX` with the resource-to-topic map (`party` for Individual and Organization), and update `streaming/__init__.py` and its users. Verify SCN-STR-01.1, 01.2 and 01.7 with the topic-mapping tests.
- [x] 3.2 Make the publishing path send TMF events:
  - keys read through the profile's `service_id`/`msisdn` mapping (customers by id);
  - order: customer first, flushing after it.

  Add `read_landing_resources`, which reuses change 1's checks and rejection messages. Make in-memory production go through `to_tmf` and validation, sharing the function `write_landing` uses. Verify:
  - SCN-STR-01.3: both sources send the same messages apart from `eventTime`;
  - SCN-STR-01.4–01.6: the existing rejection tests, adapted;
  - that keys equal today's natural keys.

## 4. Consumer

- [x] 4.1 Read each tenant's customer topic in full (assign, seek to beginning, read to the end offsets taken at start, keep the last event per id) to build the customer index. Report invalid customers. Verify with fake-Kafka tests that a later event for the same id replaces the earlier one, and that an invalid customer is reported and left out.
- [x] 4.2 Rewrite `consume_to_bronze`:
  - per message: decode, check the envelope, validate the resource, check `tenantId` against the topic, adapt, then check the Avro contract;
  - keep only canonical rows and dead-letter entries;
  - then land, write the DLQ (original key and value, with the `idp.reason` and `idp.source.*` headers), and commit.

  In `idp consume`, report the DLQ count and exit 1 when it's above zero. Verify:
  - SCN-STR-02.3: TMF in, the same canonical frames out, and no customer table;
  - SCN-STR-02.4: a customer from an earlier run resolves;
  - SCN-STR-02.5 and 02.6: an unresolvable reference and a contract violation are dead-lettered;
  - the existing tests that offsets are committed only after landing, a failed landing commits nothing, and empty topics create no snapshot.
- [x] 4.3 Verify end to end without a cluster (SCN-BRZ-03.4): generate, produce from landing with a fake producer, feed its messages to the consumer, and append to a local Iceberg Bronze. Bronze must equal the generated canonical records.

## 5. Kafka deployment and docs

- [x] 5.1 Update `create-topics.sh`:
  - the nine new suffixes plus `dlq`;
  - `customer` with `cleanup.policy: compact,delete` and unlimited retention;
  - CR names lower-cased with `tr`.

  Make `purge-topics.sh` handle the compacted topic (possible as is, since its policy includes `delete`). Update `register-schemas.sh` to post the `idp registry-schemas` output as JSON subjects, and `make schemas`. Verify with `bash -n` and by reading the rendered CRs (`kubectl apply --dry-run=client`).
- [x] 5.2 Update the docs:
  - `README.md`: the streaming section, topics, message example, contracts and DLQ;
  - `deploy/README.md`;
  - `deploy/platform/kafka/README.md`;
  - the CLI help for `produce`, `consume` and `registry-schemas`;
  - ADR-0031 in `docs/decisions/decision-log.md`, marking ADR-0013's wire-contract part as superseded.

  Verify by reading.

## 6. Verification

- [x] 6.1 `pytest -q` (default job) is green, and the golden tests are unchanged.
- [x] 6.2 On minikube:
  - run `make image` and `make topics`;
  - delete the 16 old KafkaTopic CRs, after confirmation;
  - run `make schemas` and check that the subjects in Apicurio are JSON;
  - run `make stream-from-landing`: produce and consume must report no rejections and no DLQ messages, and Bronze row counts must equal the generated counts;
  - run `make silver` and check that the fact counts match Bronze;
  - check that `t.tenant_a.customer` is compacted (topic config);
  - spot-check one message per topic with Kafka UI or `kafka-console-consumer`;
  - record the consume duration.

  Keep the DAGs paused.
