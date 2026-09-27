# Tasks

## 1. Landing zone storage

- [x] 1.0 Before any code change, record a baseline: run `idp generate` with a temporary config (10 subscribers, 5 days) and save each landing file's path and SHA-256. Verify that two runs give the same listing (the output is deterministic)
- [x] 1.1 Add `storage.landing_uri(cfg)` (`s3://<prefix>-landing` with S3, else `cfg.storage.landing`). Verify with config/storage unit tests for both modes
- [x] 1.2 `ingestion/landing.py`: make `write_landing(dataset, landing, *, storage_options=None)` write through fsspec with the same layout and bytes, and have `idp generate` generate and write one tenant at a time to `landing_uri(cfg)`. Verify that the baseline from 1.0 is byte-identical, that SCN-BRZ-03.1/03.2 tests pass, and SCN-BRZ-03.3 with an S3-shaped test (moto or an fsspec memory filesystem standing in for `s3://idp-landing`)
- [x] 1.3 Add `read_landing(landing, tenants, *, storage_options=None)`: sorted tenant folders, entities in `TABLE_TO_SUFFIX` order, one file at a time, rejecting unknown tenant / unknown entity / invalid JSON / tenant mismatch with a reason. Verify with unit tests for every rejection reason and an accepted round trip

## 2. Producer

- [x] 2.1 `streaming/producer.py`: add `publish_records(items, bootstrap, *, topic_prefix, acks)` holding the Kafka client, topic, key, flush and close logic, and make `produce_to_kafka` delegate to it. Verify that the existing `test_produce_to_kafka_…` test is unchanged and green
- [x] 2.2 `idp produce --from-landing`: read `landing_uri(cfg)` for the configured tenants, publish accepted files one tenant at a time, print per-tenant counts and the rejected files, and exit 1 if any file was rejected or no tenant folder exists. Verify with a fake `KafkaProducer`: SCN-STR-01.3 (the `(topic, key, serialized value)` sequences equal those of in-memory production for the same generated data), SCN-STR-01.4 and SCN-STR-01.5 (nothing from a rejected file is sent, other files are sent, exit status 1)
- [x] 2.3 End-to-end without a broker: `idp generate`, then `produce --from-landing` into a fake Kafka that captures the messages, then `consume_to_bronze` from those messages (fake consumer) into a temporary lake. Verify SCN-BRZ-03.4 (Bronze row counts per tenant and entity equal the landing files)

## 3. Cluster

- [x] 3.1 Add the `idp-landing` bucket to `install-infra.sh`, `make minio-mkbuckets` and `make minio-reset` (and `minio-ls`/`minio-versions` listings). Verify on minikube with `make minio-mkbuckets` twice (idempotent, SCN-PLT-02.2)
- [x] 3.2 Add `deploy/app/jobs/generate-job.yaml` (S3 env, `IDP_SUBSCRIBERS=150`, `IDP_DAYS=14`, produce-job resources) and `deploy/app/jobs/produce-from-landing-job.yaml` (`idp produce --from-landing`, Kafka and S3 env), leaving `produce-job.yaml` unchanged, and add the "keep in lock-step" comment to the generate, produce and labels Jobs. Verify with `kubectl apply --dry-run=server` for both new Jobs
- [x] 3.3 Add `make stream-from-landing` (delete old Jobs, then generate → wait → produce-from-landing → wait → consume → wait, printing each summary; stops at the first failed step), `make generate-job` and `make stream-replay` (produce-from-landing → consume). Keep `make stream` as is, and note in both help texts that they must not run concurrently. Verify with `make -n stream-from-landing` (the order, and that no step runs after a failed wait) and that `make stream`'s recipe is unchanged in `git diff`
- [x] 3.4 On minikube: `make image`, `make stream-from-landing`, then `make silver`. Verify SCN-ORC-01.6: the landing files are in `s3://idp-landing`, the produce log shows no rejected files, consume lands the same per-entity counts as the landing files, and Silver's row counts are unchanged after the MERGE (the same deterministic data). Then run `make stream` and verify SCN-ORC-01.5 (it still works in memory, and Silver counts are again unchanged)
- [x] 3.5 Failure path on minikube: put a tenant-mismatched file into `s3://idp-landing` and run `make stream-replay`. Verify SCN-ORC-01.7: the produce Job fails with the rejected file in its log, consume doesn't start, and `make` exits non-zero. Then remove the file

## 4. Docs and verification

- [x] 4.1 Update `README.md` (streaming section: the two ways to stream, `make stream` in-memory and `make stream-from-landing` source files → landing → Kafka → Bronze; how to inspect or replay the landing zone) and `deploy/README.md` (the bucket, the three-step `make stream-from-landing`, `make stream-replay`, why a rejected file stops the chain, and not running the two targets concurrently). Verify by reading
- [x] 4.2 `pytest -q` is green — 213 passed (2026-09-27)
