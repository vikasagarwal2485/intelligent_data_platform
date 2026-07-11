# Tasks — Phase 7: Streaming (Kafka) Bridge

## 1. Producer
- [ ] 1.1 `streaming/producer.py`: `produce(dataset, bootstrap)` → publish each
      table's records to `t.<tenant>.<suffix>` keyed by subscriber; return counts
- [ ] 1.2 `TABLE_TO_SUFFIX` / `SUFFIX_TO_TABLE` mapping (voice_events→usage.voice …)
- [ ] 1.3 `idp produce`: generate + publish PER TENANT (bound memory)

## 2. Consumer (Bronze only)
- [ ] 2.1 `streaming/consumer.py`: `consume_to_bronze(bootstrap, tenants, idle_timeout)`
      → drain topics into per-entity frames (read-only, stops on idle)
- [ ] 2.2 `idp consume`: call consumer then `to_bronze` only; on empty, report and
      exit 0; NEVER do Silver/Gold

## 3. Contracts
- [ ] 3.1 Ensure `config/avro/*.avsc` exist; provide registry registration mapping
      `<topic>-value` → schema files

## 4. Tests
- [ ] 4.1 Topic↔table mapping round-trip; (Silver/Gold stages already tested)
- [ ] 4.2 `make test` green

## Done when
`specs/streaming-kafka` scenarios pass.
