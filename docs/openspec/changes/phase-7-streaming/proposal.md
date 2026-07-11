# Phase 7 — Streaming (Kafka) Bridge

## Why
Feed the lakehouse through Kafka so ingestion matches the target streaming
architecture, while keeping the consumer's job to Bronze only (ADR-0009).

## What Changes
- Implement `streaming/producer.py` (publish to `t.<tenant>.<entity>`, per-tenant,
  keyed) and `streaming/consumer.py` (drain topics → per-entity frames; read-only).
- Wire `idp produce` (per-tenant generate+publish) and `idp consume` (drain →
  `to_bronze` only; exit cleanly on empty).
- Add Avro schemas + registry registration; topic naming `t.<tenant>.<entity>`.

## Impact
- Spec deltas: `streaming-kafka` (ADDED all requirements).
- Code: `src/idp/streaming/*`, `src/idp/cli.py` (`produce`, `consume`),
  `config/avro/*`.
- Decisions: ADR-0003, ADR-0009, ADR-0013.

## Spec Deltas
Satisfies every requirement in `specs/streaming-kafka`.
