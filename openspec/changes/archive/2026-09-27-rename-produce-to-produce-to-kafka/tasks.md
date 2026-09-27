# Tasks

## 1. Rename

- [x] 1.1 Rename `produce` to `produce_to_kafka` in `src/idp/streaming/producer.py` (signature, docstring and behaviour unchanged), and update the re-export and `__all__` in `src/idp/streaming/__init__.py`. Verify that `python -c "from idp.streaming import produce_to_kafka, consume_to_bronze"` succeeds and `from idp.streaming import produce` raises `ImportError`
- [x] 1.2 Update `src/idp/cli.py` (`_cmd_produce`: import and call `produce_to_kafka`), leaving the `idp produce` command unchanged. Verify that `idp produce --help` works and that a repository-wide search for the old function name (`\bproduce\(` / `import produce\b`, excluding Kafka's `KafkaProducer.send`) finds no remaining references in `src`, `tests`, `pipelines` and the docs

## 2. Verification

- [x] 2.1 Add a test to `tests/test_streaming.py` that `produce_to_kafka` publishes each record to `t.<tenant>.<suffix>` with the natural key, using a fake `KafkaProducer` (monkeypatched, no broker); this pins the renamed entry point and REQ-STR-01. Verify that `pytest -q tests/test_streaming.py` is green
- [x] 2.2 `pytest -q` is green — 205 passed in 8m34s (2026-09-27)
