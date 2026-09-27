# Proposal

## Why

The streaming bridge has two entry points in `idp.streaming`: `produce` and
`consume_to_bronze`. The consumer's name says where its data goes, but the
producer's doesn't. Read side by side, `produce` doesn't say that it publishes to
Kafka, or that it stops there: Bronze is written only by the consumer.

Naming the producer by its destination as well makes the flow readable from the
function names alone: `produce_to_kafka` → `consume_to_bronze`.

The name `produce_to_bronze` was considered and rejected. The producer never writes
the lake, so that name would suggest a path that doesn't exist.

## What Changes

- Rename `idp.streaming.producer.produce` to `produce_to_kafka`. The signature,
  arguments, return value and behaviour are unchanged.
- Update its references:
  - the re-export in `idp.streaming` (`from … import produce_to_kafka` and
    `__all__`)
  - the import and call in `idp.cli` (`_cmd_produce`)
- The old name `produce` is removed, with no alias. It's used only inside this
  repository (the package isn't published), so no external caller breaks.
- **Unchanged:**
  - the CLI command `idp produce`
  - the `make stream` target
  - the `idp-produce` Kubernetes Job (`deploy/app/jobs/produce-job.yaml`)
  - the Airflow DAGs
  - the documentation of those, which names the command, not the function

## Capabilities

### New Capabilities

None.

### Modified Capabilities

None. REQ-STR-01 specifies what "the producer" publishes and never names the
function, so no requirement changes. This is a pure refactor, and the change sets
`skip_specs: true`.

## Impact

- **Code:** `src/idp/streaming/producer.py`, `src/idp/streaming/__init__.py`,
  `src/idp/cli.py`.
- **Tests:** none reference `produce` by name (`tests/test_streaming.py` imports
  only the private `_topic` helper). The suite must stay green.
- **Docs:** only prose that names the Python function, if any. The README and
  deploy docs describe the `idp produce` command, which keeps its name.
- **Behaviour, config, deploy, specs:** none.
