# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-ORC-01 — On-demand stage jobs
The system SHALL provide independent, runnable jobs for each stage. Each reads its
input and writes its output, and each is exposed as an `idp` CLI command and as a
Kubernetes Job in `idp-app`:

- `idp generate`: synthetic source files → landing zone (REQ-BRZ-03)
- `idp produce`: synthetic data → Kafka; with `--from-landing`, the landing files →
  Kafka (REQ-STR-01)
- `idp consume`: Kafka → Bronze
- `idp silver`: incremental; `--full-refresh` rebuilds from all of Bronze
- `idp gold`: features only
- `idp score`: Gold features + models → score tables
- `idp publish`: Gold → TimescaleDB serving
- `idp labels`: seed the labels store
- `idp train`
- `idp maintain`: lake maintenance (REQ-ORC-06)

The Silver and Gold stages SHALL run on the single transformation engine
(REQ-ORC-05), whether started from the CLI, a Kubernetes Job or Airflow.

Streaming ingestion SHALL be runnable end to end with one command, in two variants:
- **`make stream`:** in-memory `idp produce`, then `idp consume`
- **`make stream-from-landing`:** `idp generate` (landing zone), then
  `idp produce --from-landing`, then `idp consume`

Each variant SHALL run its steps as Jobs in `idp-app`, in order, starting a step
only after the previous one has completed, and SHALL stop with a failure at the
first failed step. `make stream-replay` SHALL re-publish the existing landing files
and consume them, without regenerating.

#### Scenario: SCN-ORC-01.1 — Independent Silver run
- **WHEN** `idp silver` runs against a populated Bronze
- **THEN** it produces Silver on `s3://idp-silver` without touching Gold

#### Scenario: SCN-ORC-01.2 — Independent Gold run
- **WHEN** `idp gold` runs against a populated Silver
- **THEN** it produces Gold feature tables on `s3://idp-gold`

#### Scenario: SCN-ORC-01.3 — Score without a model is not an error
- **WHEN** `idp score` runs before any model has been trained
- **THEN** it writes no score tables, tells the operator to run `idp train`, and
  exits successfully

#### Scenario: SCN-ORC-01.5 — In-memory streaming unchanged
- **WHEN** `make stream` runs
- **THEN** it produces the generated data to Kafka in memory and consumes it into
  Bronze, without reading or writing the landing zone

#### Scenario: SCN-ORC-01.6 — Streaming from the landing zone
- **WHEN** `make stream-from-landing` runs
- **THEN** it writes the landing files to `s3://idp-landing`, publishes them to
  Kafka, then consumes them into Bronze, in that order; Bronze holds exactly the
  landing records, per tenant and entity

#### Scenario: SCN-ORC-01.7 — A rejected landing file stops the chain
- **WHEN** the landing zone holds a file whose records don't match its tenant, and
  `make stream-from-landing` or `make stream-replay` runs
- **THEN** the produce step fails and reports the file, consume doesn't run, and
  the command exits with a failure status
