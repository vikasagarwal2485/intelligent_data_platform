# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-STR-02 — Consumer produces Bronze only
The consumer SHALL drain the `t.<tenant>.<entity>` topics, group messages back
into per-entity frames, and append them to the Iceberg Bronze tables
(REQ-BRZ-01). It SHALL NOT perform Silver or Gold work, and SHALL exit cleanly when
topics are empty. Each consumed batch SHALL become one Bronze append (one Iceberg
snapshot) per entity.

#### Scenario: SCN-STR-02.1 — Read-only consume then Bronze
- **WHEN** `idp consume` runs after the producer completes
- **THEN** it appends the consumed records to the `bronze.<entity>` Iceberg tables,
  stored under `s3://idp-bronze`, and produces no Silver or Gold

#### Scenario: SCN-STR-02.2 — Empty topics
- **WHEN** `idp consume` runs with no messages available
- **THEN** it reports nothing consumed and exits successfully (no error), and no
  Bronze snapshot is created
