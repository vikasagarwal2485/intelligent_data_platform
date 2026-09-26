# Spec Delta

## MODIFIED Requirements

### Requirement: REQ-SLV-04 — Standalone stage reading Bronze
The Silver stage SHALL always read Bronze from the lake: object storage when
configured, otherwise the local lake directory. It SHALL NOT accept in-memory
input. It SHALL write Silver partitioned by `tenant_id`, and SHALL fail clearly if
required entity tables are missing.

#### Scenario: SCN-SLV-04.1 — Missing Bronze entities
- **WHEN** the Silver stage runs but `parties`/`accounts`/`agreements` are absent
  or empty in Bronze
- **THEN** it raises a clear error instructing to populate Bronze first

#### Scenario: SCN-SLV-04.2 — Silver reads the Bronze the landing wrote
- **WHEN** Bronze has been landed (by `idp consume` or the local `run`) and the
  Silver stage then runs as a separate process
- **THEN** it builds Silver from exactly that Bronze, read from the configured lake
  location
