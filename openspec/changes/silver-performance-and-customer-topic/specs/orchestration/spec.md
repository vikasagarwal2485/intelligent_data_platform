# Orchestration delta

## MODIFIED Requirements

### Requirement: REQ-ORC-07 — Resource profiles and scale-out
Each Spark stage SHALL run with an explicit resource profile from configuration:
- driver cores, memory and memory overhead
- executor cores, memory and memory overhead
- either a fixed executor count or dynamic-allocation bounds
- its shuffle parallelism (`shuffle_partitions`), by default twice its executor
  cores (instances × cores), or twice the local cores when it runs locally

Every shuffle of the stage, the streaming micro-batches included, SHALL use the
profile's shuffle parallelism. A stage SHALL spread its row-wise Python work (such
as Silver's conform step) over all its executor cores.

Every driver and executor container's memory limit SHALL cover its JVM heap plus
the declared overhead. A stage's capacity SHALL grow by adding executors, without
changing code. Overlapping runs of different stages SHALL NOT exceed their
containers' limits.

Each stage's application manifest SHALL be rendered from its profile into a single
committed copy (`deploy/app/spark/<stage>.yaml`). Both the manual `make` runs and
the Airflow tasks SHALL submit that copy; the Airflow DAG image SHALL take it from
there at build time rather than keep its own.

#### Scenario: SCN-ORC-07.1 — Profile applied
- **WHEN** the Gold profile sets 3 executors with 2 GiB memory and 512 MiB overhead
  each
- **THEN** the Gold application runs 3 executor pods whose memory limits are at
  least 2.5 GiB, and the driver uses its own profile

#### Scenario: SCN-ORC-07.2 — More data, more executors
- **WHEN** the lake holds 10× the subscribers and the Silver profile's executor
  count is raised
- **THEN** Silver completes without any container being OOMKilled, with no code
  change

#### Scenario: SCN-ORC-07.3 — Overlapping stages stay within limits
- **WHEN** a Silver application and a maintenance application run at the same time
- **THEN** both complete and no container is OOMKilled

#### Scenario: SCN-ORC-07.4 — One manifest for make and Airflow
- **WHEN** a stage's profile changes and the manifests are re-rendered
  (`make spark-manifests`)
- **THEN** only `deploy/app/spark/<stage>.yaml` changes, it matches the profile, and
  the next DAG image build (`make airflow-reload`) submits that same manifest

#### Scenario: SCN-ORC-07.5 — Shuffle parallelism follows the profile
- **WHEN** Silver runs with a profile of 2 executors of 1 core each, after a full
  refresh
- **THEN** its shuffle stages, the streaming fact merges included, run 4 tasks rather
  than Spark's default 200, and conforming a fact batch runs on both executors

#### Scenario: SCN-ORC-07.6 — Explicit override
- **WHEN** a profile sets `shuffle_partitions: 16`
- **THEN** the stage's shuffles use 16 partitions

