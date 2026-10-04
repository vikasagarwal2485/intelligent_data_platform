# Repository instructions

## Start here

Read [internal/README.md](internal/README.md), then
[internal/status.md](internal/status.md) and the relevant runbook before work.
Inspect `git status --short` and the current files; operator memory is a dated
aid, and must be checked against the code. Preserve unrelated user changes.

The current product guide is [README.md](README.md). Requirements live in
[openspec/specs/](openspec/specs/), active proposals in `openspec/changes/`, and
accepted history in `openspec/changes/archive/`. Consult the
[decision log](docs/decisions/decision-log.md) for architecture changes; later ADRs
can supersede earlier ones. The reviewed architecture plan includes target and
historical designs, so it does not by itself prove current behavior.

## Development

- Use Python 3.11 (the `.python-version` and CI baseline) and JDK 17. The package
  declares Python >=3.10; additional versions are not covered by this CI.
- Run commands from the repository root. `make install PYTHON=python3.11` creates
  `.venv` and installs `.[dev]`; set `PYTHON` to an installed interpreter if needed.
- `make check` verifies dependency health, Python syntax, configuration and shell
  syntax, dependency/version consistency and onboarding links.
- `make test` runs the suite, excluding the `trino` marker. PostgreSQL-specific
  tests need `IDP_TEST_POSTGRES_URL` pointing at a disposable database.
- `make mcp-trino-test` starts the isolated Docker Compose stack and runs real
  Trino tests. It wipes that stack's lake. Use `make trino-stack-down` only for
  that disposable stack. CI sets `IDP_REQUIRE_TRINO_TESTS=1` to reject skips caused
  by unavailable or misconfigured Trino.
- Prefer focused tests while iterating, then the relevant CI gates. Do not report
  skipped integrations or unexecuted GitHub workflows as passed.

## Implementation boundaries

- Every record carries `tenant_id`. Preserve tenant filters and fail-closed read
  guards; never use another tenant's data as a fallback.
- Bronze is append-only raw TM Forum event data. Silver does conformance and
  quarantines invalid records without raw PII in reasons. Keep the canonical and
  Silver contracts in `config/avro/` consistent with the TMF profiles and tests.
- Tokenize PII with each tenant's salt before Silver output. Gold features require
  consent. Labels are outside the lake and must never leak into feature inputs.
- Silver and Gold use the one Spark implementation in `src/idp/transforms/`.
  Stage functions exchange data through the Iceberg lake; pandas is for bounded
  ML workloads, reference tests and adapters. Do not add a second transform engine.
- The shared lake catalog is JDBC/SQL (SQLite locally, PostgreSQL in the cluster),
  not the provisioned Nessie service. Preserve snapshots and incremental checkpoints.
- `gold`, `train`, `score`, `publish` and `maintain` are separate stages. Score
  history appends; serving publishes the latest run with an atomic table swap.
- Airflow submits SparkApplications for all six Spark stages. Non-Spark Jobs stay
  outside Airflow. Render `deploy/app/spark/` with `make spark-manifests` when
  `spark.profiles` changes; keep the CLI, manifests and DAGs aligned.
- `deploy/platform/` owns shared services; `deploy/app/` owns application workloads.
  Check the Kubernetes context and target namespace before cluster commands.
- Keep runtime dependencies in `pyproject.toml` and `requirements.txt` synchronized,
  and the dev extra aligned with `requirements-dev.txt`. Align JVM jar pins with
  the Dockerfile and `src/idp/transforms/session.py`.
- Keep golden reference files independent of implementation changes. Regenerate
  them only when an intentional behavior change justifies a reviewed new baseline.

## Documentation and handoff

Update [RELEASE_NOTES.md](RELEASE_NOTES.md) under **Unreleased** for user-visible,
operational or workflow changes. Describe migration steps where required. A version
in package metadata is not evidence that a release or Git tag exists.

For a substantial requirement change, use the existing OpenSpec workflow: proposal,
spec deltas and tasks, implementation and verification, then archive. Repository
housekeeping follows the existing specs without requiring a new product proposal.
After spec/proposal edits regenerate traceability with `python docs/gen_traceability.py`.

Update `internal/` when you learn a durable fact or finish a material task. Include
the date, source paths, validation evidence and unresolved limits. Keep enduring
decisions in the ADR log and requirements in OpenSpec; link to them from memory.
Do not store credentials, access tokens, raw customer data or machine-specific
private paths. Do not invent successful validation or deployment status.

Default to repository files, terminal output and inline Markdown. Create external
artifacts only when explicitly requested. Cluster resets, lake resets and topic
purges require explicit authorization for the affected environment.

## Git commits

Use `<type>: <subject>` with no bracketed scope and no multiline body. Allowed
types: `feat`, `fix`, `perf`, `refactor`, `style`, `docs`, `test`, `version`,
`config`, `build`, `revert`, `other`. Never add a `Co-Authored-By: Codex` tag.
If the user supplies a Jira ticket, add an `Issue: KEY-123` footer after a blank
line. Commit or push only when requested.
