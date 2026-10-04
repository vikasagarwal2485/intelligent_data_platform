# Current handoff

Updated: 2026-10-04. Baseline inspected: `0be5efe` (`silver-performance-and-customer-topic`).

## Repository state

- Package metadata and `idp.__version__` both report `0.1.0`; no release tags were
  present at inspection. Notes distinguish this development baseline from a release.
- `openspec list --json` returned no active changes. Phases 0–15 and subsequent
  improvements are archived; requirements remain in `openspec/specs/`.
- The latest accepted ADR is ADR-0037, on-demand fraud anomaly normalization.
- Workspace work adds repository instructions, operator memory, release notes,
  Python/editor defaults and shared workspace checks; CI gains PostgreSQL coverage
  and strict Trino availability checks. No cluster operation is part of this work.
- Fresh dependency installation exposed MCP 2.x's removed `FastMCP` API; both
  dependency manifests now require `mcp>=1.2,<2`. Make selects the same Python for
  Spark drivers and workers, avoiding system/venv minor-version mismatches.
- The Trino Bronze-denial test now targets the seeded raw `bronze.tmf_party`
  table; the retired `bronze.parties` returned TABLE_NOT_FOUND rather than proving
  access denial. Onboarding paths and runtime descriptions are current.

## Validation

Verified locally on Python 3.11.15 and JDK 17.0.19 with the editable `.[dev]`
installation. Spark used loopback networking, matching driver/worker Python and
a temporary writable JVM cache in the restricted agent environment.

| Check | Observed result |
|---|---|
| `make check` | Passed: dependency health, Python syntax, metadata consistency, 126 config/shell files and 11 onboarding/memory documents |
| `actionlint .github/workflows/ci.yml` | Passed using checksum-verified actionlint 1.7.12 |
| `docker compose -f deploy/dev/trino-compose.yaml config --quiet` | Passed |
| `.venv/bin/idp --help` and MCP server construction | Passed; CLI available and v1 `FastMCP` server constructed |
| `.venv/bin/python -m pytest -x --tb=short` | 424 passed, 1 skipped, 42 deselected; the skip was the PostgreSQL test without its URL, Trino was intentionally deselected |
| `pytest -m postgres tests/test_idempotent_writes.py` with `IDP_TEST_POSTGRES_URL` | 1 passed, 15 deselected against the disposable stack's PostgreSQL 16 |
| `IDP_REQUIRE_TRINO_TESTS=1 make mcp-trino-test` | 42 passed against real Trino 480, MinIO and PostgreSQL |
| Trino availability guard | Unconfigured stack skips locally and fails with the strict CI setting, as intended |
| `git diff --check` | Passed |

The disposable integration containers, anonymous volumes and network were removed
after verification. The local `.venv` remains available and is ignored by Git.
This handoff records local validation before publishing the workspace setup on
`0.1.0`. Check Git and hosted CI for the current publication status.

## Follow-up

Inspect the three hosted CI jobs for the setup commit on `0.1.0`. Branch protection
configuration and release publication are separate administrator actions.
Recheck this handoff against the current worktree at the next session.
