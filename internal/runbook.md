# Workspace and operator runbook

## Local setup

Work from the repository root. Python 3.11 and JDK 17 match CI and the application
image. Ensure `java -version` resolves to 17 and `JAVA_HOME`, when set, points to
that JDK. Python 3.10+ is declared, but 3.11 is the verified workspace baseline.

```bash
make install PYTHON=python3.11
make check
make test
.venv/bin/idp --help
```

`PYTHON` only selects the interpreter when the venv is created. For an existing
venv, check `.venv/bin/python --version` before reusing it. `VENV` is overridable;
`PY` and `PIP` follow its location. For another already-installed environment use
`make check PY=python` and `make test PY=python`.

The first Spark run fetches Iceberg/JDBC jars from Maven into `~/.ivy2` unless
they are bundled in the image. Package install needs PyPI access. In a restricted
agent environment, use approved network access and a writable package cache.
Do not silently change dependency pins to work around network failures.

Tests use temporary lakes and Spark `local[2]`. If the host cannot resolve its
own name, set `SPARK_LOCAL_IP=127.0.0.1`. Avoid inherited `IDP_CATALOG_*`,
`IDP_S3_*` or `IDP_MLFLOW_*` overrides from a live environment during local tests.
Make exports the selected Python for Spark workers. For direct CLI/pytest use,
activate `.venv` first or set `PYSPARK_PYTHON` to that interpreter so a different
system Python is not selected by the workers.

## Check coverage

| Command | What it proves |
|---|---|
| `make check` | Installed dependencies, Python syntax, YAML/JSON/Avro JSON and shell syntax, synchronized dependency/version metadata, onboarding/memory links |
| `make test` | Default pytest suite, including local Spark, Iceberg, golden outputs, tenant/privacy guards, models, API and DAG/manifests; excludes Trino |
| `IDP_TEST_POSTGRES_URL=<disposable URL> make test` | Also exercises real Spark JDBC publishing and PostgreSQL atomic swap; otherwise that test skips |
| `make mcp-trino-test` | Real Trino/MinIO/PostgreSQL stack, tool parity, tenant pushdown, bounded results and read-only identity |
| `openspec list --json` | Current active-change inventory, when the OpenSpec CLI is installed |

The workspace checker reads tracked and unignored new files and starts no
services. Syntax checks do not prove a Kubernetes deployment succeeds. Golden
fixtures are deliberate regression baselines; do not refresh them to hide failures.

## Continuous integration

[.github/workflows/ci.yml](../.github/workflows/ci.yml) runs on pushes to branches,
pull requests and manual dispatch. It uses Python from `.python-version`, JDK 17
for Spark, read-only repository permissions, bounded job timeouts and cancellation
of superseded runs on the same branch or PR.

Three jobs run independently:

1. **Workspace checks** installs only the check dependencies, runs `make check`,
   whitespace validation and Docker Compose configuration validation.
2. **Tests and PostgreSQL publish** installs `.[dev]`, checks dependency health and
   runs the default suite with a healthy PostgreSQL 16 service. This activates the
   real JDBC publish test that normally skips locally.
3. **MCP Trino integration** installs `.[dev]` and starts the Compose test stack.
   `IDP_REQUIRE_TRINO_TESTS=1` turns unavailable/misconfigured Trino into failure.
   Failure logs include all services; cleanup always removes this disposable stack.

No deployment or release publication is performed by CI. Branch protection must
be configured by a repository administrator if these jobs are to block merges.
Local validation is separate from a hosted workflow run; record both accurately.

## Disposable integration stack

Docker must be running. The Compose stack binds PostgreSQL `15432`, MinIO `19000`
and Trino `18080`, separate from cluster port-forwards. `make mcp-trino-test`
starts it with a 300-second health wait and sets the correct `IDP_*` variables.
The integration fixture wipes only the stack identified by those addresses.

```bash
make mcp-trino-test
docker compose -f deploy/dev/trino-compose.yaml logs --tail 200
make trino-stack-down
```

## Cluster diagnosis and migration reminders

Check `kubectl config current-context` and the target environment before actions.
The current deployment steps are in [README.md](../README.md) and
[deploy/README.md](../deploy/README.md). Useful inspection commands include
`make lake-catalog-show`, `make lake-table-show TABLE=gold.credit_features`, and
`kubectl -n idp-app get sparkapplications,pods`. Bronze and landing inspection
can expose raw PII; keep outputs out of memory and shared reports.

- Changing `spark.profiles` requires `make spark-manifests` and the manifest tests.
  Rebuild the application and Airflow DAG images when their sources change.
- Existing Silver streaming checkpoints retain their original shuffle partition
  count. An authorized `make silver-full-refresh` rebuilds facts/quarantine from
  Bronze to adopt the changed count; it is not a routine verification command.
- `make topics` applies the customer topic's delete/retention settings. Do not
  purge topics to update this policy.
- Raw-Bronze migration and `customer_tenure_days` require compatible data and
  retrained models. Old models without `anomaly_range` need retraining for useful
  on-demand anomaly contributions. See [release notes](../RELEASE_NOTES.md).
- An empty MLflow champion yields an unavailable scoring model; inspect quality
  gates and train with labels instead of bypassing promotion.
- A MinIO listing alone cannot diagnose versioned delete markers. Catalog rows
  can also point at missing objects. Inspect with `make minio-versions` and
  `make lake-catalog-show` before any repair.
- `make minio-reset`, `make purge`, full refresh and cluster deletion change or
  erase state. Obtain authorization for the actual environment before using them.

## Release notes and handoff

Add meaningful changes under **Unreleased** in
[RELEASE_NOTES.md](../RELEASE_NOTES.md), including compatibility and operator steps.
For an actual release, reconcile `pyproject.toml` and `idp.__version__`, record the
date and exact commit/tag, and move the reviewed notes into that release section.
Do not imply that package version `0.1.0` was tagged or published.

Update [status.md](status.md) after material work with the date, commands, outcomes
and outstanding work. Keep credentials and ephemeral machine paths out of Git.
