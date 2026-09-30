# Tasks

## 1. Remove the Spark-stage Jobs

- [x] 1.1 Delete `deploy/app/jobs/silver-job.yaml`, `gold-job.yaml`, `score-job.yaml`, `publish-job.yaml`, `maintain-job.yaml` and `train-job.yaml` (as unstaged deletions). Verify with a new test (SCN-ORC-01.9):
  - no file under `deploy/app/jobs/` runs a Spark stage;
  - the remaining Job manifests are exactly those the Makefile applies (generate, produce, produce-from-landing, consume, labels, migrate-lake);
  - every Spark stage has its rendered `deploy/app/spark/<stage>.yaml`.

## 2. Docs

- [x] 2.1 Update `README.md`:
  - the cluster walkthrough: `idp-silver`/`idp-gold` are SparkApplications from `make silver`/`make gold`;
  - the "Verify" block: driver logs via `kubectl -n idp-app logs idp-<stage>-driver` instead of `logs job/idp-silver|gold`;
  - the stale "Gold also emits scores" line: scores come from `make score`;
  - the layout tree's `jobs/` line.
- [x] 2.2 Update `deploy/README.md`:
  - the layout tree's `jobs/` line;
  - the Spark "Rollback" note: rollback is through version control; there are no single-pod Jobs for the Spark stages.

Verify both by reading, and with `grep -rn -E "(silver|gold|score|publish|maintain|train)-job\.yaml|logs job/idp-(silver|gold|score|publish|train|maintain)"` over the README files, Makefile and deploy/ returning nothing (archived changes and the decision log are history and stay).

## 3. Verification

- [x] 3.1 `pytest -q` (the default job) is green, and `make -n silver gold score publish maintain train` still render and apply only SparkApplications (no `deploy/app/jobs/` path)
