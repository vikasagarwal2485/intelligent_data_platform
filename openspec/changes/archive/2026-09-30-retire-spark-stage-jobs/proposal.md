# Proposal

## Why

The platform runs the same `idp` stage code in three tiers:
- `make run`: local, one process;
- `make <stage>`: individual stages on minikube;
- Airflow: production, also testable on minikube.

In both cluster tiers the Spark stages (silver, gold, score, publish, maintain,
train) now run only as Spark Operator applications rendered from
`spark.profiles.<stage>`. Their old single-pod Jobs (`deploy/app/jobs/silver-job.yaml`,
`gold-`, `score-`, `publish-`, `maintain-`, `train-job.yaml`) are no longer applied by
anything: no `make` target, DAG or test uses them.

They still cost something. They are hand-written copies of each stage's env and
resources, which drift from the rendered manifests: the MLflow env had to be added
to them by hand, `maintain-job.yaml` says it is applied by `make maintain` (it isn't),
and `publish-job.yaml` points at a path that no longer exists. The docs also still
send readers to them. README lists `idp-silver`/`idp-gold` as those Jobs, with
`kubectl logs job/idp-silver` commands, and `deploy/README.md` offers them as the
rollback.

A second, hand-maintained way to run the Spark stages in the cluster works against
the point of the minikube tier, which is to exercise the same execution model as
production.

## What Changes

- **Delete the six Spark-stage Jobs:** `silver-job.yaml`, `gold-job.yaml`,
  `score-job.yaml`, `publish-job.yaml`, `maintain-job.yaml` and `train-job.yaml`.
  Rollback to them is through version control, not files kept in the tree.
- **Keep the non-Spark Jobs, which `make` uses:** `generate`, `produce`,
  `produce-from-landing`, `consume`, `labels` and the one-off `migrate-lake`.
- **Fix the docs that point at the deleted Jobs:**
  - README's cluster walkthrough: stage descriptions and the "Verify" log commands;
  - README's stale "Gold also emits scores" line (scores come from the Score stage);
  - both layout trees;
  - `deploy/README.md`'s rollback note.
- **Spec:** REQ-ORC-01 states that in every cluster tier (manual `make` runs and
  Airflow) the Spark stages run only as Spark Operator applications from their
  rendered manifests. There is no alternate Job path for them.

Considered and left out: a sandboxed "`make run` against minikube" quick check
(separate bucket prefix and catalog name). It is a possible follow-up, not part of
this cleanup.

## Capabilities

### New Capabilities

None.

### Modified Capabilities

- `orchestration`: REQ-ORC-01 makes SparkApplications the only cluster execution
  path for the Spark stages, in both the manual `make` tier and Airflow, and lists
  which stages remain Kubernetes Jobs.

## Impact

- Deleted: the six `deploy/app/jobs/*-job.yaml` files above.
- Docs: `README.md`, `deploy/README.md`.
- Tests: a check that no Spark stage has a Job manifest, and that the remaining Jobs
  are exactly the non-Spark stages the Makefile applies.
- No code, image, DAG or cluster change. Nothing applies these files today, so
  deleting them changes no behaviour. A Job left in the cluster from an old manual
  apply would expire through its `ttlSecondsAfterFinished`.
