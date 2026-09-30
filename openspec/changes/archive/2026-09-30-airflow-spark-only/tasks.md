# Tasks

## 1. Train as a Spark stage

- [x] 1.1 Add `train` to `sparkapp.SPARK_STAGES`, create `pipelines/spark/train_job.py` (a thin wrapper over `idp train`, like `score_job.py`), and give `spark.profiles.train` one executor (`instances: 1`, 1 core, `1g` + `512m`; driver unchanged at `2g` + `1g`). Verify that `idp spark-manifest train` renders a SparkApplication with `mainApplicationFile: local:///app/pipelines/spark/train_job.py`, one executor and the lake + MLflow env, and that `stage_memory_mib(cfg, "train")` totals 4.5 GiB
- [x] 1.2 `make spark-manifests` renders `deploy/app/spark/train.yaml`, and `make train` becomes `mk-check` + `run_sparkapp,train` (driver log printed, registry summary kept). Keep `deploy/app/jobs/train-job.yaml` for rollback, with a comment that `make train` no longer uses it. Verify with `tests/test_sparkapp.py` (committed manifests match the profiles for every stage, including `train`) and `make -n train`

## 2. Airflow submits only Spark applications

- [x] 2.1 Make the training DAG `train` only, as `_spark_stage("train", "train")` with `outlets = [MODEL]`. Remove `KubernetesPodOperator`, the `_stage` helper, `S3_ENV`/`CATALOG_ENV`/`MLFLOW_ENV`/`LAKE_ENV`, `SYN_ENV` and `TRAIN_RESOURCES`, and update the module docstring (four DAGs, Spark applications only, labels seeded outside Airflow). Verify with `tests/test_dags.py`: the Spark tasks are {silver, gold, score, publish, maintain, train}; every task in every DAG is a Spark application with a committed manifest (SCN-ORC-02.8); `idp_training` has exactly `train`, which outputs the model dataset; the pod-env test is removed
- [x] 2.2 Update `deploy/platform/airflow/rbac.yaml`: delete the `idp-pod-launcher` Role and RoleBinding, and add pods (get, list, watch) and `pods/log` (get) to `idp-spark-submitter`. Verify with a test that the manifest grants no pod `create`/`delete`/`exec`, and on minikube with `kubectl auth can-i` as `airflow-worker` and `airflow-scheduler`: `create pods -n idp-app` → no; `create sparkapplications` and `get pods/log` → yes (SCN-PLT-10.3)

## 3. Docs

- [x] 3.1 Update `README.md` (the run order: `make labels` seeds once, `make train` runs as a SparkApplication; the DAG list with `idp_training` = `train`; the Spark stages list) and `deploy/README.md` (Spark stages, Airflow RBAC, the training DAG no longer seeds labels). Verify by reading, and that no doc still says the training DAG runs `labels >> train` or that Airflow launches pods

## 4. Verification

- [x] 4.1 `pytest -q` (the default job) is green
- [x] 4.2 On minikube, with the DAGs paused:
  - `make image` and `make spark-manifests`, then apply the RBAC and delete the old Role/RoleBinding, then `make airflow-reload`;
  - `make mk-check` passes, and `make train` completes as a SparkApplication with one driver and one executor pod; MLflow shows a new run and versions, and the gate behaves as before (SCN-ORC-01.8);
  - trigger `idp_training` once: `train` completes, the model dataset triggers `idp_inference`, and its `score` and `publish` complete (SCN-ORC-02.1);
  - with the labels store emptied in a scratch prefix (or a fresh lake), `train` fails with the seed-labels message and inference isn't triggered (SCN-ORC-02.7);
  - afterwards, re-pause any DAGs that were paused before
