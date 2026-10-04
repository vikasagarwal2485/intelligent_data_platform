# Triggering the Spark stages and Airflow DAGs

All six Spark stages use the `idp` image and the manifests in
[../deploy/app/spark/](../deploy/app/spark/), generated from `spark.profiles` in
[../config/platform.yaml](../config/platform.yaml). The CLI, manual cluster runs
and Airflow use the same stage implementation.

| Trigger | Commands | Runtime |
|---|---|---|
| Local development | `.venv/bin/idp silver`, `gold`, `train`, `score`, `publish`, `maintain` | Local Spark master and configured lake |
| Manual cluster stage | `make silver`, `make gold`, `make train`, `make score`, `make publish`, `make maintain` | SparkApplication in `idp-app`, with driver and executors |
| Scheduled orchestration | `make airflow-trigger` or the Airflow UI | Airflow submits the same SparkApplications |

Use the [root deployment guide](../README.md) and [operator runbook](../internal/runbook.md)
for prerequisites. Cluster execution needs the platform services, application
image, Iceberg catalog database and Spark Operator watching `idp-app`. Silver
needs Bronze input. Training needs Gold features and a separately seeded labels
store; inference needs a trained model. Check the Kubernetes context first.

## Manual cluster sequence

After the environment and Bronze ingestion are ready, a first run is:

```bash
make silver
make gold
make labels
make train
make score
make publish
```

Labels stand in for observed business outcomes and live outside the lake. Gold
builds features only; `score` appends timestamped inference history; `publish`
loads staging tables through Spark JDBC and atomically swaps the latest serving
copy into TimescaleDB. Maintenance is independently runnable with `make maintain`.

Inspect applications with `kubectl -n idp-app get sparkapplications,pods` and
catalog tables with `make lake-catalog-show`. For stage logs, inspect the named
`idp-<stage>-driver` pod. `make spark-manifests` updates committed manifests after
profile edits; rebuild the application and Airflow DAG images when required.

## Airflow

The current DAG source is
[../pipelines/airflow/dags/idp_medallion_dag.py](../pipelines/airflow/dags/idp_medallion_dag.py).
All tasks use `SparkKubernetesOperator`. Airflow can submit SparkApplications and
read their pods/logs/events; it cannot create arbitrary pods in `idp-app`.
Non-Spark generate/produce/consume/labels Jobs run outside Airflow.

| DAG | Tasks | Trigger |
|---|---|---|
| `idp_features` | Silver → Gold | Hourly; emits the Gold-features dataset |
| `idp_training` | Train | Daily; emits the model dataset |
| `idp_inference` | Score → Publish | New Gold features OR a model dataset update |
| `idp_maintenance` | Maintain | Daily; emits no dataset |

Airflow >=2.9 supports the OR dataset condition. A new model triggers inference
without a separate manual scoring pass. Seed labels before starting training.

For an authorized environment, `make airflow-wire` installs/upgrades Airflow,
applies its RBAC and builds/reloads the DAG image. DAG manifests are baked into
that image from `deploy/app/spark/`. After DAG or profile changes use
`make airflow-reload` as appropriate.

```bash
make airflow-trigger  # unpauses all four; explicitly triggers features and training
make airflow-ui       # forwards the web UI to localhost:8080
```

Confirm the loaded IDs with:

```bash
kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags list
```

Inference is dataset-driven, so watch its run after features or training completes.
A task is successful only after its SparkApplication completes; failed applications
fail the task. Observe both the Airflow task logs and the Spark driver logs.

## Direct Spark entrypoints

[pipelines/spark/](../pipelines/spark/) contains thin wrappers for the stage
functions. The supported cluster manifests already invoke them in the application
image. Direct `spark-submit` requires equivalent package, jars, catalog/storage
configuration and resource settings; it is not a separate transformation engine.
The lake uses Iceberg's SQL/JDBC catalog locally and in the cluster, rather than
Nessie. `publish` remains a separate serving stage.
