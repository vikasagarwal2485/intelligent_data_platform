# Kafka topics & Avro schema registration

Step-by-step for standing up the streaming layer (plan section 3): an Apache
Kafka cluster (Strimzi), per-tenant topics, and an Apicurio Schema Registry with
the Avro contracts from [`config/avro/`](../../config/avro).

```
deploy/kafka/
  kafka-cluster.yaml     # Strimzi Kafka (KRaft, 1 node) + KafkaNodePool + entityOperator
  schema-registry.yaml   # Apicurio Registry (Confluent-compatible API)
  create-topics.sh       # KafkaTopic CRs for each tenant x entity
  register-schemas.sh    # POSTs *.avsc to the registry as <topic>-value subjects
  setup-kafka.sh         # one-shot wrapper for all of the above
```

## Prerequisites
1. A running cluster (`minikube status` → Running).
2. The Strimzi **operator** installed and Ready — this is done by `make infra`
   (`deploy/install-infra.sh` installs `strimzi/strimzi-kafka-operator`).
   The operator is what installs the `Kafka`/`KafkaNodePool` **CRDs**; without
   them `make kafka` fails with `no matches for kind "Kafka" in version
   "kafka.strimzi.io/v1beta2" — ensure CRDs are installed first`.

   Verify both the CRDs and the operator before running `make kafka`:
   ```bash
   kubectl get crd | grep strimzi
   kubectl -n idp-platform rollout status deploy/strimzi-cluster-operator
   ```
   The first command must list `kafkas.kafka.strimzi.io` and
   `kafkanodepools.kafka.strimzi.io`. If it lists nothing, (re)install the
   operator (this also installs the CRDs):
   ```bash
   helm repo add strimzi https://strimzi.io/charts/ && helm repo update
   helm upgrade --install strimzi strimzi/strimzi-kafka-operator -n idp-platform --set resources.limits.memory=768Mi --set livenessProbe.initialDelaySeconds=90 --set readinessProbe.initialDelaySeconds=90
   ```
   If the Helm release is wedged (e.g. a prior `kubectl patch` causes a
   field-ownership conflict), reinstall cleanly or use the official bundle:
   ```bash
   helm uninstall strimzi -n idp-platform
   kubectl create -f 'https://strimzi.io/install/latest?namespace=idp-platform' -n idp-platform
   ```

   > **macOS/zsh note:** don't paste shell lines with trailing `# comments` —
   > zsh treats `#` literally in interactive mode (you'll see
   > `grep: #: No such file or directory`). All snippets here are comment-free.

   > **Tuning:** configure the operator's probes/memory via Helm `--set`
   > (above), **not** `kubectl patch` — patching makes a second field owner and
   > later `helm upgrade` fails with a conflict.

   `make kafka` runs a CRD preflight automatically and prints the exact
   remediation if the CRDs are absent.

## TL;DR
```bash
make kafka      # = bash deploy/kafka/setup-kafka.sh idp-platform
```
This deploys the cluster + registry, waits for readiness, creates the topics and
registers the schemas. The detailed steps below explain what it does.

---

## Step 1 — Deploy the Kafka cluster
```bash
kubectl apply -n idp-platform -f deploy/kafka/kafka-cluster.yaml
kubectl -n idp-platform wait kafka/idp-kafka --for=condition=Ready --timeout=600s
```
- Creates a single-node KRaft Kafka named `idp-kafka` with the **Topic Operator**
  enabled (so topics can be managed as `KafkaTopic` CRs).
- In-cluster bootstrap address: `idp-kafka-kafka-bootstrap.idp-platform.svc:9092`.

> The scripts **auto-detect** whether your Strimzi serves the API at
> `kafka.strimzi.io/v1` (newer operators) or `v1beta2` (older) and render the
> manifests accordingly — so `no matches for kind "Kafka" in version
> "kafka.strimzi.io/v1beta2"` no longer happens. `kafka-cluster.yaml` is checked
> in as `v1`; `setup-kafka.sh` rewrites it to match your cluster.
>
> If `wait` times out, check `kubectl -n idp-platform describe kafka idp-kafka`.
> A common issue is `spec.kafka.version` (the Kafka broker version, distinct
> from the API version) not being supported by your operator — edit it in
> `kafka-cluster.yaml` to a supported version.

## Step 2 — Create the topics
```bash
make topics      # or: bash deploy/kafka/create-topics.sh idp-platform
```
Creates one `KafkaTopic` per **tenant × entity** following the plan's naming
`t.<tenant>.<entity>`:

| Topic (per tenant `tenant_a`, `tenant_b`) | Partitions | Purpose |
|---|---|---|
| `t.<tenant>.parties` / `.accounts` / `.agreements` / `.products` | 3 | entity (batch/CDC) |
| `t.<tenant>.usage.voice` / `.usage.data` | 6 | streaming usage events |
| `t.<tenant>.recharge` | 3 | streaming recharge events |
| `t.<tenant>.dlq` | 3 | dead-letter queue |

Verify:
```bash
kubectl -n idp-platform get kafkatopics
# or from inside the broker:
kubectl -n idp-platform exec -it idp-kafka-dual-role-0 -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

**Alternative (imperative)** — create a topic with the Kafka CLI instead of a CR:
```bash
kubectl -n idp-platform exec -it idp-kafka-dual-role-0 -- \
  bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic t.tenant_a.usage.voice --partitions 6 --replication-factor 1
```

## Step 3 — Deploy the Schema Registry
```bash
kubectl apply -n idp-platform -f deploy/kafka/schema-registry.yaml
kubectl -n idp-platform rollout status deploy/apicurio-registry
```
Apicurio exposes a **Confluent-compatible** API at `/apis/ccompat/v7`, so any
standard Schema-Registry client works against it.

## Step 4 — Register the Avro schemas
```bash
make schemas     # or: bash deploy/kafka/register-schemas.sh idp-platform
```
The script port-forwards the registry and POSTs each `.avsc` as the **value**
schema of its topic (subject = `<topic>-value`, `TopicNameStrategy`):

| Subject | Schema file |
|---|---|
| `t.<tenant>.usage.voice-value` | `config/avro/usage_event.avsc` |
| `t.<tenant>.usage.data-value` | `config/avro/usage_event.avsc` |
| `t.<tenant>.recharge-value` | `config/avro/recharge_event.avsc` |

(Entity topics are batch/CDC; register their schemas the same way by extending
the `MAP` array in `register-schemas.sh`.)

Verify:
```bash
kubectl -n idp-platform port-forward svc/apicurio-registry 8080:8080 &
curl -s http://localhost:8080/apis/ccompat/v7/subjects | tr ',' '\n'
curl -s http://localhost:8080/apis/ccompat/v7/subjects/t.tenant_a.recharge-value/versions/latest
```

---

## How producers/consumers use this
- **Bootstrap servers:** `idp-kafka-kafka-bootstrap.idp-platform.svc:9092`
- **Schema Registry URL:** `http://apicurio-registry.idp-platform.svc:8080/apis/ccompat/v7`

A NiFi/Kafka producer serializes records with the Avro serializer pointed at the
registry URL; the schema is resolved by the `<topic>-value` subject. The
`BACKWARD` compatibility mode (plan section 3) lets schemas evolve safely.
