# Kafka topics & schema registration

Step-by-step for standing up the streaming layer (plan section 3): an Apache
Kafka cluster (Strimzi), per-tenant topics carrying TM Forum create events, and an
Apicurio Schema Registry holding each topic's TMF JSON Schema (the wire contract).
The Avro contracts in [`config/avro/`](../../../config/avro) describe the canonical
records Silver conforms to (`canonical/`) and Silver's tables (`silver/`), not the
wire.

```
deploy/kafka/
  kafka-cluster.yaml     # Strimzi Kafka (KRaft, 1 node) + KafkaNodePool + entityOperator
  schema-registry.yaml   # Apicurio Registry (Confluent-compatible API)
  create-topics.sh       # KafkaTopic CRs for each tenant x TMF resource
  register-schemas.sh    # POSTs each topic's TMF JSON Schema as its <topic>-value subject
  setup-kafka.sh         # one-shot wrapper for all of the above
```

## Prerequisites
1. A running cluster (`minikube status` → Running).
2. The Strimzi **operator** installed and Ready — this is done by `make platform-services`
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

> `setup-kafka.sh` **auto-adapts to your operator** so it doesn't break across
> Strimzi versions:
> - **API version**: detects whether the group is served at `kafka.strimzi.io/v1`
>   (newer) or `v1beta2` (older) and rewrites the manifests — no more
>   `no matches for kind "Kafka" in version "kafka.strimzi.io/v1beta2"`.
> - **Kafka version**: reads the operator's supported versions
>   (`STRIMZI_KAFKA_IMAGES`) and picks the latest, so
>   `Unsupported Kafka.spec.kafka.version` doesn't happen. (`kafka-cluster.yaml`
>   defaults to `4.3.0`; the script overrides it to whatever your operator ships.)
> - **Broker sizing** (`resources`) lives on the **KafkaNodePool**, not on
>   `Kafka.spec.kafka` — the v1 API strict-rejects it there
>   (`unknown field "spec.kafka.resources"`).
>
> If `wait` still times out, the script prints the Kafka `status.conditions`,
> pod state and events so you can see whether it's a version or a
> resource/scheduling problem.

## Step 2 — Create the topics
```bash
make topics      # or: bash deploy/kafka/create-topics.sh idp-platform
```
Creates one `KafkaTopic` per **tenant × TM Forum resource**, named
`t.<tenant>.<resource>`. Each carries the resource's TMF create events
(REQ-STR-01):

| Topic (per tenant `tenant_a`, `tenant_b`) | Partitions | Policy | Carries |
|---|---|---|---|
| `t.<tenant>.customer` | 3 | delete, 7 days | `CustomerCreateEvent` (published first) |
| `t.<tenant>.party` | 3 | delete, 7 days | `IndividualCreateEvent`, `OrganizationCreateEvent` |
| `t.<tenant>.billingAccount` / `.agreement` / `.product` | 3 | delete, 7 days | `<Resource>CreateEvent` |
| `t.<tenant>.appliedCustomerBillingRate` / `.topupBalance` | 3 | delete, 7 days | fee charges / recharges |
| `t.<tenant>.productUsage` | 6 | delete, 7 days | voice and data usage |
| `t.<tenant>.dlq` | 3 | delete, 7 days | dead letters from the consumer |

The CR names are lower-cased (`t-tenant-a-productusage`); the topic names keep the
TMF resource's case.

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
  --create --topic t.tenant_a.productUsage --partitions 6 --replication-factor 1
```

## Step 3 — Deploy the Schema Registry
```bash
kubectl apply -n idp-platform -f deploy/kafka/schema-registry.yaml
kubectl -n idp-platform rollout status deploy/apicurio-registry
```
Apicurio exposes a **Confluent-compatible** API at `/apis/ccompat/v7`, so any
standard Schema-Registry client works against it.

## Step 4 — Register the topic schemas (wire contract)
```bash
make schemas     # or: bash deploy/platform/kafka/register-schemas.sh idp-platform
```
The script writes each topic's TM Forum JSON Schema with `idp registry-schemas`
(the create event, its `Idp<Resource>` resource and their TMF bases, pruned to what
they reference) and POSTs it as the **value** schema of the topic, with
`schemaType: JSON` (subject = `<topic>-value`, `TopicNameStrategy`), e.g.
`t.tenant_a.productUsage-value`. The `party` subject accepts both party events.

The Avro schemas in `config/avro/` aren't registered: `canonical/` is the contract
of the records Silver's conform step produces, which it enforces, and `silver/`
documents Silver's tables (REQ-STR-03). Avro
subjects registered before this change (`t.<tenant>.usage.voice-value` …) are
unused and can stay.

Verify:
```bash
kubectl -n idp-platform port-forward svc/apicurio-registry 8080:8080 &
curl -s http://localhost:8080/apis/ccompat/v7/subjects | tr ',' '\n'
curl -s http://localhost:8080/apis/ccompat/v7/subjects/t.tenant_a.topupBalance-value/versions/latest
```

---

## Admin UI (manage topics from a browser)

Deploy **Kafbat Kafka UI** (the maintained successor to provectus/kafka-ui) to
create/browse/configure topics, view messages and inspect the schema registry:

```bash
make kafka-ui
```
Then access it (ClusterIP, so port-forward):
```bash
kubectl -n idp-platform port-forward svc/kafka-ui 8080:8080
```
Open <http://localhost:8080>. It is preconfigured against:
- bootstrap `idp-kafka-kafka-bootstrap:9092`
- schema registry `http://apicurio-registry:8080/apis/ccompat/v7`

From the UI you can add a topic (e.g. `t.tenant_a.productUsage`), change
partitions/configs, produce/consume test messages, and view registered schemas.

> Prefer something else? Redpanda Console (`redpandadata/console`) and AKHQ are
> drop-in alternatives — point them at the same bootstrap + registry URLs.

## How producers/consumers use this
- **Bootstrap servers:** `idp-kafka-kafka-bootstrap.idp-platform.svc:9092`
- **Schema Registry URL:** `http://apicurio-registry.idp-platform.svc:8080/apis/ccompat/v7`

Payloads are plain UTF-8 JSON TMF create events. A NiFi or other producer can
validate them against the `<topic>-value` subject (schema type JSON) through the
registry URL. The `BACKWARD` compatibility mode (plan section 3) lets schemas
evolve safely.

## Message retention & on-demand purge

**Time-based retention (automatic).** Every topic is created with
`retention.ms = 7 days` and `cleanup.policy = delete` (see `create-topics.sh`), so
Kafka deletes messages older than 7 days on its own. Bronze keeps every event, so
no topic needs compaction. Change the window and
re-apply — the Strimzi Topic Operator reconciles it onto the existing topics:

```bash
RETENTION_DAYS=3 make topics      # e.g. switch to 3-day retention
```

**On-demand purge (immediate).** To empty the topics *now* without deleting them
(e.g. before a clean re-run), use `purge-topics.sh` — it truncates every partition
to the log end via `kafka-delete-records` (offset -1):

```bash
make kafka-purge                                  # purge all t.* topics
make kafka-purge TOPIC=t.tenant_a.topupBalance    # purge a single topic
# or directly:
bash deploy/platform/kafka/purge-topics.sh idp-platform [topic]
```

The topics, partitions, configs and registered schemas are preserved — only the
messages are removed. (This is the message-level counterpart to `make minio-reset`,
which clears the lake buckets.)
