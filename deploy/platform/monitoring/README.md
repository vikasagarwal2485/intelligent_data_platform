# Observability (Prometheus + Grafana)

Implements REQ-PLT-05…08 (added 2026-07-14). Everything runs from the
**kube-prometheus-stack** (Prometheus Operator, Prometheus, Grafana,
node-exporter, kube-state-metrics) plus Strimzi Kafka metrics.

| File | Purpose | Requirement |
|------|---------|-------------|
| `kube-prometheus-values.yaml` | Stack values: exporters on, Grafana + dashboard sidecar, Prometheus selects all PodMonitors | REQ-PLT-05/06/07 |
| `kafka-metrics-configmap.yaml` | JMX Prometheus exporter rules (`kafka-metrics`) referenced by the Kafka CR | REQ-PLT-08 |
| `kafka-podmonitor.yaml` | PodMonitors scraping the Kafka broker (9404) + Kafka Exporter | REQ-PLT-08 |
| `dashboards/k8s-cluster-dashboard-configmap.yaml` | "IDP — Kubernetes Cluster Overview" Grafana dashboard | REQ-PLT-07 |
| `dashboards/kafka-dashboard-configmap.yaml` | "IDP — Kafka Produce/Consume" Grafana dashboard | REQ-PLT-08 |

## Install

```bash
make platform-services          # installs the stack with these values (part of platform bring-up)
# or, to (re)apply just monitoring:
make monitoring     # helm upgrade kube-prom + apply metrics ConfigMap, PodMonitors, dashboards
make kafka          # (re-)apply the Kafka CR so it exposes JMX metrics + kafka-exporter
```

`make platform-services` already applies these values and the dashboards; `make monitoring`
is the standalone/repair path. Because the Kafka CR references the `kafka-metrics`
ConfigMap, `setup-kafka.sh` applies that ConfigMap before creating the cluster.

## View

```bash
make grafana-ui     # http://localhost:3000  (admin / idp-admin)
```

- **Kubernetes cluster** — the stack's default Kubernetes dashboards, plus
  **"IDP — Kubernetes Cluster Overview"** (node CPU%/memory%, running pods).
- **Kafka** — **"IDP — Kafka Produce/Consume"**: messages-in and bytes-in
  (produce), bytes-out (consume), request rate by type, and consumer-group lag.
  Run `make stream` to generate traffic, then watch the rates move.

## How the dashboards load

Grafana runs a sidecar that watches every namespace for ConfigMaps labelled
`grafana_dashboard: "1"` and imports their JSON. Drop a new labelled ConfigMap to
add a dashboard — no Grafana restart needed.
