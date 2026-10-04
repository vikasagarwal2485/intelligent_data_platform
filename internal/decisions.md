# Active decision reminders

Reviewed: 2026-10-04. These reminders link to the authoritative
[decision log](../docs/decisions/decision-log.md); later decisions amend several
early prototype assumptions.

| Constraint | Current decision |
|---|---|
| Tenant boundary | ADR-0005: `tenant_id` on every dataset and scoped reads; shared services with data isolation |
| Privacy and consent | ADR-0006 and the [serving spec](../openspec/specs/serving-api/spec.md): per-tenant PII tokens; Gold and scoring respect consent |
| One transform engine | ADR-0027 supersedes the pandas transform prototype; Silver/Gold use Spark locally and in the cluster |
| Catalog and incrementality | ADR-0028 amends Nessie assumptions; SQL/JDBC Iceberg catalog, incremental Silver facts, full Gold recomputation |
| Raw ingestion and model names | ADR-0032 revises ADR-0031: consumer lands raw TMF events, Silver conforms; `customer_tenure_days` replaces `account_tenure_days` |
| Runtime and resources | ADR-0029 and ADR-0036: Spark Operator, stage resource profiles and shuffle parallelism, shared Gold aggregates |
| Stage separation | ADR-0025 and ADR-0026: Gold features, train, score and publish remain independent; features OR model updates trigger inference |
| Airflow ownership | ADR-0035: SparkApplications only; labels seeded outside Airflow; no pod-creation rights for Airflow in `idp-app` |
| Model promotion | ADR-0033: MLflow quality-gated `champion`; object-store fallback when no tracking URI is configured |
| API isolation | ADR-0034: Keycloak JWT validation and tenant claim; cluster overlays enable auth |
| Analytics access | ADR-0030 supersedes pandas MCP reads: Trino pushdown, bounded results, read-only `idp-mcp` identity |
| Fraud normalization | ADR-0037: on-demand scores use the stored training anomaly range; batch scores use the scored population range |
| Deployment ownership | ADR-0018 and ADR-0020: separate platform/app directories and environment overlays |
| Topic retention | ADR-0036: customer topics use delete policy and seven-day retention; compaction is retired |

An architecture change should update the ADR log and relevant OpenSpec scenarios,
then this index. A local workaround is not a new architecture decision until it
is made explicit and justified.
