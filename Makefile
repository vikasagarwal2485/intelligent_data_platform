.DEFAULT_GOAL := help
SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
NAMESPACE_PLATFORM := idp-platform
TENANTS := tenant-a tenant-b
# MinIO client (mc): run it INSIDE the already-running MinIO server pod, which
# ships /usr/bin/mc, instead of pulling a standalone client image. The public
# `minio/mc` client repo (Docker Hub AND quay.io) now returns "unauthorized /
# pull access denied" for anonymous pulls, so a throwaway `kubectl run --image`
# pod ImagePullBackOffs. Exec avoids the pull entirely and needs no registry.
# MC_SH runs one `sh -c` script in the server pod; MC_ENV sets a config-free
# host alias `m` (HOME=/tmp so mc's config dir is writable for any pod user).
MC_SH := kubectl -n $(NAMESPACE_PLATFORM) exec -i deploy/minio -- sh -c
MC_ENV := export HOME=/tmp MC_HOST_m=http://minioadmin:minioadmin123@127.0.0.1:9000;

# ---------------------------------------------------------------------------
# Local development (Python)
# ---------------------------------------------------------------------------
.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-22s\033[0m %s\n", $$1, $$2}'

$(VENV): ## Create the virtualenv
	python3 -m venv $(VENV)

.PHONY: install
install: $(VENV) ## Install the package and dev dependencies (needs a JDK 11/17 for Spark)
	@java -version >/dev/null 2>&1 || { echo "ERROR: a JDK 11/17 is required (Silver/Gold run on Spark). Install e.g. Temurin 17 and set JAVA_HOME."; exit 1; }
	$(PIP) install -q --upgrade pip
	$(PIP) install -q -e ".[dev]"

.PHONY: test
test: ## Run the test suite
	$(PY) -m pytest

.PHONY: generate
generate: ## Generate synthetic data into the landing zone
	$(PY) -m idp.cli generate

.PHONY: run
run: ## Run the end-to-end batch pipeline (Bronze->Silver->Features->ML->Gold)
	$(PY) -m idp.cli run

.PHONY: serve
serve: ## Start the FastAPI scoring service locally on :8000 (auth off; serving lookup needs IDP_TS_*; the cluster API: make serve-cluster)
	$(PY) -m idp.cli serve

.PHONY: mcp
mcp: ## Start the MCP analytics server (stdio): 7 read-only, tenant-scoped tools (5 Silver, 2 Gold scores)
	$(PY) -m idp.cli mcp

# The MCP tools' integration stack (deploy/dev/trino-compose.yaml): Trino 480 +
# Postgres + MinIO on offset host ports. The tests build their lake in it.
TRINO_COMPOSE := docker compose -f deploy/dev/trino-compose.yaml
TRINO_TEST_ENV := IDP_CATALOG_TYPE=sql IDP_CATALOG_NAME=lake \
	IDP_CATALOG_URI=postgresql+psycopg2://postgres:idp-dev-password@127.0.0.1:15432/iceberg \
	IDP_S3_ENDPOINT=http://127.0.0.1:19000 IDP_S3_ACCESS_KEY=minioadmin IDP_S3_SECRET_KEY=minioadmin123 \
	IDP_TRINO_HOST=127.0.0.1 IDP_TRINO_PORT=18443 IDP_TRINO_CA_CERT=$(abspath deploy/dev/trino/.secrets/tls.crt)
TRINO_TEST_SECRETS := deploy/dev/trino/.secrets

.PHONY: trino-stack
trino-stack: ## Start the compose Trino test stack (Trino + Postgres + MinIO; HTTPS + JWT auth) and wait until healthy
	$(PY) deploy/platform/trino/make_secrets.py $(TRINO_TEST_SECRETS) --test-jwt --name localhost --name 127.0.0.1 --name trino
	$(TRINO_COMPOSE) up -d --wait

.PHONY: trino-stack-down
trino-stack-down: ## Stop the compose Trino test stack and drop its data
	$(TRINO_COMPOSE) down -v

.PHONY: mcp-trino-test
mcp-trino-test: trino-stack ## Run the MCP tools' Trino integration tests (parity with the pandas reference; pytest -m trino)
	$(TRINO_TEST_ENV) $(PY) -m pytest -m trino tests/test_mcp_trino.py tests/test_tenant_isolation.py

.PHONY: mcp-smoke
# The MCP server's identity for TENANT: its Keycloak MCP client (dev secret unless
# IDP_KC_SECRET_IDP_MCP_<TENANT> is set), the token endpoint behind `make keycloak-api`,
# and the certificate that verifies Trino behind `make trino-api` (REQ-MCP-06).
MCP_TENANT := $(or $(TENANT),tenant_a)
MCP_CLIENT := idp-mcp-$(MCP_TENANT)
MCP_SECRET_VAR := IDP_KC_SECRET_$(shell printf '%s' '$(MCP_CLIENT)' | tr '[:lower:]' '[:upper:]' | tr -c 'A-Z0-9' '_')
MCP_SECRET = $(or $($(MCP_SECRET_VAR)),mcp-$(subst _,-,$(MCP_TENANT))-dev-secret)
MCP_TOKEN_URL := http://127.0.0.1:18180/realms/idp/protocol/openid-connect/token
TRINO_CA_CERT := $(abspath deploy/platform/trino/.secrets/tls.crt)
MCP_ENV = IDP_MCP_TOKEN_URL=$(MCP_TOKEN_URL) IDP_MCP_CLIENT_ID=$(MCP_CLIENT) \
	IDP_MCP_CLIENT_SECRET=$(MCP_SECRET) IDP_TRINO_CA_CERT=$(TRINO_CA_CERT)

.PHONY: trino-rls-bench trino-rls-bench-stack
trino-rls-bench: ## Row-filter cost (REQ-TEN-05): MCP tool queries as TENANT's principal vs admin, 60 alternating runs, budget p50 x1.10 (needs make trino-api + keycloak-api)
	@TRINO_ADMIN_PASSWORD=$$(kubectl -n $(NAMESPACE_PLATFORM) get secret trino-passwords -o jsonpath='{.data.TRINO_ADMIN_PASSWORD}' | base64 -d) \
	  $(MCP_ENV) $(PY) -m idp.mcp.bench

trino-rls-bench-stack: ## The same check against the compose test stack (make trino-stack; build its lake with make mcp-trino-test)
	@TRINO_ADMIN_PASSWORD=$$(sed -n 's/^TRINO_ADMIN_PASSWORD=//p' $(TRINO_TEST_SECRETS)/passwords.env) \
	  $(TRINO_TEST_ENV) $(PY) -m idp.mcp.bench --test-signer $(TRINO_TEST_SECRETS) --tenant $(MCP_TENANT)

mcp-smoke: ## Call every MCP tool as TENANT's MCP identity (default tenant_a) and check other tenants are refused (needs make trino-api + keycloak-api)
	$(MCP_ENV) $(PY) -m idp.mcp.smoke

.PHONY: mcp-config
mcp-config: ## Print the Claude Desktop MCP server config for TENANT (default tenant_a; paste into claude_desktop_config.json)
	@printf '%s\n' \
	  '{' \
	  '  "mcpServers": {' \
	  '    "idp-analytics": {' \
	  '      "command": "$(abspath $(PY))",' \
	  '      "args": ["-m", "idp.cli", "mcp"],' \
	  '      "env": {' \
	  '        "IDP_TRINO_HOST": "127.0.0.1",' \
	  '        "IDP_TRINO_PORT": "8085",' \
	  '        "IDP_TRINO_CA_CERT": "$(TRINO_CA_CERT)",' \
	  '        "IDP_MCP_TOKEN_URL": "$(MCP_TOKEN_URL)",' \
	  '        "IDP_MCP_CLIENT_ID": "$(MCP_CLIENT)",' \
	  '        "IDP_MCP_CLIENT_SECRET": "$(MCP_SECRET)"' \
	  '      }' \
	  '    }' \
	  '  }' \
	  '}'
	@echo ""
	@echo "# This server answers for $(MCP_TENANT) only: Keycloak issues its client a token for"
	@echo "# mcp@$(MCP_TENANT), and Trino row-filters every query to that tenant. It needs no"
	@echo "# lake or MinIO credentials. Prereqs: 'make install' (installs mcp), 'make trino-certs'"
	@echo "# (the CA above), and keep 'make trino-api' (127.0.0.1:8085) and 'make keycloak-api'"
	@echo "# (127.0.0.1:18180) running while Claude uses it. Another tenant: TENANT=<id>."
	@echo "# Paste into (create if missing), then fully quit (Cmd-Q) and reopen Claude Desktop:"
	@echo "#   macOS:   ~/Library/Application Support/Claude/claude_desktop_config.json"
	@echo "#   Windows: %APPDATA%\\Claude\\claude_desktop_config.json"

.PHONY: smoke
smoke: ## End-to-end smoke test (pipeline + tenant isolation)
	$(PY) -m idp.cli run --no-persist

.PHONY: clean
clean: ## Remove generated data and caches
	rm -rf data/landing data/lake ml/artifacts .pytest_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +

# ---------------------------------------------------------------------------
# Local Kubernetes (minikube) - see deploy/README.md
# ---------------------------------------------------------------------------
.PHONY: mk-up
mk-up: ## Start minikube with required addons
	minikube start --cpus=8 --memory=16384 --addons=ingress,metrics-server,storage-provisioner

.PHONY: image
image: ## Build the scoring-api image INTO minikube's docker daemon (local, no registry)
	@eval $$(minikube docker-env) && docker build --build-arg IDP_GIT_SHA=$$(git rev-parse HEAD 2>/dev/null || echo unknown) -t idp/scoring-api:dev .
	@echo "Built idp/scoring-api:dev inside minikube. Now run: make platform-base && make app-deploy"

.PHONY: platform-services
platform-services: ## Add Helm repos and install platform infrastructure into idp-platform
	bash deploy/platform/install-infra.sh idp-platform

.PHONY: strimzi-fix
strimzi-fix: ## Relax Strimzi operator probes + raise memory (fixes probe failures)
	bash deploy/platform/strimzi-tune.sh idp-platform

.PHONY: trino-fix
trino-fix: ## Re-apply tuned Trino values (auto-recovers a stuck Helm release)
	bash deploy/platform/trino-tune.sh idp-platform

.PHONY: keycloak-realm
keycloak-realm: ## Render deploy/platform/keycloak/idp-realm.json from its base + the configured tenants (groups, scoring + MCP clients)
	$(PY) -m idp.cli keycloak-realm

.PHONY: keycloak
keycloak: ## Apply Keycloak with the `idp` realm (ConfigMap from deploy/platform/keycloak/idp-realm.json) and restart it to re-import
	$(PY) -m idp.cli keycloak-realm --check   # the realm must match the configured tenants: run `make keycloak-realm`
	kubectl create configmap keycloak-realm -n idp-platform \
	  --from-file=idp-realm.json=deploy/platform/keycloak/idp-realm.json --dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -n idp-platform -f deploy/platform/keycloak/keycloak.yaml
	kubectl -n idp-platform scale deploy/keycloak --replicas=1
	kubectl -n idp-platform rollout restart deploy/keycloak
	kubectl -n idp-platform rollout status deploy/keycloak --timeout=600s

TENANT ?= tenant_a
.PHONY: keycloak-token
keycloak-token: ## Print a dev access token for TENANT (default tenant_a); KIND=scoring|mcp|user (user: DEV_USER=alice): TOKEN=$$(make -s keycloak-token TENANT=tenant_b KIND=mcp)
	@KIND=$(or $(KIND),scoring) bash deploy/platform/keycloak/token.sh $(TENANT) idp-platform

.PHONY: mlflow
mlflow: ## Install/upgrade the MLflow model registry (pinned chart; creates its `mlflow` DB in TimescaleDB)
	bash deploy/platform/mlflow/install.sh idp-platform

.PHONY: mlflow-ui
mlflow-ui: ## Port-forward the MLflow UI/API to http://127.0.0.1:5050 (runs, versions, the `champion` alias)
	@echo "MLflow on http://127.0.0.1:5050"
	kubectl -n idp-platform port-forward svc/mlflow 5050:80

.PHONY: spark-operator
spark-operator: ## Install/upgrade the Kubeflow Spark Operator (pinned, watches idp-app) + Airflow SparkApplication RBAC
	bash deploy/platform/spark-operator/install.sh idp-platform

.PHONY: mk-check
mk-check: ## Check the minikube node container's real memory cap can hold the platform + the largest Spark stage profile (REQ-ORC-07)
	@bash deploy/platform/mk-check.sh

.PHONY: lake-catalog
lake-catalog: ## Create the lake's Iceberg JDBC catalog database (`iceberg` in TimescaleDB) — idempotent
	bash deploy/platform/timescaledb/lake-catalog.sh idp-platform

.PHONY: lake-catalog-rename
lake-catalog-rename: ## One-off: rename the Iceberg catalog rows to the stable name `lake` (pause DAGs + no Spark stages first; idempotent)
	bash deploy/platform/timescaledb/lake-catalog.sh idp-platform --rename-to-lake

.PHONY: lake-catalog-show
lake-catalog-show: ## Show what the Iceberg catalog (TimescaleDB `iceberg` DB) stores: every table's current metadata.json + namespace properties (read-only)
	@bash deploy/platform/timescaledb/lake-catalog-show.sh idp-platform

.PHONY: lake-table-show
lake-table-show: ## Decode one lake table's current Iceberg metadata from MinIO: schema, partitioning, snapshots (read-only). TABLE=bronze.tmf_billing_account
	@bash deploy/platform/timescaledb/lake-catalog-show.sh idp-platform table "$(TABLE)"

ROWS_LIMIT ?= 20
COLUMNS ?= *
FORMAT ?= ALIGNED
.PHONY: lake-table-rows
lake-table-rows: ## Show a lake table's rows via Trino (read-only; Bronze holds raw PII): TABLE=gold.credit_features [ROWS_LIMIT=20] [TENANT=tenant_a (default; TENANT= for all)] [COLUMNS=a,b] [FORMAT=ALIGNED|VERTICAL|CSV_HEADER|JSON]
	@case "$(TABLE)" in bronze.*|silver.*|gold.*) ;; *) echo "TABLE must be <bronze|silver|gold>.<table>, e.g. gold.credit_features (make lake-catalog-show lists them)" >&2; exit 1;; esac
	@case "$(TABLE)" in *[!a-z0-9_.]*|*.*.*) echo "TABLE must be <layer>.<table>: lower-case letters, digits and _" >&2; exit 1;; esac
	@case "$(ROWS_LIMIT)" in *[!0-9]*|"") echo "ROWS_LIMIT must be a number" >&2; exit 1;; esac
	@case "$(TENANT)" in *[!a-z0-9_]*) echo "TENANT must be a tenant id like tenant_a" >&2; exit 1;; esac
	@case "$(COLUMNS)" in "*") ;; *[!a-z0-9_,]*|"") echo "COLUMNS must be * or comma-separated column names" >&2; exit 1;; esac
	@case "$(FORMAT)" in ALIGNED|VERTICAL|CSV|CSV_HEADER|TSV|TSV_HEADER|JSON) ;; *) echo "FORMAT must be ALIGNED, VERTICAL, CSV(_HEADER), TSV(_HEADER) or JSON" >&2; exit 1;; esac
	@bash deploy/platform/trino/cli.sh $(NAMESPACE_PLATFORM) $(FORMAT) \
	  "SELECT $(COLUMNS) FROM lake.$(TABLE)$(if $(TENANT), WHERE tenant_id = '$(TENANT)',) LIMIT $(ROWS_LIMIT)"

.PHONY: lake-catalog-psql
lake-catalog-psql: ## Open a psql shell on the Iceberg catalog database (`iceberg`): tables iceberg_tables, iceberg_namespace_properties
	@bash deploy/platform/timescaledb/lake-catalog-show.sh idp-platform psql

.PHONY: nessie-fix
nessie-fix: ## Re-apply tuned Nessie values (in-memory store, fixes CrashLoop)
	bash deploy/platform/nessie-tune.sh idp-platform

.PHONY: monitoring
monitoring: ## Install Prometheus+Grafana+exporters and load cluster/Kafka dashboards
	helm repo add prometheus-community https://prometheus-community.github.io/helm-charts >/dev/null 2>&1 || true
	helm repo update prometheus-community >/dev/null 2>&1 || true
	helm upgrade --install kube-prom prometheus-community/kube-prometheus-stack -n idp-platform \
	  -f deploy/platform/monitoring/kube-prometheus-values.yaml --timeout 10m
	kubectl apply -n idp-platform -f deploy/platform/monitoring/kafka-metrics-configmap.yaml
	kubectl apply -n idp-platform -f deploy/platform/monitoring/kafka-podmonitor.yaml
	kubectl apply -n idp-platform -f deploy/platform/monitoring/dashboards/
	@echo ">> Re-run 'make kafka' so the Kafka cluster exposes JMX metrics + kafka-exporter."

.PHONY: grafana-ui
grafana-ui: ## Port-forward Grafana to http://localhost:3000 (admin/idp-admin)
	@echo "Open http://localhost:3000  (user: admin  pass: idp-admin)"
	@echo "Dashboards: 'IDP — Kubernetes Cluster Overview' and 'IDP — Kafka Produce/Consume'"
	kubectl -n idp-platform port-forward svc/kube-prom-grafana 3000:80

.PHONY: kafka
kafka: ## Deploy Kafka cluster + Schema Registry, create topics, register schemas
	bash deploy/platform/kafka/setup-kafka.sh idp-platform

.PHONY: minio-ls
minio-ls: ## List the landing + lake objects in MinIO (idp-landing / idp-bronze / idp-silver / idp-gold)
	$(MC_SH) '$(MC_ENV) for b in idp-landing idp-bronze idp-silver idp-gold; do echo "== $$b =="; mc ls -r m/$$b/; done'

.PHONY: minio-reset
minio-reset: ## Reset the lake for a clean re-run: recreate the buckets (idp-bronze/silver/gold/ml/landing) AND clear the Iceberg catalog
	# Full RECREATE, not a shallow `rm -r`. `mc rm -r --force` does NOT remove
	# object versions or delete markers, so on a versioned/suspended bucket it
	# leaves `null`-version delete markers on the reset keys (e.g. credit_features/,
	# fraud_features/) that then SHADOW new writes to those same keys — the object
	# PUTs succeed but list/get return nothing. `mc rb --force` drops the bucket and
	# ALL versions/markers/ILM; `mc mb` recreates it clean (unversioned by default).
	$(MC_SH) '$(MC_ENV) for b in idp-bronze idp-silver idp-gold idp-ml idp-landing; do echo "recreating $$b"; mc rb --force m/$$b/ 2>/dev/null || true; mc mb --ignore-existing m/$$b; done; echo done'
	# The lake's tables are registered in the Iceberg catalog (database `iceberg` on
	# TimescaleDB), not in MinIO. Emptying the buckets alone leaves catalog rows
	# pointing at deleted metadata files, and every stage then fails with
	# FileNotFoundError on *.metadata.json. So forget the tables too; the next stage
	# recreates them. (Silver's streaming checkpoints live in idp-silver and went
	# with the bucket, so the next Silver run starts from all of Bronze.)
	bash deploy/platform/timescaledb/lake-catalog.sh idp-platform --reset

ENTITY ?= agreement
LINES ?= 30
LANDING_ENTITIES := individual organization customer billingAccount agreement product appliedCustomerBillingRate topupBalance productUsage
.PHONY: landing-show
landing-show: ## Show a TM Forum landing file from MinIO (raw, untokenized PII): TENANT=tenant_a ENTITY=agreement LINES=30 (LINES=0 = whole file)
	@case " $(LANDING_ENTITIES) " in *" $(ENTITY) "*) ;; *) echo "ENTITY must be one of: $(LANDING_ENTITIES)" >&2; exit 1;; esac
	@case "$(TENANT)" in *[!a-z0-9_]*|"") echo "TENANT must be a tenant id like tenant_a" >&2; exit 1;; esac
	@case "$(LINES)" in *[!0-9]*|"") echo "LINES must be a number (0 = whole file)" >&2; exit 1;; esac
	@$(MC_SH) '$(MC_ENV) f=m/idp-landing/tenant=$(TENANT)/$(ENTITY).json; \
	  mc stat $$f >/dev/null 2>&1 || { echo "no landing file $$f (run make stream-from-landing first; make minio-ls lists them)" >&2; exit 1; }; \
	  if [ "$(LINES)" = 0 ]; then mc cat $$f; else mc head -n $(LINES) $$f; fi'

.PHONY: minio-versions
minio-versions: ## Diagnose hidden bucket state (versioning, delete markers, ILM) — a plain `ls` hides these
	$(MC_SH) '$(MC_ENV) for b in idp-bronze idp-silver idp-gold idp-ml idp-landing; do echo "== $$b =="; echo "-- versioning:"; mc version info m/$$b; echo "-- ILM rules:"; mc ilm rule ls m/$$b 2>/dev/null || echo "(none)"; echo "-- versions/delete-markers:"; mc ls --versions -r m/$$b/; done'

.PHONY: minio-mkbuckets
minio-mkbuckets: ## Create the buckets if missing (idp-bronze/silver/gold/ml + idp-landing) — idempotent repair
	$(MC_SH) '$(MC_ENV) mc mb --ignore-existing m/idp-bronze m/idp-silver m/idp-gold m/idp-ml m/idp-landing && echo "buckets:" && mc ls m/'

.PHONY: lake-archive-policy
lake-archive-policy: ## Render the Bronze archive rule; apply it to idp-bronze when ARCHIVE_TIER=<MinIO remote tier> is set
	@$(PY) -m idp.cli archive-policy --format minio --storage-class "$${ARCHIVE_TIER:-WARM}" > /tmp/idp-bronze-ilm.json
	@cat /tmp/idp-bronze-ilm.json
	@if [ -z "$$ARCHIVE_TIER" ]; then \
	  echo "# Rendered only: this MinIO has no remote tier, so a transition rule would be inert (MinIO rejects"; \
	  echo "# rules naming an unknown tier). Configure one ('mc admin tier add ...') and re-run with ARCHIVE_TIER=<name>."; \
	  echo "# On EKS apply 'idp archive-policy --format s3' with 'aws s3api put-bucket-lifecycle-configuration'."; \
	else \
	  $(MC_SH) '$(MC_ENV) mc ilm import m/idp-bronze && mc ilm rule ls m/idp-bronze' < /tmp/idp-bronze-ilm.json; \
	fi

.PHONY: minio-console
minio-console: ## Port-forward the MinIO web console to http://localhost:9001 (minioadmin/minioadmin123)
	@echo "Open http://localhost:9001  (user: minioadmin  pass: minioadmin123)"
	kubectl -n idp-platform port-forward svc/minio-console 9001:9001

CLUSTER_API_PORT ?= 18000
.PHONY: serve-cluster
serve-cluster: ## Port-forward the cluster's scoring API to 127.0.0.1:18000 (auth on: needs `make keycloak-token`); local `make serve` stays on :8000
	@echo "Cluster scoring API on http://127.0.0.1:$(CLUSTER_API_PORT)  (keep this running; Ctrl-C to stop)"
	@echo "Every endpoint except /health needs a tenant token:"
	@echo '  TOKEN=$$(make -s keycloak-token TENANT=tenant_a)'
	@echo '  curl -s localhost:$(CLUSTER_API_PORT)/serving/credit_scores/tenant_a/<service_id> -H "Authorization: Bearer $$TOKEN"'
	kubectl -n idp-app rollout status deploy/scoring-api --timeout=120s
	kubectl -n idp-app port-forward svc/scoring-api $(CLUSTER_API_PORT):80

.PHONY: trino-certs
trino-certs: ## Create Trino's TLS, service-user password and shared-secret Secrets (idempotent; ROTATE=1 regenerates, then make trino-fix)
	bash deploy/platform/trino/certs.sh $(NAMESPACE_PLATFORM) $(PY)

.PHONY: keycloak-api
keycloak-api: ## Port-forward Keycloak to 127.0.0.1:18180 (the host MCP server's token endpoint)
	@echo "Keycloak on http://127.0.0.1:18180  (keep this running while the MCP server answers)"
	kubectl -n idp-platform port-forward svc/keycloak 18180:8080

.PHONY: trino-api
trino-api: ## Port-forward Trino's HTTPS port to 127.0.0.1:8085 (for the host MCP server; trusts deploy/platform/trino/.secrets/tls.crt)
	@echo "Trino on https://127.0.0.1:8085  (keep this running while the MCP server answers)"
	kubectl -n idp-platform port-forward svc/trino 8085:8443

.PHONY: minio-api
minio-api: ## Port-forward the MinIO S3 API to 127.0.0.1:9000 (for the host MCP server to read Silver)
	@echo "MinIO S3 API on http://127.0.0.1:9000  (keep this running while the MCP server reads Silver)"
	kubectl -n idp-platform port-forward svc/minio 9000:9000

.PHONY: kafka-ui
kafka-ui: ## Deploy the Kafka admin UI (Kafbat) for managing topics
	kubectl apply -n idp-platform -f deploy/platform/kafka/kafka-ui.yaml
	kubectl -n idp-platform rollout status deploy/kafka-ui --timeout=300s
	@echo ""
	@echo "Kafka UI deployed. Access it with:"
	@echo "  kubectl -n idp-platform port-forward svc/kafka-ui 8080:8080"
	@echo "Then open http://localhost:8080"

.PHONY: stream
stream: ## Ingestion (idp-app): in-memory produce -> Kafka, then consume -> Bronze (S3). Bronze stage only. Don't run alongside `make stream-from-landing`.
	kubectl -n idp-app delete job idp-produce idp-consume --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/produce-job.yaml
	@echo ">> Producing... (follow: kubectl -n idp-app logs -f job/idp-produce)"
	kubectl -n idp-app wait --for=condition=complete job/idp-produce --timeout=900s
	kubectl -n idp-app apply -f deploy/app/jobs/consume-job.yaml
	@echo ">> Consuming -> Bronze... (follow: kubectl -n idp-app logs -f job/idp-consume)"
	kubectl -n idp-app wait --for=condition=complete job/idp-consume --timeout=600s
	@echo ">> Bronze ready. Next: make silver"

# Spark stages run as Spark Operator SparkApplications (REQ-PLT-10): render the
# stage's manifest from its profile (idp spark-manifest), apply it, and wait for its
# final state. $(1)=stage, $(2)=extra stage args (e.g. "-- --full-refresh").
define run_sparkapp
	kubectl -n idp-app delete sparkapplication idp-$(1) --ignore-not-found --wait=true
	$(PY) -m idp.cli spark-manifest $(1) $(2) | kubectl apply -f -
	$(call wait_sparkapp,idp-$(1),1800)
endef

# Wait for a SparkApplication: exit 0 on COMPLETED (printing the driver's log tail),
# exit 1 as soon as it FAILED / SUBMISSION_FAILED (printing why). $(1)=name, $(2)=timeout (s).
define wait_sparkapp
	@a=$(1); for i in $$(seq 1 $$(( $(2) / 3 ))); do \
	  s=$$(kubectl -n idp-app get sparkapplication $$a -o jsonpath='{.status.applicationState.state}' 2>/dev/null); \
	  case "$$s" in \
	    COMPLETED) echo ">> $$a COMPLETED (driver + $$(kubectl -n idp-app get sparkapplication $$a -o jsonpath='{.status.executorState}' | tr ',' '\n' | grep -c ':') executor(s))"; \
	               kubectl -n idp-app logs $$a-driver --tail=25 2>/dev/null | grep -v " WARN \| INFO "; exit 0;; \
	    FAILED|SUBMISSION_FAILED) echo ">> $$a $$s: $$(kubectl -n idp-app get sparkapplication $$a -o jsonpath='{.status.applicationState.errorMessage}')"; \
	               kubectl -n idp-app logs $$a-driver --tail=40 2>/dev/null; exit 1;; \
	  esac; sleep 3; done; echo ">> $$a timed out after $(2)s (state: $$s)"; exit 1
endef

.PHONY: spark-manifests
spark-manifests: ## Re-render the SparkApplication manifests (deploy/app/spark/, the one copy; the DAG image bakes it in) from spark.profiles
	@mkdir -p deploy/app/spark
	@for st in silver gold score publish maintain train; do \
	  $(PY) -m idp.cli spark-manifest $$st > deploy/app/spark/$$st.yaml || exit 1; \
	done; echo ">> rendered deploy/app/spark/*.yaml (run 'make airflow-reload' so the DAG image picks them up)"

# Wait for an idp-app Job to finish: exit 0 when it completes, and exit 1 (printing
# its log) as soon as it FAILS, instead of `kubectl wait --for=condition=complete`,
# which would sit out its whole timeout on a failed Job. $(1)=job name, $(2)=timeout (s).
define wait_job
	@j=$(1); for i in $$(seq 1 $$(( $(2) / 2 ))); do \
	  s=$$(kubectl -n idp-app get job $$j -o jsonpath='{.status.conditions[?(@.status=="True")].type}' 2>/dev/null); \
	  case "$$s" in \
	    *Complete*) echo ">> $$j complete"; kubectl -n idp-app logs job/$$j --tail=25; exit 0;; \
	    *Failed*) echo ">> $$j FAILED"; kubectl -n idp-app logs job/$$j --tail=40; exit 1;; \
	  esac; sleep 2; done; echo ">> $$j timed out after $(2)s"; exit 1
endef

.PHONY: stream-from-landing
stream-from-landing: ## Ingestion via the landing zone: generate -> s3://idp-landing -> produce --from-landing -> Kafka -> consume -> Bronze. Stops at the first failed step. Don't run alongside `make stream`.
	kubectl -n idp-app delete job idp-generate idp-produce-from-landing idp-consume --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/generate-job.yaml
	@echo ">> [1/3] Generating source files -> s3://idp-landing ..."
	$(call wait_job,idp-generate,900)
	kubectl -n idp-app apply -f deploy/app/jobs/produce-from-landing-job.yaml
	@echo ">> [2/3] Publishing the landing files -> Kafka ..."
	$(call wait_job,idp-produce-from-landing,900)
	kubectl -n idp-app apply -f deploy/app/jobs/consume-job.yaml
	@echo ">> [3/3] Consuming -> Bronze ..."
	$(call wait_job,idp-consume,600)
	@echo ">> Bronze ready (from the landing zone). Next: make silver"

.PHONY: generate-job
generate-job: ## Landing only (idp-app): write synthetic source files to s3://idp-landing (step 1 of stream-from-landing)
	kubectl -n idp-app delete job idp-generate --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/generate-job.yaml
	$(call wait_job,idp-generate,900)

.PHONY: stream-replay
stream-replay: ## Re-publish the EXISTING landing files (no regenerate) -> Kafka -> consume -> Bronze. Stops if a file is rejected.
	kubectl -n idp-app delete job idp-produce-from-landing idp-consume --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/produce-from-landing-job.yaml
	@echo ">> [1/2] Publishing the existing landing files -> Kafka ..."
	$(call wait_job,idp-produce-from-landing,900)
	kubectl -n idp-app apply -f deploy/app/jobs/consume-job.yaml
	@echo ">> [2/2] Consuming -> Bronze ..."
	$(call wait_job,idp-consume,600)
	@echo ">> Bronze ready. Next: make silver"

.PHONY: silver
silver: mk-check ## Silver stage (SparkApplication in idp-app): new Bronze (S3) -> canonical Silver (S3), incremental
	$(call run_sparkapp,silver,)
	@echo ">> Silver ready. Next: make gold"

.PHONY: silver-full-refresh
silver-full-refresh: mk-check ## Silver stage with --full-refresh (SparkApplication): reset the checkpoints, rebuild the facts from all of Bronze
	$(call run_sparkapp,silver,-- --full-refresh)
	@echo ">> Silver rebuilt. Next: make gold"

.PHONY: maintain
maintain: mk-check ## Lake maintenance (SparkApplication in idp-app): compact, expire snapshots, remove orphan files on every table
	$(call run_sparkapp,maintain,)

.PHONY: gold
gold: mk-check ## Gold stage (SparkApplication in idp-app): Silver (S3) -> feature tables ONLY (S3)
	$(call run_sparkapp,gold,)
	@echo ">> Gold features written. Next: make labels (once per dataset; train needs it), then make train, make score, make publish."
	@echo "   With champion models already trained: make score, then make publish."

.PHONY: score
score: mk-check ## Score stage (SparkApplication in idp-app): Gold features + models -> score tables (S3), distributed. Needs trained models ('make train').
	$(call run_sparkapp,score,)
	@echo ">> Scores written. Next: make publish"

.PHONY: labels
labels: ## Seed the labels/outcomes store (idp-app): -> s3://idp-ml/labels (training input)
	kubectl -n idp-app delete job idp-labels --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/labels-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-labels --timeout=600s
	kubectl -n idp-app logs job/idp-labels
	@echo ">> Labels seeded. Next: make train"

.PHONY: train
train: mk-check ## Train stage (SparkApplication in idp-app): Gold features + labels store -> models (MLflow registry). Needs 'make labels' first.
	$(call run_sparkapp,train,)
	@echo ">> Models registered (MLflow when IDP_MLFLOW_TRACKING_URI is set, else s3://idp-ml); see 'registry' above for the champion gate."
	@echo "   Next: make score (scores with the champions), then make publish"

.PHONY: publish
publish: mk-check ## Serving stage (SparkApplication in idp-app): Gold (S3) -> TimescaleDB (staging load + atomic swap)
	$(call run_sparkapp,publish,)
	@echo ">> Serving tables published. Next: make serve-cluster to query the scoring API"

.PHONY: timescale-psql
timescale-psql: ## Open a psql shell on TimescaleDB (inspect the gold.* serving tables)
	kubectl -n idp-platform exec -it deploy/timescaledb -- psql -U postgres -d idp \
	  -c "\dt gold.*" -c "SELECT count(*) FROM gold.credit_scores;"

.PHONY: timescale-verify
timescale-verify: ## Verify the publish stage: list every gold.* serving table with its row count + a sample row
	kubectl -n idp-platform exec deploy/timescaledb -- psql -U postgres -d idp -P pager=off \
	  -c "\dt+ gold.*" \
	  -c "SELECT schemaname, relname AS table, n_live_tup AS approx_rows FROM pg_stat_user_tables WHERE schemaname='gold' ORDER BY relname;" \
	  -c "SELECT tenant_id, count(*) AS rows FROM gold.credit_features GROUP BY tenant_id ORDER BY tenant_id;" \
	  -c "SELECT * FROM gold.credit_features LIMIT 3;" \
      -c "SELECT tenant_id, count(*) AS rows FROM gold.credit_scores GROUP BY tenant_id ORDER BY tenant_id;" \
	  -c "SELECT * FROM gold.credit_scores LIMIT 3;"

# --- Airflow orchestration (production Silver/Gold) -------------------------
.PHONY: airflow-wire
airflow-wire: ## Build DAG image into minikube + install Airflow (external metadata DB) + RBAC
	@echo ">> Building the Airflow DAG image into minikube (idp/airflow:dev)..."
	@eval $$(minikube docker-env) && docker build -t idp/airflow:dev -f deploy/platform/airflow/Dockerfile --build-context spark=deploy/app/spark pipelines/airflow/dags
	@echo ">> Ensuring the 'airflow' metadata database exists in TimescaleDB..."
	kubectl -n idp-platform exec deploy/timescaledb -- psql -U postgres -tc \
	  "SELECT 1 FROM pg_database WHERE datname='airflow'" | grep -q 1 || \
	  kubectl -n idp-platform exec deploy/timescaledb -- psql -U postgres -c "CREATE DATABASE airflow"
	helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
	helm repo update apache-airflow >/dev/null 2>&1 || true
	helm upgrade --install airflow apache-airflow/airflow -n idp-platform --version 1.15.0 \
	  -f deploy/platform/airflow/values.yaml --timeout 15m
	kubectl apply -f deploy/platform/airflow/rbac.yaml
	$(MAKE) airflow-reload

.PHONY: airflow-reload
airflow-reload: ## Rebuild the DAG image + restart Airflow so it re-parses the DAGs (run after any DAG change)
	@echo ">> Rebuilding the DAG image into minikube (idp/airflow:dev)..."
	@eval $$(minikube docker-env) && docker build -t idp/airflow:dev -f deploy/platform/airflow/Dockerfile --build-context spark=deploy/app/spark pipelines/airflow/dags
	# The image tag never changes, so pods won't restart on their own — force it so
	# the scheduler/dag-processor pick up the new baked DAGs (imagePullPolicy
	# IfNotPresent resolves the freshly rebuilt local tag).
	@echo ">> Restarting Airflow to re-parse DAGs..."
	-kubectl -n idp-platform rollout restart deploy/airflow-scheduler
	-kubectl -n idp-platform rollout restart deploy/airflow-dag-processor
	-kubectl -n idp-platform rollout restart deploy/airflow-webserver
	kubectl -n idp-platform rollout status deploy/airflow-scheduler --timeout=180s
	@echo ">> DAGs reloaded. New DAG ids may take ~30s to appear (DAG parse interval)."

.PHONY: airflow-ui
airflow-ui: ## Port-forward the Airflow web UI to http://localhost:8080 (admin/admin)
	@echo "Open http://localhost:8080  (default login: admin / admin)"
	kubectl -n idp-platform port-forward svc/airflow-webserver 8080:8080

.PHONY: airflow-trigger
airflow-trigger: ## Unpause the 4 DAGs + trigger features & training (inference is dataset-driven). Seed labels first: make labels
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags unpause idp_features
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags unpause idp_inference
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags unpause idp_training
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags unpause idp_maintenance
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags trigger idp_features
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags trigger idp_training
	@echo ">> idp_inference is dataset-driven: it auto-runs when gold features refresh OR a new model lands."

.PHONY: topics
topics: ## Create/update Kafka topics (KafkaTopic CRs; 7-day retention, RETENTION_DAYS=N to change)
	bash deploy/platform/kafka/create-topics.sh idp-platform

.PHONY: kafka-purge
kafka-purge: ## Purge ALL messages from the t.* Kafka topics now (TOPIC=t.tenant_a.topupBalance for one)
	bash deploy/platform/kafka/purge-topics.sh idp-platform "$(TOPIC)"

.PHONY: schemas
schemas: ## Register the Kafka topics' TMF JSON Schemas (wire contract, schemaType JSON)
	bash deploy/platform/kafka/register-schemas.sh idp-platform

.PHONY: platform-base
platform-base: ## Apply the platform base overlay (namespaces, RBAC, tenant isolation)
	kubectl apply -k deploy/platform/overlays/local

.PHONY: app-deploy
app-deploy: ## Deploy/redeploy the scoring-api app (idp-app); needs `make image` + `make platform-base`
	kubectl apply -k deploy/app/overlays/local
	# The dev image tag (idp/scoring-api:dev) never changes, so `apply` alone sees
	# no spec diff and won't restart pods — they'd keep the stale image. Force a
	# rollout so new pods pick up the freshly built local image (imagePullPolicy:
	# Never reuses minikube's docker daemon). `rollout restart` is a no-op-safe
	# annotation bump that also works right after a first-time create.
	kubectl -n idp-app rollout restart deploy/scoring-api
	kubectl -n idp-app rollout status deploy/scoring-api --timeout=180s

.PHONY: mk-down
mk-down: ## Stop minikube
	minikube stop
