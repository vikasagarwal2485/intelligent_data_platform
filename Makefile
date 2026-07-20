.DEFAULT_GOAL := help
SHELL := /bin/bash
VENV := .venv
PY := $(VENV)/bin/python
PIP := $(VENV)/bin/pip
NAMESPACE_PLATFORM := idp-platform
TENANTS := tenant-a tenant-b

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
install: $(VENV) ## Install the package and dev dependencies
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
serve: ## Start the FastAPI scoring service on :8000
	$(PY) -m idp.cli serve

.PHONY: mcp
mcp: ## Start the MCP analytics server (stdio): 5 read-only, tenant-scoped Silver tools
	$(PY) -m idp.cli mcp

.PHONY: mcp-smoke
mcp-smoke: ## Verify the MCP tools (local Silver, or MinIO when IDP_S3_ENDPOINT is set)
	@if [ -n "$$IDP_S3_ENDPOINT" ]; then echo ">> Reading Silver from S3/MinIO at $$IDP_S3_ENDPOINT"; \
	 elif [ ! -d data/lake/silver/dim_subscriber ]; then echo ">> Silver lake empty; running pipeline..." && $(PY) -m idp.cli run >/dev/null; fi
	@echo ">> analyze_subscribers_tool(tenant_a):"
	@$(PY) -c "import json,idp.mcp.tools as t; print(json.dumps(t.analyze_subscribers_tool('tenant_a'), indent=2))"
	@echo ">> analyze_fee_charges_tool(tenant_a):"
	@$(PY) -c "import json,idp.mcp.tools as t; print(json.dumps(t.analyze_fee_charges_tool('tenant_a'), indent=2)[:800])"

.PHONY: mcp-config
mcp-config: ## Print the Claude Desktop MCP server config (paste into claude_desktop_config.json)
	@printf '%s\n' \
	  '{' \
	  '  "mcpServers": {' \
	  '    "idp-analytics": {' \
	  '      "command": "/bin/sh",' \
	  '      "args": ["-c", "cd $(CURDIR) && exec $(abspath $(PY)) -m idp.cli mcp"]' \
	  '    }' \
	  '  }' \
	  '}'
	@echo ""
	@echo "# Paste the block above into your Claude Desktop config, then restart it:"
	@echo "#   macOS:   ~/Library/Application Support/Claude/claude_desktop_config.json"
	@echo "#   Windows: %APPDATA%\\Claude\\claude_desktop_config.json"
	@echo "# The 'cd $(CURDIR)' makes the local Silver lake (data/lake/silver) resolve."
	@echo "# Populate it first with 'make run' (or use 'make mcp-config-minio' for MinIO)."

.PHONY: mcp-config-minio
mcp-config-minio: ## Print the Claude Desktop MCP config for MinIO mode (Silver from MinIO via 127.0.0.1:9000)
	@printf '%s\n' \
	  '{' \
	  '  "mcpServers": {' \
	  '    "idp-analytics": {' \
	  '      "command": "$(abspath $(PY))",' \
	  '      "args": ["-m", "idp.cli", "mcp"],' \
	  '      "env": {' \
	  '        "IDP_S3_ENDPOINT": "http://127.0.0.1:9000",' \
	  '        "IDP_S3_ACCESS_KEY": "minioadmin",' \
	  '        "IDP_S3_SECRET_KEY": "minioadmin123",' \
	  '        "IDP_S3_BUCKET_PREFIX": "idp"' \
	  '      }' \
	  '    }' \
	  '  }' \
	  '}'
	@echo ""
	@echo "# MinIO mode. Prereqs: 'make install' (installs mcp), Silver present in idp-silver,"
	@echo "# and keep 'make minio-api' running (forwards 127.0.0.1:9000) while Claude uses it."
	@echo "# Paste into (create if missing), then fully quit (Cmd-Q) and reopen Claude Desktop:"
	@echo "#   ~/Library/Application Support/Claude/claude_desktop_config.json"

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
	@eval $$(minikube docker-env) && docker build -t idp/scoring-api:dev .
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
minio-ls: ## List the lake objects in MinIO (idp-bronze / idp-silver / idp-gold)
	kubectl -n idp-platform run mc --rm -i --restart=Never --image=minio/mc --command -- /bin/sh -c \
	  'mc alias set m http://minio:9000 minioadmin minioadmin123 >/dev/null && for b in idp-bronze idp-silver idp-gold; do echo "== $$b =="; mc ls -r m/$$b/; done'

.PHONY: minio-reset
minio-reset: ## Empty the lake buckets (idp-bronze/silver/gold) for a clean re-run
	# Full RECREATE, not a shallow `rm -r`. `mc rm -r --force` does NOT remove
	# object versions or delete markers, so on a versioned/suspended bucket it
	# leaves `null`-version delete markers on the reset keys (e.g. credit_features/,
	# fraud_features/) that then SHADOW new writes to those same keys — the object
	# PUTs succeed but list/get return nothing. `mc rb --force` drops the bucket and
	# ALL versions/markers/ILM; `mc mb` recreates it clean (unversioned by default).
	kubectl -n idp-platform run mc --rm -i --restart=Never --image=minio/mc --command -- /bin/sh -c \
	  'mc alias set m http://minio:9000 minioadmin minioadmin123 >/dev/null && for b in idp-bronze idp-silver idp-gold idp-ml; do echo "recreating $$b"; mc rb --force m/$$b/ 2>/dev/null || true; mc mb --ignore-existing m/$$b; done; echo done'

.PHONY: minio-versions
minio-versions: ## Diagnose hidden bucket state (versioning, delete markers, ILM) — a plain `ls` hides these
	kubectl -n idp-platform run mc --rm -i --restart=Never --image=minio/mc --command -- /bin/sh -c \
	  'mc alias set m http://minio:9000 minioadmin minioadmin123 >/dev/null && for b in idp-bronze idp-silver idp-gold idp-ml; do echo "== $$b =="; echo "-- versioning:"; mc version info m/$$b; echo "-- ILM rules:"; mc ilm rule ls m/$$b 2>/dev/null || echo "(none)"; echo "-- versions/delete-markers:"; mc ls --versions -r m/$$b/; done'

.PHONY: minio-mkbuckets
minio-mkbuckets: ## Create the lake buckets if missing (idp-bronze/silver/gold/ml) — idempotent repair
	kubectl -n idp-platform run mc --rm -i --restart=Never --image=minio/mc --command -- /bin/sh -c \
	  'mc alias set m http://minio:9000 minioadmin minioadmin123 >/dev/null && mc mb --ignore-existing m/idp-bronze m/idp-silver m/idp-gold m/idp-ml && echo "buckets:" && mc ls m/'

.PHONY: minio-console
minio-console: ## Port-forward the MinIO web console to http://localhost:9001 (minioadmin/minioadmin123)
	@echo "Open http://localhost:9001  (user: minioadmin  pass: minioadmin123)"
	kubectl -n idp-platform port-forward svc/minio-console 9001:9001

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
stream: ## Ingestion (idp-app): produce -> Kafka, then consume -> Bronze (S3). Bronze stage only.
	kubectl -n idp-app delete job idp-produce idp-consume --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/produce-job.yaml
	@echo ">> Producing... (follow: kubectl -n idp-app logs -f job/idp-produce)"
	kubectl -n idp-app wait --for=condition=complete job/idp-produce --timeout=900s
	kubectl -n idp-app apply -f deploy/app/jobs/consume-job.yaml
	@echo ">> Consuming -> Bronze... (follow: kubectl -n idp-app logs -f job/idp-consume)"
	kubectl -n idp-app wait --for=condition=complete job/idp-consume --timeout=600s
	@echo ">> Bronze ready. Next: make silver, then make gold."

.PHONY: silver
silver: ## Silver stage (idp-app): Bronze (S3) -> canonical Silver (S3)
	kubectl -n idp-app delete job idp-silver --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/silver-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-silver --timeout=600s
	kubectl -n idp-app logs job/idp-silver

.PHONY: gold
gold: ## Gold stage (idp-app): Silver (S3) -> feature tables ONLY (S3)
	kubectl -n idp-app delete job idp-gold --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/gold-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-gold --timeout=600s
	kubectl -n idp-app logs job/idp-gold
	@echo ">> Gold features written. Run 'make score' to add credit/fraud scores."

.PHONY: score
score: ## Score stage (idp-app): Gold features + models (store) -> score tables (S3)
	kubectl -n idp-app delete job idp-score --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/score-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-score --timeout=600s
	kubectl -n idp-app logs job/idp-score

.PHONY: labels
labels: ## Seed the labels/outcomes store (idp-app): -> s3://idp-ml/labels (training input)
	kubectl -n idp-app delete job idp-labels --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/labels-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-labels --timeout=600s
	kubectl -n idp-app logs job/idp-labels

.PHONY: train
train: ## Train stage (idp-app): Gold features + labels store -> models (s3://idp-ml). Needs 'make labels' first.
	kubectl -n idp-app delete job idp-train --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/train-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-train --timeout=600s
	kubectl -n idp-app logs job/idp-train
	@echo ">> Models written to s3://idp-ml. 'make score' now writes credit/fraud scores."

.PHONY: publish
publish: ## Serving stage (idp-app): Gold (S3) -> TimescaleDB (indexed serving copy)
	kubectl -n idp-app delete job idp-publish --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/publish-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-publish --timeout=600s
	kubectl -n idp-app logs job/idp-publish

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
	  -c "SELECT * FROM gold.credit_features LIMIT 3;"

# --- Airflow orchestration (production Silver/Gold) -------------------------
.PHONY: airflow-wire
airflow-wire: ## Build DAG image into minikube + install Airflow (external metadata DB) + RBAC
	@echo ">> Building the Airflow DAG image into minikube (idp/airflow:dev)..."
	@eval $$(minikube docker-env) && docker build -t idp/airflow:dev -f deploy/platform/airflow/Dockerfile pipelines/airflow/dags
	@echo ">> Ensuring the 'airflow' metadata database exists in TimescaleDB..."
	kubectl -n idp-platform exec deploy/timescaledb -- psql -U postgres -tc \
	  "SELECT 1 FROM pg_database WHERE datname='airflow'" | grep -q 1 || \
	  kubectl -n idp-platform exec deploy/timescaledb -- psql -U postgres -c "CREATE DATABASE airflow"
	helm repo add apache-airflow https://airflow.apache.org >/dev/null 2>&1 || true
	helm repo update apache-airflow >/dev/null 2>&1 || true
	helm upgrade --install airflow apache-airflow/airflow -n idp-platform --version 1.15.0 \
	  -f deploy/platform/airflow/values.yaml --timeout 15m
	kubectl apply -f deploy/platform/airflow/rbac.yaml

.PHONY: airflow-ui
airflow-ui: ## Port-forward the Airflow web UI to http://localhost:8080 (admin/admin)
	@echo "Open http://localhost:8080  (default login: admin / admin)"
	kubectl -n idp-platform port-forward svc/airflow-webserver 8080:8080

.PHONY: airflow-trigger
airflow-trigger: ## Trigger the Silver->Gold DAG now (unpause + trigger) via the scheduler pod
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags unpause idp_silver_gold
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow dags trigger idp_silver_gold

# --- Spark (production Silver / Gold) ---------------------------------------
.PHONY: spark-configmap
spark-configmap: ## (Re)create the idp-silver-spark + idp-gold-spark ConfigMaps (Spark job scripts)
	kubectl -n idp-app delete configmap idp-silver-spark idp-gold-spark --ignore-not-found
	kubectl -n idp-app create configmap idp-silver-spark --from-file=silver_job.py=pipelines/spark/silver_job.py
	kubectl -n idp-app create configmap idp-gold-spark --from-file=gold_job.py=pipelines/spark/gold_job.py

.PHONY: spark-silver
spark-silver: spark-configmap ## Run the Silver stage as a Spark job (spark-submit, reads Bronze -> writes Silver)
	kubectl -n idp-app delete job idp-spark-silver --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/spark-silver-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-spark-silver --timeout=900s
	kubectl -n idp-app logs job/idp-spark-silver

.PHONY: spark-gold
spark-gold: spark-configmap ## Run the Gold stage as a Spark job (spark-submit, reads Silver -> writes Gold)
	kubectl -n idp-app delete job idp-spark-gold --ignore-not-found
	kubectl -n idp-app apply -f deploy/app/jobs/spark-gold-job.yaml
	kubectl -n idp-app wait --for=condition=complete job/idp-spark-gold --timeout=900s
	kubectl -n idp-app logs job/idp-spark-gold

.PHONY: silver-engine
silver-engine: ## Set the Airflow Silver engine: make silver-engine ENGINE=spark|pandas
	@test -n "$(ENGINE)" || { echo "usage: make silver-engine ENGINE=spark|pandas"; exit 1; }
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow variables set idp_silver_engine "$(ENGINE)"
	@echo ">> idp_silver_engine=$(ENGINE). For spark, ensure the script CM exists: make spark-configmap"

.PHONY: gold-engine
gold-engine: ## Set the Airflow Gold engine: make gold-engine ENGINE=spark|pandas
	@test -n "$(ENGINE)" || { echo "usage: make gold-engine ENGINE=spark|pandas"; exit 1; }
	kubectl -n idp-platform exec deploy/airflow-scheduler -- airflow variables set idp_gold_engine "$(ENGINE)"
	@echo ">> idp_gold_engine=$(ENGINE). For spark, ensure the script CM exists: make spark-configmap"

.PHONY: topics
topics: ## Create/update Kafka topics (KafkaTopic CRs; 7-day retention, RETENTION_DAYS=N to change)
	bash deploy/platform/kafka/create-topics.sh idp-platform

.PHONY: kafka-purge
kafka-purge: ## Purge ALL messages from the t.* Kafka topics now (TOPIC=t.tenant_a.recharge for one)
	bash deploy/platform/kafka/purge-topics.sh idp-platform "$(TOPIC)"

.PHONY: schemas
schemas: ## Register Avro schemas only (config/avro/*.avsc)
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
