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
	@echo "Built idp/scoring-api:dev inside minikube. Now run: make deploy"

.PHONY: infra
infra: ## Add Helm repos and install platform infrastructure into idp-platform
	bash deploy/install-infra.sh idp-platform

.PHONY: kafka
kafka: ## Deploy Kafka cluster + Schema Registry, create topics, register schemas
	bash deploy/kafka/setup-kafka.sh idp-platform

.PHONY: topics
topics: ## Create Kafka topics only (KafkaTopic CRs)
	bash deploy/kafka/create-topics.sh idp-platform

.PHONY: schemas
schemas: ## Register Avro schemas only (config/avro/*.avsc)
	bash deploy/kafka/register-schemas.sh idp-platform

.PHONY: deploy
deploy: ## Apply the local overlay (namespaces, RBAC, isolation, scoring API)
	kubectl apply -k deploy/overlays/local

.PHONY: namespaces
namespaces: deploy ## Alias for `deploy` (kept for backwards compatibility)

.PHONY: mk-down
mk-down: ## Stop minikube
	minikube stop
