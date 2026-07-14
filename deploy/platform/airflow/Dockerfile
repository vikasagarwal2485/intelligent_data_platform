# Minimal Airflow image with the IDP DAGs baked in.
#
# The source repo is private, so git-sync can't clone it without credentials
# (git-sync-init crashloops with an auth error). Baking the DAGs into a local
# image removes any dependency on GitHub egress or credentials for the demo.
#
# Build INTO minikube's docker daemon (no registry needed) — `make airflow-wire`
# does this for you. The build CONTEXT is the dags directory itself, so the repo
# root .dockerignore (which excludes pipelines/) does not apply:
#   eval $(minikube docker-env) && \
#     docker build -t idp/airflow:dev -f deploy/airflow/Dockerfile pipelines/airflow/dags
#
# Base tag matches the pinned chart 1.15.0 (Airflow 2.9.3). The official image
# already bundles the cncf.kubernetes provider the DAGs need.
FROM apache/airflow:2.9.3

COPY . /opt/airflow/dags/
