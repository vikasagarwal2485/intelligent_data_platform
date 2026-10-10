# The platform's Superset image (REQ-PLT-12): the pinned upstream image plus the
# Trino and Postgres drivers, the OIDC client for the Keycloak sign-in, and the
# platform's glue module (idp_superset.py). Built into minikube: make superset-image
FROM apache/superset:6.1.0
USER root
RUN . /app/.venv/bin/activate && uv pip install --no-cache \
      "trino[sqlalchemy]==0.340.0" "psycopg2-binary>=2.9,<3" "authlib>=1.3,<2"
COPY idp_superset.py /app/idp_ext/idp_superset.py
# /app/pythonpath is where the chart mounts superset_config.py, so the module lives apart
ENV PYTHONPATH=/app/pythonpath:/app/idp_ext
USER superset
