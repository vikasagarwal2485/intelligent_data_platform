# Scoring API image (plan 5.6 / 6.6). Builds the idp package and runs the
# FastAPI service via `idp serve`.
FROM python:3.11-slim

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# Install dependencies first for better layer caching.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Install the package. Editable keeps config.py able to resolve
# config/platform.yaml relative to the project root (/app).
COPY pyproject.toml ./
COPY src ./src
COPY config ./config
RUN pip install -e .

EXPOSE 8000

# Non-root for safety.
RUN useradd -m appuser && chown -R appuser /app
USER appuser

CMD ["idp", "serve", "--host", "0.0.0.0", "--port", "8000"]
