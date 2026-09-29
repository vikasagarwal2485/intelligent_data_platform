# The idp image: runs every stage (`idp <stage>`) and the scoring API (`idp serve`).
# Silver/Gold run on Spark (single transformation engine, REQ-ORC-05), so the
# image carries a headless JRE plus pyspark, with the S3A and Iceberg jars
# pre-installed so no stage downloads anything at run time.
# Pinned to bookworm: its default JRE is OpenJDK 17, the Java Spark 3.5 supports
# with Arrow. The floating `python:3.11-slim` tag moved to trixie (OpenJDK 21), where
# Arrow-based Spark (mapInPandas scoring) fails: "DirectByteBuffer.<init>(long, int)
# not available".
FROM python:3.11-slim-bookworm

WORKDIR /app
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    JAVA_HOME=/usr/lib/jvm/default-java

# JRE for Spark (Debian bookworm default-jre-headless = OpenJDK 17); procps for
# the `ps` Spark's launch scripts call.
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# Install dependencies first for better layer caching.
COPY requirements.txt ./
RUN pip install -r requirements.txt

# Baked into pyspark's jars dir (idp.transforms.session then skips fetching them):
#  * S3A (MinIO/S3) -- versions must match pyspark 3.5's Hadoop 3.3.4 client;
#  * Iceberg runtime + AWS bundle (S3FileIO) and the JDBC drivers of the SQL
#    catalog (SQLite locally, Postgres in the cluster) -- must match
#    idp.transforms.session.ICEBERG_PACKAGES (tests/test_catalog.py checks this).
RUN python - <<'PY'
import os, urllib.request
import pyspark
jars = os.path.join(os.path.dirname(pyspark.__file__), "jars")
base = "https://repo1.maven.org/maven2"
for path in ("org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
             "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
             "org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.10.0/iceberg-spark-runtime-3.5_2.12-1.10.0.jar",
             "org/apache/iceberg/iceberg-aws-bundle/1.10.0/iceberg-aws-bundle-1.10.0.jar",
             "org/xerial/sqlite-jdbc/3.46.1.3/sqlite-jdbc-3.46.1.3.jar",
             "org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"):
    dest = os.path.join(jars, os.path.basename(path))
    urllib.request.urlretrieve(f"{base}/{path}", dest)
    print("installed", dest, os.path.getsize(dest))
PY

# Install the package. Editable keeps config.py able to resolve
# config/platform.yaml relative to the project root (/app).
COPY pyproject.toml ./
COPY src ./src
COPY config ./config
RUN pip install -e .

# Spark on Kubernetes (REQ-PLT-10): the Spark Operator runs Silver/Gold/Score/
# publish/maintain as SparkApplications from THIS image. Their driver and executor
# pods start the container with `driver ...` / `executor ...`, which the entrypoint
# turns into spark-submit / the executor backend (pip pyspark ships the Spark
# jars, incl. spark-kubernetes, but not Spark's docker entrypoint). Any other
# command (`idp serve`, `idp consume`, ...) runs unchanged.
ENV SPARK_HOME=/usr/local/lib/python3.11/site-packages/pyspark
COPY pipelines/spark ./pipelines/spark
COPY docker/spark-entrypoint.sh /opt/spark-entrypoint.sh
RUN chmod +x /opt/spark-entrypoint.sh

EXPOSE 8000

# Non-root for safety.
RUN useradd -m appuser && chown -R appuser /app
USER appuser

ENTRYPOINT ["/opt/spark-entrypoint.sh"]
CMD ["idp", "serve", "--host", "0.0.0.0", "--port", "8000"]
