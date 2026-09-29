#!/bin/bash
# Container entrypoint for Spark on Kubernetes (REQ-PLT-10), adapted from Apache
# Spark 3.5's kubernetes/dockerfiles/spark/entrypoint.sh (Apache License 2.0).
#
# The Spark Operator starts the driver pod with `driver ...` and the driver starts
# executor pods with `executor`; anything else (e.g. `idp serve`, `idp consume`)
# is run as given, so the one idp image still serves every stage and the API.
set -e

SPARK_CLASSPATH="${SPARK_CLASSPATH:+$SPARK_CLASSPATH:}${SPARK_HOME}/jars/*"
env | grep SPARK_JAVA_OPT_ | sort -t_ -k4 -n | sed 's/[^=]*=\(.*\)/\1/g' > /tmp/java_opts.txt || true
readarray -t SPARK_EXECUTOR_JAVA_OPTS < /tmp/java_opts.txt
if [ -n "${SPARK_EXTRA_CLASSPATH}" ]; then SPARK_CLASSPATH="$SPARK_CLASSPATH:$SPARK_EXTRA_CLASSPATH"; fi
if [ -n "${SPARK_CONF_DIR}" ]; then SPARK_CLASSPATH="$SPARK_CONF_DIR:$SPARK_CLASSPATH"; fi
SPARK_CLASSPATH="$SPARK_CLASSPATH:$PWD"

case "$1" in
  driver)
    shift 1
    CMD=(
      "$SPARK_HOME/bin/spark-submit"
      --conf "spark.driver.bindAddress=$SPARK_DRIVER_BIND_ADDRESS"
      --conf "spark.executorEnv.SPARK_DRIVER_POD_IP=$SPARK_DRIVER_BIND_ADDRESS"
      --deploy-mode client
      "$@"
    )
    ;;
  executor)
    shift 1
    CMD=(
      "${JAVA_HOME}/bin/java"
      "${SPARK_EXECUTOR_JAVA_OPTS[@]}"
      -Xms"$SPARK_EXECUTOR_MEMORY"
      -Xmx"$SPARK_EXECUTOR_MEMORY"
      -cp "$SPARK_CLASSPATH"
      org.apache.spark.scheduler.cluster.k8s.KubernetesExecutorBackend
      --driver-url "$SPARK_DRIVER_URL"
      --executor-id "$SPARK_EXECUTOR_ID"
      --cores "$SPARK_EXECUTOR_CORES"
      --app-id "$SPARK_APPLICATION_ID"
      --hostname "$SPARK_EXECUTOR_POD_IP"
      --resourceProfileId "$SPARK_RESOURCE_PROFILE_ID"
      --podName "$SPARK_EXECUTOR_POD_NAME"
    )
    ;;
  *)
    CMD=("$@")
    ;;
esac
exec "${CMD[@]}"
