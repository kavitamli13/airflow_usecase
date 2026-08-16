#!/bin/bash
# ==========================================================================
# platform_install_additions.sh
#
# LAYER 1 — PLATFORM (run once, use-case-agnostic)
#
# NOT a standalone script — these are blocks to merge into your existing
# install script, in the order shown. Nothing in this file mentions any
# specific use case (fraud, churn, predictive maintenance, ...). It only
# provisions shared platform connectivity and shared storage.
#
# This changes rarely — only when a new *service* joins the platform
# (e.g. you add a second Kafka cluster), never when someone builds a new
# use case on top of it. New use cases run usecase_onboarding.sh instead,
# which needs none of this.
# ==========================================================================


# --------------------------------------------------------------------------
# 1) NEW PLATFORM VARIABLES
#    Add alongside your existing POSTGRES_* / RABBITMQ_* / DATAHUB_* block,
#    near the top of the script.
# --------------------------------------------------------------------------

############################################
# Kafka
############################################
KAFKA_BOOTSTRAP_SERVERS="kafka.data-platform.svc.cluster.local:9092"

############################################
# ClickHouse (platform admin/service account — used to provision and
# write to per-use-case tables inside the shared 'analytics' database.
# Individual use cases do NOT get their own ClickHouse user/database;
# they just get their own tables, namespaced by name.)
############################################
CLICKHOUSE_HOST="clickhouse.data-platform.svc.cluster.local"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_DB="analytics"
CLICKHOUSE_ADMIN_USER="platform_svc"
CLICKHOUSE_ADMIN_PASSWORD="ChangeMeClickhouseAdminPass123"   # move to a real secret manager before go-live

############################################
# Nessie / Iceberg lake
############################################
NESSIE_URI="http://nessie.data-platform.svc.cluster.local:19120/api/v1"
ICEBERG_WAREHOUSE="hdfs://hdfs-namenode.data-platform.svc.cluster.local:9000/warehouse"

############################################
# Spark job API
# All Spark work platform-wide (any use case) is submitted through this
# service rather than DAGs/scripts building their own spark-submit command.
# It fronts the spark-client pod and is reached over the existing ingress
# (see `kubectl get ingress spark-job-api-ingress -n data-platform`), so
# callers (e.g. Airflow workers) just need plain HTTP egress to it, not
# kubectl/RBAC into the cluster.
############################################
SPARK_JOB_API_HOST="jobapi.data-platform.tcs.private.cloud"
SPARK_JOB_API_PORT="80"
SPARK_JOB_API_SCHEME="http"

# Note: POSTGRES_HOST / POSTGRES_PORT / POSTGRES_PASSWORD (superuser) are
# assumed to already exist from your current script — Layer 2 reuses them
# to provision each use case's own database, it doesn't need new platform
# vars for that.


# --------------------------------------------------------------------------
# 2) CREATE PLATFORM CREDENTIALS SECRET
#    Add this BEFORE the "CREATE VALUES FILE" step — the values.yaml
#    references this secret via envFrom, so it must exist first.
#
#    Holds ADMIN-level credentials for provisioning/writing shared
#    infrastructure only. Use-case application credentials (e.g. a fraud
#    Postgres app user) are generated per use case by
#    usecase_onboarding.sh and never live here.
# --------------------------------------------------------------------------

log "Creating platform credentials secret"

kubectl create secret generic data-platform-creds \
  -n $NAMESPACE \
  --from-literal=CLICKHOUSE_ADMIN_USER="${CLICKHOUSE_ADMIN_USER}" \
  --from-literal=CLICKHOUSE_ADMIN_PASSWORD="${CLICKHOUSE_ADMIN_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -


# --------------------------------------------------------------------------
# 3) CREATE SHARED /models AND /scripts PVCs
#    Add this BEFORE the "CREATE VALUES FILE" step, same reason as above —
#    the values.yaml's extraVolumes reference these claim names.
#
#    ONE pair of PVCs for the whole platform. Every use case gets its own
#    subfolder inside them (created by usecase_onboarding.sh, not here) —
#    e.g. /models/fraud/, /models/churn/. No new PVC per use case.
#    Sized generously since multiple use cases will share this volume.
# --------------------------------------------------------------------------

log "Creating shared models and scripts PVCs"

cat <<EOF | kubectl apply -n $NAMESPACE -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: platform-models
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: longhorn
  resources:
    requests:
      storage: 50Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: platform-scripts
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: longhorn
  resources:
    requests:
      storage: 10Gi
EOF


# --------------------------------------------------------------------------
# 4) VALUES.YAML ADDITIONS
#    Insert these under the existing `workers:` block in your values.yaml
#    heredoc, as siblings of `replicas`, `resources`, `env`, `affinity`.
#    (Mirror the same env/envFrom/extraVolumes under `scheduler:` too, only
#    if any of these tasks ever run there instead of on a worker.)
# --------------------------------------------------------------------------

: <<'VALUES_YAML_SNIPPET'
workers:
  # ...existing replicas / resources / affinity stay as-is...

  env:
    # ...existing HTTP_PROXY / HTTPS_PROXY / NO_PROXY / CELERY vars stay...
    - name: KAFKA_BOOTSTRAP_SERVERS
      value: "${KAFKA_BOOTSTRAP_SERVERS}"
    - name: CLICKHOUSE_HOST
      value: "${CLICKHOUSE_HOST}"
    - name: CLICKHOUSE_PORT
      value: "${CLICKHOUSE_PORT}"
    - name: CLICKHOUSE_DB
      value: "${CLICKHOUSE_DB}"
    - name: NESSIE_URI
      value: "${NESSIE_URI}"
    - name: ICEBERG_WAREHOUSE
      value: "${ICEBERG_WAREHOUSE}"

  # Pulls CLICKHOUSE_ADMIN_USER / CLICKHOUSE_ADMIN_PASSWORD from the
  # secret created in step 2 above.
  envFrom:
    - secretRef:
        name: data-platform-creds

  extraVolumes:
    - name: models
      persistentVolumeClaim:
        claimName: platform-models
    - name: scripts
      persistentVolumeClaim:
        claimName: platform-scripts

  extraVolumeMounts:
    - name: models
      mountPath: /models
    - name: scripts
      mountPath: /opt/airflow/scripts
VALUES_YAML_SNIPPET

# Note: DAGs no longer need the Spark client binary (spark-submit +
# Iceberg/Nessie/Kafka jars) baked into the Airflow image. All Spark work
# is submitted to spark-job-api instead (step 5 below registers the
# connection); the Airflow worker only needs plain HTTP egress to it.


# --------------------------------------------------------------------------
# 5) REGISTER PLATFORM CONNECTIONS
#    Add this right after your existing "REGISTER DATAHUB CONNECTION" block
#    — same pattern, same reasoning: credentials live in the Connection,
#    not hardcoded into DAG/python files. These are shared by every use
#    case; nothing use-case-specific is registered here.
# --------------------------------------------------------------------------

log "Registering platform connections"

# conn-type 'kafka' requires apache-airflow-providers-apache-kafka in your
# image. If it's not installed, use --conn-type 'generic' instead — no
# DAG or spark-submit config depends on the provider being present.
kubectl exec -n $NAMESPACE deploy/airflow-webserver -- \
  airflow connections add 'kafka_default' \
  --conn-type 'kafka' \
  --conn-host "${KAFKA_BOOTSTRAP_SERVERS}" \
  || echo "kafka_default may already exist -- run 'airflow connections delete kafka_default' first to replace it."

kubectl exec -n $NAMESPACE deploy/airflow-webserver -- \
  airflow connections add 'clickhouse_default' \
  --conn-type 'generic' \
  --conn-host "${CLICKHOUSE_HOST}" \
  --conn-port "${CLICKHOUSE_PORT}" \
  --conn-login "${CLICKHOUSE_ADMIN_USER}" \
  --conn-password "${CLICKHOUSE_ADMIN_PASSWORD}" \
  --conn-schema "${CLICKHOUSE_DB}" \
  || echo "clickhouse_default may already exist -- run 'airflow connections delete clickhouse_default' first to replace it."

kubectl exec -n $NAMESPACE deploy/airflow-webserver -- \
  airflow connections add 'nessie_default' \
  --conn-type 'generic' \
  --conn-host "${NESSIE_URI}" \
  --conn-extra "{\"warehouse\": \"${ICEBERG_WAREHOUSE}\"}" \
  || echo "nessie_default may already exist -- run 'airflow connections delete nessie_default' first to replace it."

# Fronts the spark-client pod. conn-host is just the ingress hostname (no
# scheme/port) -- Airflow Connections keep those in --conn-schema /
# --conn-port respectively, and airflow_fraud_pipeline_dag.py's
# _spark_job_api_base_url() reassembles them into a URL at task run time.
kubectl exec -n $NAMESPACE deploy/airflow-webserver -- \
  airflow connections add 'spark_job_api_default' \
  --conn-type 'http' \
  --conn-host "${SPARK_JOB_API_HOST}" \
  --conn-port "${SPARK_JOB_API_PORT}" \
  --conn-schema "${SPARK_JOB_API_SCHEME}" \
  || echo "spark_job_api_default may already exist -- run 'airflow connections delete spark_job_api_default' first to replace it."


# --------------------------------------------------------------------------
# 6) APPLYING THIS
#    Your existing script already uses `helm upgrade --install`, so after
#    merging steps 1-4 above, you do NOT need to uninstall/reinstall —
#    just re-run the script. Order matters: secret (2) and PVCs (3) must
#    exist before the helm upgrade step runs, since the values.yaml (4)
#    references both. Step 5 runs after the webserver pod is up, same as
#    your existing DataHub registration block.
#
#    After this runs once, ship new use cases with usecase_onboarding.sh
#    — no Helm, no PVCs, no editing this file.
# --------------------------------------------------------------------------
