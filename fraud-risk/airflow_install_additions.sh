#!/bin/bash
# ==========================================================================
# airflow_install_additions.sh
#
# NOT a standalone script — these are blocks to merge into your existing
# install script, in the order shown. Each section header notes where it
# goes relative to what you already have.
# ==========================================================================


# --------------------------------------------------------------------------
# 1) NEW VARIABLES
#    Add alongside your existing POSTGRES_* / RABBITMQ_* / DATAHUB_* blocks,
#    near the top of the script.
# --------------------------------------------------------------------------

############################################
# Kafka
############################################
KAFKA_BOOTSTRAP_SERVERS="kafka.data-platform.svc.cluster.local:9092"

############################################
# ClickHouse
############################################
CLICKHOUSE_HOST="clickhouse.data-platform.svc.cluster.local"
CLICKHOUSE_PORT="8123"
CLICKHOUSE_DB="analytics"
CLICKHOUSE_USER="fraud_svc"
CLICKHOUSE_PASSWORD="ChangeMeClickhousePass123"   # move to a real secret manager before go-live

############################################
# Nessie / Iceberg lake
############################################
NESSIE_URI="http://nessie.data-platform.svc.cluster.local:19120/api/v1"
ICEBERG_WAREHOUSE="hdfs://hdfs-namenode.data-platform.svc.cluster.local:9000/warehouse"

############################################
# Fraud application database (separate from the Airflow metadata DB)
############################################
FRAUD_DB="fraud"
FRAUD_DB_USER="fraud_svc"
FRAUD_DB_PASSWORD="ChangeMeFraudDbPass123"        # move to a real secret manager before go-live


# --------------------------------------------------------------------------
# 2) CREATE PIPELINE CREDENTIALS SECRET
#    Add this BEFORE the "CREATE VALUES FILE" step — the values.yaml
#    references this secret via envFrom, so it must exist first.
#    Keeps ClickHouse/fraud-DB credentials out of the values file and out
#    of shell history, unlike the current plaintext POSTGRES_PASSWORD /
#    RABBITMQ_PASSWORD approach.
# --------------------------------------------------------------------------

log "Creating pipeline credentials secret"

kubectl create secret generic fraud-pipeline-creds \
  -n $NAMESPACE \
  --from-literal=CLICKHOUSE_USER="${CLICKHOUSE_USER}" \
  --from-literal=CLICKHOUSE_PASSWORD="${CLICKHOUSE_PASSWORD}" \
  --from-literal=FRAUD_DB_USER="${FRAUD_DB_USER}" \
  --from-literal=FRAUD_DB_PASSWORD="${FRAUD_DB_PASSWORD}" \
  --dry-run=client -o yaml | kubectl apply -f -


# --------------------------------------------------------------------------
# 3) CREATE /models AND /scripts PVCs
#    Add this BEFORE the "CREATE VALUES FILE" step, same reason as above —
#    the values.yaml's extraVolumes reference these claim names.
# --------------------------------------------------------------------------

log "Creating models and scripts PVCs"

cat <<EOF | kubectl apply -n $NAMESPACE -f -
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: fraud-models
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: longhorn
  resources:
    requests:
      storage: 5Gi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: fraud-scripts
spec:
  accessModes:
    - ReadWriteMany
  storageClassName: longhorn
  resources:
    requests:
      storage: 1Gi
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

  # Pulls CLICKHOUSE_USER / CLICKHOUSE_PASSWORD / FRAUD_DB_USER /
  # FRAUD_DB_PASSWORD from the secret created in step 2 above.
  envFrom:
    - secretRef:
        name: fraud-pipeline-creds

  extraVolumes:
    - name: models
      persistentVolumeClaim:
        claimName: fraud-models
    - name: scripts
      persistentVolumeClaim:
        claimName: fraud-scripts

  extraVolumeMounts:
    - name: models
      mountPath: /models
    - name: scripts
      mountPath: /opt/airflow/scripts
VALUES_YAML_SNIPPET

# Note: the Spark client itself (spark-submit binary + Iceberg/Nessie/Kafka
# jars) isn't something Helm values can add — it needs to be baked into
# your custom image (sharathcnagendran/airflow-datahub:2.11.0-fix2) at
# build time, or mounted from an image that already has it.


# --------------------------------------------------------------------------
# 5) CREATE FRAUD APPLICATION DATABASE + SERVICE USER
#    Add this right after your existing "Ensuring Airflow database exists"
#    block. Deliberately does NOT reuse the postgres superuser for the
#    pipeline's application writes.
# --------------------------------------------------------------------------

log "Ensuring fraud database and service user exist"

kubectl run pg-client-check --rm -i --restart=Never --image=postgres:16 -n data-platform \
  --env="PGPASSWORD=${POSTGRES_PASSWORD}" \
  -- psql -h "${POSTGRES_HOST}" -U postgres <<EOF
SELECT 'CREATE DATABASE ${FRAUD_DB}'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${FRAUD_DB}')\gexec

DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${FRAUD_DB_USER}') THEN
    CREATE ROLE ${FRAUD_DB_USER} LOGIN PASSWORD '${FRAUD_DB_PASSWORD}';
  END IF;
END
\$\$;

GRANT ALL PRIVILEGES ON DATABASE ${FRAUD_DB} TO ${FRAUD_DB_USER};
EOF

log "Fraud database check complete"

# After this, run schema.sql's PostgreSQL DDL against ${FRAUD_DB} (as
# ${FRAUD_DB_USER}, not postgres) to create transactions_scored.


# --------------------------------------------------------------------------
# 6) REGISTER PIPELINE CONNECTIONS
#    Add this right after your existing "REGISTER DATAHUB CONNECTION" block
#    — same pattern, same reasoning: credentials live in the Connection,
#    not hardcoded into DAG/python files.
# --------------------------------------------------------------------------

log "Registering pipeline connections"

# conn-type 'kafka' requires apache-airflow-providers-apache-kafka in your
# image. If it's not installed, use --conn-type 'generic' instead — the DAG
# and spark-submit env vars don't depend on the provider being present.
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
  --conn-login "${CLICKHOUSE_USER}" \
  --conn-password "${CLICKHOUSE_PASSWORD}" \
  --conn-schema "${CLICKHOUSE_DB}" \
  || echo "clickhouse_default may already exist -- run 'airflow connections delete clickhouse_default' first to replace it."

kubectl exec -n $NAMESPACE deploy/airflow-webserver -- \
  airflow connections add 'nessie_default' \
  --conn-type 'generic' \
  --conn-host "${NESSIE_URI}" \
  --conn-extra "{\"warehouse\": \"${ICEBERG_WAREHOUSE}\"}" \
  || echo "nessie_default may already exist -- run 'airflow connections delete nessie_default' first to replace it."

kubectl exec -n $NAMESPACE deploy/airflow-webserver -- \
  airflow connections add 'fraud_db_default' \
  --conn-type 'postgres' \
  --conn-host "${POSTGRES_HOST}" \
  --conn-port "${POSTGRES_PORT}" \
  --conn-login "${FRAUD_DB_USER}" \
  --conn-password "${FRAUD_DB_PASSWORD}" \
  --conn-schema "${FRAUD_DB}" \
  || echo "fraud_db_default may already exist -- run 'airflow connections delete fraud_db_default' first to replace it."


# --------------------------------------------------------------------------
# 7) APPLYING THIS
#    Your existing script already uses `helm upgrade --install`, so after
#    merging steps 1-4 above, you do NOT need to uninstall/reinstall —
#    just re-run the script. Order matters: secret (2) and PVCs (3) must
#    exist before the helm upgrade step runs, since the values.yaml (4)
#    references both. Steps 5 and 6 run after Airflow's webserver pod is
#    up, same as your existing DataHub registration block.
# --------------------------------------------------------------------------
