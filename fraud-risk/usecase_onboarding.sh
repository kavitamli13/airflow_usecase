#!/bin/bash
# ==========================================================================
# usecase_onboarding.sh
#
# LAYER 2 — USE-CASE ONBOARDING (run once per use case, parameterized)
#
# This is what you run when someone builds a new use case on the platform.
# No Helm, no PVCs, no touching platform_install_additions.sh. It assumes
# Layer 1 has already been applied once (kafka_default / clickhouse_default
# / nessie_default connections exist, platform-models / platform-scripts
# PVCs exist and are mounted at /models and /opt/airflow/scripts).
#
# What it creates, all namespaced by --usecase so use cases never collide:
#   - a dedicated Postgres database + least-privilege service user
#   - that use case's own Postgres connection ("<usecase>_db_default")
#   - a subfolder for this use case inside the SHARED models/scripts PVCs
#   - Airflow Variables, each prefixed "<usecase>__"
#
# Requires POSTGRES_HOST / POSTGRES_PORT / POSTGRES_PASSWORD (superuser)
# to already be set in your shell — same vars your platform script uses.
#
# Usage:
#   ./usecase_onboarding.sh \
#     --usecase fraud \
#     --namespace data-platform \
#     --var min_recall=0.75 \
#     --var min_precision=0.60
# ==========================================================================

set -euo pipefail

NAMESPACE="data-platform"
USE_CASE=""
VARS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --usecase)   USE_CASE="$2";  shift 2 ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --var)       VARS+=("$2");   shift 2 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

if [[ -z "$USE_CASE" ]]; then
  echo "Usage: $0 --usecase <name> [--namespace ns] [--var key=value ...]"
  exit 1
fi

log() { echo "[onboard:${USE_CASE}] $*"; }

DB_NAME="${USE_CASE}"
DB_USER="${USE_CASE}_svc"
DB_PASSWORD="$(openssl rand -base64 24 | tr -d '=+/')"   # placeholder -- move to a real secret manager before go-live
DB_CONN_NAME="${USE_CASE}_db_default"


# --------------------------------------------------------------------------
# 1) App-scoped Postgres database + least-privilege service user.
#    Uses the platform Postgres SUPERUSER (already available from your
#    existing script), never a shared app-level credential — each use
#    case gets its own isolated DB/user pair.
# --------------------------------------------------------------------------
log "Ensuring '${DB_NAME}' database and '${DB_USER}' service user exist"

kubectl run "pg-client-check-${USE_CASE}" --rm -i --restart=Never --image=postgres:16 -n "${NAMESPACE}" \
  --env="PGPASSWORD=${POSTGRES_PASSWORD}" \
  -- psql -h "${POSTGRES_HOST}" -U postgres <<EOF
SELECT 'CREATE DATABASE ${DB_NAME}'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${DB_NAME}')\gexec

DO \$\$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = '${DB_USER}') THEN
    CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}';
  ELSE
    ALTER ROLE ${DB_USER} PASSWORD '${DB_PASSWORD}';
  END IF;
END
\$\$;

GRANT ALL PRIVILEGES ON DATABASE ${DB_NAME} TO ${DB_USER};
EOF

log "Database ready. Now run this use case's own DDL (e.g. schema.sql's"
log "Postgres section) against '${DB_NAME}' as '${DB_USER}', not postgres."


# --------------------------------------------------------------------------
# 2) Register this use case's own Postgres connection.
#    kafka_default / clickhouse_default / nessie_default already exist
#    platform-wide from Layer 1 -- nothing to register for those here.
# --------------------------------------------------------------------------
log "Registering '${DB_CONN_NAME}' connection"

kubectl exec -n "${NAMESPACE}" deploy/airflow-webserver -- \
  airflow connections add "${DB_CONN_NAME}" \
  --conn-type 'postgres' \
  --conn-host "${POSTGRES_HOST}" \
  --conn-port "${POSTGRES_PORT}" \
  --conn-login "${DB_USER}" \
  --conn-password "${DB_PASSWORD}" \
  --conn-schema "${DB_NAME}" \
  || echo "${DB_CONN_NAME} may already exist -- run 'airflow connections delete ${DB_CONN_NAME}' first to replace it."


# --------------------------------------------------------------------------
# 3) Subfolders inside the SHARED platform-models / platform-scripts PVCs.
#    No new PVCs -- just a namespaced subfolder per use case, so DAG/script
#    paths for one use case can never shadow another's.
# --------------------------------------------------------------------------
log "Creating /models/${USE_CASE}/ and /opt/airflow/scripts/${USE_CASE}/"

kubectl exec -n "${NAMESPACE}" deploy/airflow-scheduler -- \
  bash -c "mkdir -p /models/${USE_CASE} /opt/airflow/scripts/${USE_CASE}"

log "Deploy this use case's DAG + supporting scripts into"
log "  /opt/airflow/scripts/${USE_CASE}/  (e.g. via CI/CD or kubectl cp)"
log "and its trained model artifact into /models/${USE_CASE}/"


# --------------------------------------------------------------------------
# 4) Airflow Variables, namespaced "<usecase>__key" so two use cases'
#    tunables never collide (fraud__min_recall vs churn__min_recall).
# --------------------------------------------------------------------------
log "Registering ${#VARS[@]} namespaced Airflow Variable(s)"

for kv in "${VARS[@]}"; do
  key="${kv%%=*}"
  value="${kv#*=}"
  namespaced_key="${USE_CASE}__${key}"
  kubectl exec -n "${NAMESPACE}" deploy/airflow-webserver -- \
    airflow variables set "${namespaced_key}" "${value}"
  log "  ${namespaced_key} = ${value}"
done

log "Onboarding complete."
log "'${DB_USER}' password: ${DB_PASSWORD}"
log "  -> this script does not persist it anywhere -- copy it into a real"
log "     secret manager now, and reference it from ${DB_CONN_NAME} there"
log "     going forward, before go-live."


# --------------------------------------------------------------------------
# EXAMPLE INVOCATION -- fraud detection use case
# --------------------------------------------------------------------------
#   ./usecase_onboarding.sh \
#     --usecase fraud \
#     --var min_recall=0.75 \
#     --var min_precision=0.60
#
# Produces:
#   DB:          fraud            (owned by fraud_svc)
#   Connection:  fraud_db_default
#   Folders:     /models/fraud/, /opt/airflow/scripts/fraud/
#   Variables:   fraud__min_recall = 0.75, fraud__min_precision = 0.60
