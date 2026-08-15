"""
airflow_fraud_pipeline_dag.py

Daily orchestration for the fraud-detection use case:
  1. Snapshot labeled history from the Iceberg lake table for training.
  2. Retrain the model (train_fraud_model.py).
  3. Evaluate against a minimum-quality gate before promoting.
  4. Promote the candidate model to the path the streaming scorer reads from.
  5. Refresh ClickHouse daily aggregates that back the Superset dashboard.
  6. Emit dataset + lineage metadata to DataHub via REST (no Airflow plugin).

This DAG assumes the two-layer provisioning split:
  - Layer 1 (platform_install_additions.sh, run once): registers the
    shared 'clickhouse_default' / 'nessie_default' / 'kafka_default'
    connections and mounts the shared platform-models / platform-scripts
    PVCs at /models and /opt/airflow/scripts.
  - Layer 2 (usecase_onboarding.sh --usecase fraud, run once for this use
    case): creates the 'fraud_db_default' connection, the fraud__* Airflow
    Variables, and the /models/fraud/ + /opt/airflow/scripts/fraud/
    subfolders this DAG reads from and writes to.

Configuration lookup pattern:
  - Credentials & endpoints (ClickHouse, Nessie)   -> Airflow Connections
  - Business/pipeline parameters (quality gate)     -> Airflow Variables,
    namespaced "fraud__..." so they never collide with another use case's
    variables of the same name.

Place this file in /opt/airflow/scripts/fraud/ (not the shared scripts
root), alongside train_fraud_model.py and datahub_emitter.py.
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Namespaced under the shared platform-models / platform-scripts PVCs by
# usecase_onboarding.sh -- do NOT write to /models/ directly, only /models/fraud/.
USE_CASE = "fraud"
MODEL_DIR = f"/models/{USE_CASE}"
SCRIPTS_DIR = f"/opt/airflow/scripts/{USE_CASE}"

MODEL_CANDIDATE_PATH = f"{MODEL_DIR}/fraud_model_candidate.joblib"
MODEL_PRODUCTION_PATH = f"{MODEL_DIR}/fraud_model_production.joblib"
METRICS_PATH = f"{MODEL_DIR}/metrics_candidate.json"
TRAINING_SNAPSHOT_PATH = "/data/lake/fraud/transactions_scored_history.parquet"


def _evaluate_model(**context):
    with open(METRICS_PATH) as f:
        metrics = json.load(f)

    # Tunable from Airflow UI -> Admin -> Variables, no redeploy needed.
    # Namespaced "fraud__..." (set by usecase_onboarding.sh) so this never
    # collides with another use case's own min_recall/min_precision.
    min_recall = float(Variable.get("fraud__min_recall", default_var=0.75))
    min_precision = float(Variable.get("fraud__min_precision", default_var=0.60))

    print(f"Candidate metrics: {metrics} (gate: recall>={min_recall}, precision>={min_precision})")
    if metrics["recall"] < min_recall or metrics["precision"] < min_precision:
        raise ValueError(
            f"Candidate model failed quality gate "
            f"(recall={metrics['recall']:.2f}, precision={metrics['precision']:.2f}). "
            f"Production model is unchanged."
        )
    context["ti"].xcom_push(key="metrics", value=metrics)


def _promote_model(**context):
    import shutil
    shutil.copyfile(MODEL_CANDIDATE_PATH, MODEL_PRODUCTION_PATH)
    print(f"Promoted {MODEL_CANDIDATE_PATH} -> {MODEL_PRODUCTION_PATH}")
    # The streaming scorer re-checks this path every micro-batch, so no
    # restart is required — the new model is picked up within ~10 seconds.


def _refresh_clickhouse_aggregates(**context):
    """
    Uses the platform-wide 'clickhouse_default' Connection (Layer 1,
    admin-level service account shared across use cases) rather than a
    hardcoded host/credentials. This use case only owns its own TABLES
    (transactions_scored_analytics, fraud_metrics_daily) inside the
    shared 'analytics' database -- it doesn't get its own ClickHouse
    database or user.
    """
    import clickhouse_connect

    conn = BaseHook.get_connection("clickhouse_default")
    client = clickhouse_connect.get_client(
        host=conn.host,
        port=conn.port or 8123,
        username=conn.login,
        password=conn.password,
        database=conn.schema or "analytics",
    )
    client.command("""
        INSERT INTO fraud_metrics_daily
        SELECT
            toDate(scored_at) AS metric_date,
            count(*) AS total_transactions,
            sum(is_flagged) AS flagged_transactions,
            sum(is_flagged * amount) AS flagged_amount,
            avg(fraud_probability) AS avg_fraud_probability
        FROM transactions_scored_analytics
        WHERE toDate(scored_at) = yesterday()
        GROUP BY metric_date
    """)


def _emit_datahub_lineage(**context):
    from datahub_emitter import emit_dataset, emit_lineage, emit_pipeline_and_task

    kafka_urn = emit_dataset("kafka", "transactions", "Raw transaction events")
    lake_urn = emit_dataset("iceberg", "fraud.transactions_scored_history", "Full scored transaction history")
    pg_urn = emit_dataset("postgres", "fraud.transactions_scored", "Latest scored transactions (operational)")
    ch_urn = emit_dataset("clickhouse", "analytics.transactions_scored_analytics", "Analytics feed for Superset")
    model_urn = emit_dataset("file", f"models/{USE_CASE}/fraud_model_production", "Production fraud-scoring model artifact")

    emit_lineage([kafka_urn], lake_urn)
    emit_lineage([kafka_urn], pg_urn)
    emit_lineage([lake_urn], ch_urn)
    emit_lineage([lake_urn], model_urn)

    emit_pipeline_and_task(
        flow_id="fraud_detection_pipeline",
        flow_name="Fraud detection retraining + lineage pipeline",
        task_id="daily_retrain_run",
        task_name="Daily retrain, evaluate, promote",
        input_urns=[lake_urn],
        output_urns=[model_urn, ch_urn],
    )


with DAG(
    dag_id="fraud_detection_pipeline",
    default_args=default_args,
    description="Daily retrain + promote + lineage for the real-time fraud detection use case",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["fraud-detection", "demo"],
) as dag:

    # {{ conn.nessie_default.host }} / .extra_dejson.warehouse resolve at
    # task-render time from the platform-wide 'nessie_default' Connection
    # (Layer 1) -- nothing about Nessie's location is hardcoded here.
    snapshot_training_data = BashOperator(
        task_id="snapshot_training_data",
        bash_command=(
            "spark-submit "
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 "
            "--conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog "
            "--conf spark.sql.catalog.nessie.uri={{ conn.nessie_default.host }} "
            "--conf spark.sql.catalog.nessie.ref=main "
            "--conf spark.sql.catalog.nessie.warehouse={{ conn.nessie_default.extra_dejson.warehouse }} "
            f"-e \"spark.table('nessie.fraud.transactions_scored_history')"
            f".write.mode('overwrite').parquet('{TRAINING_SNAPSHOT_PATH}')\""
        ),
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command=(
            f"python {SCRIPTS_DIR}/train_fraud_model.py "
            f"--input {TRAINING_SNAPSHOT_PATH} "
            f"--model-out {MODEL_CANDIDATE_PATH} "
            f"--metrics-out {METRICS_PATH}"
        ),
    )

    evaluate_model = PythonOperator(
        task_id="evaluate_model",
        python_callable=_evaluate_model,
    )

    promote_model = PythonOperator(
        task_id="promote_model",
        python_callable=_promote_model,
    )

    refresh_clickhouse_aggregates = PythonOperator(
        task_id="refresh_clickhouse_aggregates",
        python_callable=_refresh_clickhouse_aggregates,
    )

    emit_datahub_lineage = PythonOperator(
        task_id="emit_datahub_lineage",
        python_callable=_emit_datahub_lineage,
    )

    (
        snapshot_training_data
        >> train_model
        >> evaluate_model
        >> promote_model
        >> refresh_clickhouse_aggregates
        >> emit_datahub_lineage
    )
