"""
airflow_fraud_pipeline_dag.py

Daily orchestration for the fraud-detection use case:
  1. Snapshot labeled history from the Iceberg lake table for training.
  2. Retrain the model (train_fraud_model.py).
  3. Evaluate against a minimum-quality gate before promoting.
  4. Promote the candidate model to the path the streaming scorer reads from.
  5. Refresh ClickHouse daily aggregates that back the Superset dashboard.
  6. Emit dataset + lineage metadata to DataHub via REST (no Airflow plugin).

Place this file in your Airflow dags/ folder. Only standard Airflow
operators are used — no extra provider packages beyond the Postgres/HTTP
providers you likely already have.
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

MODEL_CANDIDATE_PATH = "/models/fraud_model_candidate.joblib"
MODEL_PRODUCTION_PATH = "/models/fraud_model_production.joblib"
METRICS_PATH = "/models/metrics_candidate.json"
TRAINING_SNAPSHOT_PATH = "/data/lake/fraud/transactions_scored_history.parquet"

# Minimum quality bar — below this, the DAG fails at evaluate_model and the
# previous production model keeps serving. Tune these to your actual data.
MIN_RECALL = 0.75
MIN_PRECISION = 0.60


def _evaluate_model(**context):
    with open(METRICS_PATH) as f:
        metrics = json.load(f)

    print(f"Candidate metrics: {metrics}")
    if metrics["recall"] < MIN_RECALL or metrics["precision"] < MIN_PRECISION:
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


def _emit_datahub_lineage(**context):
    from datahub_emitter import emit_dataset, emit_lineage, emit_pipeline_and_task

    kafka_urn = emit_dataset("kafka", "transactions", "Raw transaction events")
    lake_urn = emit_dataset("iceberg", "fraud.transactions_scored_history", "Full scored transaction history")
    pg_urn = emit_dataset("postgres", "fraud.transactions_scored", "Latest scored transactions (operational)")
    ch_urn = emit_dataset("clickhouse", "analytics.transactions_scored_analytics", "Analytics feed for Superset")
    model_urn = emit_dataset("file", "models/fraud_model_production", "Production fraud-scoring model artifact")

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

    snapshot_training_data = BashOperator(
        task_id="snapshot_training_data",
        bash_command=(
            "spark-submit "
            "--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0 "
            "--conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog "
            "--conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 "
            "--conf spark.sql.catalog.nessie.ref=main "
            f"-e \"spark.table('nessie.fraud.transactions_scored_history')"
            f".write.mode('overwrite').parquet('{TRAINING_SNAPSHOT_PATH}')\""
        ),
    )

    train_model = BashOperator(
        task_id="train_model",
        bash_command=(
            f"python /opt/airflow/scripts/train_fraud_model.py "
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

    refresh_clickhouse_aggregates = PostgresOperator(
        # Swap for a ClickHouse-compatible operator/hook if you don't route
        # this through Postgres; shown here as a stand-in for "run this SQL".
        task_id="refresh_clickhouse_aggregates",
        postgres_conn_id="clickhouse_analytics",
        sql="""
            INSERT INTO fraud_metrics_daily
            SELECT
                toDate(scored_at) AS metric_date,
                count(*) AS total_transactions,
                sum(is_flagged) AS flagged_transactions,
                sum(is_flagged * amount) AS flagged_amount,
                avg(fraud_probability) AS avg_fraud_probability
            FROM transactions_scored_analytics
            WHERE toDate(scored_at) = yesterday()
            GROUP BY metric_date;
        """,
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
