"""
airflow_fraud_pipeline_dag.py

Daily orchestration for the fraud-detection use case:
  1. Snapshot labeled history from the Iceberg lake table for training
     -- submitted as a JAR job to spark-job-api (NOT a raw spark-submit
     issued by this DAG; see _submit_spark_job / _poll_spark_job below).
  2. Retrain the model (train_fraud_model.py) -- this is a plain
     scikit-learn script, not a Spark job, so it still runs directly via
     BashOperator on the Airflow worker.
  3. Evaluate against a minimum-quality gate before promoting.
  4. Promote the candidate model to the path the streaming scorer reads from.
  5. Refresh ClickHouse daily aggregates that back the Superset dashboard.
  6. Emit dataset + lineage metadata to DataHub via REST (no Airflow plugin).

This DAG assumes the two-layer provisioning split:
  - Layer 1 (platform_install_additions.sh, run once): registers the
    shared 'clickhouse_default' / 'nessie_default' / 'kafka_default' /
    'spark_job_api_default' connections and mounts the shared
    platform-models / platform-scripts PVCs at /models and
    /opt/airflow/scripts.
  - Layer 2 (usecase_onboarding.sh --usecase fraud, run once for this use
    case): creates the 'fraud_db_default' connection, the fraud__* Airflow
    Variables (including fraud__snapshot_artifact_path and
    fraud__snapshot_entry_point -- see below), and the /models/fraud/ +
    /opt/airflow/scripts/fraud/ subfolders this DAG reads from and writes to.

Configuration lookup pattern:
  - Credentials & endpoints (ClickHouse, Nessie, spark-job-api) -> Airflow
    Connections
  - Business/pipeline parameters (quality gate, spark-job-api artifact
    paths) -> Airflow Variables, namespaced "fraud__..." so they never
    collide with another use case's variables of the same name.

SPARK JOB SUBMISSION -- IMPORTANT
----------------------------------
The platform does not allow DAGs to build and issue their own
`spark-submit` command. All Spark work goes through the spark-job-api
service instead, which builds and runs the spark-submit command on the
spark-client pod on our behalf. The API is reached over the platform
ingress (see 'spark_job_api_default' Connection, registered in
platform_install_additions.sh) so this DAG makes a plain HTTP call --
it does not need `kubectl` inside the Airflow worker image.

Because spark-job-api currently only supports job_type="jar" with a Java
entry_point, the Iceberg snapshot step below is NOT the inline PySpark
one-liner the previous version of this DAG ran with `spark-submit -e ...`.
It instead submits a pre-built, pre-uploaded JAR that performs the same
"read nessie.fraud.transactions_scored_history, write it out as parquet"
step. That JAR is not included in this repo -- see the note on
fraud__snapshot_artifact_path below for what needs to be built and hosted
before this task will run.

ASSUMPTIONS flagged inline (confirm against the real spark-job-api spec
and adjust _submit_spark_job / _poll_spark_job if wrong):
  - POST {base_url}/jobs/submit returns JSON containing a job id under
    either "job_id" or "id".
  - GET {base_url}/jobs/{job_id} returns JSON containing a "status" field
    that is one of a small set of terminal/non-terminal strings.
  - The submit payload accepts an optional "args" list of strings passed
    through to the JAR's main(). If spark-job-api does not support this,
    the JAR will need to read its parameters from env vars / a config
    file baked into the artifact instead, and job_args below can be
    dropped.

Place this file in /opt/airflow/scripts/fraud/ (not the shared scripts
root), alongside train_fraud_model.py and datahub_emitter.py.
"""

import json
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

import os

extra = "localhost,127.0.0.1,10.96.0.0/12,10.244.0.0/16,10.174.222.148,10.174.222.144,10.174.222.47,10.174.222.101,10.174.222.179,10.174.222.87,10.174.222.68,.svc,.cluster.local,10.0.0.0/8,172.16.0.0/12,192.168.0.0/16,kubernetes.default,::1,activemq.activemq-artemis-operator.tcs.private.cloud,cassandra.cassandra.tcs.private.cloud,data-platform.tcs.private.cloud,redisinsight-redis-ha-tenant.tcs.private.cloud,rabbitmq-rabbitmq-tenant.tcs.private.cloud,datahub.datahub-tenant.tcs.private.cloud,kafka-data-platform.tcs.private.cloud,fission-data-platform.tcs.private.cloud,mongodb-data-platform.tcs.private.cloud,apisix-data-platform.tcs.private.cloud,cli-server-data-platform.tcs.private.cloud,clickhouse-data-platform.tcs.private.cloud,hdfs.data-platform.tcs.private.cloud,seatunnel.seatunnel.tcs.private.cloud,hive.data-platform.tcs.private.cloud,spark.data-platform.tcs.private.cloud,jobapi.data-platform.tcs.private.cloud,superset-superset-tenant-a.tcs.private.cloud,zookeeper.zookeeper.tcs.private.cloud,kubeflow.kubeflow.tcs.private.cloud,airflow.data-platform.tcs.private.cloud,superset-superset-tenant-a.tcs.private.cloud"

#existing = os.environ.get("NO_PROXY", "")
os.environ["NO_PROXY"] = extra  
os.environ["no_proxy"] = extra


default_args = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# Namespaced under the shared platform-models / platform-scripts PVCs by
# usecase_onboarding.sh -- do NOT write to /models/ directly, only /models/fraud/.
USE_CASE = "fraud"
MODEL_DIR = f"/models/{USE_CASE}"
#SCRIPTS_DIR = f"/opt/airflow/scripts/{USE_CASE}"
SCRIPTS_DIR = f"/opt/airflow/dags/data-platform/airflow_usecase/fraud-risk/"
MODEL_CANDIDATE_PATH = f"{MODEL_DIR}/fraud_model_candidate.joblib"
MODEL_PRODUCTION_PATH = f"{MODEL_DIR}/fraud_model_production.joblib"
METRICS_PATH = f"{MODEL_DIR}/metrics_candidate.json"
TRAINING_SNAPSHOT_PATH = "hdfs://hdfscluster/data/lake/fraud/transactions_scored_history.parquet"


# --------------------------------------------------------------------------
# spark-job-api client helpers -- shared by any task in this DAG (or a
# future one) that needs to run something on Spark. Nothing here is
# fraud-specific; if a second use case needs the same thing, lift this
# into a shared module under the platform scripts root instead of copying it.
# --------------------------------------------------------------------------

def _spark_job_api_base_url() -> str:
    """
    Resolves from the platform-wide 'spark_job_api_default' Connection
    (Layer 1), reached via the existing ingress
    (jobapi.data-platform.tcs.private.cloud) rather than `kubectl exec`
    into the pod -- keeps this DAG a plain HTTP client with no cluster
    RBAC of its own.
    """
    conn = BaseHook.get_connection("spark_job_api_default")
    scheme = conn.schema or "http"
    port = f":{conn.port}" if conn.port else ""
    return f"{scheme}://{conn.host}{port}"


def _submit_spark_job(name: str, artifact_path: str, entry_point: str,
                       job_type: str = "jar", job_args=None) -> str:
    """
    POSTs a job to spark-job-api. `artifact_path` must already be
    reachable by the spark-job-api pod (e.g. a raw GitHub URL, or an
    internal artifact store URL) -- this function does NOT upload
    anything, it only references something uploaded ahead of time.
    Returns the job id spark-job-api assigns.
    """
    payload = {
        "name": name,
        "job_type": job_type,
        "artifact_path": artifact_path,
        "entry_point": entry_point,
    }
    if job_args:
        # ASSUMPTION: spark-job-api accepts an "args" list passed through
        # to the JAR's main(). Confirm against the real API spec.
        payload["args"] = job_args

    base_url = _spark_job_api_base_url()
    resp = requests.post(f"{base_url}/jobs/submit", json=payload, timeout=300)
    resp.raise_for_status()
    body = resp.json()
    job_id = body.get("job_id") or body.get("id")
    if not job_id:
        raise ValueError(f"spark-job-api response had no job id: {body}")
    return job_id


def _poll_spark_job(job_id: str, poll_interval: int, timeout: int):
    """
    Polls spark-job-api until the job reaches a terminal state.
    ASSUMPTION: GET {base_url}/jobs/{job_id} returns {"status": "..."}.
    Adjust the endpoint path and the status strings below to match the
    real API once confirmed.
    """
    base_url = _spark_job_api_base_url()
    terminal_success = {"SUCCEEDED", "SUCCESS", "COMPLETED", "FINISHED"}
    terminal_failure = {"FAILED", "ERROR", "CANCELLED"}

    elapsed = 0
    while elapsed < timeout:
        resp = requests.get(f"{base_url}/jobs/{job_id}", timeout=30)
        resp.raise_for_status()
        status = str(resp.json().get("status", "UNKNOWN")).upper()

        if status in terminal_success:
            print(f"spark-job-api job {job_id} succeeded (status={status}).")
            return
        if status in terminal_failure:
            raise RuntimeError(f"spark-job-api job {job_id} failed (status={status}).")

        print(f"spark-job-api job {job_id} still running (status={status}); "
              f"checking again in {poll_interval}s.")
        time.sleep(poll_interval)
        elapsed += poll_interval

    raise TimeoutError(f"spark-job-api job {job_id} did not reach a terminal "
                        f"state within {timeout}s.")


def _run_and_wait_for_spark_job(name: str, artifact_path: str, entry_point: str,
                                 job_type: str = "jar", job_args=None):
    poll_interval = int(Variable.get("fraud__spark_job_poll_interval_sec", default_var=10))
    timeout = int(Variable.get("fraud__spark_job_timeout_sec", default_var=1800))

    job_id = _submit_spark_job(name, artifact_path, entry_point, job_type, job_args)
    print(f"Submitted spark-job-api job '{name}' -> job_id={job_id}")
    _poll_spark_job(job_id, poll_interval=poll_interval, timeout=timeout)

def _copy_training_data_to_local(**context):
    """
    Copy the training snapshot from HDFS to local storage so pandas can read it
    without needing Java. This is a workaround until the Airflow image has Java.
    """
    import subprocess
    import os
    
    local_path = f"{MODEL_DIR}/training_snapshot.parquet"
    hdfs_path = "hdfs://hdfscluster/data/lake/fraud/transactions_scored_history.parquet"
    
    # Ensure the directory exists
    os.makedirs(MODEL_DIR, exist_ok=True)
    
    # Copy from HDFS to local
    result = subprocess.run(
        ["hdfs", "dfs", "-copyToLocal", "-f", hdfs_path, local_path],
        capture_output=True,
        text=True
    )
    
    if result.returncode != 0:
        raise RuntimeError(f"Failed to copy from HDFS: {result.stderr}")
    
    print(f"Copied {hdfs_path} -> {local_path}")
    context["ti"].xcom_push(key="training_snapshot_path", value=local_path)
    return local_path
# --------------------------------------------------------------------------
# Task callables
# --------------------------------------------------------------------------

def _snapshot_training_data(**context):
    """
    Submits the Iceberg-snapshot-to-parquet step as a JAR job on
    spark-job-api instead of issuing spark-submit ourselves.

    fraud__snapshot_artifact_path (Airflow Variable, set by
    usecase_onboarding.sh) must point to a pre-built, pre-uploaded JAR --
    e.g. hosted the same way as the sample
    'hello-spark-1.0.jar' in the spark-job-api example -- that:
      1. Connects to the Nessie catalog (uri + warehouse below)
      2. Reads nessie.fraud.transactions_scored_history
      3. Writes it to TRAINING_SNAPSHOT_PATH as parquet

    This DAG does not build or upload that JAR; it only references it.
    """
    nessie_conn = BaseHook.get_connection("nessie_default")
    nessie_uri = nessie_conn.host
    warehouse = (nessie_conn.extra_dejson or {}).get("warehouse", "")

    artifact_path = Variable.get("fraud__snapshot_artifact_path")
    entry_point = Variable.get(
        "fraud__snapshot_entry_point",
        default_var="com.fraud.SnapshotTrainingDataJob",
    )

    _run_and_wait_for_spark_job(
        name="fraud-snapshot-training-data",
        artifact_path=artifact_path,
        entry_point=entry_point,
        job_type="jar",
        job_args=[
            "--nessie-uri", nessie_uri,
            "--warehouse", warehouse,
            "--table", "fraud.transactions_scored_history",
            "--output", TRAINING_SNAPSHOT_PATH,
        ],
    )


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

    # Iceberg snapshot now goes through spark-job-api (see
    # _snapshot_training_data above) instead of a spark-submit BashOperator.
    snapshot_training_data = PythonOperator(
        task_id="snapshot_training_data",
        python_callable=_snapshot_training_data,
    )
    copy_training_data = PythonOperator(
        task_id="copy_training_data_to_local",
        python_callable=_copy_training_data_to_local,
    )
    # Plain scikit-learn retraining -- not a Spark job, so this still runs
    # directly on the Airflow worker via BashOperator, unchanged.
    train_model = BashOperator(
        task_id="train_model",
        bash_command=(
            f"python {SCRIPTS_DIR}/train_fraud_model.py "
            f"--input {MODEL_DIR}/training_snapshot.parquet "
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
        >> copy_training_data
        >> train_model
        >> evaluate_model
        >> promote_model
        >> refresh_clickhouse_aggregates
        >> emit_datahub_lineage
    )
