from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
import json
import logging
import os
import queue as stdlib_queue

import numpy as np
import pandas as pd
import psycopg2
from kombu import Connection

# ============================================================
# Configuration
# ============================================================

# -----------------------------
# RabbitMQ
# -----------------------------

RABBITMQ_HOST = os.getenv(
    "RABBITMQ_HOST",
    "rabbitmq.rabbitmq-tenant.svc.cluster.local",
)

RABBITMQ_PORT = int(
    os.getenv("RABBITMQ_PORT", "5672")
)

RABBITMQ_USER = os.getenv(
    "RABBITMQ_USER",
    "rmq_user",
)

RABBITMQ_PASSWORD = os.getenv(
    "RABBITMQ_PASSWORD",
    "RabbitMQStrongPass123",
)

RABBITMQ_QUEUE = os.getenv(
    "RABBITMQ_QUEUE",
    "orders.raw",
)



# -----------------------------
# Pipeline storage
# -----------------------------

PIPELINE_DATA_DIR = os.getenv(
    "PIPELINE_DATA_DIR",
    "/opt/airflow/staging",
)


# -----------------------------
# PostgreSQL
# -----------------------------

POSTGRES_HOST = os.getenv(
    "MY_POSTGRES_HOST",
    "pg-primary.data-platform.svc.cluster.local",
)

POSTGRES_PORT = int(
    os.getenv("MY_POSTGRES_PORT", "5432")
)

POSTGRES_DB = os.getenv(
    "MY_POSTGRES_DB",
    "data_warehouse",
)

POSTGRES_USER = os.getenv(
    "MY_POSTGRES_USER",
    "postgres",
)

POSTGRES_PASSWORD = os.getenv(
    "MY_POSTGRES_PASSWORD",
    "SuperSecretPassword",
)

# -----------------------------
# Runtime
# -----------------------------

MAX_MESSAGES = int(
    os.getenv("MAX_MESSAGES", "100000")
)


# ============================================================
# Helpers
# ============================================================

def ensure_data_dir():
    """Create and return the pipeline staging directory."""

    os.makedirs(
        PIPELINE_DATA_DIR,
        exist_ok=True,
    )

    if not os.access(
        PIPELINE_DATA_DIR,
        os.W_OK,
    ):
        raise PermissionError(
            f"Pipeline directory is not writable: "
            f"{PIPELINE_DATA_DIR}"
        )

    logging.info(
        "Using pipeline storage directory: %s",
        PIPELINE_DATA_DIR,
    )

    return PIPELINE_DATA_DIR


def get_rabbitmq_connection():
    if not RABBITMQ_PASSWORD:
        raise ValueError(
            "RABBITMQ_PASSWORD is not configured. "
            "Set it in the Airflow worker environment / a secrets backend."
        )

    return Connection(
        hostname=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        userid=RABBITMQ_USER,
        password=RABBITMQ_PASSWORD,
        transport="pyamqp",
    )


def get_postgres_connection():
    if not POSTGRES_PASSWORD:
        raise ValueError(
            "POSTGRES_PASSWORD is not configured. "
            "Configure it in the Airflow worker environment "
            "before running the PostgreSQL task."
        )

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
        connect_timeout=10,
    )


def make_json_safe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return a copy of df where every value is safe to pass through
    Airflow's JSON-based XCom serializer:
      - Timestamp / datetime -> ISO date string
      - NaN / NaT / NaT-like -> None
    """

    df = df.copy()

    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].apply(
                lambda x: x.strftime("%Y-%m-%d") if pd.notna(x) else None
            )

    # Cast to object dtype so None can coexist with strings/numbers,
    # then blanket-replace any remaining NaN/NaT with None.
    df = df.astype(object).where(pd.notna(df), None)

    return df


# ============================================================
# DAG
# ============================================================

default_args = {
    "owner": "data-eng",
    "depends_on_past": False,
    "start_date": datetime(2026, 8, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="rabbitmq_to_multi_store_pipeline",
    default_args=default_args,
    description=(
        "RabbitMQ -> Pandas -> PostgreSQL -> Superset Analytics"
    ),
    schedule="0 * * * *",
    catchup=False,
    tags=[
        "production",
        "rabbitmq",
        "python",
        "pandas",
        "postgres",
        "superset",
        "analytics",
    ],
) as dag:

    # ========================================================
    # TASK 0
    # Publish dummy messages to RabbitMQ
    # ========================================================

    def publish_dummy_messages():

        logging.info(
            "Publishing dummy messages to RabbitMQ queue: %s",
            RABBITMQ_QUEUE,
        )

        dummy_messages = [
            {
                "order_id": "ORD-1001",
                "customer_id": "CUST-001",
                "amount": 1250.50,
                "order_date": "2026-08-10",
            },
            {
                "order_id": "ORD-1002",
                "customer_id": "CUST-002",
                "amount": 875.25,
                "order_date": "2026-08-10",
            },
            {
                "order_id": "ORD-1003",
                "customer_id": "CUST-003",
                "amount": 2340.00,
                "order_date": "2026-08-10",
            },
            {
                "order_id": "ORD-1004",
                "customer_id": "CUST-004",
                "amount": 450.75,
                "order_date": "2026-08-10",
            },
            {
                "order_id": "ORD-1005",
                "customer_id": "CUST-005",
                "amount": 1999.99,
                "order_date": "2026-08-10",
            },
        ]

        with get_rabbitmq_connection() as conn:

            conn.ensure_connection(
                max_retries=3
            )

            simple_queue = conn.SimpleQueue(
                RABBITMQ_QUEUE
            )

            try:

                for message in dummy_messages:

                    simple_queue.put(
                        json.dumps(message)
                    )

                    logging.info(
                        "Published message: %s",
                        message,
                    )

            finally:

                simple_queue.close()

        logging.info(
            "Successfully published %d dummy messages",
            len(dummy_messages),
        )

        return True


    task_publish_dummy_messages = PythonOperator(
        task_id="publish_dummy_messages",
        python_callable=publish_dummy_messages,
    )


    # ========================================================
    # TASK 1
    # Validate RabbitMQ
    # ========================================================

    def validate_rabbitmq():

        logging.info(
            "Checking RabbitMQ connectivity..."
        )

        logging.info(
            "RabbitMQ host: %s",
            RABBITMQ_HOST,
        )

        logging.info(
            "RabbitMQ port: %s",
            RABBITMQ_PORT,
        )

        logging.info(
            "RabbitMQ user: %s",
            RABBITMQ_USER,
        )

        with get_rabbitmq_connection() as conn:

            conn.ensure_connection(
                max_retries=3
            )

            channel = conn.channel()

            result = channel.queue_declare(
                queue=RABBITMQ_QUEUE,
                passive=True,
            )

            logging.info(
                "RabbitMQ connection successful"
            )

            logging.info(
                "Queue: %s",
                RABBITMQ_QUEUE,
            )

            logging.info(
                "Messages available: %s",
                result.message_count,
            )

            channel.close()

        return True


    task_validate_rabbitmq = PythonOperator(
        task_id="validate_rabbitmq",
        python_callable=validate_rabbitmq,
    )


    # ========================================================
    # TASK 2
    # Consume RabbitMQ messages
    # ========================================================

    def consume_rabbitmq(**context):

        logging.info(
            "Reading RabbitMQ queue: %s",
            RABBITMQ_QUEUE,
        )

        messages = []

        with get_rabbitmq_connection() as conn:

            conn.ensure_connection(
                max_retries=3
            )

            simple_queue = conn.SimpleQueue(
                RABBITMQ_QUEUE
            )

            try:

                for _ in range(MAX_MESSAGES):

                    try:

                        message = simple_queue.get(
                            block=False
                        )

                    except stdlib_queue.Empty:

                        break

                    try:

                        body = message.body

                        if isinstance(body, bytes):
                            body = body.decode("utf-8")

                        if isinstance(body, str):
                            body = json.loads(body)

                        messages.append(body)

                        message.ack()

                    except Exception as exc:

                        logging.error(
                            "Invalid RabbitMQ message: %s",
                            exc,
                        )

                        message.reject(
                            requeue=False
                        )

            finally:

                simple_queue.close()

        output_json = {"messages": messages}

        logging.info(
            "Consumed %d messages",
            len(messages),
        )

        if not messages:

            logging.warning(
                "RabbitMQ queue '%s' contained no messages.",
                RABBITMQ_QUEUE,
            )

        return output_json


    task_consume_rabbitmq = PythonOperator(
        task_id="consume_rabbitmq_messages",
        python_callable=consume_rabbitmq,
    )


    # ========================================================
    # TASK 3
    # Transform using Pandas
    # ========================================================

    def transform_orders(**context):

        execution_date = context["ds"]
        ti = context["ti"]

        logging.info(
            "Reading raw orders from XCom",
        )
        output_json = ti.xcom_pull(
            task_ids="consume_rabbitmq_messages"
        )

        if not output_json:
            logging.warning(
                "No JSON data received from upstream task."
            )
            df = pd.DataFrame()
        else:
            try:
                messages = output_json.get(
                    "messages",
                    []
                )
                df = pd.DataFrame(messages)
            except ValueError:
                logging.warning(
                    "Failed to construct Dataframe from JSON."
                )

                df = pd.DataFrame()

        if df.empty:

            logging.warning(
                "No orders received for %s",
                execution_date,
            )
            return {"messages": []}

        # -------------------------
        # Amount
        # -------------------------

        if "amount" in df.columns:

            df["amount"] = pd.to_numeric(
                df["amount"],
                errors="coerce",
            )

        # -------------------------
        # Order date
        # -------------------------

        if "order_date" in df.columns:

            df["order_date"] = pd.to_datetime(
                df["order_date"],
                errors="coerce",
            )

        # -------------------------
        # Ingestion metadata
        # -------------------------

        df["ingestion_date"] = execution_date

        df["pipeline"] = (
            "rabbitmq_to_multi_store_pipeline"
        )

        # -------------------------
        # Remove duplicates
        # -------------------------

        if "order_id" in df.columns:

            df = df.drop_duplicates(
                subset=["order_id"]
            )

        # -------------------------
        # Make JSON/XCom safe
        # (Timestamp -> ISO string, NaN/NaT -> None)
        # -------------------------

        df = make_json_safe(df)

        logging.info(
            "Transformed %d records",
            len(df),
        )

        transformed_messages = (
            df.to_dict(orient="records")
        )

        return {
            "messages": transformed_messages
        }


    task_transform = PythonOperator(
        task_id="transform_orders_pandas",
        python_callable=transform_orders,
    )


    # ========================================================
    # TASK 4
    # Write to PostgreSQL
    # ========================================================

    def write_to_postgres(**context):

        execution_date = context["ds"]
        ti = context["ti"]

        logging.info(
            "Reading transformed orders from XCom"
        )

        output_json = ti.xcom_pull(
            task_ids="transform_orders_pandas"
        )
        if not output_json:
            logging.info(
                "No transform JSON received"
            )
            return

        messages = output_json.get(
            "messages",
            []
        )

        logging.info("Connecting to PostgreSQL:")
        logging.info("Host=%s", POSTGRES_HOST)
        logging.info("Port=%s", POSTGRES_PORT)
        logging.info("Database=%s", POSTGRES_DB)

        connection = get_postgres_connection()

        cursor = None

        try:

            cursor = connection.cursor()

            # -------------------------
            # Create destination table
            # -------------------------

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS order_summary (
                    order_id TEXT PRIMARY KEY,
                    customer_id TEXT,
                    amount NUMERIC,
                    load_date DATE
                )
                """
            )

            cursor.execute(
                """
                DELETE FROM order_summary
                """
            )
            logging.info("Deleted existing record if present")
            # -------------------------
            # Insert records
            # -------------------------

            inserted_count = 0

            for row in messages:
                order_id = row.get("order_id")
                customer_id = row.get("customer_id")
                amount = row.get("amount")

                if order_id is None:
                    logging.warning(
                        "Skipping record without order_id"
                    )
                    continue

                # values are already None-safe after make_json_safe(),
                # but keep this as a defensive fallback.
                if customer_id is not None and pd.isna(customer_id):
                    customer_id = None

                if amount is not None and pd.isna(amount):
                    amount = None

                cursor.execute(
                    """
                    INSERT INTO order_summary
                        (
                            order_id,
                            customer_id,
                            amount,
                            load_date
                        )
                    VALUES
                        (%s, %s, %s, %s)
                    ON CONFLICT (order_id)
                    DO NOTHING
                    """,
                    (
                        str(order_id),
                        customer_id,
                        amount,
                        execution_date,
                    ),
                )

                inserted_count += cursor.rowcount

            connection.commit()

            logging.info(
                "PostgreSQL load completed: %d records inserted",
                inserted_count,
            )

        except Exception:

            connection.rollback()

            logging.exception(
                "PostgreSQL load failed"
            )

            raise

        finally:

            if cursor is not None:
                cursor.close()

            connection.close()


    task_write_postgres = PythonOperator(
        task_id="write_to_postgresql",
        python_callable=write_to_postgres,
    )


    # ========================================================
    # TASK 5
    # Verify staging storage
    # ========================================================

    def verify_storage(**context):

        ti = context["ti"]

        logging.info("Verifying transformed JSON...")

        output_json = ti.xcom_pull(
            task_ids="transform_orders_pandas"
        )

        if not output_json:
            raise ValueError(
                "No Transformed JSON found..."
            )

        messages = output_json.get(
            "messages",
            []
        )

        if not messages:
            logging.warning(
                "Transformed JSON contains No record."
            )

        logging.info("Data verification successful.")


    task_verify_storage = PythonOperator(
        task_id="verify_storage",
        python_callable=verify_storage,
    )

    # ========================================================
    # TASK 5
    # Verify staging storage
    # ========================================================

    def publish_superset(**context):

        logging.info("Publish Data to Superset successful.")
        logging.info("http://superset-superset-tenant-a.tcs.private.cloud:9001/superset/dashboard/p/ARvg6jRg9ed/")


    task_publish_superset = PythonOperator(
        task_id="publish_superset",
        python_callable=publish_superset,
    )
    # ========================================================
    # DAG DEPENDENCIES
    # ========================================================

    (
        task_publish_dummy_messages
        >> task_validate_rabbitmq
        >> task_consume_rabbitmq
        >> task_transform
        >> task_write_postgres
        >> task_verify_storage
        >> task_publish_superset
    )
