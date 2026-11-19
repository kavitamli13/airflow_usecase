
from datetime import datetime, timedelta
import logging

from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from trino.dbapi import connect


CONNECTION_ID = "trino_data_platform"

TARGET_CATALOGS = ["hive", "iceberg", "kafka"]

logger = logging.getLogger(__name__)


def get_trino_connection():
    """Build a Trino connection from the saved Airflow connection."""

    airflow_conn = BaseHook.get_connection(CONNECTION_ID)
    extra = airflow_conn.extra_dejson or {}

    return connect(
        host=airflow_conn.host,
        port=airflow_conn.port or 8080,
        user=airflow_conn.login or "airflow-test",
        http_scheme=extra.get("http_scheme", "http"),
        request_timeout=30,
    )


def quote_identifier(value):
    """Safely quote a Trino catalog/schema/table identifier."""
    return '"' + value.replace('"', '""') + '"'


def execute_query(sql):
    """Execute SQL and return columns and rows."""

    conn = get_trino_connection()

    try:
        cursor = conn.cursor()
        logger.info("Executing SQL: %s", sql)
        cursor.execute(sql)

        columns = (
            [column[0] for column in cursor.description]
            if cursor.description else []
        )
        rows = cursor.fetchall() if cursor.description else []

        logger.info("Columns: %s", columns)
        logger.info("Rows returned: %s", len(rows))

        return columns, rows

    finally:
        conn.close()


def test_trino_connection(**context):
    columns, rows = execute_query(
        "SELECT version() AS trino_version, "
        "current_user AS trino_user"
    )

    if not rows:
        raise AirflowException("Trino returned no server information")

    result = dict(zip(columns, rows[0]))

    logger.info("Trino version: %s", result["trino_version"])
    logger.info("Trino user: %s", result["trino_user"])

    if result["trino_user"] != "airflow-test":
        raise AirflowException(
            f"Unexpected Trino user: {result['trino_user']}"
        )

    logger.info("TRINO CONNECTION TEST PASSED")


def test_sql_execution(**context):
    columns, rows = execute_query(
        "SELECT 1 AS integration_test, "
        "current_user AS trino_user"
    )

    if not rows or rows[0][0] != 1:
        raise AirflowException("Basic SQL execution test failed")

    if rows[0][1] != "airflow-test":
        raise AirflowException("Unexpected Trino execution user")

    logger.info("SQL execution successful: %s", rows[0])


def discover_catalogs(**context):
    columns, rows = execute_query("SHOW CATALOGS")

    catalogs = [row[0] for row in rows]

    logger.info("Available Trino catalogs: %s", catalogs)

    if not catalogs:
        raise AirflowException("No Trino catalogs were discovered")

    context["ti"].xcom_push(
        key="available_catalogs",
        value=catalogs
    )

    return catalogs


def inspect_catalog(catalog, **context):
    """Discover schemas and tables for an individual catalog."""

    available_catalogs = context["ti"].xcom_pull(
        task_ids="discover_catalogs",
        key="available_catalogs"
    ) or []

    if catalog not in available_catalogs:
        raise AirflowSkipException(
            f"Catalog '{catalog}' is not configured in Trino"
        )

    quoted_catalog = quote_identifier(catalog)

    logger.info("Inspecting catalog: %s", catalog)

    # Discover schemas
    _, schema_rows = execute_query(
        f"SHOW SCHEMAS FROM {quoted_catalog}"
    )

    schemas = [row[0] for row in schema_rows]

    logger.info(
        "Catalog '%s' schemas: %s",
        catalog,
        schemas
    )

    if not schemas:
        logger.warning(
            "No schemas visible in catalog '%s'",
            catalog
        )
        return

    # Discover tables in each visible, non-system schema.
    for schema in schemas:
        if schema.lower() in ("information_schema", "system"):
            continue

        quoted_schema = quote_identifier(schema)

        try:
            _, table_rows = execute_query(
                f"SHOW TABLES FROM {quoted_catalog}.{quoted_schema}"
            )

            tables = [row[0] for row in table_rows]

            logger.info(
                "Catalog: %s | Schema: %s | Tables: %s",
                catalog,
                schema,
                tables[:20]
            )

            if len(tables) > 20:
                logger.info(
                    "Additional tables omitted from log: %s",
                    len(tables) - 20
                )

        except Exception:
            logger.exception(
                "Table discovery failed for %s.%s",
                catalog,
                schema
            )
            raise

    logger.info(
        "CATALOG DISCOVERY PASSED: %s",
        catalog
    )


default_args = {
    "owner": "data-platform",
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="trino_sql_integration_test",
    description="End-to-end Trino and catalog integration tests",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["integration", "trino", "hive", "iceberg", "kafka"],
) as dag:

    connection_test = PythonOperator(
        task_id="test_trino_connection",
        python_callable=test_trino_connection,
    )

    sql_test = PythonOperator(
        task_id="test_sql_execution",
        python_callable=test_sql_execution,
    )

    catalog_discovery = PythonOperator(
        task_id="discover_catalogs",
        python_callable=discover_catalogs,
    )

    hive_test = PythonOperator(
        task_id="inspect_hive_catalog",
        python_callable=inspect_catalog,
        op_kwargs={"catalog": "hive"},
    )

    iceberg_test = PythonOperator(
        task_id="inspect_iceberg_catalog",
        python_callable=inspect_catalog,
        op_kwargs={"catalog": "iceberg"},
    )

    kafka_test = PythonOperator(
        task_id="inspect_kafka_catalog",
        python_callable=inspect_catalog,
        op_kwargs={"catalog": "kafka"},
    )

    connection_test >> [sql_test, catalog_discovery]

    catalog_discovery >> [
        hive_test,
        iceberg_test,
        kafka_test,
    ]
