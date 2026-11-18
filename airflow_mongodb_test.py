
import hashlib
import logging
import re

from datetime import datetime, timezone

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

from pymongo import MongoClient
from pymongo.errors import PyMongoError


DAG_ID = "mongodb_crud_integration_test"

MONGODB_CONNECTION_ID = "mongodb_data_platform"

DATABASE_NAME = "airflow_integration_test"

logger = logging.getLogger(__name__)


def get_mongodb_client():

    conn = BaseHook.get_connection(MONGODB_CONNECTION_ID)

    extra = conn.extra_dejson

    auth_source = (
        extra.get("authSource")
        or conn.schema
        or "admin"
    )

    client = MongoClient(
        host=conn.host,
        port=conn.port or 27017,
        username=conn.login,
        password=conn.password,
        authSource=auth_source,
        replicaSet=extra.get(
            "replicaSet",
            "rs-data-platform"
        ),
        serverSelectionTimeoutMS=5000,
        connectTimeoutMS=5000,
    )

    return client


def get_collection_name(context):

    run_id = context["run_id"]

    safe_run_id = re.sub(
        r"[^a-zA-Z0-9_]",
        "_",
        run_id
    )

    run_hash = hashlib.sha256(
        run_id.encode()
    ).hexdigest()[:10]

    return f"crud_test_{safe_run_id[:35]}_{run_hash}"


def test_connection(**context):

    client = get_mongodb_client()

    try:

        ping = client.admin.command("ping")

        hello = client.admin.command("hello")

        if ping.get("ok") != 1:
            raise RuntimeError("MongoDB ping failed")

        if hello.get("setName") != "rs-data-platform":
            raise RuntimeError(
                "Unexpected MongoDB replica set"
            )

        logger.info("MongoDB authentication successful")
        logger.info("MongoDB ping successful")
        logger.info(
            "Replica set: %s",
            hello.get("setName")
        )

        logger.info(
            "Primary: %s",
            hello.get("primary")
        )

    finally:
        client.close()


def create_collection(**context):

    client = get_mongodb_client()

    try:

        database = client[DATABASE_NAME]

        collection_name = get_collection_name(context)

        # Safe reset for retries of this specific DAG run
        if collection_name in database.list_collection_names():
            database.drop_collection(collection_name)

        database.create_collection(collection_name)

        logger.info(
            "Created collection: %s",
            collection_name
        )

    finally:
        client.close()


def insert_documents(**context):

    client = get_mongodb_client()

    try:

        collection_name = get_collection_name(context)

        collection = client[DATABASE_NAME][collection_name]

        documents = [
            {
                "record_id": "record-001",
                "name": "Customer Alpha",
                "department": "Engineering",
                "status": "active",
                "source": "airflow"
            },
            {
                "record_id": "record-002",
                "name": "Customer Beta",
                "department": "Analytics",
                "status": "active",
                "source": "airflow"
            },
            {
                "record_id": "record-003",
                "name": "Customer Gamma",
                "department": "Finance",
                "status": "active",
                "source": "airflow"
            }
        ]

        result = collection.insert_many(documents)

        if len(result.inserted_ids) != 3:
            raise RuntimeError(
                "Expected 3 inserted documents"
            )

        logger.info(
            "Successfully inserted %s documents",
            len(result.inserted_ids)
        )

    finally:
        client.close()


def read_documents(**context):

    client = get_mongodb_client()

    try:

        collection_name = get_collection_name(context)

        collection = client[DATABASE_NAME][collection_name]

        documents = list(
            collection.find(
                {},
                {"_id": 0}
            ).sort("record_id", 1)
        )

        if len(documents) != 3:
            raise RuntimeError(
                f"Expected 3 documents, found {len(documents)}"
            )

        logger.info("Retrieved MongoDB documents:")

        for document in documents:
            logger.info("%s", document)

    finally:
        client.close()


def update_document(**context):

    client = get_mongodb_client()

    try:

        collection_name = get_collection_name(context)

        collection = client[DATABASE_NAME][collection_name]

        result = collection.update_one(
            {"record_id": "record-001"},
            {
                "$set": {
                    "status": "updated",
                    "updated_by": "airflow"
                }
            }
        )

        if result.modified_count != 1:
            raise RuntimeError(
                "Document update failed"
            )

        updated_document = collection.find_one(
            {"record_id": "record-001"}
        )

        if (
            not updated_document
            or updated_document.get("status") != "updated"
        ):
            raise RuntimeError(
                "Updated document validation failed"
            )

        logger.info(
            "Document successfully updated: %s",
            updated_document
        )

    finally:
        client.close()


def delete_documents(**context):

    client = get_mongodb_client()

    try:

        collection_name = get_collection_name(context)

        collection = client[DATABASE_NAME][collection_name]

        result = collection.delete_many({})

        if result.deleted_count != 3:
            raise RuntimeError(
                f"Expected to delete 3 documents, "
                f"deleted {result.deleted_count}"
            )

        logger.info(
            "Successfully deleted %s documents",
            result.deleted_count
        )

    finally:
        client.close()


def verify_cleanup(**context):

    client = get_mongodb_client()

    try:

        collection_name = get_collection_name(context)

        database = client[DATABASE_NAME]

        collection = database[collection_name]

        remaining = collection.count_documents({})

        if remaining != 0:
            raise RuntimeError(
                f"Cleanup failed: {remaining} documents remain"
            )

        logger.info(
            "Cleanup verified. No documents remain."
        )

        # Remove the empty, run-specific collection too.
        database.drop_collection(collection_name)

        if collection_name in database.list_collection_names():
            raise RuntimeError(
                "Collection cleanup failed"
            )

        logger.info(
            "Collection %s successfully removed",
            collection_name
        )

        logger.info(
            "MONGODB CRUD INTEGRATION TEST PASSED"
        )

    finally:
        client.close()


with DAG(
    dag_id=DAG_ID,
    description="End-to-end MongoDB CRUD integration test",
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    tags=["integration", "mongodb", "database"],
) as dag:

    connection_test = PythonOperator(
        task_id="test_connection",
        python_callable=test_connection,
    )

    create = PythonOperator(
        task_id="create_collection",
        python_callable=create_collection,
    )

    insert = PythonOperator(
        task_id="insert_documents",
        python_callable=insert_documents,
    )

    read = PythonOperator(
        task_id="read_documents",
        python_callable=read_documents,
    )

    update = PythonOperator(
        task_id="update_document",
        python_callable=update_document,
    )

    delete = PythonOperator(
        task_id="delete_documents",
        python_callable=delete_documents,
    )

    verify = PythonOperator(
        task_id="verify_cleanup",
        python_callable=verify_cleanup,
    )

    (
        connection_test
        >> create
        >> insert
        >> read
        >> update
        >> delete
        >> verify
    )
