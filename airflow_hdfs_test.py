
import logging
import requests
import pendulum

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from urllib.parse import quote

logger = logging.getLogger(__name__)

with DAG(
    dag_id="hdfs_file_operations_test",
    description="Test HDFS directory creation, file upload, read, verification and deletion through HttpFS",
    start_date=pendulum.datetime(2026, 11, 17, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["hdfs", "integration-test"],
) as dag:

    @task
    def test_hdfs_file_operations(**context):

        # Read Airflow connection
        conn = BaseHook.get_connection("hdfs_default")

        scheme = conn.schema or "http"
        host = conn.host
        port = conn.port or 14000
        username = conn.login or "hdfs"

        base_url = f"{scheme}://{host}:{port}/webhdfs/v1"

        # Unique path for each DAG run
        run_id = context["run_id"]
        safe_run_id = "".join(
            c if c.isalnum() or c in "-_" else "_"
            for c in run_id
        )

        test_dir = f"/tmp/airflow_hdfs_test_{safe_run_id}"
        file_path = f"{test_dir}/sample.txt"

        encoded_dir = quote(test_dir, safe="/")
        encoded_file = quote(file_path, safe="/")

        session = requests.Session()
        session.params = {"user.name": username}

        expected_content = (
            "HDFS integration test successful.\n"
            "Created by Apache Airflow.\n"
        )

        try:
            # 1. Create directory
            response = session.put(
                f"{base_url}{encoded_dir}",
                params={"op": "MKDIRS"},
                timeout=30,
            )
            response.raise_for_status()

            mkdir_result = response.json().get("boolean")
            if mkdir_result is not True:
                raise RuntimeError(
                    f"HDFS directory creation failed: {response.text}"
                )

            logger.info("Created HDFS directory: %s", test_dir)

            # 2. Upload file
            response = session.put(
                f"{base_url}{encoded_file}",
                params={"op": "CREATE", "overwrite": "true"},
                data=expected_content.encode("utf-8"),
                headers={"Content-Type": "application/octet-stream"},
                timeout=60,
            )
            response.raise_for_status()

            logger.info("Uploaded file: %s", file_path)

            # 3. Read file back
            response = session.get(
                f"{base_url}{encoded_file}",
                params={"op": "OPEN"},
                timeout=60,
            )
            response.raise_for_status()

            actual_content = response.content.decode("utf-8")

            if actual_content != expected_content:
                raise AssertionError(
                    "HDFS file content does not match uploaded content"
                )

            logger.info("File read-back and content verification passed")

            # 4. Verify file metadata
            response = session.get(
                f"{base_url}{encoded_file}",
                params={"op": "GETFILESTATUS"},
                timeout=30,
            )
            response.raise_for_status()

            file_status = response.json()["FileStatus"]
            actual_size = file_status["length"]
            expected_size = len(expected_content.encode("utf-8"))

            if actual_size != expected_size:
                raise AssertionError(
                    f"File size mismatch: expected {expected_size}, got {actual_size}"
                )

            logger.info(
                "File metadata verified. Size: %s bytes",
                actual_size,
            )

            logger.info("HDFS file operations test PASSED")

            return {
                "status": "SUCCESS",
                "directory": test_dir,
                "file": file_path,
                "file_size": actual_size,
                "content_verified": True,
            }

        finally:
            # 5. Clean up test directory
            try:
                response = session.delete(
                    f"{base_url}{encoded_dir}",
                    params={"op": "DELETE", "recursive": "true"},
                    timeout=30,
                )
                response.raise_for_status()

                logger.info("Cleaned up HDFS test directory: %s", test_dir)

            except Exception:
                logger.exception(
                    "HDFS cleanup failed for %s", test_dir
                )

            session.close()

    test_hdfs_file_operations()
