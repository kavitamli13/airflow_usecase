
"""
Data Platform - Phase 1 Connectivity Validation

Checks DNS resolution and TCP connectivity to configured
Kubernetes services from Airflow worker pods.

Airflow Variable required:
    connectivity_targets
"""

import json
import logging
import socket
import time
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule


logger = logging.getLogger(__name__)

CONNECTION_TIMEOUT = 5


with DAG(
    dag_id="data_platform_connectivity_validation",
    description="Phase 1: Validate connectivity to data platform services",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["data-platform", "connectivity", "phase-1"],
) as dag:

    @task
    def load_targets():
        """Load connectivity targets from Airflow Variables."""

        targets = Variable.get(
            "connectivity_targets",
            deserialize_json=True,
        )

        if not isinstance(targets, list):
            raise AirflowException(
                "connectivity_targets must contain a JSON array"
            )

        enabled_targets = [
            target for target in targets
            if target.get("enabled", True)
        ]

        logger.info("Total configured targets: %s", len(targets))
        logger.info("Enabled targets: %s", len(enabled_targets))

        return enabled_targets

    @task
    def probe_target(target: dict):
        """Check DNS and TCP connectivity for one service."""

        name = target.get("name", "Unknown")
        category = target.get("category", "Uncategorized")
        host = target.get("host")
        port = target.get("port")
        worker = socket.gethostname()

        result = {
            "name": name,
            "category": category,
            "host": host,
            "port": port,
            "worker": worker,
            "status": "FAILED",
            "dns_ms": None,
            "tcp_ms": None,
            "resolved_ips": [],
            "message": "",
        }

        if not host or not port:
            result["status"] = "INVALID_TARGET"
            result["message"] = "Host or port is missing"
            return result

        try:
            port = int(port)

            if not 1 <= port <= 65535:
                raise ValueError("Port must be between 1 and 65535")

            result["port"] = port

        except (ValueError, TypeError) as exc:
            result["status"] = "INVALID_TARGET"
            result["message"] = str(exc)
            return result

        # Step 1: DNS resolution
        try:
            dns_start = time.monotonic()

            addresses = socket.getaddrinfo(
                host,
                port,
                type=socket.SOCK_STREAM,
            )

            result["dns_ms"] = round(
                (time.monotonic() - dns_start) * 1000,
                2,
            )

            result["resolved_ips"] = sorted({
                address[4][0] for address in addresses
            })

        except socket.gaierror as exc:
            result["status"] = "DNS_FAILED"
            result["message"] = str(exc)
            return result

        # Step 2: TCP connection
        try:
            tcp_start = time.monotonic()

            with socket.create_connection(
                (host, port),
                timeout=CONNECTION_TIMEOUT,
            ):
                pass

            result["tcp_ms"] = round(
                (time.monotonic() - tcp_start) * 1000,
                2,
            )

            result["status"] = "PASS"
            result["message"] = "DNS and TCP connection successful"

        except (socket.timeout, TimeoutError) as exc:
            result["status"] = "TIMEOUT"
            result["message"] = str(exc) or "Connection timed out"

        except ConnectionRefusedError as exc:
            result["status"] = "CONNECTION_REFUSED"
            result["message"] = str(exc)

        except OSError as exc:
            result["status"] = "CONNECTION_FAILED"
            result["message"] = str(exc)

        logger.info(
            "Connectivity result: %s | %s:%s | %s | worker=%s",
            name,
            host,
            port,
            result["status"],
            worker,
        )

        return result

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def generate_report(results):
        """Generate a consolidated connectivity report."""

        if not results:
            raise AirflowException("No connectivity results were returned")

        passed = 0
        failed = 0

        logger.info("")
        logger.info("=" * 110)
        logger.info("DATA PLATFORM CONNECTIVITY VALIDATION REPORT")
        logger.info("=" * 110)

        for result in results:
            status = result.get("status", "UNKNOWN")

            if status == "PASS":
                passed += 1
            else:
                failed += 1

            logger.info(
                "%-25s | %-20s | %-35s | %-5s | %-20s | DNS=%s ms | TCP=%s ms",
                result.get("name"),
                status,
                result.get("host"),
                result.get("port"),
                result.get("worker"),
                result.get("dns_ms"),
                result.get("tcp_ms"),
            )

            if result.get("message"):
                logger.info("    Details: %s", result["message"])

            if result.get("resolved_ips"):
                logger.info(
                    "    Resolved IPs: %s",
                    ", ".join(result["resolved_ips"]),
                )

        logger.info("=" * 110)
        logger.info("TOTAL TARGETS : %s", len(results))
        logger.info("PASSED        : %s", passed)
        logger.info("FAILED        : %s", failed)
        logger.info("=" * 110)

        if failed:
            raise AirflowException(
                f"Connectivity validation completed: "
                f"{passed} passed, {failed} failed. "
                f"Review this task's logs for details."
            )

        logger.info("All configured connectivity checks passed.")

    # Execute connectivity tests using Airflow workers.
    configured_targets = load_targets()

    probe_results = probe_target.expand(
        target=configured_targets
    )

    generate_report(probe_results)
