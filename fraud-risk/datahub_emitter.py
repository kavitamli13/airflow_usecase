"""
datahub_emitter.py

Pushes dataset and lineage metadata to DataHub via its REST (GMS) endpoint,
using only the core `acryl-datahub` SDK — NOT the acryl-datahub-airflow-plugin.

Why this approach: the Airflow plugin auto-infers lineage from DAG structure,
but pins specific Airflow/OpenLineage provider versions and is a common
source of dependency conflicts. This module calls the same underlying REST
API the plugin uses, but only needs the lightweight core package:

    pip install acryl-datahub

Called explicitly from Airflow tasks (see airflow_fraud_pipeline_dag.py) —
you control exactly what gets emitted and when, instead of relying on
auto-instrumentation.

Env vars:
    DATAHUB_GMS_URL   e.g. http://datahub-gms:8080  (default shown below)
    DATAHUB_GMS_TOKEN optional, if your DataHub instance requires auth
"""

import os
import time
from typing import List, Optional

from datahub.emitter.mce_builder import make_dataset_urn, make_data_job_urn, make_data_flow_urn
from datahub.emitter.mcp import MetadataChangeProposalWrapper
from datahub.emitter.rest_emitter import DatahubRestEmitter
from datahub.metadata.schema_classes import (
    DatasetPropertiesClass,
    UpstreamLineageClass,
    UpstreamClass,
    DatasetLineageTypeClass,
    DataFlowInfoClass,
    DataJobInfoClass,
    DataJobInputOutputClass,
)

GMS_URL = os.environ.get("DATAHUB_GMS_URL", "http://datahub-gms:8080")
GMS_TOKEN = os.environ.get("DATAHUB_GMS_TOKEN")  # None if auth is not enabled

_emitter = None


def get_emitter() -> DatahubRestEmitter:
    global _emitter
    if _emitter is None:
        _emitter = DatahubRestEmitter(gms_server=GMS_URL, token=GMS_TOKEN)
    return _emitter


def emit_dataset(platform: str, name: str, description: str, env: str = "PROD") -> str:
    """
    Registers/updates a dataset entity, e.g. platform='postgres', name='fraud.transactions_scored'.
    Returns the dataset's URN for use in lineage calls.
    """
    urn = make_dataset_urn(platform=platform, name=name, env=env)
    mcp = MetadataChangeProposalWrapper(
        entityUrn=urn,
        aspect=DatasetPropertiesClass(description=description),
    )
    get_emitter().emit(mcp)
    return urn


def emit_lineage(upstream_urns: List[str], downstream_urn: str):
    """
    Records that `downstream_urn` is derived from each URN in `upstream_urns`.
    """
    upstreams = [
        UpstreamClass(dataset=u, type=DatasetLineageTypeClass.TRANSFORMED)
        for u in upstream_urns
    ]
    mcp = MetadataChangeProposalWrapper(
        entityUrn=downstream_urn,
        aspect=UpstreamLineageClass(upstreams=upstreams),
    )
    get_emitter().emit(mcp)


def emit_pipeline_and_task(
    flow_id: str,
    flow_name: str,
    task_id: str,
    task_name: str,
    orchestrator: str = "airflow",
    input_urns: Optional[List[str]] = None,
    output_urns: Optional[List[str]] = None,
) -> str:
    """
    Registers the Airflow DAG as a DataFlow and the calling task as a DataJob,
    with input/output dataset lineage attached directly to the task.
    Returns the DataJob URN.
    """
    flow_urn = make_data_flow_urn(orchestrator=orchestrator, flow_id=flow_id, cluster="prod")
    get_emitter().emit(
        MetadataChangeProposalWrapper(
            entityUrn=flow_urn,
            aspect=DataFlowInfoClass(name=flow_name),
        )
    )

    job_urn = make_data_job_urn(orchestrator=orchestrator, flow_id=flow_id, job_id=task_id, cluster="prod")
    get_emitter().emit(
        MetadataChangeProposalWrapper(
            entityUrn=job_urn,
            aspect=DataJobInfoClass(name=task_name, type="BATCH"),
        )
    )

    if input_urns or output_urns:
        get_emitter().emit(
            MetadataChangeProposalWrapper(
                entityUrn=job_urn,
                aspect=DataJobInputOutputClass(
                    inputDatasets=input_urns or [],
                    outputDatasets=output_urns or [],
                ),
            )
        )

    return job_urn


if __name__ == "__main__":
    # Smoke test — registers the pipeline's core datasets and lineage.
    kafka_urn = emit_dataset("kafka", "transactions", "Raw transaction events")
    lake_urn = emit_dataset("iceberg", "fraud.transactions_scored_history", "Full scored transaction history")
    pg_urn = emit_dataset("postgres", "fraud.transactions_scored", "Latest scored transactions (operational)")
    ch_urn = emit_dataset("clickhouse", "analytics.transactions_scored_analytics", "Analytics feed for Superset")

    emit_lineage([kafka_urn], lake_urn)
    emit_lineage([kafka_urn], pg_urn)
    emit_lineage([lake_urn], ch_urn)

    emit_pipeline_and_task(
        flow_id="fraud_detection_pipeline",
        flow_name="Fraud detection retraining + lineage pipeline",
        task_id="emit_lineage",
        task_name="Emit DataHub lineage",
        input_urns=[kafka_urn],
        output_urns=[lake_urn, pg_urn, ch_urn],
    )
    print("DataHub metadata emitted successfully.")
