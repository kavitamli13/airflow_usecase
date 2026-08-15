"""
spark_streaming_scorer.py

Reads transactions from Kafka, applies the trained fraud model in
micro-batches, and writes scored results to:
  - PostgreSQL   (operational store — latest scored transactions)
  - Iceberg      (data lake, via Nessie catalog — full history for retraining/audit)
  - ClickHouse   (analytics store — feeds the Superset dashboard)

Run:
    spark-submit \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\
org.postgresql:postgresql:42.7.3 \
        --conf spark.sql.catalog.nessie=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.nessie.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
        --conf spark.sql.catalog.nessie.uri=http://nessie:19120/api/v1 \
        --conf spark.sql.catalog.nessie.ref=main \
        --conf spark.sql.catalog.nessie.warehouse=s3a://lake/warehouse \
        spark_streaming_scorer.py

Model reload: the model path is re-checked at the start of every micro-batch
(cheap stat call), so promoting a new model (see Airflow DAG) is picked up
within one batch interval without restarting this job.
"""

import json
import os

import joblib
import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, current_timestamp, lit
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
)

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")
MODEL_PATH = os.environ.get("MODEL_PATH", "/models/fraud_model_production.joblib")

PG_URL = os.environ.get("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/fraud")
PG_PROPS = {
    "user": os.environ.get("PG_USER", "fraud_svc"),
    "password": os.environ.get("PG_PASSWORD", ""),
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_URL = os.environ.get("CLICKHOUSE_JDBC_URL", "jdbc:clickhouse://clickhouse:8123/analytics")
CLICKHOUSE_PROPS = {
    "user": os.environ.get("CLICKHOUSE_USER", "default"),
    "password": os.environ.get("CLICKHOUSE_PASSWORD", ""),
    "driver": "com.clickhouse.jdbc.ClickHouseDriver",
}

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("account_id", StringType()),
    StructField("amount", DoubleType()),
    StructField("merchant_category", StringType()),
    StructField("location", StringType()),
    StructField("event_time", StringType()),
])

# Model is loaded once per executor via broadcast — avoids re-loading the
# joblib artifact on every micro-batch partition.
_model_cache = {"path": None, "model": None}


def get_model():
    if _model_cache["path"] != MODEL_PATH or _model_cache["model"] is None:
        _model_cache["model"] = joblib.load(MODEL_PATH)
        _model_cache["path"] = MODEL_PATH
    return _model_cache["model"]


def score_partition(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Applies the SAME feature engineering as train_fraud_model.py.
    For a real deployment, factor this into a shared package imported by
    both the training script and this job so the two never drift apart.
    """
    if pdf.empty:
        pdf["fraud_probability"] = []
        return pdf

    pdf = pdf.sort_values(["account_id", "event_time"]).copy()
    pdf["event_time"] = pd.to_datetime(pdf["event_time"])

    grp = pdf.groupby("account_id")["amount"]
    pdf["amount_zscore_for_account"] = (
        (pdf["amount"] - grp.transform("mean")) / grp.transform("std").replace(0, 1)
    ).fillna(0)

    pdf["hours_since_last_txn"] = (
        pdf.groupby("account_id")["event_time"].diff().dt.total_seconds() / 3600.0
    ).fillna(24.0)

    pdf = pdf.set_index("event_time")
    pdf["txns_last_1h_for_account"] = (
        pdf.groupby("account_id")["amount"].rolling("1h").count().reset_index(level=0, drop=True)
    )
    pdf = pdf.reset_index()

    feature_cols = [
        "amount", "amount_zscore_for_account", "txns_last_1h_for_account",
        "hours_since_last_txn", "merchant_category", "location",
    ]
    model = get_model()
    pdf["fraud_probability"] = model.predict_proba(pdf[feature_cols])[:, 1]
    pdf["is_flagged"] = (pdf["fraud_probability"] >= 0.5).astype(int)
    return pdf


def write_batch(batch_df: DataFrame, batch_id: int):
    if batch_df.rdd.isEmpty():
        return

    scored_pdf = batch_df.toPandas()
    scored_pdf = score_partition(scored_pdf)
    scored = batch_df.sparkSession.createDataFrame(scored_pdf) \
        .withColumn("scored_at", current_timestamp())

    # 1. Operational store — latest scored transactions for ops teams
    scored.select(
        "transaction_id", "account_id", "amount", "merchant_category",
        "location", "event_time", "fraud_probability", "is_flagged", "scored_at",
    ).write.jdbc(PG_URL, "transactions_scored", mode="append", properties=PG_PROPS)

    # 2. Data lake — full history via Iceberg/Nessie, used for retraining + audit
    scored.writeTo("nessie.fraud.transactions_scored_history").append()

    # 3. Analytics store — feeds Superset dashboards
    scored.select(
        "transaction_id", "account_id", "amount", "merchant_category",
        "location", "fraud_probability", "is_flagged", "scored_at",
    ).write.jdbc(CLICKHOUSE_URL, "transactions_scored_analytics", mode="append", properties=CLICKHOUSE_PROPS)

    print(f"Batch {batch_id}: wrote {scored.count()} scored records "
          f"({scored_pdf['is_flagged'].sum()} flagged as fraud).")


def main():
    spark = (
        SparkSession.builder
        .appName("fraud-streaming-scorer")
        .getOrCreate()
    )

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw.select(from_json(col("value").cast("string"), TRANSACTION_SCHEMA).alias("data"))
        .select("data.*")
    )

    query = (
        parsed.writeStream
        .foreachBatch(write_batch)
        .option("checkpointLocation", "/checkpoints/fraud_streaming_scorer")
        .trigger(processingTime="10 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()
