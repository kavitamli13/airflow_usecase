"""
train_fraud_model.py

Trains the fraud-scoring model from historical, labeled transaction data
sitting in the lake (Iceberg table), and writes:
  - a model artifact (joblib)
  - a metrics.json report (used by Airflow to gate promotion)

This is intentionally a scikit-learn RandomForest for clarity and fast
iteration. For higher data volumes, swap the training portion for Spark
MLlib (same feature engineering, distributed fit) or a Kubeflow Pipeline
if you want a fully containerized, versioned training run — the feature
logic below is written to translate directly to either.

Run:
    python train_fraud_model.py \
        --input /data/lake/fraud/transactions_scored_history.parquet \
        --model-out /models/fraud_model_candidate.joblib \
        --metrics-out /models/metrics_candidate.json
"""

import argparse
import json

import joblib
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    average_precision_score,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline

FEATURE_COLUMNS_NUMERIC = [
    "amount",
    "amount_zscore_for_account",
    "txns_last_1h_for_account",
    "hours_since_last_txn",
]
FEATURE_COLUMNS_CATEGORICAL = ["merchant_category", "location"]
LABEL_COLUMN = "is_fraud"


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Shared feature logic — this MUST match the transformation applied in
    spark_streaming_scorer.py at inference time, or the model will see a
    different feature distribution live than it did during training.
    """
    df = df.sort_values(["account_id", "event_time"]).copy()
    df["event_time"] = pd.to_datetime(df["event_time"])

    # Per-account rolling stats
    grp = df.groupby("account_id")["amount"]
    df["amount_zscore_for_account"] = (
        (df["amount"] - grp.transform("mean")) / grp.transform("std").replace(0, 1)
    ).fillna(0)

    df["hours_since_last_txn"] = (
        df.groupby("account_id")["event_time"].diff().dt.total_seconds() / 3600.0
    ).fillna(24.0)  # first-seen transaction: treat as low velocity

    # Rolling 1h transaction count per account (velocity feature)
    df = df.set_index("event_time")
    df["txns_last_1h_for_account"] = (
        df.groupby("account_id")["amount"]
        .rolling("1h")
        .count()
        .reset_index(level=0, drop=True)
    )
    df = df.reset_index()

    return df


def build_pipeline() -> Pipeline:
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), FEATURE_COLUMNS_CATEGORICAL),
        ],
        remainder="passthrough",
    )
    model = RandomForestClassifier(
        n_estimators=200,
        max_depth=8,
        class_weight="balanced",  # fraud is rare — don't let the model just predict "not fraud"
        random_state=42,
        n_jobs=-1,
    )
    return Pipeline([("preprocess", preprocessor), ("model", model)])

def read_training_data(path):
    """Read from HDFS or local filesystem using PySpark."""
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("train-fraud-model").getOrCreate()
    df = spark.read.parquet(path).toPandas()
    spark.stop()
    return df

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True, help="Parquet/CSV of historical labeled transactions")
    parser.add_argument("--model-out", required=True)
    parser.add_argument("--metrics-out", required=True)
    args = parser.parse_args()

    if args.input.endswith(".parquet"):
        raw = read_training_data(args.input)
    else:
        raw = pd.read_csv(args.input)

    df = engineer_features(raw)
    feature_cols = FEATURE_COLUMNS_NUMERIC + FEATURE_COLUMNS_CATEGORICAL
    X = df[feature_cols]
    y = df[LABEL_COLUMN]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    pipeline = build_pipeline()
    pipeline.fit(X_train, y_train)

    y_pred = pipeline.predict(X_test)
    y_proba = pipeline.predict_proba(X_test)[:, 1]

    metrics = {
        "precision": precision_score(y_test, y_pred),
        "recall": recall_score(y_test, y_pred),
        "roc_auc": roc_auc_score(y_test, y_proba),
        "avg_precision": average_precision_score(y_test, y_proba),
        "n_train": len(X_train),
        "n_test": len(X_test),
        "fraud_rate_test": float(np.mean(y_test)),
    }

    joblib.dump(pipeline, args.model_out)
    with open(args.metrics_out, "w") as f:
        json.dump(metrics, f, indent=2)

    print("Training complete.")
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
