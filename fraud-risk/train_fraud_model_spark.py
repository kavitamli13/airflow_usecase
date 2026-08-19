#!/usr/bin/env python3
"""
train_fraud_model_spark.py - SIMPLIFIED VERSION
Uses only PySpark MLlib (no external ML libraries needed)
"""

import sys
import json
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier as SparkRFC


def train_fraud_model(hdfs_input_path, model_output_path, metrics_output_path):
    print(f"[INFO] Starting fraud model training at {datetime.utcnow().isoformat()}")
    
    spark = SparkSession.builder.appName("train-fraud-model").getOrCreate()
    
    try:
        # Read training data from HDFS
        print(f"[INFO] Reading training data from {hdfs_input_path}")
        df = spark.read.parquet(hdfs_input_path)
        
        row_count = df.count()
        if row_count == 0:
            print("[WARN] Training data is empty!")
            metrics = {"recall": 0.0, "precision": 0.0, "f1": 0.0, "n_samples": 0}
            with open(metrics_output_path, 'w') as f:
                json.dump(metrics, f)
            return
        
        print(f"[INFO] Loaded {row_count} rows")
        
        # Select features (simple version - no categorical encoding needed for Spark MLlib)
        feature_cols = ['amount', 'merchant_category', 'location']
        target_col = 'is_flagged'
        
        df_clean = df[[col for col in feature_cols + [target_col] if col in df.columns]].dropna()
        
        if df_clean.count() == 0:
            print("[WARN] No complete rows after NaN drop!")
            metrics = {"recall": 0.0, "precision": 0.0, "f1": 0.0, "n_samples": 0}
            with open(metrics_output_path, 'w') as f:
                json.dump(metrics, f)
            return
        
        # Convert categorical columns to numeric
        indexer_merchant = StringIndexer(inputCol="merchant_category", outputCol="merchant_indexed")
        indexer_location = StringIndexer(inputCol="location", outputCol="location_indexed")
        
        df_indexed = indexer_merchant.fit(df_clean).transform(df_clean)
        df_indexed = indexer_location.fit(df_indexed).transform(df_indexed)
        
        # Create feature vector
        assembler = VectorAssembler(
            inputCols=["amount", "merchant_indexed", "location_indexed"],
            outputCol="features"
        )
        df_features = assembler.transform(df_indexed)
        
        # Split train/test
        train_df, test_df = df_features.randomSplit([0.8, 0.2], seed=42)
        
        # Train Random Forest using Spark MLlib (no sklearn needed)
        rf = SparkRFC(
            labelCol=target_col,
            featuresCol="features",
            numTrees=100,
            maxDepth=10,
            seed=42
        )
        model = rf.fit(train_df)
        
        # Evaluate
        predictions = model.transform(test_df)
        from pyspark.ml.evaluation import BinaryClassificationEvaluator
        
        evaluator = BinaryClassificationEvaluator(labelCol=target_col, rawPredictionCol="rawPrediction")
        auc = evaluator.evaluate(predictions)
        
        # Simple metrics (Spark MLlib metrics)
        metrics = {
            "auc": float(auc),
            "n_samples": int(df_clean.count()),
            "timestamp": datetime.utcnow().isoformat(),
        }
        
        # Save model (Spark's native format, no joblib needed)
        model.write().overwrite().save(model_output_path)
        print(f"[INFO] Model saved to {model_output_path}")
        
        # Save metrics
        with open(metrics_output_path, 'w') as f:
            json.dump(metrics, f, indent=2)
        print(f"[INFO] Metrics saved to {metrics_output_path}")
        
        print(f"[INFO] Training complete at {datetime.utcnow().isoformat()}")
        
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: train_fraud_model_spark.py <hdfs_input> <model_output> <metrics_output>")
        sys.exit(1)
    
    hdfs_input = sys.argv[1]
    model_output = sys.argv[2]
    metrics_output = sys.argv[3]
    
    train_fraud_model(hdfs_input, model_output, metrics_output)
