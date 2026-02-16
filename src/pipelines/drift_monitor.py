import mlflow
import yaml
from pyspark.sql import SparkSession
import numpy as np
from src.common.config import load_model_config

spark = SparkSession.builder.getOrCreate()

MODEL_KEY = "churn"
config = load_model_config(MODEL_KEY)

model_name = config.registered_model_name
model_uri = f"models:/{model_name}@Champion"

# Load baseline
baseline = mlflow.artifacts.load_dict(
    f"{model_uri}/artifacts/baseline_stats.json"
)

# Load recent scored data using parameterized output catalog
scores_table = f"{config.output.catalog}.{config.output.schema}.{config.output.table}"
df = spark.table(scores_table)
pdf = df.toPandas()

drift_metrics = []

for col, stats in baseline.items():
    current_mean = pdf[col].mean()
    z_score = abs(current_mean - stats["mean"]) / stats["std"]

    drift_metrics.append({
        "feature": col,
        "z_score": z_score,
        "drifted": z_score > 3
    })

# Write drift metrics using parameterized monitoring catalog
monitoring_table = f"{config.output.catalog}.monitoring.churn_drift_metrics"
spark.createDataFrame(drift_metrics) \
    .write.mode("append") \
    .saveAsTable(monitoring_table)
