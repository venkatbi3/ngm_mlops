import mlflow
import yaml
from pyspark.sql import SparkSession
import numpy as np

spark = SparkSession.builder.getOrCreate()

MODEL_KEY = "churn"
config = yaml.safe_load(open(f"src/models/{MODEL_KEY}/config.yml"))

model_name = config["registered_model_name"]
model_uri = f"models:/{model_name}@Champion"

# Load baseline
baseline = mlflow.artifacts.load_dict(
    f"{model_uri}/artifacts/baseline_stats.json"
)

# Load recent scored data
df = spark.table("ngm_prd_ml.predictions.churn_batch_scores")
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

spark.createDataFrame(drift_metrics) \
    .write.mode("append") \
    .saveAsTable("ngm_prd_ml.monitoring.churn_drift_metrics")
