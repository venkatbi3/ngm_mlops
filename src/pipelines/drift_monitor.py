"""
Model drift monitoring pipeline. Compares recent data statistics to baseline and logs drift metrics.
"""
import argparse
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import mlflow
import yaml
from pyspark.sql import SparkSession
import numpy as np
from src.common.config import load_model_config
from src.common.logger import get_logger

logger = get_logger(__name__)


def main():
    """Main drift monitoring pipeline."""
    parser = argparse.ArgumentParser(
        description="Monitor model drift",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--model",
        type=str,
        required=True,
        help="Model key to monitor for drift (e.g., churn, fraud)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        choices=["dev", "uat", "preprod", "prod"],
        help="Environment to monitor drift in (default: dev)"
    )
    
    args = parser.parse_args()
    model_key = args.model
    environment = args.env
    
    logger.info(f"Starting drift monitoring for model: {model_key} in {environment}")
    
    spark = SparkSession.builder.getOrCreate()
    
    config = load_model_config(model_key)
    
    model_name = config.registered_model_name
    model_uri = f"models:/{model_name}@Champion"
    
    logger.info(f"Loading Champion model: {model_name}")
    
    try:
        # Load baseline
        baseline = mlflow.artifacts.load_dict(
            f"{model_uri}/artifacts/baseline_stats.json"
        )
        logger.info("✓ Loaded baseline statistics")
    except Exception as e:
        logger.error(f"Failed to load baseline: {e}")
        raise
    
    try:
        # Load recent scored data using parameterized output catalog
        scores_table = f"{config.output.catalog}.{config.output.schema}.{config.output.table}"
        logger.info(f"Loading scored data from {scores_table}")
        df = spark.table(scores_table)
        pdf = df.toPandas()
        logger.info(f"✓ Loaded {len(pdf):,} scored records")
    except Exception as e:
        logger.error(f"Failed to load scored data: {e}")
        raise
    
    drift_metrics = []
    
    for col, stats in baseline.items():
        current_mean = pdf[col].mean()
        z_score = abs(current_mean - stats["mean"]) / stats["std"]
        
        is_drifted = z_score > 3
        
        drift_metrics.append({
            "feature": col,
            "z_score": z_score,
            "drifted": is_drifted,
            "environment": environment
        })
        
        if is_drifted:
            logger.warning(f"⚠️  Drift detected in feature '{col}': z_score={z_score:.2f}")
    
    try:
        # Write drift metrics to model schema
        monitoring_table = f"{config.output.catalog}.{config.output.schema}.{model_key}_drift_metrics"
        logger.info(f"Writing drift metrics to {monitoring_table}")
        spark.createDataFrame(drift_metrics) \
            .write.mode("append") \
            .saveAsTable(monitoring_table)
        logger.info(f"✅ Drift monitoring completed. {len(drift_metrics)} features monitored")
    except Exception as e:
        logger.error(f"Failed to write drift metrics: {e}")
        raise


if __name__ == "__main__":
    main()
