"""
Model drift monitoring pipeline. Compares recent data statistics to baseline and logs drift metrics.
"""
import argparse
import sys
import logging
import os
import time
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import mlflow
import yaml
from pyspark.sql import SparkSession
import numpy as np
from src.common.config import load_model_config
from src.common.logger import get_logger, setup_logging, log_with_context, log_performance
from src.common.exceptions import DataLoadError, ConfigError

logger = get_logger(__name__)


def main():
    """Main drift monitoring pipeline."""
    # Configure logging
    log_dir = os.getenv("PIPELINE_LOG_DIR", "logs")
    setup_logging(log_dir=log_dir, level=logging.INFO)
    
    pipeline_start = time.time()
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
    
    log_with_context(
        logger, logging.INFO,
        "Starting drift monitoring",
        {"model_key": model_key, "environment": environment}
    )
    
    spark = SparkSession.builder.getOrCreate()
    
    try:
        config = load_model_config(model_key)
    except FileNotFoundError as e:
        log_with_context(
            logger, logging.ERROR,
            "Config file not found",
            {"model_key": model_key, "error": str(e)}
        )
        raise ConfigError(f"Config not found for {model_key}", config_path=f"src/models/{model_key}/config.yml") from e
    
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
        log_with_context(
            logger, logging.ERROR,
            "Failed to load baseline statistics",
            {"model_name": model_name, "error": str(e)}
        )
        raise DataLoadError(f"Failed to load baseline statistics", context={"model_uri": model_uri, "error": str(e)}) from e
    
    try:
        # Load recent scored data using parameterized output catalog
        scores_table = f"{config.output.catalog}.{config.output.schema}.{config.output.table}"
        logger.info(f"Loading scored data from {scores_table}")
        
        load_start = time.time()
        df = spark.table(scores_table)
        pdf = df.toPandas()
        load_duration = time.time() - load_start
        
        log_with_context(
            logger, logging.INFO,
            "Scored data loaded",
            {"table": scores_table, "row_count": len(pdf), "duration_seconds": round(load_duration, 2)}
        )
        log_performance(logger, "drift_data_loading", load_duration * 1000)
    except Exception as e:
        log_with_context(
            logger, logging.ERROR,
            "Failed to load scored data",
            {"table": scores_table, "error": str(e)}
        )
        raise DataLoadError(f"Failed to load scored data", context={"table": scores_table, "error": str(e)}) from e
    
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
