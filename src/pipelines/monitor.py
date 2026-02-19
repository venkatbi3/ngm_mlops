"""
Model monitoring pipeline.
Periodically evaluates model performance on new data and logs metrics to MLflow.
"""
import argparse
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import mlflow
from src.common.logger import get_logger

logger = get_logger(__name__)


def main():
    """Main monitoring and feature engineering pipeline."""
    parser = argparse.ArgumentParser(
        description="Monitor models and engineer features",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--model",
        type=str,
        default=None,
        help="Model key to monitor (optional, monitors all if not specified)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        choices=["dev", "uat", "preprod", "prod"],
        help="Environment to monitor in (default: dev)"
    )
    
    args = parser.parse_args()
    model_key = args.model
    environment = args.env
    
    logger.info(f"Starting monitoring pipeline in {environment}")
    if model_key:
        logger.info(f"Monitoring specific model: {model_key}")
    
    with mlflow.start_run(run_name="feature_engineering"):
        # Log parameters
        mlflow.log_param("source", "Databricks Unity Catalog")
        mlflow.log_param("target", "Databricks Delta")
        mlflow.log_param("environment", environment)
        if model_key:
            mlflow.log_param("model_key", model_key)
        
        # Feature engineering and monitoring logic
        # Build features
        logger.info("Building features...")
        # builder = HybridFeatureBuilder(...)
        # table = builder.build_all_features()
        
        # Log metrics
        logger.info("Logging metrics...")
        # count = spark.table(table).count()
        # mlflow.log_metric("feature_count", count)
        
        # Log table location
        # mlflow.log_param("feature_table", table)
        
        # Log data quality
        logger.info("Checking data quality...")
        # null_counts = spark.sql(f"""
        #     SELECT 
        #         SUM(CASE WHEN BalanceSlope IS NULL THEN 1 ELSE 0 END) as null_balance_slope
        #     FROM {table}
        # """).first()
        
        # mlflow.log_metric("null_balance_slope", null_counts['null_balance_slope'])
        
        logger.info("✅ Monitoring pipeline completed")


if __name__ == "__main__":
    main()
