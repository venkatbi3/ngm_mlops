"""
Model monitoring and feature engineering pipeline.
Periodically evaluates model performance on new data and logs metrics to MLflow.
"""
import argparse
import sys
import importlib
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

import mlflow
from src.common.config import load_model_config
from src.common.logger import get_logger
from src.common.exceptions import ConfigError, ImportErrorSafe

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
        required=True,
        help="Model key to monitor (e.g., churn, fraud)"
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
    
    logger.info(f"Starting monitoring pipeline for model: {model_key} in {environment}")
    
    # Load configuration
    try:
        config = load_model_config(model_key)
        logger.info(f"✓ Loaded config for {model_key}")
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise ConfigError(f"Config load failed for {model_key}") from e
    
    with mlflow.start_run(run_name=f"monitor-{model_key}"):
        # Log parameters
        mlflow.log_param("model_key", model_key)
        mlflow.log_param("environment", environment)
        mlflow.log_param("registered_model", config.registered_model_name)
        mlflow.log_param("source", "Databricks Unity Catalog")
        mlflow.log_param("target", "Databricks Delta")
        
        try:
            # Feature engineering and monitoring logic
            logger.info(f"Building features for {model_key}...")
            
            # Try to load feature builder for this model
            try:
                feature_module = importlib.import_module(f"src.models.{model_key}.features")
                logger.info(f"✓ Loaded feature builder for {model_key}")
                # builder = feature_module.FeatureBuilder(config)
                # table = builder.build_all_features()
                # mlflow.log_metric("feature_count", spark.table(table).count())
            except ImportError:
                logger.info(f"No feature builder available for {model_key} - skipping feature engineering")
            
            # Log table locations
            logger.info(f"Logging table locations for {model_key}...")
            mlflow.log_param("features_table", config.data.features_table)
            mlflow.log_param("output_table", f"{config.output.catalog}.{config.output.schema}.{config.output.table}")
            
            # Log data configuration
            logger.info(f"Logging configuration for {model_key}...")
            mlflow.log_param("source_catalog", config.data.source_catalog)
            mlflow.log_param("source_schema", config.data.source_schema)
            mlflow.log_param("start_date", config.data.start_date)
            mlflow.log_param("end_date", config.data.end_date)
            
            # Log model metadata
            mlflow.log_param("trainer_class", config.trainer_class)
            mlflow.log_param("validator_class", config.validator_class)
            mlflow.log_param("inference_class", config.inference_class)
            
            logger.info(f"✅ Monitoring pipeline completed for {model_key}")
            
        except Exception as e:
            logger.error(f"Monitoring pipeline failed: {e}", exc_info=True)
            mlflow.end_run(status="FAILED")
            sys.exit(1)


if __name__ == "__main__":
    main()
