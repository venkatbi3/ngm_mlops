"""
Inference pipeline for batch scoring. Loads the Champion model, runs inference, and writes output.
"""
import argparse
import sys
import importlib
import re
import logging
import os
import time
from datetime import datetime
from pathlib import Path

import mlflow
from mlflow.tracking import MlflowClient

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.config import load_model_config
from src.common.logger import get_logger, setup_logging, log_with_context, log_performance
from src.common.exceptions import ConfigError, ImportErrorSafe, DataLoadError

logger = get_logger(__name__)


def _validate_model_key(key: str) -> None:
    """Validate model key format."""
    if not re.match(r"^[A-Za-z0-9_\-]+$", key):
        raise ConfigError(f"Invalid model key format: {key}")


def main():
    """Main inference pipeline."""
    # Configure logging
    log_dir = os.getenv("PIPELINE_LOG_DIR", "logs")
    setup_logging(log_dir=log_dir, level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Run batch inference",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--model",
        type=str,
        required=True,
        help="Model key to use for inference (e.g., churn, fraud)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        choices=["dev", "uat", "preprod", "prod"],
        help="Environment to run inference in (default: dev)"
    )
    
    args = parser.parse_args()
    MODEL_KEY = args.model
    environment = args.env
    pipeline_start = time.time()
    
    _validate_model_key(MODEL_KEY)
    
    log_with_context(
        logger, logging.INFO,
        "Starting inference pipeline",
        {"model_key": MODEL_KEY, "environment": environment, "timestamp": datetime.now().isoformat()}
    )

    try:
        config = load_model_config(MODEL_KEY)
        client = MlflowClient()
        model_name = config.registered_model_name
    except FileNotFoundError as e:
        log_with_context(
            logger, logging.ERROR,
            "Config file not found",
            {"model_key": MODEL_KEY, "error": str(e)}
        )
        raise ConfigError(f"Config not found for {MODEL_KEY}", config_path=f"src/models/{MODEL_KEY}/config.yml") from e

    model_uri = f"models:/{model_name}@Champion"
    
    logger.info(f"Loading Champion model: {model_name}")
    try:
        load_start = time.time()
        model = mlflow.pyfunc.load_model(model_uri)
        load_duration = time.time() - load_start
        log_with_context(
            logger, logging.INFO,
            "Champion model loaded",
            {"model_name": model_name, "uri": model_uri, "duration_seconds": round(load_duration, 2)}
        )
        log_performance(logger, "model_loading", load_duration * 1000)
    except Exception as e:
        log_with_context(
            logger, logging.ERROR,
            "Failed to load champion model",
            {"model_name": model_name, "uri": model_uri, "error": str(e)}
        )
        raise DataLoadError(f"Failed to load Champion model", context={"uri": model_uri, "error": str(e)}) from e

    # Resolve version + run_id
    try:
        model_version = client.get_model_version_by_alias(model_name, "Champion")
        run_id = model_version.run_id
        version = model_version.version
        logger.info(f"Champion version: {version}, run_id: {run_id}")
    except Exception as e:
        log_with_context(
            logger, logging.ERROR,
            "Failed to resolve model version",
            {"model_name": model_name, "error": str(e)}
        )
        raise ConfigError(f"Could not resolve model version", context={"model_name": model_name, "error": str(e)}) from e

    scored_at = datetime.utcnow()

    # Safe dynamic import of inference class
    try:
        inference_module = importlib.import_module(f"src.models.{MODEL_KEY}.inference")
        InferenceClass = getattr(inference_module, config.inference_class)
        logger.info(f"✓ Loaded inference class: {config.inference_class}")
    except (ImportError, AttributeError) as e:
        log_with_context(
            logger, logging.ERROR,
            "Inference class import failed",
            {"model_key": MODEL_KEY, "inference_class": config.inference_class, "error": str(e)}
        )
        raise ImportErrorSafe(f"Inference import failed", module=f"src.models.{MODEL_KEY}.inference", attribute=config.inference_class) from e

    inference = InferenceClass(config)
    inference.metadata = {
        "model_name": model_name,
        "model_version": version,
        "run_id": run_id,
        "scored_at": scored_at,
    }

    try:
        # Load features
        features_start = time.time()
        logger.info(f"Loading features from {config.data.features_table}")
        features_df = inference.load_features()
        features_duration = time.time() - features_start
        
        feature_count = features_df.count() if hasattr(features_df, 'count') else len(features_df)
        log_with_context(
            logger, logging.INFO,
            "Features loaded",
            {"row_count": feature_count, "duration_seconds": round(features_duration, 2)}
        )
        log_performance(logger, "features_loading", features_duration * 1000)
        
        # Score
        scoring_start = time.time()
        logger.info("Starting model scoring")
        predictions = inference.score(model, features_df)
        scoring_duration = time.time() - scoring_start
        
        pred_count = len(predictions) if hasattr(predictions, '__len__') else predictions.count()
        log_with_context(
            logger, logging.INFO,
            "Predictions generated",
            {"prediction_count": pred_count, "duration_seconds": round(scoring_duration, 2)}
        )
        log_performance(logger, "model_scoring", scoring_duration * 1000)
        
        # Write output
        write_start = time.time()
        logger.info(f"Writing predictions to {config.output.table}")
        inference.write_output(predictions)
        write_duration = time.time() - write_start
        
        log_with_context(
            logger, logging.INFO,
            "Predictions written",
            {"table": config.output.table, "duration_seconds": round(write_duration, 2)}
        )
        log_performance(logger, "output_writing", write_duration * 1000)
        
        total_duration = time.time() - pipeline_start
        log_with_context(
            logger, logging.INFO,
            "Batch inference completed",
            {"model_key": MODEL_KEY, "predictions": pred_count, "total_duration_seconds": round(total_duration, 2)}
        )
    except Exception as e:
        total_duration = time.time() - pipeline_start
        log_with_context(
            logger, logging.ERROR,
            "Inference pipeline failed",
            {"model_key": MODEL_KEY, "error": str(e), "duration_seconds": round(total_duration, 2)}
        )
        raise DataLoadError(f"Inference pipeline failed: {e}") from e


if __name__ == "__main__":
    main()


if __name__ == "__main__":
    main()
