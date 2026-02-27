"""
Model validation pipeline.
Validates candidate models and promotes to Champion if they pass tests.
"""
import argparse
import sys
import importlib
import re
import logging
import os
import time
from pathlib import Path
from mlflow.tracking import MlflowClient
import mlflow

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.config import load_model_config
from src.common.logger import get_logger, setup_logging, log_with_context, log_performance
from src.common.exceptions import ConfigError, ImportErrorSafe, ModelTrainingError

logger = get_logger(__name__)


def _validate_model_key(key: str) -> None:
    """Validate model key format."""
    if not re.match(r"^[A-Za-z0-9_\-]+$", key):
        raise ConfigError(f"Invalid model key format: {key}")


def main():
    """Main validation pipeline."""
    # Configure logging
    log_dir = os.getenv("PIPELINE_LOG_DIR", "logs")
    setup_logging(log_dir=log_dir, level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Validate ML models",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--model",
        type=str,
        required=True,
        help="Model key to validate (e.g., churn, fraud)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="rnd",
        choices=["rnd", "dev", "uat", "preprod", "prod"],
        help="Environment to validate in (default: rnd)"
    )
    
    args = parser.parse_args()
    model_key = args.model
    environment = args.env
    pipeline_start = time.time()
    
    _validate_model_key(model_key)
    
    log_with_context(
        logger, logging.INFO,
        "Starting validation pipeline",
        {"model_key": model_key, "environment": environment}
    )
    
    # Load config
    try:
        config = load_model_config(model_key)
        client = MlflowClient()
        model_name = config.registered_model_name
        logger.info(f"Validating model: {model_name}")
    except FileNotFoundError as e:
        log_with_context(
            logger, logging.ERROR,
            "Config file not found",
            {"model_key": model_key, "error": str(e)}
        )
        raise ConfigError(f"Config not found for {model_key}", config_path=f"src/models/{model_key}/config.yml") from e
    
    # Get latest Challenger version (or version moved by training pipeline)
    try:
        logger.info("Looking for candidate model version to validate...")
        
        # Try to get the latest version without a stable alias (should have been created by training)
        versions = client.get_latest_versions(model_name, stages=None) or []
        
        if not versions:
            log_with_context(
                logger, logging.ERROR,
                "No candidate versions found",
                {"model_name": model_name}
            )
            raise ConfigError(f"No candidate model versions found", context={"model_name": model_name})
        
        # Get the most recent version
        candidate = versions[0]
        candidate_version = candidate.version
        run_id = candidate.run_id
        
        log_with_context(
            logger, logging.INFO,
            "Found candidate version",
            {"candidate_version": candidate_version, "run_id": run_id}
        )
        
    except ConfigError:
        raise
    except Exception as e:
        logger.error(f"Failed to find candidate model: {e}", exc_info=True)
        raise ConfigError(f"Could not find candidate model for {model_name}", context={"error": str(e)}) from e
    
    # Dynamically import validator class
    try:
        validator_module = importlib.import_module(f"src.models.{model_key}.validator")
        validator_cls = getattr(validator_module, config.validator_class)
        logger.info(f"✓ Loaded validator class: {config.validator_class}")
    except (ImportError, AttributeError) as e:
        log_with_context(
            logger, logging.ERROR,
            "Validator import failed",
            {"model_key": model_key, "validator_class": config.validator_class, "error": str(e)}
        )
        raise ImportErrorSafe(f"Validator import failed", module=f"src.models.{model_key}.validator", attribute=config.validator_class) from e
    
    # Initialize validator
    validator = validator_cls(config)
    
    # Run validation
    val_start = time.time()
    try:
        logger.info(f"Running validation for version {candidate_version}")
        is_valid = validator.validate(run_id)
        val_duration = time.time() - val_start
        log_performance(logger, "model_validation", val_duration * 1000)
        
        if is_valid:
            log_with_context(
                logger, logging.INFO,
                "Model passed validation",
                {"model_name": model_name, "version": candidate_version, "duration_seconds": round(val_duration, 2)}
            )
            
            # Check if there's a current Champion
            try:
                current_champion = client.get_model_version_by_alias(model_name, "Champion")
                logger.info(f"Current Champion: version {current_champion.version}")
                
                # Archive the old champion
                client.set_registered_model_alias(
                    model_name, "Archived", current_champion.version
                )
                logger.info(f"Archived previous Champion (version {current_champion.version})")
            except Exception:
                # No current champion, that's ok
                logger.info("No previous Champion found (first promotion)")
            
            # Promote candidate to Champion
            client.set_registered_model_alias(
                model_name, "Champion", candidate_version
            )
            log_with_context(
                logger, logging.INFO,
                "Model promoted to Champion",
                {"model_name": model_name, "version": candidate_version}
            )
            
            # Log promotion event
            mlflow.set_tag("promotion_status", "promoted_to_champion")
            
        else:
            log_with_context(
                logger, logging.ERROR,
                "Model failed validation",
                {"model_name": model_name, "version": candidate_version}
            )
            raise ModelTrainingError(f"Model validation failed", model_key=model_key, context={"version": candidate_version})
            
    except ModelTrainingError:
        raise
    except Exception as e:
        logger.error(f"Validation execution failed: {e}", exc_info=True)
        raise ModelTrainingError(f"Validation execution failed", model_key=model_key, context={"error": str(e)}) from e
    
    total_duration = time.time() - pipeline_start
    log_with_context(
        logger, logging.INFO,
        "Validation pipeline completed",
        {"model_key": model_key, "total_duration_seconds": round(total_duration, 2)}
    )


if __name__ == "__main__":
    main()