"""
Model validation pipeline.
Validates candidate models and promotes to Champion if they pass tests.
"""
import sys
import importlib
import re
from mlflow.tracking import MlflowClient
import mlflow

from src.common.config import load_model_config
from src.common.logger import get_logger
from src.common.exceptions import ConfigError, ImportErrorSafe

logger = get_logger(__name__)


def _validate_model_key(key: str) -> None:
    """Validate model key format."""
    if not re.match(r"^[A-Za-z0-9_\-]+$", key):
        raise ConfigError(f"Invalid model key: {key}")


def main(argv=None):
    """Main validation pipeline."""
    argv = argv or sys.argv
    
    if len(argv) < 2:
        raise ConfigError("MODEL_KEY argument required (e.g., python validate.py churn)")
    
    model_key = argv[1]
    _validate_model_key(model_key)
    
    logger.info(f"Starting validation for model: {model_key}")
    
    # Load config
    config = load_model_config(model_key)
    client = MlflowClient()
    model_name = config.registered_model_name
    
    logger.info(f"Validating model: {model_name}")
    
    # Get latest Challenger version (or version moved by training pipeline)
    try:
        logger.info("Looking for candidate model version to validate...")
        
        # Try to get the latest version without a stable alias (should have been created by training)
        versions = client.get_latest_versions(model_name, stages=None) or []
        
        if not versions:
            logger.error("No candidate versions found")
            raise ConfigError(f"No candidate model versions for {model_name}")
        
        # Get the most recent version
        candidate = versions[0]
        candidate_version = candidate.version
        run_id = candidate.run_id
        
        logger.info(f"Checking candidate version {candidate_version} (run_id: {run_id})")
        
    except Exception as e:
        logger.error(f"Failed to find candidate model: {e}")
        raise ConfigError(f"Could not find candidate model for {model_name}") from e
    
    # Dynamically import validator class
    try:
        validator_module = importlib.import_module(f"src.models.{model_key}.validator")
        validator_cls = getattr(validator_module, config.validator_class)
        logger.info(f"✓ Loaded validator class: {config.validator_class}")
    except Exception as e:
        logger.error(f"Failed to import validator: {e}")
        raise ImportErrorSafe(f"Validator import failed: {e}") from e
    
    # Initialize validator
    validator = validator_cls(config)
    
    # Run validation
    try:
        logger.info(f"Running validation for version {candidate_version}")
        is_valid = validator.validate(run_id)
        
        if is_valid:
            logger.info(f"✓ Model version {candidate_version} passed validation")
            
            # Check if there's a current Champion
            try:
                current_champion = client.get_model_version_by_alias(model_name, "Champion")
                logger.info(f"Current Champion: version {current_champion.version}")
                
                # Archive the old champion
                client.set_registered_model_alias(
                    model_name, "Archived", current_champion.version
                )
                logger.info(f"Archived previous Champion (version {current_champion.version})")
            except:
                # No current champion, that's ok
                logger.info("No previous Champion found (first promotion)")
            
            # Promote candidate to Champion
            client.set_registered_model_alias(
                model_name, "Champion", candidate_version
            )
            logger.info(f"✅ Promoted version {candidate_version} to Champion")
            
            # Log promotion event
            mlflow.set_tag("promotion_status", "promoted_to_champion")
            
        else:
            logger.error(f"✗ Model version {candidate_version} failed validation")
            raise ConfigError(f"Model validation failed for version {candidate_version}")
            
    except Exception as e:
        logger.error(f"Validation execution failed: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
