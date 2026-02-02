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
    if not re.match(r"^[A-Za-z0-9_\-]+$", key):
        raise ConfigError(f"Invalid model key: {key}")


def main(argv=None):
    argv = argv or sys.argv
    if len(argv) < 2:
        logger.error("MODEL_KEY argument missing")
        raise ConfigError("MODEL_KEY argument missing")

    MODEL_KEY = argv[1]
    _validate_model_key(MODEL_KEY)

    # Load and validate config
    config = load_model_config(MODEL_KEY)

    client = MlflowClient()
    model_name = config.registered_model_name

    versions = client.get_latest_versions(model_name, stages=["None"]) or []
    if not versions:
        logger.error("No candidate versions found for model %s", model_name)
        raise ConfigError(f"No candidate versions found for model {model_name}")

    latest = versions[0]
    model_uri = latest.run_id

    # Safe dynamic import of validator
    try:
        validator_module = importlib.import_module(f"models.{MODEL_KEY}.validator")
        validator_cls = getattr(validator_module, config.validator_class)
    except Exception as e:
        logger.exception("Failed to import validator for %s", MODEL_KEY)
        raise ImportErrorSafe(str(e)) from e

    validator = validator_cls(config)

    try:
        if validator.validate(model_uri):
            client.set_registered_model_alias(model_name, "Challenger", latest.version)
            logger.info("Model %s passed validation and is set as Challenger", model_name)
        else:
            logger.error("Model %s failed validation", model_name)
            raise ConfigError("Model validation failed")
    except Exception:
        logger.exception("Validation execution failed for model %s", model_name)
        raise


if __name__ == "__main__":
    main()
