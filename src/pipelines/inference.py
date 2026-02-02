import sys
import importlib
import re
from datetime import datetime

import mlflow
from mlflow.tracking import MlflowClient

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

    config = load_model_config(MODEL_KEY)
    client = MlflowClient()
    model_name = config.registered_model_name

    model_uri = f"models:/{model_name}@Champion"

    try:
        model = mlflow.pyfunc.load_model(model_uri)
    except Exception as e:
        logger.exception("Failed to load Champion model %s", model_uri)
        raise

    # Resolve version + run_id
    try:
        model_version = client.get_model_version_by_alias(model_name, "Champion")
        run_id = model_version.run_id
        version = model_version.version
    except Exception as e:
        logger.exception("Failed to resolve model version for %s", model_name)
        raise

    scored_at = datetime.utcnow()

    # Safe dynamic import of inference class
    try:
        inference_module = importlib.import_module(f"models.{MODEL_KEY}.inference")
        InferenceClass = getattr(inference_module, config.inference_class)
    except Exception as e:
        logger.exception("Failed to import inference class for %s", MODEL_KEY)
        raise ImportErrorSafe(str(e)) from e

    inference = InferenceClass(config)
    inference.metadata = {
        "model_name": model_name,
        "model_version": version,
        "run_id": run_id,
        "scored_at": scored_at,
    }

    try:
        features_df = inference.load_features()
        predictions = inference.score(model, features_df)
        inference.write_output(predictions)
        logger.info("Batch inference completed for model %s", model_name)
    except Exception:
        logger.exception("Inference pipeline failed for model %s", model_name)
        raise


if __name__ == "__main__":
    main()
