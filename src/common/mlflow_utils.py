import logging
from typing import Any, Dict, Optional
import mlflow
from mlflow.models import ModelSignature
from src.common.logger import get_logger, log_with_context
from src.common.exceptions import ModelTrainingError

"""
Common model utilities for MLflow operations.
"""

logger = get_logger(__name__)


def log_model(
    model: Any,
    artifact_path: str,
    signature: Optional[ModelSignature] = None,
    input_example: Optional[Any] = None,
    tags: Optional[Dict[str, str]] = None,
) -> None:
    """
    Log a model to MLflow.
    
    Args:
        model: The model object to log
        artifact_path: Path where the model will be stored
        signature: Model signature for input/output schema
        input_example: Example input for the model
        tags: Dictionary of tags to log with the model
    """
    try:
        mlflow.log_model(
            model,
            artifact_path=artifact_path,
            signature=signature,
            input_example=input_example,
            tags=tags,
        )
        log_with_context(
            logger, logging.INFO,
            "Model logged to MLflow",
            {"artifact_path": artifact_path, "tags": tags}
        )
    except Exception as e:
        logger.error(f"Failed to log model: {e}", exc_info=True)
        raise ModelTrainingError(f"Model logging failed", context={"artifact_path": artifact_path, "error": str(e)}) from e


def log_metrics(metrics: Dict[str, float]) -> None:
    """Log metrics to MLflow."""
    try:
        for key, value in metrics.items():
            mlflow.log_metric(key, value)
        logger.info(f"Logged {len(metrics)} metrics")
    except Exception as e:
        logger.error(f"Failed to log metrics: {e}", exc_info=True)
        raise


def log_params(params: Dict[str, Any]) -> None:
    """Log parameters to MLflow."""
    try:
        for key, value in params.items():
            mlflow.log_param(key, value)
        logger.info(f"Logged {len(params)} parameters")
    except Exception as e:
        logger.error(f"Failed to log parameters: {e}", exc_info=True)
        raise