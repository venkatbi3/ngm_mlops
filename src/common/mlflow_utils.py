import logging
from typing import Any, Dict, Optional
import mlflow
from mlflow.models import ModelSignature

"""
Common model utilities for MLflow operations.
"""

logger = logging.getLogger(__name__)


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
    mlflow.log_model(
        model,
        artifact_path=artifact_path,
        signature=signature,
        input_example=input_example,
        tags=tags,
    )
    logger.info(f"Model logged at {artifact_path}")


def log_metrics(metrics: Dict[str, float]) -> None:
    """Log metrics to MLflow."""
    for key, value in metrics.items():
        mlflow.log_metric(key, value)
    logger.info(f"Logged {len(metrics)} metrics")


def log_params(params: Dict[str, Any]) -> None:
    """Log parameters to MLflow."""
    for key, value in params.items():
        mlflow.log_param(key, value)
    logger.info(f"Logged {len(params)} parameters")