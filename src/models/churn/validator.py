import logging
import mlflow
from src.models.base import BaseValidator
from src.common.logger import get_logger, log_with_context
from src.common.exceptions import ModelTrainingError

logger = get_logger(__name__)


class ChurnValidator(BaseValidator):

    def __init__(self, config):
        self.config = config

    def validate(self, run_id: str) -> bool:
        """
        Validate model by checking metrics against thresholds.
        
        Args:
            run_id: MLflow run ID of the trained model
            
        Returns:
            True if model passes validation, False otherwise
        """
        logger.info(f"Validating model (run_id: {run_id})")
        try:
            run = mlflow.get_run(run_id)
            auc = run.data.metrics.get("auc", 0)
            
            threshold = self.config.metrics.auc_threshold  # e.g., 0.75
            
            log_with_context(
                logger, logging.INFO,
                "Validation metrics retrieved",
                {"auc": round(auc, 4), "threshold": threshold, "passed": auc >= threshold}
            )
            
            if auc >= threshold:
                logger.info(f"Model validation passed: AUC {auc:.4f} >= threshold {threshold}")
                return True
            else:
                logger.warning(f"Model validation failed: AUC {auc:.4f} < threshold {threshold}")
                return False
        except Exception as e:
            logger.error(f"Validation failed: {e}", exc_info=True)
            raise ModelTrainingError(f"Validation execution failed", context={"run_id": run_id, "error": str(e)}) from e
