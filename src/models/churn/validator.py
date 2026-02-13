import mlflow
from src.models.base import BaseValidator

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
        try:
            run = mlflow.get_run(run_id)
            auc = run.data.metrics.get("auc", 0)
            
            threshold = self.config.metrics.auc_threshold
            
            if auc >= threshold:
                return True
            else:
                return False
        except Exception as e:
            raise ValueError(f"Failed to validate model: {e}") from e
