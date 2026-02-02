import mlflow
from common.base_validator import BaseValidator

class ChurnValidator(BaseValidator):

    def __init__(self, config):
        self.config = config

    def validate(self, model_uri: str) -> bool:
        run = mlflow.get_run(model_uri.split("/")[-1])
        auc = run.data.metrics["auc"]

        return auc >= self.config["metrics"]["auc_threshold"]
