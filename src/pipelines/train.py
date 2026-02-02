import logging
import sys
import yaml
import importlib
import mlflow
import os
from src.common.exceptions import DataLoadError, ModelTrainingError
from models.churn.serving import ChurnServingModel

logger = logging.getLogger(__name__)

MODEL_KEY = sys.argv[1]
CONFIG_PATH = f"src/models/{MODEL_KEY}/config.yml"

try:
    config_path = f"src/models/{MODEL_KEY}/config.yml"
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config not found: {config_path}")
    
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    trainer_module = importlib.import_module(
        f"models.{MODEL_KEY}.trainer"
    )
    Trainer = getattr(trainer_module, config["trainer_class"])
    trainer = Trainer(config)
    
    with mlflow.start_run(run_name=MODEL_KEY):
        try:
            logger.info(f"Loading data from {config['data']['features_table']}")
            trainer.X, trainer.y = trainer.load_data()
            if len(trainer.X) == 0:
                raise DataLoadError("No training data loaded")
            logger.info(f"Loaded {len(trainer.X)} samples")
        except Exception as e:
            mlflow.end_run(status="FAILED")
            raise DataLoadError(f"Failed to load training data: {str(e)}") from e
        
        try:
            logger.info("Starting model training")
            model = trainer.train()
            logger.info("Model training completed")
        except Exception as e:
            mlflow.end_run(status="FAILED")
            raise ModelTrainingError(f"Training failed: {str(e)}") from e
        
        try:
            metrics = trainer.evaluate(model)
            for k, v in metrics.items():
                mlflow.log_metric(k, v)
            logger.info(f"Metrics: {metrics}")
        except Exception as e:
            logger.error(f"Evaluation failed: {e}")
            raise
        
        mlflow.pyfunc.log_model(
            artifact_path="model",
            python_model=ChurnServingModel(),
            artifacts={"model": mlflow.sklearn.save_model(model, path="/tmp/model")},
            registered_model_name=config["registered_model_name"]
        )
        logger.info("Model logged to MLflow")

except Exception as e:
    logger.error(f"Pipeline failed: {e}", exc_info=True)
    sys.exit(1)
