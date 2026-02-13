"""
Model training pipeline.
Loads data, trains model, evaluates, and logs to MLflow.
"""
import logging
import sys
import os
import importlib
import mlflow
from datetime import datetime

from src.common.config import load_model_config
from src.common.logger import get_logger
from src.common.exceptions import DataLoadError, ModelTrainingError, ConfigError

logger = get_logger(__name__)


def main(argv=None):
    """Main training pipeline."""
    argv = argv or sys.argv
    
    if len(argv) < 2:
        raise ConfigError("MODEL_KEY argument required (e.g., python train.py churn)")
    
    model_key = argv[1]
    logger.info(f"Starting training pipeline for model: {model_key}")
    
    # Load configuration
    try:
        config = load_model_config(model_key)
        logger.info(f"✓ Loaded config for {model_key}")
    except Exception as e:
        logger.error(f"Failed to load config: {e}")
        raise ConfigError(f"Config load failed for {model_key}") from e
    
    # Initialize MLflow
    experiment_name = f"/Users/{os.getenv('USER', 'mluser')}/{model_key}-training"
    mlflow.set_experiment(experiment_name)
    
    # Start training run
    with mlflow.start_run(run_name=f"{model_key}-{datetime.now().strftime('%Y%m%d-%H%M%S')}") as run:
        run_id = run.info.run_id
        logger.info(f"Started MLflow run: {run_id}")
        
        try:
            # Log configuration
            mlflow.log_params({
                "model_key": model_key,
                "registered_model_name": config.registered_model_name,
                "trainer_class": config.trainer_class,
            })
            
            # Dynamically import trainer class
            try:
                trainer_module = importlib.import_module(f"src.models.{model_key}.trainer")
                trainer_cls = getattr(trainer_module, config.trainer_class)
                logger.info(f"✓ Loaded trainer class: {config.trainer_class}")
            except Exception as e:
                logger.error(f"Failed to import trainer: {e}")
                mlflow.end_run(status="FAILED")
                raise ConfigError(f"Trainer import failed: {e}") from e
            
            # Initialize trainer
            trainer = trainer_cls(config)
            
            # Load data
            try:
                logger.info(f"Loading data from {config.data.features_table}")
                X, y = trainer.load_data()
                
                if len(X) == 0:
                    raise DataLoadError("No training data loaded")
                
                logger.info(f"✓ Loaded {len(X):,} samples, {X.shape[1]} features")
                mlflow.log_metric("training_samples", len(X))
                mlflow.log_metric("feature_count", X.shape[1])
                
            except Exception as e:
                logger.error(f"Data loading failed: {e}")
                mlflow.end_run(status="FAILED")
                raise DataLoadError(f"Failed to load training data: {e}") from e
            
            # Train model
            try:
                logger.info("Starting model training")
                model = trainer.train()
                logger.info("✓ Model training completed")
                mlflow.log_param("status", "training_complete")
                
            except Exception as e:
                logger.error(f"Training failed: {e}")
                mlflow.end_run(status="FAILED")
                raise ModelTrainingError(f"Training failed: {e}") from e
            
            # Evaluate model
            try:
                logger.info("Evaluating model")
                metrics = trainer.evaluate(model)
                
                for metric_name, metric_value in metrics.items():
                    mlflow.log_metric(metric_name, metric_value)
                    logger.info(f"  {metric_name}: {metric_value:.4f}")
                
                logger.info("✓ Model evaluation completed")
                
            except Exception as e:
                logger.error(f"Evaluation failed: {e}")
                raise
            
            # Log model artifact
            try:
                logger.info(f"Logging model to MLflow")
                
                # Save model using trainer's preferred method
                artifact_path = "model"
                model_uri = mlflow.sklearn.log_model(model, artifact_path)
                
                # Register model
                mlflow.register_model(
                    model_uri=f"runs:/{run_id}/{artifact_path}",
                    name=config.registered_model_name
                )
                
                logger.info(f"✓ Model registered: {config.registered_model_name}")
                mlflow.log_param("model_registered", "true")
                
            except Exception as e:
                logger.error(f"Model logging failed: {e}")
                raise
            
            # Mark run as successful
            mlflow.set_tag("status", "success")
            logger.info(f"✅ Training pipeline completed. Run ID: {run_id}")
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            mlflow.end_run(status="FAILED")
            sys.exit(1)


if __name__ == "__main__":
    main()
