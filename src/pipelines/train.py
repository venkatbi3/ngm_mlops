"""
Model training pipeline.
Loads data, trains model, evaluates, and logs to MLflow.
"""
import argparse
import logging
import sys
import os
import importlib
import mlflow
from datetime import datetime
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.common.config import load_model_config
from src.common.logger import get_logger, setup_logging, log_with_context, log_performance
from src.common.exceptions import DataLoadError, ModelTrainingError, ConfigError, ImportErrorSafe
from src.models.registry import ModelRegistry
import time

logger = get_logger(__name__)


def main():
    """Main training pipeline."""
    # Configure logging
    log_dir = os.getenv("PIPELINE_LOG_DIR", "logs")
    setup_logging(log_dir=log_dir, level=logging.INFO)
    parser = argparse.ArgumentParser(
        description="Train ML models",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--model",
        type=str,
        required=True,
        help="Model key to train (e.g., churn, fraud)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="rnd",
        choices=["rnd", "dev", "uat", "preprod", "prod"],
        help="Environment to train in (default: rnd)"
    )
    
    args = parser.parse_args()
    model_key = args.model
    environment = args.env
    pipeline_start = time.time()
    
    log_with_context(
        logger, logging.INFO,
        "Starting training pipeline",
        {"model_key": model_key, "environment": environment, "timestamp": datetime.now().isoformat()}
    )
    
    # Load configuration
    try:
        config = load_model_config(model_key)
        logger.info(f"✓ Loaded config for {model_key}")
    except FileNotFoundError as e:
        log_with_context(
            logger, logging.ERROR,
            "Config file not found",
            {"model_key": model_key, "error": str(e)}
        )
        raise ConfigError(f"Config not found for {model_key}", config_path=f"src/models/{model_key}/config.yml") from e
    except Exception as e:
        logger.error(f"Failed to load config: {e}", exc_info=True)
        raise ConfigError(f"Config load failed for {model_key}", context={"error": str(e)}) from e
    
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
                "environment": environment,
                "registered_model_name": config.registered_model_name,
                "trainer_class": config.trainer_class,
            })
            
            # Dynamically import trainer class
            try:
                trainer_module = importlib.import_module(f"src.models.{model_key}.trainer")
                trainer_cls = getattr(trainer_module, config.trainer_class)
                logger.info(f"✓ Loaded trainer class: {config.trainer_class}")
            except (ImportError, AttributeError) as e:
                log_with_context(
                    logger, logging.ERROR,
                    "Trainer import failed",
                    {"model_key": model_key, "trainer_class": config.trainer_class, "error": str(e)}
                )
                mlflow.end_run(status="FAILED")
                raise ImportErrorSafe(f"Trainer import failed: {e}", module=f"src.models.{model_key}.trainer", attribute=config.trainer_class) from e
            
            # Initialize trainer
            trainer = trainer_cls(config)
            
            # Load data
            data_start = time.time()
            try:
                logger.info(f"Loading data from {config.data.features_table}")
                X, y = trainer.load_data()
                
                if len(X) == 0:
                    raise DataLoadError("No training data loaded", table=config.data.features_table)
                
                data_duration = time.time() - data_start
                log_with_context(
                    logger, logging.INFO,
                    f"Data loaded successfully",
                    {"samples": len(X), "features": X.shape[1], "duration_seconds": round(data_duration, 2)}
                )
                mlflow.log_metric("training_samples", len(X))
                mlflow.log_metric("feature_count", X.shape[1])
                log_performance(logger, "data_loading", data_duration * 1000)
                
            except DataLoadError as e:
                logger.error(f"Data loading failed: {e}", exc_info=True)
                mlflow.end_run(status="FAILED")
                raise
            except Exception as e:
                logger.error(f"Data loading failed: {e}", exc_info=True)
                mlflow.end_run(status="FAILED")
                raise DataLoadError(f"Failed to load training data", table=config.data.features_table, context={"error": str(e)}) from e
            
            # Train model
            training_start = time.time()
            try:
                logger.info("Starting model training")
                model = trainer.train()
                training_duration = time.time() - training_start
                
                log_with_context(
                    logger, logging.INFO,
                    "Model training completed",
                    {"model_key": model_key, "duration_seconds": round(training_duration, 2)}
                )
                mlflow.log_param("status", "training_complete")
                log_performance(logger, "model_training", training_duration * 1000)
                
            except Exception as e:
                logger.error(f"Training failed: {e}", exc_info=True)
                mlflow.end_run(status="FAILED")
                raise ModelTrainingError(f"Training failed", model_key=model_key, context={"error": str(e)}) from e
            
            # Evaluate model
            eval_start = time.time()
            try:
                logger.info("Evaluating model")
                metrics = trainer.evaluate(model)
                eval_duration = time.time() - eval_start
                
                for metric_name, metric_value in metrics.items():
                    mlflow.log_metric(metric_name, metric_value)
                    logger.info(f"  {metric_name}: {metric_value:.4f}")
                
                log_with_context(
                    logger, logging.INFO,
                    "Model evaluation completed",
                    {"metrics": metrics, "duration_seconds": round(eval_duration, 2)}
                )
                log_performance(logger, "model_evaluation", eval_duration * 1000)
                
            except Exception as e:
                logger.error(f"Evaluation failed: {e}", exc_info=True)
                raise ModelTrainingError(f"Evaluation failed", model_key=model_key, context={"error": str(e)}) from e
            
            # Log model artifact
            try:
                logger.info(f"Logging model to MLflow")
                
                artifact_path = "model"
                mlflow.sklearn.log_model(model, artifact_path)
                
                # Register or update model in registry
                registry = ModelRegistry()
                try:
                    registry.register_model(
                        run_id=run_id,
                        artifact_path=artifact_path,
                        model_name=config.registered_model_name
                    )
                    logger.info(f"✓ Model registered: {config.registered_model_name}")
                except Exception as e:
                    if "already exists" in str(e):
                        logger.warning(f"Model {config.registered_model_name} already exists, will be versioned")
                    else:
                        raise
                
                mlflow.log_param("model_registered", "true")
                
            except Exception as e:
                logger.error(f"Model logging/registration failed: {e}")
                raise
            
            # Mark run as successful
            mlflow.set_tag("status", "success")
            total_duration = time.time() - pipeline_start
            log_with_context(
                logger, logging.INFO,
                "Training pipeline completed successfully",
                {"run_id": run_id, "model_key": model_key, "total_duration_seconds": round(total_duration, 2)}
            )
            log_performance(logger, "training_pipeline_total", total_duration * 1000)
            
        except Exception as e:
            total_duration = time.time() - pipeline_start
            log_with_context(
                logger, logging.ERROR,
                "Training pipeline failed",
                {"model_key": model_key, "error": str(e), "duration_seconds": round(total_duration, 2)}
            )
            mlflow.end_run(status="FAILED")
            sys.exit(1)


if __name__ == "__main__":
    main()
