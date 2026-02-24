#!/usr/bin/env python
"""
Register a mock churn model to Databricks MLflow for development/testing.
This allows you to register and test the model without data access.
"""

import sys
import argparse
import numpy as np
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

import mlflow
from sklearn.ensemble import RandomForestClassifier
from common.logger import get_logger
from models.registry import ModelRegistry

logger = get_logger(__name__)


def register_mock_model(model_key: str = "churn", environment: str = "dev"):
    """
    Create and register a mock model to MLflow.
    
    Args:
        model_key: Model to register (churn, fraud)
        environment: Environment (dev, uat, prod)
    """
    logger.info(f"Registering mock {model_key} model for {environment}")

    # Create dummy training data
    np.random.seed(42)
    n_samples = 200
    n_features = 8

    X_dummy = np.random.randn(n_samples, n_features)
    
    if model_key == "churn":
        y_dummy = np.random.binomial(1, 0.3, n_samples)  # ~30% churn
        model_name = "churn_model"
        metrics_dict = {
            "auc": 0.82,
            "precision": 0.78,
            "recall": 0.85,
        }
    elif model_key == "fraud":
        y_dummy = np.random.binomial(1, 0.05, n_samples)  # ~5% fraud
        model_name = "fraud_model"
        metrics_dict = {
            "auc": 0.85,
            "precision": 0.80,
            "recall": 0.88,
        }
    else:
        raise ValueError(f"Unknown model key: {model_key}")

    # Train dummy model
    model = RandomForestClassifier(
        n_estimators=100,
        max_depth=15,
        random_state=42
    )
    model.fit(X_dummy, y_dummy)
    logger.info(f"✓ Trained dummy {model_key} model")

    # Set MLflow experiment
    experiment_name = f"/Users/admin/{model_key}-development"
    mlflow.set_experiment(experiment_name)
    logger.info(f"✓ Set experiment: {experiment_name}")

    # Start MLflow run
    with mlflow.start_run(
        run_name=f"mock-{model_key}-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
    ) as run:
        run_id = run.info.run_id
        logger.info(f"✓ Started MLflow run: {run_id}")

        try:
            # Log model
            mlflow.sklearn.log_model(model, "model")
            logger.info("✓ Logged model artifact")

            # Log parameters
            mlflow.log_params({
                "model_key": model_key,
                "model_type": "RandomForest",
                "n_estimators": 100,
                "max_depth": 15,
                "dataset": "mock-data",
                "environment": environment,
                "registration_type": "development-mock"
            })
            logger.info("✓ Logged parameters")

            # Log metrics
            for metric_name, metric_value in metrics_dict.items():
                mlflow.log_metric(metric_name, metric_value)
            logger.info("✓ Logged metrics")

            mlflow.log_metric("training_samples", n_samples)
            mlflow.log_metric("feature_count", n_features)

            # Log tags
            mlflow.set_tags({
                "status": "development",
                "purpose": "mock-registration",
                "no_data_access": "true",
                "created_at": datetime.utcnow().isoformat()
            })
            logger.info("✓ Tagged run")

        except Exception as e:
            logger.error(f"Failed to log model: {e}")
            mlflow.end_run(status="FAILED")
            raise

    # Register model
    try:
        registry = ModelRegistry()
        model_uri = registry.register_model(
            run_id=run_id,
            artifact_path="model",
            model_name=model_name
        )
        logger.info(f"✓ Registered model: {model_uri}")

        # Promote to Champion
        try:
            registry.promote_model(model_name, "1", "Champion")
            logger.info(f"✓ Promoted version 1 to Champion")
        except Exception as e:
            if "already" in str(e).lower():
                logger.warning(f"Model already promoted: {e}")
            else:
                raise

        logger.info(f"\n{'='*60}")
        logger.info(f"✅ Mock {model_key} model registered successfully!")
        logger.info(f"{'='*60}")
        logger.info(f"Run ID:        {run_id}")
        logger.info(f"Model:         {model_name}")
        logger.info(f"Version:       1")
        logger.info(f"Status:        Champion")
        logger.info(f"Environment:   {environment}")
        logger.info(f"\nLoad model in Databricks:")
        logger.info(f"  model = mlflow.pyfunc.load_model('models:/{model_name}@Champion')")
        logger.info(f"{'='*60}\n")

        return run_id, model_uri

    except Exception as e:
        logger.error(f"Failed to register model: {e}")
        raise


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Register mock ML model to Databricks"
    )
    parser.add_argument(
        "--model",
        type=str,
        default="churn",
        choices=["churn", "fraud"],
        help="Model to register (default: churn)"
    )
    parser.add_argument(
        "--env",
        type=str,
        default="dev",
        choices=["dev", "uat", "prod"],
        help="Environment (default: dev)"
    )

    args = parser.parse_args()

    try:
        register_mock_model(args.model, args.env)
    except Exception as e:
        logger.error(f"Failed to register model: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
